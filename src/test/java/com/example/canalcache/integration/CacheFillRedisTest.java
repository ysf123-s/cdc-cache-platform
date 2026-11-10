package com.example.canalcache.integration;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.annotation.CacheStrategy;
import com.example.canalcache.controller.CacheManagementController;
import com.example.canalcache.detector.BigKeyDetector;
import com.example.canalcache.entity.User;
import com.example.canalcache.model.AutoCacheConfig;
import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.model.BinlogVersion;
import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.parser.FlatMessageBinlogParser;
import com.example.canalcache.service.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

/** Opt-in real Redis tests. Unique keys only; never flush a shared database. */
@EnabledIfSystemProperty(named = "cache.test.redis.host", matches = ".+")
class CacheFillRedisTest {
    private LettuceConnectionFactory factory;
    private StringRedisTemplate redis;
    private AutoCacheConfig config;
    private CacheAnnotationProcessor processor;
    private CachedQueryService queries;
    private CanalCacheSyncService sync;
    private VersionControllerWithLua versions;
    private final ObjectMapper json = new ObjectMapper();
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final ExecutorService executor = Executors.newFixedThreadPool(3);

    @BeforeEach
    void setUp() {
        factory = connectionFactory(System.getProperty("cache.test.redis.host"));
        redis = new StringRedisTemplate(factory);
        config = configuration("fill-test:" + UUID.randomUUID() + ":#id");
        processor = processor(config);
        queries = newQueries();
        versions = new VersionControllerWithLua(redis);
        sync = new CanalCacheSyncService(mock(CanalConnectorWrapper.class), new BinlogParser(),
                new FlatMessageBinlogParser(), processor, versions, mock(BigKeyDetector.class), redis, json, metrics);
    }

    @AfterEach
    void tearDown() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        redis.delete(List.of(key(1), key(2), key(3), key(4), key(5)));
        factory.destroy();
        metrics.close();
    }

    @Test
    void updateInvalidationRejectsOldValueAndNextQueryCachesNewValue() throws Exception {
        invalidationRace(EventType.UPDATE, user(1, "old"));
        assertThat(queries.getById("users", 1L, User.class, () -> user(1, "new")).getName()).isEqualTo("new");
        assertThat(data(1)).contains("new");
        assertThat(redis.opsForHash().hasKey(key(1), "load_token")).isFalse();
        assertThat(redis.getExpire(key(1))).isBetween(1L, 1800L);
    }

    @Test
    void deleteInvalidationRejectsOldValue() throws Exception {
        invalidationRace(EventType.DELETE, user(1, "deleted"));
    }

    @Test
    void insertInvalidationRejectsLateNegativeCache() throws Exception {
        invalidationRace(EventType.INSERT, null);
        assertThat(queries.getById("users", 1L, User.class, () -> user(1, "inserted")).getName())
                .isEqualTo("inserted");
        assertThat(json.readTree(data(1)).get("name").asText()).isEqualTo("inserted");
    }

    private void invalidationRace(EventType event, User old) throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        Future<User> first = executor.submit(() -> queries.getById("users", 1L, User.class, () -> {
            entered.countDown(); waitOn(release); return old;
        }));
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(redis.opsForHash().get(key(1), "load_token")).isNotNull();
            assertThat(data(1)).isNull();
            event(event, 1);
            assertThat(redis.hasKey(key(1))).isFalse();
            release.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo(old);
            assertThat(redis.hasKey(key(1))).isFalse();
            assertThat(fills("rejected")).isEqualTo(1);
        } finally { release.countDown(); }
    }

    @Test
    void manualHttpInvalidationAlsoRejectsOldFill() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        var controller = new CacheManagementController(processor, redis, metrics, sync, new CacheBrowseService(redis));
        var mvc = org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup(controller).build();
        Future<User> first = executor.submit(() -> queries.getById("users", 1L, User.class, () -> {
            entered.countDown(); waitOn(release); return user(1, "old");
        }));
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                            .post("/cache/entries/users/invalidate").contentType("application/json").content("{\"ids\":[1]}"))
                    .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isOk())
                    .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.deletedKeys").value(1));
            release.countDown(); first.get(5, TimeUnit.SECONDS);
            assertThat(redis.hasKey(key(1))).isFalse();
        } finally { release.countDown(); }
    }

    @Test
    void batchInvalidationRejectsOnlyAffectedIdAndRetainsSingleLoaderCall() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        Future<?> batch = executor.submit(() -> queries.getByIds("users", List.of(1L, 2L, 3L, 2L), User.class, ids -> {
            assertThat(ids).containsExactly(1L, 2L, 3L);
            calls.incrementAndGet(); entered.countDown(); waitOn(release);
            return List.of(user(1, "old"), user(2, "valid"));
        }, User::getId));
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            event(EventType.UPDATE, 1);
            release.countDown(); batch.get(5, TimeUnit.SECONDS);
            assertThat(calls.get()).isEqualTo(1);
            assertThat(redis.hasKey(key(1))).isFalse();
            assertThat(data(2)).contains("valid");
            assertThat(data(3)).isEqualTo("null");
            assertThat(redis.getExpire(key(3))).isBetween(1L, 30L);
            assertThat(fills("written")).isEqualTo(2);
            assertThat(fills("rejected")).isEqualTo(1);
            var hit = queries.getByIds("users", List.of(3L, 2L), User.class,
                    ids -> { throw new AssertionError("must hit"); }, User::getId);
            assertThat(hit.missingIds()).containsExactly(3L);
            assertThat(hit.items()).extracting(User::getId).containsExactly(2L);
        } finally { release.countDown(); }
    }

    @Test
    void newerInstanceFillSurvivesOlderLoaderFinishingLast() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        Future<User> first = executor.submit(() -> queries.getById("users", 1L, User.class, () -> {
            entered.countDown(); waitOn(release); return user(1, "old");
        }));
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            newQueries().getById("users", 1L, User.class, () -> user(1, "new"));
            release.countDown(); first.get(5, TimeUnit.SECONDS);
            assertThat(data(1)).contains("new");
        } finally { release.countDown(); }
    }

    @Test
    void failedOldLoaderCannotCleanNewOwnersToken() throws Exception {
        CountDownLatch oldEntered = new CountDownLatch(1), newEntered = new CountDownLatch(1);
        CountDownLatch failOld = new CountDownLatch(1), finishNew = new CountDownLatch(1);
        Future<User> old = executor.submit(() -> queries.getById("users", 1L, User.class, () -> {
            oldEntered.countDown(); waitOn(failOld); throw new DataAccessResourceFailureException("old failed");
        }));
        try {
            assertThat(oldEntered.await(5, TimeUnit.SECONDS)).isTrue();
            Future<User> newer = executor.submit(() -> newQueries().getById("users", 1L, User.class, () -> {
                newEntered.countDown(); waitOn(finishNew); return user(1, "new");
            }));
            assertThat(newEntered.await(5, TimeUnit.SECONDS)).isTrue();
            Object token = redis.opsForHash().get(key(1), "load_token");
            failOld.countDown();
            assertThatThrownBy(() -> old.get(5, TimeUnit.SECONDS)).hasRootCauseInstanceOf(DataAccessResourceFailureException.class);
            assertThat(redis.opsForHash().get(key(1), "load_token")).isEqualTo(token);
            finishNew.countDown(); newer.get(5, TimeUnit.SECONDS);
            assertThat(data(1)).contains("new");
        } finally { failOld.countDown(); finishNew.countDown(); }
    }

    @Test
    void newerLoaderFailureDoesNotRestoreOlderWritePermission() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        Future<User> old = executor.submit(() -> queries.getById("users", 1L, User.class, () -> {
            entered.countDown(); waitOn(release); return user(1, "old");
        }));
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> newQueries().getById("users", 1L, User.class,
                    () -> { throw new DataAccessResourceFailureException("new failed"); }))
                    .isInstanceOf(DataAccessResourceFailureException.class);
            release.countDown(); old.get(5, TimeUnit.SECONDS);
            assertThat(redis.hasKey(key(1))).isFalse();
        } finally { release.countDown(); }
    }

    @Test
    void expiredTokenRejectsLateFill() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        Future<User> old = executor.submit(() -> queries.getById("users", 1L, User.class, () -> {
            entered.countDown(); waitOn(release); return user(1, "old");
        }));
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(redis.getExpire(key(1))).isBetween(1L, 30L);
            // Shorten this test key's lease rather than blocking the suite for 30 seconds.
            redis.expire(key(1), Duration.ofMillis(30));
            await().atMost(Duration.ofSeconds(3)).until(() -> !redis.hasKey(key(1)));
            release.countDown(); old.get(5, TimeUnit.SECONDS);
            assertThat(redis.hasKey(key(1))).isFalse();
        } finally { release.countDown(); }
    }

    @Test
    void versionedDeleteAlsoCancelsPendingLoad() throws Exception {
        config.setEnableVersionControl(true);
        invalidationRace(EventType.UPDATE, user(1, "old"));
    }

    @Test
    void sameInstanceWaitersFinishEvenWhenInvalidationRejectsFill() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        Future<User> first = executor.submit(() -> queries.getById("users", 1L, User.class, () -> {
            entered.countDown(); waitOn(release); return user(1, "old");
        }));
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            Future<User> waiter = executor.submit(() -> queries.getById("users", 1L, User.class,
                    () -> { throw new AssertionError("must share first load"); }));
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                var coalesced = metrics.find("cache.query.coalesced").tag("table", "users").counter();
                assertThat(coalesced).isNotNull();
                assertThat(coalesced.count()).isEqualTo(1);
            });
            event(EventType.UPDATE, 1);
            release.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS).getName()).isEqualTo("old");
            assertThat(waiter.get(5, TimeUnit.SECONDS).getName()).isEqualTo("old");
            assertThat(redis.hasKey(key(1))).isFalse();
            assertThat(queries.getById("users", 1L, User.class, () -> user(1, "new")).getName()).isEqualTo("new");
        } finally { release.countDown(); }
    }

    @Test
    void refreshModesCancelMarkerOnlyLoadWithoutWarmingColdData() throws Exception {
        for (boolean versioned : List.of(false, true)) {
            config.setStrategy(CacheStrategy.REFRESH_MODE);
            config.setEnableVersionControl(versioned);
            invalidationRace(EventType.UPDATE, user(1, "old"));
            metrics.clear();
        }
    }

    @Test
    void refreshStillUpdatesWarmDataAndRejectsOlderVersions() {
        queries.getById("users", 1L, User.class, () -> user(1, "old"));
        assertThat(versions.updateIfPresent(key(1), "fresh", 200)).isTrue();
        assertThat(data(1)).isEqualTo("fresh");
        assertThat(versions.updateIfNewerVersion(key(1), "newer", new BinlogVersion(0, 1, 100, (1L << 32) + 100), 200)).isTrue();
        assertThat(versions.updateIfNewerVersion(key(1), "older", new BinlogVersion(0, 1, 99, (1L << 32) + 99), 200)).isFalse();
        assertThat(data(1)).isEqualTo("newer");
        assertThat(redis.getExpire(key(1))).isBetween(1L, 200L);
        assertThat(versions.deleteIfNewerVersion(key(1), new BinlogVersion(0, 1, 101, (1L << 32) + 101))).isTrue();
    }

    @Test
    void twoIndependentJvmsRejectOldFillAfterParentInvalidationAndReload() throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        ProcessBuilder builder = new ProcessBuilder(java, "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                CacheFillRedisProcess.class.getName(), System.getProperty("cache.test.redis.host"), config.getKeyTemplate());
        Process child = builder.redirectErrorStream(true).start();
        BufferedReader output = new BufferedReader(new InputStreamReader(child.getInputStream()));
        try {
            Future<String> ready = executor.submit(() -> {
                String line;
                while ((line = output.readLine()) != null) if (line.equals("LOAD_READY")) return line;
                return "child exited before loading";
            });
            assertThat(ready.get(15, TimeUnit.SECONDS)).isEqualTo("LOAD_READY");
            event(EventType.UPDATE, 1);
            queries.getById("users", 1L, User.class, () -> user(1, "parent-new"));
            child.getOutputStream().write(1); child.getOutputStream().flush();
            assertThat(child.waitFor(15, TimeUnit.SECONDS)).isTrue();
            assertThat(child.exitValue()).as(output.lines().reduce("", (a, b) -> a + b + "\n")).isZero();
            assertThat(data(1)).contains("parent-new").doesNotContain("child-old");
        } finally { child.destroyForcibly(); output.close(); }
    }

    private CachedQueryService newQueries() { return new CachedQueryService(redis, processor, json, metrics); }
    private String key(long id) { return config.buildCacheKeySimple(id); }
    private String data(long id) { return (String) redis.opsForHash().get(key(id), "data"); }
    private double fills(String result) {
        var counter = metrics.find("cache.query.fills").tags("table", "users", "result", result).counter();
        return counter == null ? 0 : counter.count();
    }
    private void event(EventType type, long id) {
        sync.handleEvent(BinlogEvent.builder().tableName("users").eventType(type)
                .beforeData(Map.of("id", String.valueOf(id))).afterData(Map.of("id", String.valueOf(id), "name", "new"))
                .binlogFile("mysql-bin.000001").binlogPosition(100L).build());
    }
    static void waitOn(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) throw new AssertionError("barrier timed out");
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
    }
    static LettuceConnectionFactory connectionFactory(String host) {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(host,
                Integer.parseInt(System.getenv().getOrDefault("CACHE_TEST_REDIS_PORT", "6379")));
        config.setDatabase(15);
        config.setPassword(RedisPassword.of(System.getenv().getOrDefault("CACHE_TEST_REDIS_PASSWORD", "")));
        LettuceConnectionFactory factory = new LettuceConnectionFactory(config);
        factory.afterPropertiesSet(); factory.start();
        // Establish the connection before timing loader/invalidation races.
        try (var connection = factory.getConnection()) {
            assertThat(connection.ping()).isEqualTo("PONG");
        }
        return factory;
    }
    static AutoCacheConfig configuration(String template) {
        AutoCacheConfig config = new AutoCacheConfig(); config.setTable("users"); config.setKeyTemplate(template);
        config.setStrategy(CacheStrategy.DELETE_MODE); config.setExpireSeconds(1800); config.setIdField("id");
        config.setEntityClass(User.class); config.setIdFieldType(Long.class); return config;
    }
    static CacheAnnotationProcessor processor(AutoCacheConfig config) {
        return new CacheAnnotationProcessor() {
            @Override public AutoCacheConfig getConfig(String table) { return config; }
        };
    }
    static User user(long id, String name) { User user = new User(); user.setId(id); user.setName(name); return user; }
}
