package com.example.canalcache.service;

import com.example.canalcache.entity.User;
import com.example.canalcache.model.AutoCacheConfig;
import com.example.canalcache.model.BatchQueryResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CachedQueryServiceTest {
    private final RedisTemplate<String, String> redis = mock(RedisTemplate.class);
    private final HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
    private final CacheAnnotationProcessor processor = mock(CacheAnnotationProcessor.class);
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final ObjectMapper json = new ObjectMapper();
    private CachedQueryService service;

    @BeforeEach
    void setUp() {
        AutoCacheConfig config = new AutoCacheConfig();
        config.setKeyTemplate("user:#id");
        config.setExpireSeconds(1800);
        when(processor.getConfig("users")).thenReturn(config);
        when(redis.opsForHash()).thenReturn(hashes);
        when(redis.getStringSerializer()).thenReturn(RedisSerializer.string());
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenAnswer(invocation -> ((RedisScript<?>) invocation.getArgument(0)).getResultType() == String.class
                        ? null : 1L);
        service = new CachedQueryService(redis, processor, json, metrics);
    }

    @Test
    void batchLoadsOnlyMissingIdsAndPreservesFirstOccurrenceOrder() throws Exception {
        when(redis.executePipelined(any(RedisCallback.class), any(RedisSerializer.class)))
                .thenReturn(Arrays.asList(json.writeValueAsString(user(2)), null, null),
                        Arrays.asList(null, null));
        when(redis.executePipelined(any(RedisCallback.class))).thenReturn(List.of(1L, 1L));
        Function<List<Long>, List<User>> loader = mock(Function.class);
        when(loader.apply(List.of(1L, 3L))).thenReturn(List.of(user(1)));

        BatchQueryResult<User> result = service.getByIds("users", List.of(2L, 1L, 3L, 2L),
                User.class, loader, User::getId);

        assertThat(result.items()).extracting(User::getId).containsExactly(2L, 1L);
        assertThat(result.missingIds()).containsExactly(3L);
        verify(loader, times(1)).apply(List.of(1L, 3L));
        assertThat(count("cache.query.items", "result", "hit")).isEqualTo(1);
        assertThat(count("cache.query.items", "result", "miss")).isEqualTo(2);
        assertThat(count("cache.query.database.calls")).isEqualTo(1);
        assertThat(count("cache.query.database.ids")).isEqualTo(2);
    }

    @Test
    void cachedNullInBatchDoesNotQueryDatabase() throws Exception {
        when(redis.executePipelined(any(RedisCallback.class), any(RedisSerializer.class)))
                .thenReturn(List.of("null", json.writeValueAsString(user(2))));
        Function<List<Long>, List<User>> loader = mock(Function.class);

        BatchQueryResult<User> result = service.getByIds("users", List.of(99L, 2L),
                User.class, loader, User::getId);

        assertThat(result.missingIds()).containsExactly(99L);
        assertThat(result.items()).extracting(User::getId).containsExactly(2L);
        verifyNoInteractions(loader);
    }

    @Test
    void concurrentColdQueriesShareOneDatabaseLoad() throws Exception {
        var executor = Executors.newFixedThreadPool(8);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger loads = new java.util.concurrent.atomic.AtomicInteger();
        List<Future<User>> requests = new ArrayList<>();
        try {
            for (int i = 0; i < 8; i++) {
                requests.add(executor.submit(() -> service.getById("users", 1L, User.class, () -> {
                    loads.incrementAndGet(); entered.countDown(); waitOn(release); return user(1);
                })));
            }
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(count("cache.query.coalesced")).isEqualTo(7));
            release.countDown();
            for (Future<User> request : requests) {
                assertThat(request.get(5, TimeUnit.SECONDS).getId()).isEqualTo(1);
            }
            assertThat(loads.get()).isEqualTo(1);
        } finally {
            release.countDown(); executor.shutdownNow();
        }
    }

    @Test
    void unrelatedIdCanLoadWhileAnotherIdIsBlocked() throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Future<User> first = executor.submit(() -> service.getById("users", 1L, User.class, () -> {
                entered.countDown(); waitOn(release); return user(1);
            }));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            Future<User> other = executor.submit(() -> service.getById("users", 2L, User.class, () -> user(2)));
            assertThat(other.get(5, TimeUnit.SECONDS).getId()).isEqualTo(2);
            release.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS).getId()).isEqualTo(1);
        } finally {
            release.countDown(); executor.shutdownNow();
        }
    }

    @Test
    void failedLoaderReleasesWaitersAndAllowsNextLoad() throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Future<User> first = executor.submit(() -> service.getById("users", 1L, User.class, () -> {
                entered.countDown(); waitOn(release);
                throw new DataAccessResourceFailureException("database offline");
            }));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            Future<User> waiter = executor.submit(() -> service.getById("users", 1L, User.class, () -> {
                throw new AssertionError("waiter must not load");
            }));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(count("cache.query.coalesced")).isEqualTo(1));
            release.countDown();
            assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(DataAccessResourceFailureException.class);
            assertThatThrownBy(() -> waiter.get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(DataAccessResourceFailureException.class);
            verify(redis, never()).execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString());
            assertThat(service.getById("users", 1L, User.class, () -> user(1)).getId()).isEqualTo(1);
        } finally {
            release.countDown(); executor.shutdownNow();
        }
    }

    @Test
    void redisFillFailureIsReportedAndDoesNotLeakLoadingOwnership() {
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString()))
                .thenThrow(new RedisConnectionFailureException("redis offline")).thenReturn(1L);

        assertThatThrownBy(() -> service.getById("users", 1L, User.class, () -> user(1)))
                .isInstanceOf(RedisConnectionFailureException.class);
        assertThat(service.getById("users", 1L, User.class, () -> user(1)).getId()).isEqualTo(1);
    }

    @Test
    void recheckUsesCacheFilledBetweenInitialReadAndOwnership() throws Exception {
        when(redis.execute(any(RedisScript.class), eq(List.of("user:1")), anyString(), eq("30")))
                .thenReturn(json.writeValueAsString(user(1)));

        User result = service.getById("users", 1L, User.class, () -> {
            throw new AssertionError("recheck hit must not query database");
        });

        assertThat(result.getId()).isEqualTo(1);
        assertThat(count("cache.query.database.calls")).isZero();
    }

    @Test
    void cleanupFailurePreservesOriginalFailureAndReleasesOwnership() {
        when(redis.execute(any(RedisScript.class), anyList(), anyString()))
                .thenThrow(new RedisConnectionFailureException("cleanup offline"));
        DataAccessResourceFailureException failure = new DataAccessResourceFailureException("database offline");

        assertThatThrownBy(() -> service.getById("users", 1L, User.class, () -> { throw failure; }))
                .isSameAs(failure)
                .satisfies(error -> assertThat(error.getSuppressed()).hasSize(1)
                        .allSatisfy(suppressed -> assertThat(suppressed)
                                .isInstanceOf(RedisConnectionFailureException.class)));
        assertThat(service.getById("users", 1L, User.class, () -> user(1)).getId()).isEqualTo(1);
    }

    @Test
    void overlappingBatchesDoNotWaitBeforeCompletingTheirOwnIds() throws Exception {
        // 单条请求拥有 ID 1；重叠批次必须先加载自己的 ID 2，再等待 ID 1。
        var executor = Executors.newFixedThreadPool(3);
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch secondEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(redis.executePipelined(any(RedisCallback.class), any(RedisSerializer.class)))
                .thenReturn(Arrays.asList(null, null));
        try {
            Future<User> first = executor.submit(() -> service.getById("users", 1L, User.class, () -> {
                firstEntered.countDown(); waitOn(release); return user(1);
            }));
            assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
            Future<BatchQueryResult<User>> batch = executor.submit(() -> service.getByIds("users",
                    List.of(1L, 2L), User.class, ids -> {
                        assertThat(ids).containsExactly(2L); secondEntered.countDown(); return List.of(user(2));
                    }, User::getId));
            assertThat(secondEntered.await(5, TimeUnit.SECONDS)).isTrue();
            release.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS).getId()).isEqualTo(1);
            assertThat(batch.get(5, TimeUnit.SECONDS).items()).extracting(User::getId).containsExactly(1L, 2L);
        } finally {
            release.countDown(); executor.shutdownNow();
        }
    }

    private double count(String name, String... tags) {
        var search = metrics.find(name).tag("table", "users").tags(tags);
        return search.counter() == null ? 0 : search.counter().count();
    }

    private static void waitOn(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("test barrier timed out");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); throw new AssertionError(e);
        }
    }

    private static User user(long id) {
        User user = new User(); user.setId(id); user.setName("User-" + id); return user;
    }
}
