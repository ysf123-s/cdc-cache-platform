package com.example.canalcache.integration;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.detector.BigKeyDetector;
import com.example.canalcache.model.AutoCacheConfig;
import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.parser.FlatMessageBinlogParser;
import com.example.canalcache.service.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/** Opt-in real Redis tests for secondary index caches. Unique keys only; never flush a shared database. */
@EnabledIfSystemProperty(named = "cache.test.redis.host", matches = ".+")
class CacheIndexRedisTest {
    private LettuceConnectionFactory factory;
    private StringRedisTemplate redis;
    private String prefix;
    private AutoCacheConfig config;
    private CachedQueryService cached;
    private CanalCacheSyncService sync;
    private final List<String> ownedKeys = new ArrayList<>();
    private final ObjectMapper json = new ObjectMapper();
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();

    @BeforeEach
    void setUp() {
        factory = CacheFillRedisTest.connectionFactory(System.getProperty("cache.test.redis.host"));
        redis = new StringRedisTemplate(factory);
        prefix = "index-test:" + UUID.randomUUID() + ":";
        config = CacheFillRedisTest.configuration(prefix + "user:#id");
        config.setIndexKeys(List.of(AutoCacheConfig.IndexKey.parse(prefix + "user-email:#email")));
        CacheAnnotationProcessor processor = CacheFillRedisTest.processor(config);
        cached = new CachedQueryService(redis, processor, json, metrics);
        sync = new CanalCacheSyncService(mock(CanalConnectorWrapper.class), new BinlogParser(),
                new FlatMessageBinlogParser(), processor, new VersionControllerWithLua(redis),
                mock(BigKeyDetector.class), redis, json, metrics);
    }

    @AfterEach
    void tearDown() {
        redis.delete(ownedKeys);
        factory.destroy();
        metrics.close();
    }

    @Test
    void idListIsCachedOnceThenDroppedWhenEmailChanges() {
        Supplier<List<Long>> loader = mock(Supplier.class);
        when(loader.get()).thenReturn(List.of(1L));

        assertThat(cached.getIndexedIds("users", "email", "a@x.com", loader)).containsExactly(1L);
        assertThat(cached.getIndexedIds("users", "email", "a@x.com", loader)).containsExactly(1L);

        verify(loader, times(1)).get();
        assertThat(redis.opsForHash().get(index("a@x.com"), "data")).isEqualTo("[1]");
        assertThat(redis.getExpire(index("a@x.com"))).isBetween(1L, 1800L);

        sync.handleEvent(emailChange("a@x.com", "b@x.com"));

        assertThat(redis.hasKey(index("a@x.com"))).isFalse();
    }

    @Test
    void lookupStartedBeforeInvalidationCannotBackfillItsStaleResult() {
        List<Long> result = cached.getIndexedIds("users", "email", "a@x.com", () -> {
            // 回源已读到旧结果，回填之前 binlog 失效到达。
            sync.handleEvent(emailChange("a@x.com", "b@x.com"));
            return List.of(1L);
        });

        assertThat(result).containsExactly(1L);
        assertThat(redis.hasKey(index("a@x.com"))).isFalse();
        assertThat(metrics.get("cache.index.fills").tag("result", "rejected").counter().count()).isEqualTo(1);
    }

    @Test
    void emptyListUsesShortTtlAndInsertClearsIt() {
        assertThat(cached.getIndexedIds("users", "email", "c@x.com", List::of)).isEmpty();
        assertThat(redis.getExpire(index("c@x.com"))).isBetween(1L, 30L);

        sync.handleEvent(BinlogEvent.builder().tableName("users").eventType(EventType.INSERT)
                .afterData(Map.of("id", "2", "email", "c@x.com")).build());

        assertThat(redis.hasKey(index("c@x.com"))).isFalse();
    }

    @Test
    void groupPurgeAlsoClearsIndexNamespace() {
        redis.opsForHash().put(own("user:1"), "data", "null");
        redis.opsForHash().put(index("a@x.com"), "data", "[1]");
        redis.opsForHash().put(index("b@x.com"), "load_token", "pending");

        long deleted = new CacheNamespacePurger(redis, new CacheBrowseService(redis)).purge("users", config);

        assertThat(deleted).isEqualTo(3);
        assertThat(redis.hasKey(index("a@x.com"))).isFalse();
        assertThat(redis.hasKey(index("b@x.com"))).isFalse();
    }

    private BinlogEvent emailChange(String from, String to) {
        return BinlogEvent.builder().tableName("users").eventType(EventType.UPDATE)
                .beforeData(Map.of("id", "1", "email", from)).afterData(Map.of("id", "1", "email", to)).build();
    }

    private String index(String email) {
        return own("user-email:" + email);
    }

    private String own(String suffix) {
        String key = prefix + suffix;
        ownedKeys.add(key);
        return key;
    }
}
