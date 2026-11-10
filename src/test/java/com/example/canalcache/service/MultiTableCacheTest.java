package com.example.canalcache.service;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.example.canalcache.annotation.CacheStrategy;
import com.example.canalcache.detector.BigKeyDetector;
import com.example.canalcache.entity.Department;
import com.example.canalcache.entity.User;
import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.parser.FlatMessageBinlogParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class MultiTableCacheTest {
    @Test
    void scannedTablesShareQueriesAndInvalidationWithSeparateKeysAndTtls() {
        CacheAnnotationProcessor processor = new CacheAnnotationProcessor();
        ReflectionTestUtils.setField(processor, "basePackage", "com.example.canalcache.entity");
        processor.init();
        assertThat(processor.getAllTables()).containsExactlyInAnyOrder("users", "departments");
        assertThat(processor.getConfig("departments").getStrategy()).isEqualTo(CacheStrategy.DELETE_MODE);
        assertThat(processor.getConfig("departments").getExpireSeconds()).isEqualTo(600);

        RedisTemplate<String, String> redis = mock(RedisTemplate.class);
        HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
        when(redis.opsForHash()).thenReturn(hashes);
        ObjectMapper json = new ObjectMapper();
        SimpleMeterRegistry metrics = new SimpleMeterRegistry();
        CachedQueryService queries = new CachedQueryService(redis, processor, json, metrics);
        Department department = new Department(); department.setId(1L); department.setName("R&D");
        User user = new User(); user.setId(1L); user.setName("Alice");

        assertThat(queries.getById("users", 1L, User.class, () -> user)).isEqualTo(user);
        assertThat(queries.getById("departments", 1L, Department.class, () -> department)).isEqualTo(department);

        CanalCacheSyncService sync = new CanalCacheSyncService(mock(CanalConnectorWrapper.class),
                new BinlogParser(), new FlatMessageBinlogParser(), processor,
                new VersionControllerWithLua(redis), mock(BigKeyDetector.class), redis, json, metrics);
        sync.handleEvent(BinlogEvent.builder().tableName("users").eventType(CanalEntry.EventType.UPDATE)
                .afterData(Map.of("id", "1", "name", "new")).build());
        sync.handleEvent(BinlogEvent.builder().tableName("departments").eventType(CanalEntry.EventType.UPDATE)
                .afterData(Map.of("id", "1", "name", "new")).build());

        verify(redis).delete("user:1");
        verify(redis).delete("department:1");
        assertThat(metrics.get("cache.query.database.calls").tag("table", "users").counter().count()).isEqualTo(1);
        assertThat(metrics.get("cache.query.database.calls").tag("table", "departments").counter().count()).isEqualTo(1);
    }
}
