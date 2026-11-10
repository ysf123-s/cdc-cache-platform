package com.example.canalcache.service;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.CanalCacheSyncApplication;
import com.example.canalcache.annotation.CacheStrategy;
import com.example.canalcache.detector.BigKeyDetector;
import com.example.canalcache.entity.User;
import com.example.canalcache.model.AutoCacheConfig;
import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.parser.FlatMessageBinlogParser;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CacheRefreshFormatTest {
    @ParameterizedTest
    @CsvSource({"REFRESH_MODE,false", "REFRESH_MODE,true", "HYBRID_MODE,false", "HYBRID_MODE,true"})
    @SuppressWarnings("unchecked")
    void refreshedRowHasSameFormatAsQueryAndCanBeReadWithoutDatabase(CacheStrategy strategy, boolean versioned) throws Exception {
        var json = new CanalCacheSyncApplication().objectMapper();
        var config = new AutoCacheConfig();
        config.setTable("users"); config.setEntityClass(User.class); config.setIdField("id");
        config.setKeyTemplate("user:#id"); config.setStrategy(strategy); config.setExpireSeconds(1800);
        config.setEnableVersionControl(versioned);
        var processor = mock(CacheAnnotationProcessor.class);
        when(processor.getConfig("users")).thenReturn(config);
        var versions = mock(VersionControllerWithLua.class);
        RedisTemplate<String, String> redis = mock(RedisTemplate.class);
        var metrics = new SimpleMeterRegistry();
        var sync = new CanalCacheSyncService(mock(CanalConnectorWrapper.class), new BinlogParser(),
                new FlatMessageBinlogParser(), processor, versions, mock(BigKeyDetector.class), redis, json, metrics);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", "3"); row.put("name", "张三"); row.put("email", null); row.put("phone", "123");
        row.put("created_at", "2026-11-04 10:20:30");
        sync.handleEvent(BinlogEvent.builder().tableName("users").eventType(EventType.UPDATE)
                .afterData(row).binlogFile("mysql-bin.000001").binlogPosition(100L).build());
        var payload = ArgumentCaptor.forClass(String.class);
        if (versioned) verify(versions).updateIfNewerVersion(eq("user:3"), payload.capture(), any(), eq(1800));
        else verify(versions).updateIfPresent(eq("user:3"), payload.capture(), eq(1800));

        User expected = new User();
        expected.setId(3L); expected.setName("张三"); expected.setPhone("123");
        expected.setCreatedAt(Date.from(Instant.parse("2026-11-04T02:20:30Z")));
        assertThat(json.readTree(payload.getValue())).isEqualTo(json.readTree(json.writeValueAsString(expected)));
        HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
        when(redis.opsForHash()).thenReturn(hashes);
        when(hashes.get("user:3", "data")).thenReturn(payload.getValue());
        Supplier<User> loader = mock(Supplier.class);
        var query = new CachedQueryService(redis, processor, json, metrics);
        assertThat(query.getById("users", 3L, User.class, loader)).isEqualTo(expected);
        verifyNoInteractions(loader);
    }
}
