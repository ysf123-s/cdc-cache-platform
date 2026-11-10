package com.example.canalcache.service;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.annotation.CacheStrategy;
import com.example.canalcache.detector.BigKeyDetector;
import com.example.canalcache.model.AutoCacheConfig;
import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.parser.FlatMessageBinlogParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CachePrimaryKeyChangeTest {
    @ParameterizedTest
    @EnumSource(CacheStrategy.class)
    void primaryKeyChangeDeletesOldKeyAndPreservesNewKeyStrategy(CacheStrategy strategy) {
        var fixture = new Fixture(strategy, false);
        fixture.sync.handleEvent(update(1L, "2"));
        verify(fixture.redis).delete("user:1");
        if (strategy == CacheStrategy.DELETE_MODE) {
            verify(fixture.redis).delete("user:2");
        } else {
            verify(fixture.versions).updateIfPresent(eq("user:2"), anyString(), eq(1800));
        }
    }

    @ParameterizedTest
    @EnumSource(CacheStrategy.class)
    void primaryKeyChangeUsesSameBinlogVersionForOldAndNewKey(CacheStrategy strategy) {
        var fixture = new Fixture(strategy, true);
        fixture.sync.handleEvent(update(1L, "2"));
        var version = org.mockito.ArgumentCaptor.forClass(com.example.canalcache.model.BinlogVersion.class);
        verify(fixture.versions).deleteIfNewerVersion(eq("user:1"), version.capture());
        if (strategy == CacheStrategy.DELETE_MODE) {
            verify(fixture.versions).deleteIfNewerVersion("user:2", version.getValue());
        } else {
            verify(fixture.versions).updateIfNewerVersion(eq("user:2"), anyString(), eq(version.getValue()), eq(1800));
        }
        verifyNoInteractions(fixture.redis);
    }

    @ParameterizedTest
    @CsvSource({"1,1", "1,2"})
    void ordinaryUpdateDeletesEachCacheKeyOnlyOnce(long before, long after) {
        var fixture = new Fixture(CacheStrategy.DELETE_MODE, false);
        fixture.sync.handleEvent(update(before, Long.toString(after)));
        verify(fixture.redis).delete("user:" + after);
        if (before != after) verify(fixture.redis).delete("user:" + before);
        verifyNoMoreInteractions(fixture.redis);
    }

    private static BinlogEvent update(Object before, Object after) {
        return BinlogEvent.builder().tableName("users").eventType(EventType.UPDATE)
                .beforeData(Map.of("id", before, "name", "before"))
                .afterData(Map.of("id", after, "name", "after"))
                .binlogFile("binlog.000002").binlogPosition(100L).build();
    }

    private static class Fixture {
        final RedisTemplate<String, String> redis = mock(RedisTemplate.class);
        final VersionControllerWithLua versions = mock(VersionControllerWithLua.class);
        final CanalCacheSyncService sync;
        Fixture(CacheStrategy strategy, boolean versionControl) {
            var config = new AutoCacheConfig();
            config.setEntityClass(com.example.canalcache.entity.User.class);
            config.setTable("users"); config.setIdField("id"); config.setKeyTemplate("user:#id");
            config.setStrategy(strategy); config.setExpireSeconds(1800); config.setEnableVersionControl(versionControl);
            var processor = mock(CacheAnnotationProcessor.class);
            when(processor.getConfig("users")).thenReturn(config);
            sync = new CanalCacheSyncService(mock(CanalConnectorWrapper.class), new BinlogParser(),
                    new FlatMessageBinlogParser(), processor, versions, mock(BigKeyDetector.class), redis,
                    new ObjectMapper(), new SimpleMeterRegistry());
        }
    }
}
