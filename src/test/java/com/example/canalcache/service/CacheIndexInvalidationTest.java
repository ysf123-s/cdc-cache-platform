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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CacheIndexInvalidationTest {
    private final RedisTemplate<String, String> redis = mock(RedisTemplate.class);
    private final VersionControllerWithLua versions = mock(VersionControllerWithLua.class);

    @Test
    void emailChangeDeletesOldAndNewIndexKeys() {
        sync(CacheStrategy.DELETE_MODE).handleEvent(event(EventType.UPDATE,
                Map.of("id", "1", "email", "a@x.com"), Map.of("id", "1", "email", "b@x.com")));
        verify(redis).delete("user:1");
        verify(redis).delete(List.of("user-email:a@x.com", "user-email:b@x.com"));
    }

    @ParameterizedTest
    @EnumSource(CacheStrategy.class)
    void indexKeysAreDeletedForEveryStrategy(CacheStrategy strategy) {
        sync(strategy).handleEvent(event(EventType.UPDATE,
                Map.of("id", "1", "email", "a@x.com"), Map.of("id", "1", "email", "b@x.com")));
        verify(redis).delete(List.of("user-email:a@x.com", "user-email:b@x.com"));
    }

    @Test
    void updateThatKeepsEmailAndIdLeavesIndexAlone() {
        sync(CacheStrategy.DELETE_MODE).handleEvent(event(EventType.UPDATE,
                Map.of("id", "1", "email", "a@x.com", "name", "old"),
                Map.of("id", "1", "email", "a@x.com", "name", "new")));
        verify(redis).delete("user:1");
        verifyNoMoreInteractions(redis);
    }

    @Test
    void primaryKeyChangeWithSameEmailStillDeletesIndexBecauseIdListChanged() {
        sync(CacheStrategy.DELETE_MODE).handleEvent(event(EventType.UPDATE,
                Map.of("id", "1", "email", "a@x.com"), Map.of("id", "2", "email", "a@x.com")));
        verify(redis).delete(List.of("user-email:a@x.com"));
    }

    @Test
    void insertClearsEmptyListCachedForTheNewEmail() {
        sync(CacheStrategy.DELETE_MODE).handleEvent(event(EventType.INSERT, null, Map.of("id", "2", "email", "c@x.com")));
        verify(redis).delete("user:2");
        verify(redis).delete(List.of("user-email:c@x.com"));
    }

    @Test
    void deleteUsesBeforeImageEmail() {
        sync(CacheStrategy.DELETE_MODE).handleEvent(event(EventType.DELETE, Map.of("id", "3", "email", "d@x.com"), null));
        verify(redis).delete("user:3");
        verify(redis).delete(List.of("user-email:d@x.com"));
    }

    @Test
    void minimalRowImageWithoutEmailInAfterImageDeletesBeforeImageKey() {
        sync(CacheStrategy.DELETE_MODE).handleEvent(event(EventType.UPDATE,
                Map.of("id", "1", "email", "a@x.com"), Map.of("name", "new")));
        verify(redis).delete("user:1");
        verify(redis).delete(List.of("user-email:a@x.com"));
    }

    @Test
    void nullEmailHasNoIndexKey() {
        Map<String, Object> after = new HashMap<>(Map.of("id", "4"));
        after.put("email", null);
        sync(CacheStrategy.DELETE_MODE).handleEvent(event(EventType.INSERT, null, after));
        verify(redis).delete("user:4");
        verifyNoMoreInteractions(redis);
    }

    @Test
    void indexDeletionFailurePropagatesSoTheBatchIsRetried() {
        when(redis.delete(anyCollection())).thenThrow(new RedisConnectionFailureException("down"));
        assertThatThrownBy(() -> sync(CacheStrategy.DELETE_MODE).handleEvent(
                        event(EventType.INSERT, null, Map.of("id", "2", "email", "c@x.com"))))
                .isInstanceOf(RedisConnectionFailureException.class);
    }

    private CanalCacheSyncService sync(CacheStrategy strategy) {
        AutoCacheConfig config = new AutoCacheConfig();
        config.setEntityClass(com.example.canalcache.entity.User.class);
        config.setTable("users");
        config.setIdField("id");
        config.setKeyTemplate("user:#id");
        config.setIndexKeys(List.of(AutoCacheConfig.IndexKey.parse("user-email:#email")));
        config.setStrategy(strategy);
        config.setExpireSeconds(1800);
        CacheAnnotationProcessor processor = mock(CacheAnnotationProcessor.class);
        when(processor.getConfig("users")).thenReturn(config);
        return new CanalCacheSyncService(mock(CanalConnectorWrapper.class), new BinlogParser(),
                new FlatMessageBinlogParser(), processor, versions, mock(BigKeyDetector.class), redis,
                new ObjectMapper(), new SimpleMeterRegistry());
    }

    private static BinlogEvent event(EventType type, Map<String, Object> before, Map<String, Object> after) {
        return BinlogEvent.builder().tableName("users").eventType(type).beforeData(before).afterData(after)
                .binlogFile("binlog.000002").binlogPosition(100L).build();
    }
}
