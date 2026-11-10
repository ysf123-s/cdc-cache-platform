package com.example.canalcache.service;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.annotation.CacheStrategy;
import com.example.canalcache.detector.BigKeyDetector;
import com.example.canalcache.model.AutoCacheConfig;
import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.model.BinlogVersion;
import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.parser.FlatMessageBinlogParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CanalCacheSyncServiceRecoveryTest {

    @Test
    void connectorUsesPartitionCount() {
        CanalConnectorWrapper wrapper = new CanalConnectorWrapper();
        ReflectionTestUtils.setField(wrapper, "partitionCount", 6);

        assertThat(wrapper.getConsumerThreadCount()).isEqualTo(6);
    }

    @Test
    void flatMessageModeDisablesVersionControl() throws Exception {
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        when(connector.isFlatMessageMode()).thenReturn(true);

        CanalCacheSyncService service = newService(connector);
        AutoCacheConfig config = config(true);

        assertThat(invokeShouldUseVersionControl(service, config)).isFalse();
    }

    @Test
    void entryModeKeepsVersionControlWhenConfigEnabled() throws Exception {
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        when(connector.isFlatMessageMode()).thenReturn(false);

        CanalCacheSyncService service = newService(connector);
        AutoCacheConfig config = config(true);

        assertThat(invokeShouldUseVersionControl(service, config)).isTrue();
    }

    @Test
    void entryModeStillDisablesVersionControlWhenConfigDisabled() throws Exception {
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        when(connector.isFlatMessageMode()).thenReturn(false);

        CanalCacheSyncService service = newService(connector);
        AutoCacheConfig config = config(false);

        assertThat(invokeShouldUseVersionControl(service, config)).isFalse();
    }

    @Test
    void deleteEventDeletesCacheEvenWhenStrategyIsRefresh() throws Exception {
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        when(connector.isFlatMessageMode()).thenReturn(true);
        RedisTemplate<String, String> redisTemplate = mock(RedisTemplate.class);

        CanalCacheSyncService service = newService(connector, redisTemplate);
        AutoCacheConfig config = config(false);
        config.setStrategy(CacheStrategy.REFRESH_MODE);
        BinlogEvent event = BinlogEvent.builder()
                .eventType(EventType.DELETE)
                .binlogFile("mq")
                .binlogPosition(10L)
                .build();

        invokeExecuteStrategy(service, config, event, "user:1", Map.of("id", "1", "name", "Alice"));

        verify(redisTemplate).delete("user:1");
        verify(redisTemplate, never()).opsForHash();
    }

    @Test
    void refreshStrategySkipsColdDataWhenVersionControlDisabled() throws Exception {
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        when(connector.isFlatMessageMode()).thenReturn(true);
        VersionControllerWithLua versionController = mock(VersionControllerWithLua.class);
        RedisTemplate<String, String> redisTemplate = mock(RedisTemplate.class);
        when(versionController.updateIfPresent(anyString(), anyString(), anyInt())).thenReturn(false);

        CanalCacheSyncService service = new CanalCacheSyncService(
                connector,
                new BinlogParser(),
                new FlatMessageBinlogParser(),
                mock(CacheAnnotationProcessor.class),
                versionController,
                mock(BigKeyDetector.class),
                redisTemplate,
                new ObjectMapper(),
                new SimpleMeterRegistry()
        );
        AutoCacheConfig config = config(false);
        config.setStrategy(CacheStrategy.REFRESH_MODE);
        config.setExpireSeconds(1800);
        BinlogEvent event = BinlogEvent.builder()
                .eventType(EventType.UPDATE)
                .binlogFile("mq")
                .binlogPosition(10L)
                .build();

        invokeExecuteStrategy(service, config, event, "user:1", Map.of("id", "1", "name", "Alice"));

        verify(versionController).updateIfPresent(eq("user:1"), anyString(), eq(1800));
        verify(redisTemplate, never()).opsForHash();
        verify(redisTemplate, never()).expire(anyString(), anyInt(), any());
    }

    @Test
    void hybridStrategyDeletesBigKeyInsteadOfRefreshing() throws Exception {
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        when(connector.isFlatMessageMode()).thenReturn(true);
        RedisTemplate<String, String> redisTemplate = mock(RedisTemplate.class);
        BigKeyDetector bigKeyDetector = mock(BigKeyDetector.class);
        when(bigKeyDetector.isBigKey(any(), anyInt())).thenReturn(true);

        CanalCacheSyncService service = newService(connector, redisTemplate, bigKeyDetector);
        AutoCacheConfig config = config(false);
        config.setStrategy(CacheStrategy.HYBRID_MODE);
        config.setMaxSizeBytes(10240);
        BinlogEvent event = BinlogEvent.builder()
                .eventType(EventType.UPDATE)
                .binlogFile("mq")
                .binlogPosition(10L)
                .build();

        invokeExecuteStrategy(service, config, event, "user:1", Map.of("id", "1", "name", "Alice"));

        verify(redisTemplate).delete("user:1");
        verify(redisTemplate, never()).opsForHash();
    }

    @Test
    void processEventsHandlesPartitionBatchBeforeReturning() throws Exception {
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        when(connector.isFlatMessageMode()).thenReturn(false);
        CacheAnnotationProcessor processor = mock(CacheAnnotationProcessor.class);
        AutoCacheConfig config = config(true);
        config.setTable("users");
        config.setIdField("id");
        config.setKeyTemplate("user:#id");
        config.setStrategy(CacheStrategy.REFRESH_MODE);
        config.setExpireSeconds(1800);
        VersionControllerWithLua versionController = mock(VersionControllerWithLua.class);
        when(versionController.updateIfNewerVersion(anyString(), anyString(), any(BinlogVersion.class), anyInt()))
                .thenAnswer(invocation -> {
                    Thread.sleep(30);
                    return true;
                });
        when(processor.getConfig("users")).thenReturn(config);

        CanalCacheSyncService service = new CanalCacheSyncService(
                connector,
                new BinlogParser(),
                new FlatMessageBinlogParser(),
                processor,
                versionController,
                mock(BigKeyDetector.class),
                mock(RedisTemplate.class),
                new ObjectMapper(),
                new SimpleMeterRegistry()
        );

        List<BinlogEvent> events = List.of(
                event("1", 100L),
                event("2", 200L),
                event("3", 300L)
        );

        invokeProcessEvents(service, events);

        verify(versionController).updateIfNewerVersion(
                eq("user:1"), anyString(), eq(new BinlogVersion(0, 0, 100, 100)), eq(1800));
        verify(versionController).updateIfNewerVersion(
                eq("user:2"), anyString(), eq(new BinlogVersion(0, 0, 200, 200)), eq(1800));
        verify(versionController).updateIfNewerVersion(
                eq("user:3"), anyString(), eq(new BinlogVersion(0, 0, 300, 300)), eq(1800));
    }

    @Test
    void buildVersionPacksEpochFileNoAndOffsetIntoFiveSixteenThirtyTwoBits() throws Exception {
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        CanalCacheSyncService service = newService(connector);
        Method method = CanalCacheSyncService.class.getDeclaredMethod(
                "buildVersion",
                long.class,
                String.class,
                long.class
        );
        method.setAccessible(true);

        BinlogVersion version = (BinlogVersion) method.invoke(service, 1L, "mysql-bin.000003", 1000L);

        long expectedEncodedVersion = (1L << 48) | (3L << 32) | 1000L;
        assertThat(version).isEqualTo(new BinlogVersion(1L, 3L, 1000L, expectedEncodedVersion));
    }

    @Test
    void parseBinlogFileNoRejectsInvalidFormat() throws Exception {
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        CanalCacheSyncService service = newService(connector);
        Method method = CanalCacheSyncService.class.getDeclaredMethod("parseBinlogFileNo", String.class);
        method.setAccessible(true);

        assertThatThrownBy(() -> method.invoke(service, "mysql-bin.invalid"))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("Invalid binlog file name: mysql-bin.invalid");
    }

    private CanalCacheSyncService newService(CanalConnectorWrapper connector) {
        return newService(connector, mock(RedisTemplate.class));
    }

    private CanalCacheSyncService newService(CanalConnectorWrapper connector,
                                             RedisTemplate<String, String> redisTemplate) {
        return newService(connector, redisTemplate, mock(BigKeyDetector.class));
    }

    private CanalCacheSyncService newService(CanalConnectorWrapper connector,
                                             RedisTemplate<String, String> redisTemplate,
                                             BigKeyDetector bigKeyDetector) {
        return new CanalCacheSyncService(
                connector,
                new BinlogParser(),
                new FlatMessageBinlogParser(),
                mock(CacheAnnotationProcessor.class),
                mock(VersionControllerWithLua.class),
                bigKeyDetector,
                redisTemplate,
                new ObjectMapper(),
                new SimpleMeterRegistry()
        );
    }

    private boolean invokeShouldUseVersionControl(CanalCacheSyncService service,
                                                  AutoCacheConfig config) throws Exception {
        Method method = CanalCacheSyncService.class.getDeclaredMethod(
                "shouldUseVersionControl",
                AutoCacheConfig.class
        );
        method.setAccessible(true);
        return (boolean) method.invoke(service, config);
    }

    private void invokeExecuteStrategy(CanalCacheSyncService service,
                                       AutoCacheConfig config,
                                       BinlogEvent event,
                                       String cacheKey,
                                       Map<String, Object> data) throws Exception {
        Method method = CanalCacheSyncService.class.getDeclaredMethod(
                "executeStrategy",
                AutoCacheConfig.class,
                BinlogEvent.class,
                String.class,
                Map.class
        );
        method.setAccessible(true);
        method.invoke(service, config, event, cacheKey, data);
    }

    private void invokeProcessEvents(CanalCacheSyncService service,
                                     List<BinlogEvent> events) throws Exception {
        Method method = CanalCacheSyncService.class.getDeclaredMethod(
                "processEvents",
                List.class
        );
        method.setAccessible(true);
        method.invoke(service, events);
    }

    private BinlogEvent event(String id, long position) {
        return BinlogEvent.builder()
                .tableName("users")
                .eventType(EventType.UPDATE)
                .afterData(Map.of("id", id, "name", "Alice"))
                .binlogFile("mysql-bin.000000")
                .binlogPosition(position)
                .build();
    }

    private AutoCacheConfig config(boolean enableVersionControl) {
        AutoCacheConfig config = new AutoCacheConfig();
        config.setEntityClass(com.example.canalcache.entity.User.class);
        config.setEnableVersionControl(enableVersionControl);
        return config;
    }
}
