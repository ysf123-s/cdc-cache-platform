package com.example.canalcache.service;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.example.canalcache.annotation.CacheStrategy;
import com.example.canalcache.detector.BigKeyDetector;
import com.example.canalcache.model.AutoCacheConfig;
import com.example.canalcache.model.BinlogVersion;
import com.example.canalcache.model.KafkaCanalRecord;
import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.parser.FlatMessageBinlogParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CanalBatchRecoveryTest {
    private final CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
    private final CanalConnectorWrapper.PartitionConsumer consumer = mock(CanalConnectorWrapper.PartitionConsumer.class);
    private final CacheAnnotationProcessor processor = mock(CacheAnnotationProcessor.class);
    private final RedisTemplate<String, String> redis = mock(RedisTemplate.class);
    private final VersionControllerWithLua versions = new VersionControllerWithLua(redis);
    private final AutoCacheConfig config = new AutoCacheConfig();
    private CanalCacheSyncService service;

    @BeforeEach
    void setUp() {
        config.setTable("users");
        config.setIdField("id");
        config.setKeyTemplate("user:#id");
        config.setStrategy(CacheStrategy.DELETE_MODE);
        config.setEnableVersionControl(true);
        when(processor.getConfig("users")).thenReturn(config);
        service = new CanalCacheSyncService(connector, new BinlogParser(),
                new FlatMessageBinlogParser(), processor, versions,
                mock(BigKeyDetector.class), redis, new ObjectMapper(), new SimpleMeterRegistry());
    }

    @Test
    @Timeout(10)
    void redisFailureRollsBackAndRetriesBeforeAcknowledging() {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(record(CanalEntry.EventType.UPDATE)));
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenThrow(new RedisConnectionFailureException("Redis unavailable"))
                .thenReturn(1L);
        ReflectionTestUtils.setField(service, "running", true);
        doAnswer(invocation -> {
            ReflectionTestUtils.setField(service, "running", false);
            return null;
        }).when(consumer).ack();

        ReflectionTestUtils.invokeMethod(service, "doSyncLoop", consumer);

        var order = inOrder(consumer);
        order.verify(consumer).getRecordsWithoutAck();
        order.verify(consumer).rollback();
        order.verify(consumer).getRecordsWithoutAck();
        order.verify(consumer).ack();
        verify(consumer, times(1)).ack();
        assertThat(service.getStatus().lastSuccessAt()).isNotNull();
        assertThat(service.getStatus().lastError().type()).isEqualTo("RedisConnectionFailureException");
        assertThat(service.getStatus().lastSuccessAt())
                .isAfterOrEqualTo(service.getStatus().lastError().occurredAt());
    }

    @Test
    void everyRedisWriteStrategyPropagatesConnectionFailure() {
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenThrow(new RedisConnectionFailureException("Redis unavailable"));
        BinlogVersion version = new BinlogVersion(0, 1, 100, (1L << 32) | 100);

        assertThatThrownBy(() -> versions.deleteIfNewerVersion("user:1", version))
                .isInstanceOf(RedisConnectionFailureException.class);
        assertThatThrownBy(() -> versions.updateIfNewerVersion("user:1", "{}", version, 60))
                .isInstanceOf(RedisConnectionFailureException.class);
        assertThatThrownBy(() -> versions.updateIfPresent("user:1", "{}", 60))
                .isInstanceOf(RedisConnectionFailureException.class);
    }

    @Test
    void malformedRowChangeDoesNotAcknowledgeBatch() {
        CanalEntry.Entry entry = CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setStoreValue(ByteString.copyFrom(new byte[]{10, 5, 1}))
                .build();
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(
                new KafkaCanalRecord("canal-test", 0, 0L, new Message(1L, List.of(entry)))));

        assertThatThrownBy(() -> service.processBatch(consumer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to parse RowChange");
        verify(consumer, never()).ack();
    }

    @Test
    void filteredDdlBatchIsAcknowledged() {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(record(CanalEntry.EventType.CREATE)));

        assertThat(service.processBatch(consumer)).isTrue();

        verify(consumer).ack();
        verifyNoInteractions(redis);
        assertThat(service.getStatus().lastSuccessAt()).isNotNull();
    }

    @ParameterizedTest
    @EnumSource(CacheStrategy.class)
    void insertInvalidatesBeforeAcknowledgingForEveryStrategy(CacheStrategy strategy) {
        config.setStrategy(strategy);
        config.setEnableVersionControl(false);
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(record(CanalEntry.EventType.INSERT)));
        when(redis.delete("user:1")).thenReturn(true);

        assertThat(service.processBatch(consumer)).isTrue();

        var order = inOrder(redis, consumer);
        order.verify(redis).delete("user:1");
        order.verify(consumer).ack();
        verify(redis, never()).execute(any(RedisScript.class), anyList(), any(Object[].class));
    }

    @Test
    void emptyPollIsNotAcknowledged() {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of());

        assertThat(service.processBatch(consumer)).isFalse();

        verify(consumer, never()).ack();
        assertThat(service.getStatus().lastSuccessAt()).isNull();
    }

    @Test
    void failedAcknowledgmentDoesNotRecordSuccessfulBatch() {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(record(CanalEntry.EventType.CREATE)));
        doThrow(new IllegalStateException("commit failed")).when(consumer).ack();

        assertThatThrownBy(() -> service.processBatch(consumer)).isInstanceOf(IllegalStateException.class);
        assertThat(service.getStatus().lastSuccessAt()).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"INSERT", "UPDATE"})
    void flatMessageWithMqFileNameDeletesCacheThenAcknowledges(String eventType) {
        when(connector.isFlatMessageMode()).thenReturn(true);
        FlatMessage message = new FlatMessage();
        message.setType(eventType);
        message.setTable("users");
        message.setIsDdl(false);
        message.setData(List.of(Map.of("id", "1", "name", "new")));
        when(consumer.getFlatMessagesWithoutAck()).thenReturn(List.of(message));
        when(redis.delete("user:1")).thenReturn(true);

        assertThat(service.processBatch(consumer)).isTrue();

        var order = inOrder(redis, consumer);
        order.verify(redis).delete("user:1");
        order.verify(consumer).ack();
    }

    private KafkaCanalRecord record(CanalEntry.EventType type) {
        CanalEntry.Column id = CanalEntry.Column.newBuilder().setName("id").setValue("1").build();
        CanalEntry.RowChange change = CanalEntry.RowChange.newBuilder().setEventType(type)
                .addRowDatas(CanalEntry.RowData.newBuilder().addBeforeColumns(id).addAfterColumns(id)).build();
        CanalEntry.Entry entry = CanalEntry.Entry.newBuilder()
                .setHeader(CanalEntry.Header.newBuilder().setTableName("users")
                        .setLogfileName("mysql-bin.000001").setLogfileOffset(100L))
                .setEntryType(CanalEntry.EntryType.ROWDATA).setStoreValue(change.toByteString()).build();
        return new KafkaCanalRecord("canal-test", 0, 0L, new Message(1L, List.of(entry)));
    }
}
