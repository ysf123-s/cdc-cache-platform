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
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
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
                mock(BigKeyDetector.class), redis, new ObjectMapper(), metrics);
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
    void malformedEntryIsDeadLetteredWhileOtherEntriesAreProcessed() {
        config.setEnableVersionControl(false);
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(
                recordOf(malformedEntry(), rowEntry(CanalEntry.EventType.INSERT, column("id", "1")))));
        when(redis.delete("user:1")).thenReturn(true);

        assertThat(service.processBatch(consumer)).isTrue();

        var order = inOrder(redis, consumer);
        order.verify(redis).delete("user:1");
        order.verify(redis).execute(any(RedisScript.class), eq(List.of(DeadLetterStore.STREAM_KEY)), any(Object[].class));
        order.verify(consumer).ack();
        assertThat(deadLettered("MALFORMED_ENTRY")).isEqualTo(1);
    }

    @Test
    void missingPrimaryKeyIsDeadLetteredAndLaterRowsStillInvalidate() {
        config.setEnableVersionControl(false);
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(recordOf(
                rowEntry(CanalEntry.EventType.UPDATE, column("name", "no id")),
                rowEntry(CanalEntry.EventType.UPDATE, column("id", "2")))));
        when(redis.delete("user:2")).thenReturn(true);

        assertThat(service.processBatch(consumer)).isTrue();

        verify(redis).delete("user:2");
        verify(consumer).ack();
        assertThat(deadLettered("MISSING_PRIMARY_KEY")).isEqualTo(1);
    }

    @Test
    void deadLetterWriteFailureRollsBackInsteadOfLosingTheEvent() {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(recordOf(malformedEntry())));
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenThrow(new RedisConnectionFailureException("Redis unavailable"));

        assertThatThrownBy(() -> service.processBatch(consumer)).isInstanceOf(RedisConnectionFailureException.class);

        verify(consumer, never()).ack();
        assertThat(deadLettered("MALFORMED_ENTRY")).isZero();
    }

    @Test
    void failedBatchWritesNoDeadLettersSoRetriesCannotDuplicateThem() {
        config.setEnableVersionControl(false);
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(
                recordOf(malformedEntry(), rowEntry(CanalEntry.EventType.INSERT, column("id", "1")))));
        when(redis.delete("user:1")).thenThrow(new RedisConnectionFailureException("Redis unavailable"));

        assertThatThrownBy(() -> service.processBatch(consumer)).isInstanceOf(RedisConnectionFailureException.class);

        verify(redis, never()).execute(any(RedisScript.class), anyList(), any(Object[].class));
        verify(consumer, never()).ack();
    }

    @ParameterizedTest
    @EnumSource(value = CanalEntry.EventType.class, names = {"TRUNCATE", "ERASE", "RENAME"})
    void tableLevelDdlPurgesRegisteredGroupBeforeAcknowledging(CanalEntry.EventType type) {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(recordOf(ddlEntry("users", type))));
        when(redis.execute(any(RedisScript.class), eq(List.of()), any(Object[].class)))
                .thenReturn(List.of("0", List.of("user:1", "user:2", "user:stats")));
        when(redis.unlink(List.of("user:1", "user:2"))).thenReturn(2L);

        assertThat(service.processBatch(consumer)).isTrue();

        var order = inOrder(redis, consumer);
        order.verify(redis).unlink(List.of("user:1", "user:2"));
        order.verify(consumer).ack();
        assertThat(metrics.get("canal.cache.purged").counter().count()).isEqualTo(2);
    }

    @Test
    void ddlThatKeepsRowsOrTargetsUnregisteredTableDoesNotTouchRedis() {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(recordOf(
                ddlEntry("users", CanalEntry.EventType.ALTER), ddlEntry("orders", CanalEntry.EventType.TRUNCATE))));

        assertThat(service.processBatch(consumer)).isTrue();

        verify(consumer).ack();
        verifyNoInteractions(redis);
    }

    @Test
    void retryDelayDoublesUpToThirtySeconds() {
        assertThat(List.of(1, 2, 3, 4, 5, 6, 50).stream().map(CanalCacheSyncService::retryDelayMs))
                .containsExactly(1_000L, 2_000L, 4_000L, 8_000L, 16_000L, 30_000L, 30_000L);
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
        return recordOf(rowEntry(type, column("id", "1")));
    }

    private double deadLettered(String reason) {
        var counter = metrics.find(DeadLetterStore.METRIC).tag("reason", reason).counter();
        return counter == null ? 0 : counter.count();
    }

    private static KafkaCanalRecord recordOf(CanalEntry.Entry... entries) {
        return new KafkaCanalRecord("example", 0, 1L, new Message(1L, List.of(entries)));
    }

    private static CanalEntry.Column column(String name, String value) {
        return CanalEntry.Column.newBuilder().setName(name).setValue(value).build();
    }

    private static CanalEntry.Entry rowEntry(CanalEntry.EventType type, CanalEntry.Column... columns) {
        CanalEntry.RowData.Builder row = CanalEntry.RowData.newBuilder();
        for (CanalEntry.Column column : columns) {
            row.addBeforeColumns(column).addAfterColumns(column);
        }
        return entry("users", CanalEntry.RowChange.newBuilder().setEventType(type).addRowDatas(row).build().toByteString());
    }

    private static CanalEntry.Entry ddlEntry(String table, CanalEntry.EventType type) {
        return entry(table, CanalEntry.RowChange.newBuilder().setEventType(type).setIsDdl(true)
                .setSql(type + " TABLE " + table).build().toByteString());
    }

    private static CanalEntry.Entry malformedEntry() {
        return entry("users", ByteString.copyFrom(new byte[]{10, 5, 1}));
    }

    private static CanalEntry.Entry entry(String table, ByteString storeValue) {
        return CanalEntry.Entry.newBuilder()
                .setHeader(CanalEntry.Header.newBuilder().setTableName(table)
                        .setLogfileName("mysql-bin.000001").setLogfileOffset(100L))
                .setEntryType(CanalEntry.EntryType.ROWDATA).setStoreValue(storeValue).build();
    }
}
