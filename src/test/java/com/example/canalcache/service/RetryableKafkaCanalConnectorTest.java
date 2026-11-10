package com.example.canalcache.service;

import com.alibaba.otter.canal.protocol.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RetryableKafkaCanalConnectorTest {
    @Test
    void archiveReadPreservesKafkaLocationAndOverridesOnlyItsResetPolicy() {
        RetryableKafkaCanalConnector connector = new RetryableKafkaCanalConnector("localhost:9092", "example", "archive", 100, false, "earliest");
        KafkaConsumer<String, Message> kafka = mock(KafkaConsumer.class);
        ReflectionTestUtils.setField(connector, "kafkaConsumer", kafka);
        TopicPartition partition = new TopicPartition("example", 2);
        Message message = new Message(1L, List.of());
        when(kafka.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(Map.of(partition,
                List.of(new ConsumerRecord<>("example", 2, 123L, "key", message)))));
        var record = connector.getRecordsWithoutAck(50L, TimeUnit.MILLISECONDS).get(0);
        assertThat(record.topic()).isEqualTo("example");
        assertThat(record.partition()).isEqualTo(2);
        assertThat(record.offset()).isEqualTo(123L);
        assertThat(record.message()).isSameAs(message);
        var properties = (java.util.Properties) ReflectionTestUtils.getField(connector, "properties");
        assertThat(properties.getProperty("auto.offset.reset")).isEqualTo("earliest");
        var original = new RetryableKafkaCanalConnector("localhost:9092", "example", "cache", 100, false);
        var originalProperties = (java.util.Properties) ReflectionTestUtils.getField(original, "properties");
        assertThat(originalProperties.getProperty("auto.offset.reset")).isEqualTo("latest");
        connector.rollback();
        verify(kafka).seek(partition, 123L);
        verify(kafka, never()).commitSync();
    }

    @Test
    void entryRollbackRewindsEveryPartitionWithoutCommitting() {
        RetryableKafkaCanalConnector connector = new RetryableKafkaCanalConnector("localhost:9092", "example", "test", 100, false);
        KafkaConsumer<String, Message> kafka = mock(KafkaConsumer.class);
        ReflectionTestUtils.setField(connector, "kafkaConsumer", kafka);
        TopicPartition first = new TopicPartition("example", 0);
        TopicPartition second = new TopicPartition("example", 1);
        when(kafka.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(Map.of(
                first, List.of(new ConsumerRecord<>("example", 0, 10L, "1", new Message(1L, List.of()))),
                second, List.of(new ConsumerRecord<>("example", 1, 20L, "2", new Message(2L, List.of()))))));

        connector.getListWithoutAck(50L, TimeUnit.MILLISECONDS);
        connector.rollback();

        verify(kafka).seek(first, 10L);
        verify(kafka).seek(second, 20L);
        verify(kafka, never()).commitSync();
    }

    @Test
    void malformedFlatMessageCanRewindAllPartitionsWithoutNullConsumer() {
        RetryableKafkaCanalConnector connector = new RetryableKafkaCanalConnector("localhost:9092", "example", "test", 100, true);
        KafkaConsumer<String, String> kafka = mock(KafkaConsumer.class);
        ReflectionTestUtils.setField(connector, "kafkaConsumer2", kafka);
        TopicPartition first = new TopicPartition("example", 0);
        TopicPartition second = new TopicPartition("example", 1);
        when(kafka.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(Map.of(
                first, List.of(new ConsumerRecord<>("example", 0, 10L, "1", "broken-json")),
                second, List.of(new ConsumerRecord<>("example", 1, 20L, "2", "{}")))));

        assertThatThrownBy(() -> connector.getFlatListWithoutAck(50L, TimeUnit.MILLISECONDS))
                .isInstanceOf(RuntimeException.class);
        connector.rollback();

        verify(kafka).seek(first, 10L);
        verify(kafka).seek(second, 20L);
        verify(kafka, never()).commitSync();
    }
}
