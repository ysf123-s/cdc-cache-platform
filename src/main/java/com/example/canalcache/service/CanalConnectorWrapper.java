package com.example.canalcache.service;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.example.canalcache.model.KafkaCanalRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * MQ 版本的 Canal Kafka 连接工厂。
 *
 * <p>当前实现按消费线程创建独立 connector：一个消费线程对应一个
 * KafkaCanalConnector，由 Kafka consumer group 分配 partition，避免多个线程共享 Kafka consumer。</p>
 */
@Slf4j
@Component
public class CanalConnectorWrapper {

    @Value("${canal.mq.kafka.servers}")
    private String kafkaServers;

    @Value("${canal.mq.kafka.topic:${canal.destination:example}}")
    private String topic;

    @Value("${canal.mq.kafka.group-id:canal-cache-sync-mq}")
    private String groupId;

    @Value("${canal.mq.kafka.partition-count:1}")
    private int partitionCount;

    @Value("${canal.mq.kafka.batch-size:100}")
    private Integer batchSize;

    @Value("${canal.mq.kafka.poll-timeout-ms:0}")
    private long pollTimeoutMs;

    @Value("${canal.mq.kafka.flat-message:false}")
    private boolean flatMessage;

    /**
     * 创建一个 Canal Kafka consumer，partition 固定传 null，使用 Kafka consumer group 自动分配分区。
     */
    public PartitionConsumer createPartitionConsumer() {
        RetryableKafkaCanalConnector connector = new RetryableKafkaCanalConnector(
                kafkaServers,
                topic,
                groupId,
                batchSize,
                flatMessage
        );
        connector.connect();
        connector.subscribe();
        log.info("已连接 Canal Kafka topic: {}, group: {}, partition: auto", topic, groupId);
        return new PartitionConsumer(connector, pollTimeoutMs);
    }

    public PartitionConsumer createArchiveConsumer(String archiveGroupId) {
        if (flatMessage) {
            throw new IllegalArgumentException("Platform archiving requires flat-message=false");
        }
        if (archiveGroupId == null || archiveGroupId.isBlank() || groupId.equals(archiveGroupId)) {
            throw new IllegalArgumentException("Archive group must be non-empty and different from the cache group");
        }
        RetryableKafkaCanalConnector connector = new RetryableKafkaCanalConnector(
                kafkaServers, topic, archiveGroupId, batchSize, false, "earliest");
        connector.connect();
        connector.subscribe();
        return new PartitionConsumer(connector, pollTimeoutMs);
    }

    /**
     * 需要启动的消费线程数。
     */
    public int getConsumerThreadCount() {
        return Math.max(1, partitionCount);
    }

    public boolean isFlatMessageMode() {
        return flatMessage;
    }

    public static class PartitionConsumer implements AutoCloseable {
        private final RetryableKafkaCanalConnector connector;
        private final long pollTimeoutMs;
        private volatile boolean connected = true;

        PartitionConsumer(RetryableKafkaCanalConnector connector, long pollTimeoutMs) {
            this.connector = connector;
            this.pollTimeoutMs = pollTimeoutMs;
        }

        public List<FlatMessage> getFlatMessagesWithoutAck() {
            ensureConnected();
            return connector.getFlatListWithoutAck(pollTimeoutMs, TimeUnit.MILLISECONDS);
        }

        public List<Message> getMessagesWithoutAck() {
            ensureConnected();
            return connector.getListWithoutAck(pollTimeoutMs, TimeUnit.MILLISECONDS);
        }

        public List<KafkaCanalRecord> getRecordsWithoutAck() {
            ensureConnected();
            return connector.getRecordsWithoutAck(pollTimeoutMs, TimeUnit.MILLISECONDS);
        }

        public void ack() {
            if (connected) {
                connector.ack();
            }
        }

        public void rollback() {
            if (connected) {
                connector.rollback();
            }
        }

        @Override
        public void close() {
            if (connected) {
                connector.disconnect();
                connected = false;
                log.info("已断开 Canal Kafka consumer");
            }
        }

        private void ensureConnected() {
            if (!connected) {
                throw new IllegalStateException("尚未连接 Canal Kafka consumer");
            }
        }
    }
}
