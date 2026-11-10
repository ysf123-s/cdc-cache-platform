package com.example.canalcache.service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.example.canalcache.model.KafkaCanalRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 针对 Canal 1.1.8 Kafka connector 的项目适配。
 * 官方 rollback 会逐分区 seek 后 commit，FlatMessage 分支还误用了 kafkaConsumer。
 * 这里记录本批每个分区的起点，失败只 seek，全部成功仍由官方 ack 提交。
 */
class RetryableKafkaCanalConnector extends KafkaCanalConnector {
    private final Map<TopicPartition, Long> batchStartOffsets = new LinkedHashMap<>();

    RetryableKafkaCanalConnector(String servers, String topic, String groupId,
                                Integer batchSize, boolean flatMessage) {
        super(servers, topic, null, groupId, batchSize, flatMessage);
    }

    RetryableKafkaCanalConnector(String servers, String topic, String groupId,
                                Integer batchSize, boolean flatMessage, String offsetReset) {
        this(servers, topic, groupId, batchSize, flatMessage);
        properties.put("auto.offset.reset", offsetReset);
    }

    @Override
    public List<Message> getListWithoutAck(Long timeout, TimeUnit unit) {
        return getRecordsWithoutAck(timeout, unit).stream().map(KafkaCanalRecord::message).toList();
    }

    public List<KafkaCanalRecord> getRecordsWithoutAck(Long timeout, TimeUnit unit) {
        ConsumerRecords<String, Message> records = pollBatch(kafkaConsumer, timeout, unit);
        List<KafkaCanalRecord> messages = new ArrayList<>();
        for (ConsumerRecord<String, Message> record : records) {
            messages.add(new KafkaCanalRecord(record.topic(), record.partition(), record.offset(), record.value()));
        }
        return messages;
    }

    @Override
    public List<FlatMessage> getFlatListWithoutAck(Long timeout, TimeUnit unit) {
        ConsumerRecords<String, String> records = pollBatch(kafkaConsumer2, timeout, unit);
        List<FlatMessage> messages = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            messages.add(JSON.parseObject(record.value(), FlatMessage.class));
        }
        return messages;
    }

    private <T> ConsumerRecords<String, T> pollBatch(KafkaConsumer<String, T> consumer,
                                                    Long timeout, TimeUnit unit) {
        batchStartOffsets.clear();
        ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(unit.toMillis(timeout)));
        // 先记录整批位点，再解析消息。解析中途失败也能回到所有分区的批次起点。
        for (ConsumerRecord<String, T> record : records) {
            batchStartOffsets.putIfAbsent(new TopicPartition(record.topic(), record.partition()), record.offset());
        }
        return records;
    }

    @Override
    public void rollback() {
        KafkaConsumer<?, ?> consumer = flatMessage ? kafkaConsumer2 : kafkaConsumer;
        for (Map.Entry<TopicPartition, Long> entry : batchStartOffsets.entrySet()) {
            consumer.seek(entry.getKey(), entry.getValue());
        }
    }
}
