package com.example.canalcache.model;

import com.alibaba.otter.canal.protocol.Message;

/** A Kafka record and its original location; never replace the offset with a binlog position. */
public record KafkaCanalRecord(String topic, int partition, long offset, Message message) {
}
