package com.example.canalcache.model;

import java.time.Instant;

/**
 * 当前进程的诊断快照。线程存活不等于 Redis/Kafka 可用，lastError 保留最近一次错误。
 */
public record SyncStatus(boolean serviceStarted, int configuredConsumers, int activeConsumers,
                         Instant lastSuccessAt, ErrorInfo lastError) {
    public record ErrorInfo(Instant occurredAt, String consumerThread, String type, String message) {
    }
}
