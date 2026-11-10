package com.example.canalcache.platform.subscription;

public record DeliveryAttempt(
        @com.fasterxml.jackson.annotation.JsonFormat(shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING) long attemptId,
        @com.fasterxml.jackson.annotation.JsonFormat(shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING) long taskId,
        int attemptNumber,
        String status,
        Integer httpStatus,
        long durationMs,
        String errorMessage,
        String responseSummary,
        String nextRetryAt,
        String createdAt) {
}
