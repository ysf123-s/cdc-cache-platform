package com.example.canalcache.platform.subscription;

public record DeliveryTask(
        @com.fasterxml.jackson.annotation.JsonFormat(shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING) long taskId,
        String deliveryId,
        String eventId,
        String subscriptionId,
        String roundType,
        String roundId,
        String targetName,
        String targetUrl,
        String status,
        int attemptCount,
        int maxAttempts,
        String leaseUntil,
        String leaseOwner,
        String nextRetryAt,
        String lastError,
        String eventPayload,
        String subscriptionSnapshot,
        String createdAt,
        String updatedAt) {
    public record Page(java.util.List<DeliveryTask> items, String nextCursor) { }
}
