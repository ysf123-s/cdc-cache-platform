package com.example.canalcache.platform.replay;

import java.util.List;

public record ReplayJob(
        String jobId,
        String requestKey,
        String sourceId,
        String database,
        String table,
        String subscriptionId,
        String status,
        int totalEvents,
        int matchedEvents,
        int skippedEvents,
        int failedEvents,
        List<String> eventIds,
        String subscriptionSnapshot,
        String targetUrl,
        String lastError,
        String createdAt,
        String updatedAt) {

    public record Request(
            String requestKey,
            String subscriptionId,
            List<String> eventIds) {
    }
}
