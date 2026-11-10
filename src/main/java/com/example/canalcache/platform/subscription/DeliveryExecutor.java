package com.example.canalcache.platform.subscription;

import com.example.canalcache.platform.PlatformProperties;
import com.example.canalcache.platform.history.ArchiveStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class DeliveryExecutor {
    private static final Logger log = LoggerFactory.getLogger(DeliveryExecutor.class);

    private final PlatformProperties properties;
    private final ArchiveStore store;
    private final HttpClient httpClient;
    private final ObjectMapper json;
    private final String instanceId;

    public DeliveryExecutor(PlatformProperties properties, ArchiveStore store,
                            HttpClient httpClient, ObjectMapper json) {
        this.properties = properties;
        this.store = store;
        this.httpClient = httpClient != null ? httpClient : HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        this.json = json;
        this.instanceId = "exec-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Poll and process one batch of claimed delivery tasks.
     * Returns the number of tasks processed.
     */
    @Scheduled(fixedDelayString = "${platform.dispatch-delay-ms:500}")
    public void dispatchPending() { dispatch(10); }

    public int dispatch(int batchSize) {
        int processed = 0;
        while (processed < batchSize && !Thread.currentThread().isInterrupted()) {
            List<DeliveryTask> tasks = store.claimTasks(instanceId, 1, properties.getLeaseSeconds());
            if (tasks.isEmpty()) break;
            executeTask(tasks.get(0));
            processed++;
        }
        return processed;
    }

    /**
     * Append one attempt to a failed task, retaining its delivery ID and attempt history.
     */
    public DeliveryTask manualRetry(long taskId) {
        DeliveryTask task = store.findDeliveryTask(taskId);
        if (task == null) {
            throw new NoSuchElementException("Delivery task not found: " + taskId);
        }
        if (!"FAILED".equalsIgnoreCase(task.status())) {
            throw new IllegalStateException("Task is in '" + task.status() + "' state; only FAILED tasks can be manually retried");
        }

        boolean reset = store.resetTaskForRetry(taskId);
        if (!reset) {
            throw new IllegalStateException("Failed to reset task; status may have changed concurrently");
        }
        return store.findDeliveryTask(taskId);
    }

    private void executeTask(DeliveryTask task) {
        int attemptNum = task.attemptCount() + 1;
        long startNanos = System.nanoTime();

        int timeoutMs = 5000;
        PlatformProperties.TargetConfig targetConfig = properties.getTargets().get(task.targetName());
        if (targetConfig != null && targetConfig.getTimeoutMs() > 0) {
            timeoutMs = targetConfig.getTimeoutMs();
        }

        String payload = buildHttpPayload(task, attemptNum);

        HttpResponse<String> response = null;
        Exception error = null;

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(task.targetUrl()))
                    .timeout(Duration.ofMillis(timeoutMs))
                    .header("Content-Type", "application/json; charset=UTF-8")
                    .header("X-Delivery-Id", task.deliveryId())
                    .header("X-Event-Id", task.eventId())
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();

            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return; // Keep the lease for restart recovery; no fabricated HTTP result.
        } catch (IOException | IllegalArgumentException e) {
            error = e;
        }

        long durationMs = (System.nanoTime() - startNanos) / 1_000_000L;
        handleExecutionResult(task, attemptNum, durationMs, response, error);
    }

    private void handleExecutionResult(DeliveryTask task, int attemptNum, long durationMs,
                                       HttpResponse<String> response, Exception error) {
        Integer statusCode = response != null ? response.statusCode() : null;
        String responseSummary = response != null && response.body() != null
                ? truncate(response.body().trim(), 500) : null;
        String errorSummary = error != null ? truncate(error.getClass().getSimpleName() + ": " + error.getMessage(), 500) : null;

        if (statusCode != null && statusCode >= 200 && statusCode < 300) {
            // Success (2xx)
            DeliveryAttempt attempt = new DeliveryAttempt(
                    0L, task.taskId(), attemptNum, "SUCCEEDED", statusCode, durationMs,
                    null, responseSummary, null, Instant.now().toString()
            );
            store.finishDelivery(task, attempt);
            return;
        }

        // Failure handling
        boolean isRetryable = false;
        String failureReason;

        if (error != null) {
            isRetryable = (error instanceof IOException || error instanceof HttpTimeoutException);
            failureReason = errorSummary;
        } else if (statusCode != null) {
            if (statusCode == 429 || statusCode >= 500) {
                isRetryable = true;
                failureReason = "HTTP " + statusCode + (responseSummary != null ? " - " + responseSummary : "");
            } else if (statusCode >= 300 && statusCode < 400) {
                isRetryable = false;
                failureReason = "HTTP " + statusCode + " Redirection not followed";
            } else {
                // Other 4xx
                isRetryable = false;
                failureReason = "HTTP " + statusCode + " Client Error" + (responseSummary != null ? " - " + responseSummary : "");
            }
        } else {
            failureReason = "Unknown error";
        }

        if (isRetryable && attemptNum < task.maxAttempts()) {
            // Schedule next retry with exponential backoff
            long backoffSeconds = Math.min(300, properties.getBackoffInitialSeconds() * (1L << Math.min(attemptNum - 1, 9)));
            Instant nextRetry = Instant.now().plusSeconds(backoffSeconds);

            DeliveryAttempt attempt = new DeliveryAttempt(
                    0L, task.taskId(), attemptNum, "RETRY_WAIT", statusCode, durationMs,
                    failureReason, responseSummary, nextRetry.toString(), Instant.now().toString()
            );
            store.finishDelivery(task, attempt);
        } else {
            // Final failure
            DeliveryAttempt attempt = new DeliveryAttempt(
                    0L, task.taskId(), attemptNum, "FAILED", statusCode, durationMs,
                    failureReason, responseSummary, null, Instant.now().toString()
            );
            store.finishDelivery(task, attempt);
        }
    }

    private String buildHttpPayload(DeliveryTask task, int attemptNum) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("deliveryId", task.deliveryId());
        map.put("eventId", task.eventId());
        map.put("subscriptionId", task.subscriptionId());
        map.put("targetName", task.targetName());
        map.put("attempt", attemptNum);
        map.put("timestamp", System.currentTimeMillis());

        try {
            map.put("event", json.readValue(task.eventPayload(), Object.class));
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Stored delivery payload is invalid", e);
        }

        try {
            return json.writeValueAsString(map);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Cannot encode delivery payload", e);
        }
    }

    private static String truncate(String text, int maxLength) {
        if (text == null) return null;
        return text.length() <= maxLength ? text : text.substring(0, maxLength);
    }
}
