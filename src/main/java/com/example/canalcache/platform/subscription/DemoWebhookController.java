package com.example.canalcache.platform.subscription;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 贯穿示例与故障演示专用的内置 Webhook 接收端。
 * 用于演示 HTTP 投递、接收端宕机/限流/超时重试、以及根据同一 deliveryId 的天然幂等去重。
 */
@RestController
@RequestMapping("/platform/demo/webhook")
public class DemoWebhookController {

    public record WebhookRecord(
            String deliveryId,
            String eventId,
            String receivedAt,
            int deliverCount,
            Map<String, Object> payload
    ) {}

    public record ChaosConfig(String mode) {}

    private final Map<String, WebhookRecord> deliveryRecords = new ConcurrentHashMap<>();
    private final List<WebhookRecord> history = new CopyOnWriteArrayList<>();
    private volatile String chaosMode = "NORMAL";
    private final AtomicInteger failCounter = new AtomicInteger(0);

    @PostMapping
    public ResponseEntity<Map<String, Object>> receiveWebhook(
            @RequestHeader(value = "X-Delivery-Id", required = false) String deliveryIdHeader,
            @RequestHeader(value = "X-Event-Id", required = false) String eventIdHeader,
            @RequestBody(required = false) Map<String, Object> body) {

        String deliveryId = deliveryIdHeader;
        String eventId = eventIdHeader;
        if (body != null) {
            if (deliveryId == null && body.get("deliveryId") != null) {
                deliveryId = String.valueOf(body.get("deliveryId"));
            }
            if (eventId == null && body.get("eventId") != null) {
                eventId = String.valueOf(body.get("eventId"));
            }
        }
        if (deliveryId == null) {
            deliveryId = "anonymous-" + UUID.randomUUID();
        }

        // 故障模拟
        switch (chaosMode.toUpperCase()) {
            case "500", "CHAOS_500" -> {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(Map.of("error", "Simulated 500 Internal Server Error", "deliveryId", deliveryId));
            }
            case "429", "CHAOS_429" -> {
                return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                        .body(Map.of("error", "Simulated 429 Too Many Requests", "deliveryId", deliveryId));
            }
            case "TIMEOUT", "CHAOS_TIMEOUT" -> {
                try {
                    Thread.sleep(8000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return ResponseEntity.ok(Map.of("status", "delayed", "deliveryId", deliveryId));
            }
            case "FAIL_ONCE" -> {
                if (failCounter.incrementAndGet() == 1) {
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                            .body(Map.of("error", "Simulated first-time failure", "deliveryId", deliveryId));
                }
            }
            default -> {}
        }

        // 幂等记录与统计
        WebhookRecord existing = deliveryRecords.get(deliveryId);
        int count = existing != null ? existing.deliverCount() + 1 : 1;
        WebhookRecord record = new WebhookRecord(
                deliveryId,
                eventId,
                Instant.now().toString(),
                count,
                body != null ? body : Collections.emptyMap()
        );

        deliveryRecords.put(deliveryId, record);
        history.add(record);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "RECEIVED");
        response.put("deliveryId", deliveryId);
        response.put("deliverCount", count);
        response.put("isDuplicate", count > 1);
        response.put("message", count > 1
                ? "Duplicate delivery recognized; idempotent acknowledgment returned"
                : "Event delivery accepted successfully");

        return ResponseEntity.ok(response);
    }

    @GetMapping("/records")
    public Map<String, Object> listRecords() {
        return Map.of(
                "totalReceived", history.size(),
                "distinctDeliveries", deliveryRecords.size(),
                "currentChaosMode", chaosMode,
                "history", history
        );
    }

    @PostMapping("/chaos")
    public Map<String, Object> setChaosMode(@RequestBody ChaosConfig config) {
        this.chaosMode = config.mode() != null ? config.mode() : "NORMAL";
        this.failCounter.set(0);
        return Map.of("chaosMode", this.chaosMode, "message", "Chaos mode updated successfully");
    }

    @DeleteMapping("/records")
    public Map<String, Object> reset() {
        this.deliveryRecords.clear();
        this.history.clear();
        this.chaosMode = "NORMAL";
        this.failCounter.set(0);
        return Map.of("status", "RESET", "message", "Records cleared and chaos reset to NORMAL");
    }
}
