package com.example.canalcache.warehouse;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.model.BinlogEvent;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Profile("stockout-demo")
@RequestMapping("/warehouse")
@ConditionalOnProperty(prefix = "platform", name = "enabled", havingValue = "true")
public class StockoutController {
    private final StockoutStore store;
    private final ObjectMapper json;

    public StockoutController(StockoutStore store, ObjectMapper json) { this.store = store; this.json = json; }

    // 投递信封中的 deliveryId、attempt 等由平台负责；本业务以来源 eventId 去重。
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Notification(String eventId, BinlogEvent event) { }
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record InventoryRow(Long id, String sku, @JsonProperty("available_qty") Integer quantity) { }

    @PostMapping("/stockout-events")
    public StockoutStore.Task receive(@RequestBody Notification notification) {
        if (notification.eventId() == null || !notification.eventId().matches("[a-f0-9]{64}")) {
            throw new IllegalArgumentException("eventId must be a lowercase SHA-256 value");
        }
        BinlogEvent event = notification.event();
        if (event == null || !"inventory".equals(event.getTableName()) || event.getEventType() != EventType.UPDATE
                || event.getEventTimestamp() <= 0) {
            throw new IllegalArgumentException("Expected an inventory UPDATE event with its timestamp");
        }
        InventoryRow before = json.convertValue(event.getBeforeData(), InventoryRow.class);
        InventoryRow after = json.convertValue(event.getAfterData(), InventoryRow.class);
        if (before == null || before.quantity() == null || before.quantity() <= 0
                || after == null || after.quantity() == null || after.quantity() != 0
                || after.id() == null || after.id() <= 0 || after.sku() == null || after.sku().isBlank() || after.sku().length() > 64) {
            throw new IllegalArgumentException("Expected positive stock changing to zero, with an inventory ID and SKU");
        }
        return store.receive(notification.eventId(), after.id(), after.sku(), event.getEventTimestamp());
    }

    @GetMapping("/stockout-tasks")
    public StockoutStore.Page list(@RequestParam(required = false) String status,
            @RequestParam(required = false) String eventId, @RequestParam(required = false) Long cursor,
            @RequestParam(defaultValue = "20") int limit) {
        if (status != null && !status.equals("PENDING") && !status.equals("HANDLED")) {
            throw new IllegalArgumentException("status must be PENDING or HANDLED");
        }
        if (eventId != null && !eventId.matches("[a-f0-9]{64}")) throw new IllegalArgumentException("Invalid eventId");
        if (cursor != null && cursor <= 0) throw new IllegalArgumentException("cursor must be positive");
        if (limit < 1 || limit > 100) throw new IllegalArgumentException("limit must be between 1 and 100");
        return store.list(status, eventId, cursor, limit);
    }

    @PostMapping("/stockout-tasks/{id}/complete")
    public StockoutStore.Task complete(@PathVariable long id) {
        StockoutStore.Task task = store.complete(id);
        if (task == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stockout task not found");
        return task;
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public java.util.Map<String, String> invalidInput(IllegalArgumentException error) {
        return java.util.Map.of("error", error.getMessage());
    }
}
