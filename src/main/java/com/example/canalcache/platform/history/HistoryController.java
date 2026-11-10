package com.example.canalcache.platform.history;

import com.example.canalcache.platform.PlatformProperties;
import com.example.canalcache.platform.recovery.RecoveryPlan;
import com.example.canalcache.platform.recovery.RecoveryService;
import com.example.canalcache.service.CanalCacheSyncService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/platform")
@ConditionalOnProperty(prefix = "platform", name = "enabled", havingValue = "true")
public class HistoryController {
    private final PlatformProperties properties;
    private final TableRegistry tables;
    private final ArchiveStore store;
    private final ArchiveConsumer consumer;
    private final CanalCacheSyncService cache;
    private final RecoveryService recoveryService;

    @org.springframework.beans.factory.annotation.Autowired
    public HistoryController(PlatformProperties properties,
                             TableRegistry tables, ArchiveStore store, ArchiveConsumer consumer,
                             CanalCacheSyncService cache, RecoveryService recoveryService) {
        this.properties = properties;
        this.tables = tables;
        this.store = store;
        this.consumer = consumer;
        this.cache = cache;
        this.recoveryService = recoveryService;
    }

    public HistoryController(PlatformProperties properties,
                             TableRegistry tables, ArchiveStore store, ArchiveConsumer consumer,
                             CanalCacheSyncService cache) {
        this(properties, tables, store, consumer, cache, null);
    }

    @GetMapping("/tables")
    public List<TableRegistry.TableView> tables() { return tables.views(); }

    @PostMapping("/tables/{table}/recovery-baseline")
    public com.example.canalcache.platform.recovery.RecoveryBaseline confirmBaseline(@PathVariable String table) {
        return recoveryService.confirmBaseline(table);
    }

    @GetMapping("/events")
    public ArchivedChange.Page search(@RequestParam(required = false) String sourceId,
            @RequestParam(required = false) String table, @RequestParam(required = false) String key,
            @RequestParam(required = false) String operation, @RequestParam(required = false) String cursor,
            @RequestParam(required = false) Long from, @RequestParam(required = false) Long to,
            @RequestParam(defaultValue = "20") int limit) {
        if (sourceId != null && !properties.getSourceId().equals(sourceId)) { throw notFound("Unknown source"); }
        if (table != null && tables.get(properties.getDatabase(), table) == null) { throw notFound("Unknown table"); }
        if (key != null && (key.isEmpty() || key.length() > 255)) { throw badRequest("key must contain 1-255 characters"); }
        if (operation != null && !List.of("INSERT", "UPDATE", "DELETE").contains(operation)) {
            throw badRequest("operation must be INSERT, UPDATE or DELETE");
        }
        if (limit < 1 || limit > 100) { throw badRequest("limit must be between 1 and 100"); }
        if ((from != null && from < 0) || (to != null && to < 0) || (from != null && to != null && from > to)) {
            throw badRequest("Invalid event-time range");
        }
        Long before = null;
        if (cursor != null) {
            if (!cursor.matches("[1-9][0-9]{0,18}")) { throw badRequest("cursor must be a positive sequence number"); }
            try { before = Long.valueOf(cursor); }
            catch (NumberFormatException e) { throw badRequest("cursor exceeds signed BIGINT"); }
        }
        return store.search(properties, new ArchivedChange.Filter(before, table, key, operation, from, to, limit));
    }

    @GetMapping("/events/{eventId}")
    public ArchivedChange detail(@PathVariable String eventId) {
        if (!eventId.matches("[a-f0-9]{64}")) { throw badRequest("eventId must be a lowercase SHA-256 value"); }
        ArchivedChange event = store.find(eventId, properties);
        if (event == null) { throw notFound("Unknown event"); }
        return event;
    }

    @GetMapping("/consumers/status")
    public Map<String, Object> status() {
        // No DB, Kafka or Redis access: this endpoint remains useful when one dependency is unavailable.
        return Map.of("cache", cache.getStatus(), "archive", consumer.status());
    }

    @PostMapping("/recovery-plans")
    @ResponseStatus(HttpStatus.CREATED)
    public RecoveryPlan createPlan(@RequestBody Map<String, String> request) {
        if (recoveryService == null) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Recovery service is not available");
        }
        String eventId = request != null ? request.get("eventId") : null;
        if (eventId == null || !eventId.matches("[a-f0-9]{64}")) {
            throw badRequest("eventId must be a 64-character lowercase SHA-256 value");
        }
        try {
            return recoveryService.createPlan(eventId);
        } catch (java.util.NoSuchElementException e) {
            throw notFound(e.getMessage());
        } catch (IllegalArgumentException e) {
            throw badRequest(e.getMessage());
        }
    }

    @GetMapping("/recovery-plans/{planId}")
    public RecoveryPlan getPlan(@PathVariable String planId) {
        if (recoveryService == null) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Recovery service is not available");
        }
        if (!planId.matches("[a-f0-9]{64}")) {
            throw badRequest("planId must be a 64-character lowercase SHA-256 value");
        }
        RecoveryPlan plan = recoveryService.getPlan(planId);
        if (plan == null) {
            throw notFound("Recovery plan not found");
        }
        return plan;
    }

    @GetMapping("/recovery-plans/{planId}/sql")
    public ResponseEntity<String> downloadSql(@PathVariable String planId) {
        if (recoveryService == null) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Recovery service is not available");
        }
        if (!planId.matches("[a-f0-9]{64}")) {
            throw badRequest("planId must be a 64-character lowercase SHA-256 value");
        }
        RecoveryPlan plan = recoveryService.getPlan(planId);
        if (plan == null) {
            throw notFound("Recovery plan not found");
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(new MediaType("text", "plain", java.nio.charset.StandardCharsets.UTF_8));
        headers.set(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"recovery-" + planId + ".sql\"");

        return new ResponseEntity<>(plan.generatedSql(), headers, HttpStatus.OK);
    }

    private ResponseStatusException badRequest(String reason) { return new ResponseStatusException(HttpStatus.BAD_REQUEST, reason); }
    private ResponseStatusException notFound(String reason) { return new ResponseStatusException(HttpStatus.NOT_FOUND, reason); }
}
