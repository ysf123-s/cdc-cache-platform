package com.example.canalcache.controller;

import com.example.canalcache.dto.IdBatchRequest;
import com.example.canalcache.model.AutoCacheConfig;
import com.example.canalcache.model.SyncStatus;
import com.example.canalcache.service.CacheAnnotationProcessor;
import com.example.canalcache.service.CacheBrowseService;
import com.example.canalcache.service.CachedQueryService;
import com.example.canalcache.service.CanalCacheSyncService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/** 本地演示管理接口：操作对象限定为已注册的表和 ID，不接收任意 Redis key。 */
@RestController
@RequestMapping("/cache")
public class CacheManagementController {
    private final CacheAnnotationProcessor processor;
    private final RedisTemplate<String, String> redis;
    private final MeterRegistry registry;
    private final CanalCacheSyncService syncService;
    private final CacheBrowseService browser;

    public CacheManagementController(CacheAnnotationProcessor processor, RedisTemplate<String, String> redis,
                                     MeterRegistry registry, CanalCacheSyncService syncService,
                                     CacheBrowseService browser) {
        this.processor = processor;
        this.redis = redis;
        this.registry = registry;
        this.syncService = syncService;
        this.browser = browser;
    }

    @GetMapping("/groups/{table}/entries")
    public CacheBrowseService.KeyPage browse(@PathVariable String table,
                                             @RequestParam(required = false) String cursor,
                                             @RequestParam(required = false) String count) {
        AutoCacheConfig config = requireConfig(table);
        String start = cursor == null ? "0" : cursor;
        if (!start.matches("0|[1-9][0-9]{0,19}") || new BigInteger(start).bitLength() > 64) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "cursor must be an unsigned 64-bit integer");
        }
        String requestedCount = count == null ? "20" : count;
        if (!requestedCount.matches("[1-9][0-9]{0,2}") || Integer.parseInt(requestedCount) > 100) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "count must be between 1 and 100");
        }
        return browser.browse(table, config, start, Integer.parseInt(requestedCount));
    }

    @GetMapping("/overview")
    public Overview overview() {
        List<GroupView> groups = processor.getAllTables().stream().sorted().map(table -> {
            AutoCacheConfig config = processor.getConfig(table);
            long hits = count("cache.query.items", "table", table, "result", "hit");
            long misses = count("cache.query.items", "table", table, "result", "miss");
            return new GroupView(table, config.getKeyTemplate(), config.getStrategy().name(),
                    config.getExpireSeconds(), CachedQueryService.NULL_CACHE_SECONDS,
                    hits, misses, hits + misses == 0 ? null : (double) hits / (hits + misses),
                    count("cache.query.database.calls", "table", table),
                    count("cache.query.database.ids", "table", table),
                    count("cache.query.coalesced", "table", table));
        }).toList();
        return new Overview(Instant.now(), syncService.getStatus(), groups,
                count("canal.events.processed"), count("canal.events.failed"), count("canal.cache.deleted"));
    }

    @GetMapping("/entries/{table}/{id}")
    public EntryView entry(@PathVariable String table, @PathVariable Long id) {
        AutoCacheConfig config = requireConfig(table);
        if (id <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "id must be positive");
        }
        String key = config.buildCacheKeySimple(id);
        Map<Object, Object> hash = redis.opsForHash().entries(key);
        Long ttl = redis.getExpire(key);
        return new EntryView(table, id.toString(), key, hash, ttl);
    }

    @PostMapping("/entries/{table}/invalidate")
    public InvalidationResult invalidate(@PathVariable String table, @RequestBody IdBatchRequest request) {
        AutoCacheConfig config = requireConfig(table);
        request.validate();
        List<String> keys = request.ids().stream().distinct().map(config::buildCacheKeySimple).toList();
        long deleted = redis.delete(keys);
        registry.counter("cache.management.invalidated", "table", table).increment(deleted);
        return new InvalidationResult(keys.size(), deleted);
    }

    private AutoCacheConfig requireConfig(String table) {
        AutoCacheConfig config = processor.getConfig(table);
        if (config == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Cache table is not registered");
        }
        return config;
    }

    private long count(String name, String... tags) {
        Counter counter = registry.find(name).tags(tags).counter();
        return counter == null ? 0 : (long) counter.count();
    }

    public record Overview(Instant capturedAt, SyncStatus sync, List<GroupView> groups,
                           long processedEvents, long failedBatches, long deletedKeys) {
    }

    public record GroupView(String table, String keyTemplate, String strategy, int expireSeconds,
                            int nullExpireSeconds, long hits, long misses, Double hitRate,
                            long databaseCalls, long databaseIds, long coalescedLoads) {
    }

    public record EntryView(String table, String id, String key, Map<Object, Object> hash, Long ttlSeconds) {
    }

    public record InvalidationResult(int requestedKeys, long deletedKeys) {
    }
}
