package com.example.canalcache.service;

import com.example.canalcache.model.AutoCacheConfig;
import com.example.canalcache.model.BatchQueryResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 共享缓存查询：批量读取 Hash、只加载缺失 ID、复用同实例同 key 的在途加载。
 * 在途记录只保存尚未结束的加载，不构成本地数据缓存。
 */
@Service
public class CachedQueryService {
    public static final int NULL_CACHE_SECONDS = 30;
    public static final int LOAD_TOKEN_SECONDS = 30;
    private static final String BEGIN_LOAD_SCRIPT = """
            local data = redis.call('HGET', KEYS[1], 'data')
            if data then return data end
            redis.call('HSET', KEYS[1], 'load_token', ARGV[1])
            redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
            return nil
            """;
    private static final String FILL_SCRIPT = """
            if redis.call('HGET', KEYS[1], 'load_token') ~= ARGV[3]
                    or redis.call('HEXISTS', KEYS[1], 'data') == 1 then
                return 0
            end
            redis.call('HSET', KEYS[1], 'data', ARGV[1], 'version', '0')
            redis.call('HDEL', KEYS[1], 'load_token')
            redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
            return 1
            """;
    private static final String CLEANUP_SCRIPT = """
            if redis.call('HGET', KEYS[1], 'load_token') ~= ARGV[1] then return 0 end
            return redis.call('HDEL', KEYS[1], 'load_token')
            """;
    private static final DefaultRedisScript<String> BEGIN_LOAD =
            new DefaultRedisScript<>(BEGIN_LOAD_SCRIPT, String.class);
    private static final DefaultRedisScript<Long> FILL_IF_TOKEN_MATCHES =
            new DefaultRedisScript<>(FILL_SCRIPT, Long.class);
    private static final DefaultRedisScript<Long> CLEANUP_LOAD =
            new DefaultRedisScript<>(CLEANUP_SCRIPT, Long.class);

    private final RedisTemplate<String, String> redis;
    private final CacheAnnotationProcessor processor;
    private final ObjectMapper objectMapper;
    private final MeterRegistry registry;
    private final ConcurrentHashMap<String, CompletableFuture<String>> inFlight = new ConcurrentHashMap<>();

    public CachedQueryService(RedisTemplate<String, String> redis, CacheAnnotationProcessor processor,
                              ObjectMapper objectMapper, MeterRegistry registry) {
        this.redis = redis;
        this.processor = processor;
        this.objectMapper = objectMapper;
        this.registry = registry;
    }

    public <T> T getById(String table, Long id, Class<T> type, Supplier<T> loader) {
        BatchQueryResult<T> result = getByIds(table, List.of(id), type, ignored -> {
            T item = loader.get();
            return item == null ? List.of() : List.of(item);
        }, ignored -> id);
        return result.items().isEmpty() ? null : result.items().get(0);
    }

    public <T> BatchQueryResult<T> getByIds(String table, List<Long> ids, Class<T> type,
                                           Function<List<Long>, List<T>> loader,
                                           Function<T, Long> idExtractor) {
        List<Long> uniqueIds = ids.stream().distinct().toList();
        AutoCacheConfig config = processor.getConfig(table);
        List<Object> cached = read(config, uniqueIds);
        Map<Long, String> values = new LinkedHashMap<>();
        Map<Long, CompletableFuture<String>> owned = new LinkedHashMap<>();
        Map<Long, CompletableFuture<String>> pending = new LinkedHashMap<>();

        for (int i = 0; i < uniqueIds.size(); i++) {
            Long id = uniqueIds.get(i);
            Object value = cached.get(i);
            registry.counter("cache.query.items", "table", table,
                    "result", value == null ? "miss" : "hit").increment();
            if (value != null) {
                values.put(id, value.toString());
            } else {
                CompletableFuture<String> mine = new CompletableFuture<>();
                CompletableFuture<String> previous = inFlight.putIfAbsent(config.buildCacheKeySimple(id), mine);
                if (previous == null) {
                    owned.put(id, mine);
                    pending.put(id, mine);
                } else {
                    registry.counter("cache.query.coalesced", "table", table).increment();
                    pending.put(id, previous);
                }
            }
        }

        // 必须先完成自己负责的 ID，再等待其他加载者，避免重叠批次相互等待。
        if (!owned.isEmpty()) {
            loadOwned(table, config, owned, loader, idExtractor);
        }
        for (Map.Entry<Long, CompletableFuture<String>> entry : pending.entrySet()) {
            values.put(entry.getKey(), entry.getValue().join());
        }

        List<T> items = new ArrayList<>();
        List<Long> missingIds = new ArrayList<>();
        for (Long id : uniqueIds) {
            T item = deserialize(values.get(id), type, config.buildCacheKeySimple(id));
            if (item == null) {
                missingIds.add(id);
            } else {
                items.add(item);
            }
        }
        return new BatchQueryResult<>(items, missingIds);
    }

    private <T> void loadOwned(String table, AutoCacheConfig config,
                               Map<Long, CompletableFuture<String>> owned,
                               Function<List<Long>, List<T>> loader, Function<T, Long> idExtractor) {
        Map<Long, String> tokens = new LinkedHashMap<>();
        owned.keySet().forEach(id -> tokens.put(id, UUID.randomUUID().toString()));
        try {
            List<Long> ids = new ArrayList<>(owned.keySet());
            // 原子复查并登记本次加载。失效 DEL 会同时清除 data 和 load_token。
            List<Object> rechecked = beginLoad(config, tokens);
            Map<Long, String> results = new LinkedHashMap<>();
            List<Long> missing = new ArrayList<>();
            for (int i = 0; i < ids.size(); i++) {
                if (rechecked.get(i) == null) {
                    missing.add(ids.get(i));
                } else {
                    results.put(ids.get(i), rechecked.get(i).toString());
                }
            }
            if (!missing.isEmpty()) {
                registry.counter("cache.query.database.calls", "table", table).increment();
                registry.counter("cache.query.database.ids", "table", table).increment(missing.size());
                Map<Long, T> loaded = new LinkedHashMap<>();
                for (T item : loader.apply(missing)) {
                    loaded.put(idExtractor.apply(item), item);
                }
                Map<Long, String> fills = new LinkedHashMap<>();
                for (Long id : missing) {
                    fills.put(id, serialize(loaded.get(id), config.buildCacheKeySimple(id)));
                }
                fill(table, config, fills, tokens);
                results.putAll(fills);
            }
            owned.forEach((id, future) -> future.complete(results.get(id)));
        } catch (RuntimeException | Error e) {
            try {
                cleanup(config, tokens);
            } catch (RuntimeException | Error cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            // 等待者也要结束并收到失败；不能把数据库/Redis 故障缓存为不存在。
            owned.values().forEach(future -> future.completeExceptionally(e));
            throw e;
        } finally {
            owned.forEach((id, future) -> inFlight.remove(config.buildCacheKeySimple(id), future));
        }
    }

    private List<Object> read(AutoCacheConfig config, List<Long> ids) {
        if (ids.size() == 1) {
            return Collections.singletonList(redis.opsForHash().get(config.buildCacheKeySimple(ids.get(0)), "data"));
        }
        return redis.executePipelined((RedisCallback<Object>) connection -> {
            for (Long id : ids) {
                connection.hashCommands().hGet(bytes(config.buildCacheKeySimple(id)), bytes("data"));
            }
            return null;
        }, redis.getStringSerializer());
    }

    private List<Object> beginLoad(AutoCacheConfig config, Map<Long, String> tokens) {
        if (tokens.size() == 1) {
            Long id = tokens.keySet().iterator().next();
            return Collections.singletonList(redis.execute(BEGIN_LOAD,
                    List.of(config.buildCacheKeySimple(id)), tokens.get(id), String.valueOf(LOAD_TOKEN_SECONDS)));
        }
        return redis.executePipelined((RedisCallback<Object>) connection -> {
            tokens.forEach((id, token) -> connection.scriptingCommands().eval(bytes(BEGIN_LOAD_SCRIPT),
                    ReturnType.VALUE, 1, bytes(config.buildCacheKeySimple(id)),
                    bytes(token), bytes(String.valueOf(LOAD_TOKEN_SECONDS))));
            return null;
        }, redis.getStringSerializer());
    }

    private void fill(String table, AutoCacheConfig config, Map<Long, String> values, Map<Long, String> tokens) {
        List<Object> outcomes;
        if (values.size() == 1) {
            Long id = values.keySet().iterator().next();
            String json = values.get(id);
            outcomes = Collections.singletonList(redis.execute(FILL_IF_TOKEN_MATCHES,
                    List.of(config.buildCacheKeySimple(id)), json, String.valueOf(ttl(config, json)), tokens.get(id)));
        } else {
            outcomes = redis.executePipelined((RedisCallback<Object>) connection -> {
                values.forEach((id, json) -> connection.scriptingCommands().eval(bytes(FILL_SCRIPT),
                        ReturnType.INTEGER, 1, bytes(config.buildCacheKeySimple(id)),
                        bytes(json), bytes(String.valueOf(ttl(config, json))), bytes(tokens.get(id))));
                return null;
            });
        }
        for (Object outcome : outcomes) {
            registry.counter("cache.query.fills", "table", table,
                    "result", Long.valueOf(1).equals(outcome) ? "written" : "rejected").increment();
        }
    }

    private void cleanup(AutoCacheConfig config, Map<Long, String> tokens) {
        if (tokens.size() == 1) {
            Long id = tokens.keySet().iterator().next();
            redis.execute(CLEANUP_LOAD, List.of(config.buildCacheKeySimple(id)), tokens.get(id));
        } else {
            redis.executePipelined((RedisCallback<Object>) connection -> {
                tokens.forEach((id, token) -> connection.scriptingCommands().eval(bytes(CLEANUP_SCRIPT),
                        ReturnType.INTEGER, 1, bytes(config.buildCacheKeySimple(id)), bytes(token)));
                return null;
            });
        }
    }

    private int ttl(AutoCacheConfig config, String json) {
        return "null".equals(json) ? NULL_CACHE_SECONDS : config.getExpireSeconds();
    }

    private String serialize(Object value, String key) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize cache data for " + key, e);
        }
    }

    private <T> T deserialize(String json, Class<T> type, String key) {
        try {
            return objectMapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Invalid cache data for " + key, e);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
