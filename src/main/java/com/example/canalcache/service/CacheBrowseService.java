package com.example.canalcache.service;

import com.example.canalcache.model.AutoCacheConfig;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 一次请求只执行一次 SCAN；COUNT 为扫描提示，不截断结果而丢失游标批次中的 key。 */
@Service
public class CacheBrowseService {
    private static final DefaultRedisScript<List> SCAN_PAGE = new DefaultRedisScript<>("""
            return redis.call('SCAN', ARGV[1], 'MATCH', ARGV[2], 'COUNT', ARGV[3], 'TYPE', 'hash')
            """, List.class);
    private final RedisTemplate<String, String> redis;
    public CacheBrowseService(RedisTemplate<String, String> redis) { this.redis = redis; }

    public KeyPage browse(String table, AutoCacheConfig config, String cursor, int count) {
        List<?> result = redis.execute(SCAN_PAGE, List.of(), cursor, config.keyPrefix() + "*", String.valueOf(count));
        String nextCursor = result.get(0).toString();
        Map<String, KeyView> entries = new LinkedHashMap<>();
        for (Object value : (List<?>) result.get(1)) {
            String key = value.toString();
            String suffix = key.substring(config.keyPrefix().length());
            try {
                long id = Long.parseLong(suffix);
                // 排除同前缀下不属于本组件主键布局的条目，如 user:stats、user:01。
                if (id > 0 && Long.toString(id).equals(suffix)) entries.put(key, new KeyView(suffix, key));
            } catch (NumberFormatException ignored) {
                // Redis 命名空间可能含其他元数据；它们不是可按 Long 主键维护的缓存。
            }
        }
        return new KeyPage(table, cursor, nextCursor, count, List.copyOf(entries.values()));
    }

    /** ID 用字符串传给浏览器，避免 JavaScript Number 截断 Long 主键。 */
    public record KeyView(String id, String key) { }
    public record KeyPage(String table, String cursor, String nextCursor, int count, List<KeyView> entries) { }
}
