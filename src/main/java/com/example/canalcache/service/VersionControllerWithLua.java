package com.example.canalcache.service;

import com.example.canalcache.model.BinlogVersion;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Collections;

/**
 * 基于 Redis Lua 脚本的版本控制器。
 *
 * <p>缓存数据和版本号存放在同一个 Redis Hash 中：
 * <pre>
 *   HSET user:42  data     "{...json...}"
 *                 version  4294967296000
 * </pre>
 * 读取缓存时只需要：{@code HGET user:42 data}。
 * </p>
 */
@Slf4j
@Component
public class VersionControllerWithLua {

    private final RedisTemplate<String, String> redisTemplate;

    /**
     * 更新脚本。
     *
     * 脚本比较的是一个打包后的 {@code encodedVersion} 单值，
     * 不是在 Lua 内部分别比较 epoch/fileNo/offset。
     *
     * KEYS[1]=cacheKey
     * 版本：ARGV[1]=encodedVersion, 数据：ARGV[2]=data, 过期时间：ARGV[3]=expireSeconds
     *
     * <p>如果 key 不存在，则直接跳过写入，避免把冷数据重新写回缓存。</p>
     */
    private static final String UPDATE_WITH_VERSION_LUA = """
        local currentVersion = tonumber(redis.call('HGET', KEYS[1], 'version') or '0')
        local newVersion = tonumber(ARGV[1])
        if newVersion > currentVersion then
            redis.call('HDEL', KEYS[1], 'load_token')
            if redis.call('HEXISTS', KEYS[1], 'data') == 0 then
                return 0
            end
            redis.call('HSET', KEYS[1],
                'data', ARGV[2],
                'version', ARGV[1])
            redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
            return 1
        else
            return 0
        end
        """;

    /**
     * 无版本控制更新脚本。
     *
     * <p>仅当 key 已存在时才刷新 data 和 TTL，不把冷数据写回缓存。</p>
     *
     * <p>KEYS[1]=cacheKey
     * ARGV[1]=data, ARGV[2]=expireSeconds</p>
     */
    private static final String UPDATE_IF_PRESENT_LUA = """
        redis.call('HDEL', KEYS[1], 'load_token')
        if redis.call('HEXISTS', KEYS[1], 'data') == 0 then
            return 0
        end
        redis.call('HSET', KEYS[1],
            'data', ARGV[1],
            'version', '0')
        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
        return 1
        """;

    /**
     * 删除脚本。
     *
     * 只有当新的打包版本 {@code encodedVersion} 大于当前已存版本时，
     * 才会删除缓存 key。
     *
     * KEYS[1]=cacheKey
     * ARGV[1]=encodedVersion
     *
     * 规定：KEYS[] 存放要操作的Redis key
     *      ARGV[] 存放普通参数
     */
    private static final String DELETE_WITH_VERSION_LUA = """
        local currentVersion = tonumber(redis.call('HGET', KEYS[1], 'version') or '0')
        local newVersion = tonumber(ARGV[1])
        if newVersion <= currentVersion then
            return 0
        end
        return redis.call('DEL', KEYS[1])
        """;

    private final DefaultRedisScript<Long> updateWithVersionScript;
    private final DefaultRedisScript<Long> updateIfPresentScript;
    private final DefaultRedisScript<Long> deleteWithVersionScript;

    // 构造器
    public VersionControllerWithLua(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.updateWithVersionScript = new DefaultRedisScript<>(UPDATE_WITH_VERSION_LUA, Long.class);
        this.updateIfPresentScript = new DefaultRedisScript<>(UPDATE_IF_PRESENT_LUA, Long.class);
        this.deleteWithVersionScript = new DefaultRedisScript<>(DELETE_WITH_VERSION_LUA, Long.class);
    }

    /**
     * 带版本控制的缓存删除。
     */
    public boolean deleteIfNewerVersion(String cacheKey, BinlogVersion newVersion) {
        // 执行 Lua 脚本：KEYS 存 Redis key，ARGV 存普通参数。
        Long result = redisTemplate.execute(
                deleteWithVersionScript,
                Collections.singletonList(cacheKey),
                String.valueOf(newVersion.encodedVersion())
        );
        // 用 equals 比较 Long，避免自动拆箱。
        boolean success = Long.valueOf(1L).equals(result);
        if (!success) {
            log.debug("Skipped delete for {} due to older version {}", cacheKey, newVersion);
        }
        return success;
    }

    /**
     * 带版本控制的缓存更新。
     */
    public boolean updateIfNewerVersion(String cacheKey, String newData, BinlogVersion newVersion, int expireSeconds) {
        Long result = redisTemplate.execute(
                updateWithVersionScript,
                Collections.singletonList(cacheKey),
                String.valueOf(newVersion.encodedVersion()),
                newData,
                String.valueOf(expireSeconds)
        );
        boolean success = Long.valueOf(1L).equals(result);
        if (!success) {
            log.debug("Skipped update for {} due to older version {}", cacheKey, newVersion);
        }
        return success;
    }

    /**
     * 无版本控制但要求 key 已存在的缓存刷新。
     */
    public boolean updateIfPresent(String cacheKey, String newData, int expireSeconds) {
        Long result = redisTemplate.execute(
                updateIfPresentScript,
                Collections.singletonList(cacheKey),
                newData,
                String.valueOf(expireSeconds)
        );
        boolean success = Long.valueOf(1L).equals(result);
        if (!success) {
            log.debug("Skipped update for {} because cache key does not exist", cacheKey);
        }
        return success;
    }

    /**
     * 从 Hash 字段 {@code version} 读取当前版本号。
     */
    public long getCurrentVersion(String cacheKey) {
        try {
            Object version = redisTemplate.opsForHash().get(cacheKey, "version");
            return version != null ? Long.parseLong(version.toString()) : 0;
        } catch (Exception e) {
            log.warn("Failed to get current version for {}", cacheKey, e);
            return 0;
        }
    }

    /**
     * 强制写入版本号，主要用于测试或恢复场景。
     */
    public void forceSetVersion(String cacheKey, long version) {
        redisTemplate.opsForHash().put(cacheKey, "version", String.valueOf(version));
    }

    /**
     * 不做版本比较，直接删除缓存。
     */
    public void deleteCache(String cacheKey) {
        redisTemplate.delete(cacheKey);
    }
}
