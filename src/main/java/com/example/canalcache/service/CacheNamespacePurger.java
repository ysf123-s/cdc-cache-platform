package com.example.canalcache.service;

import com.example.canalcache.model.AutoCacheConfig;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.function.Function;

/**
 * 清空一个缓存分组：用 SCAN 分批遍历命名空间，每批 UNLINK。
 *
 * <p>不使用 KEYS，避免长时间阻塞 Redis；主键命名空间只删除“前缀 + 规范 Long 主键”的 Hash，
 * 规则与管理页浏览一致，同前缀下的其他元数据不受影响；二级索引命名空间下的 Hash 全部删除。
 * 注册时已校验各命名空间互不重叠。</p>
 *
 * <p>SCAN 保证遍历期间一直存在的 key 都会被返回。清空前已开始回源的请求留下的 load_token
 * 会随 key 一起删除，其查到的旧数据无法回填。</p>
 */
@Component
public class CacheNamespacePurger {
    static final int SCAN_COUNT = 500;

    private final RedisTemplate<String, String> redis;
    private final CacheBrowseService browser;

    public CacheNamespacePurger(RedisTemplate<String, String> redis, CacheBrowseService browser) {
        this.redis = redis;
        this.browser = browser;
    }

    /** 返回实际删除的 key 数。可重复执行。 */
    public long purge(String table, AutoCacheConfig config) {
        long deleted = purgeAll(cursor -> {
            CacheBrowseService.KeyPage page = browser.browse(table, config, cursor, SCAN_COUNT);
            return new CacheBrowseService.ScanPage(page.nextCursor(),
                    page.entries().stream().map(CacheBrowseService.KeyView::key).toList());
        });
        for (AutoCacheConfig.IndexKey index : config.getIndexKeys()) {
            deleted += purgeAll(cursor -> browser.scan(index.prefix(), cursor, SCAN_COUNT));
        }
        return deleted;
    }

    private long purgeAll(Function<String, CacheBrowseService.ScanPage> scanner) {
        long deleted = 0;
        String cursor = "0";
        do {
            CacheBrowseService.ScanPage page = scanner.apply(cursor);
            if (!page.keys().isEmpty()) {
                Long removed = redis.unlink(page.keys());
                deleted += removed == null ? 0 : removed;
            }
            cursor = page.nextCursor();
        } while (!"0".equals(cursor));
        return deleted;
    }
}
