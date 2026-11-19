package com.example.canalcache.service;

import com.example.canalcache.loader.CacheDataLoader;
import com.example.canalcache.model.BatchQueryResult;
import org.springframework.stereotype.Service;
import java.util.List;

/** 统一查询入口：根据注册分组找到加载器，复用原有缓存、批量和回填保护。 */
@Service
public class CacheQueryService {
    private final CacheLoaderRegistry registry;
    private final CachedQueryService cached;
    public CacheQueryService(CacheLoaderRegistry registry, CachedQueryService cached) {
        this.registry = registry; this.cached = cached;
    }
    public <T> T getById(String table, Long id, Class<T> type) {
        CacheDataLoader<T> loader = registry.get(table, type);
        return cached.getById(table, id, type, () -> loader.loadById(id));
    }
    public <T> BatchQueryResult<T> getByIds(String table, List<Long> ids, Class<T> type) {
        CacheDataLoader<T> loader = registry.get(table, type);
        return cached.getByIds(table, ids, type, loader::loadByIds, loader::idOf);
    }
    /** 先查二级索引缓存得到主键列表，再走主键缓存取实体。调用方应再核对实体的列值，过滤掉刚被修改的行。 */
    public <T> List<T> getByIndex(String table, String column, String value, Class<T> type) {
        CacheDataLoader<T> loader = registry.get(table, type);
        List<Long> ids = cached.getIndexedIds(table, column, value, () -> loader.loadIdsByIndex(column, value));
        return ids.isEmpty() ? List.of() : cached.getByIds(table, ids, type, loader::loadByIds, loader::idOf).items();
    }
}
