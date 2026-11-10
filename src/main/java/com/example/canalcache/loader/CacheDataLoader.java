package com.example.canalcache.loader;

import java.util.List;

/** 业务接入契约：实现数据库读取，Redis 处理由组件负责。T 是本分组实体类型。 */
public interface CacheDataLoader<T> {
    String table();
    Class<T> entityType();
    T loadById(Long id);
    List<T> loadByIds(List<Long> ids);
    Long idOf(T entity);
}
