package com.example.canalcache.loader;

import java.util.List;

/** 业务接入契约：实现数据库读取，Redis 处理由组件负责。T 是本分组实体类型。 */
public interface CacheDataLoader<T> {
    String table();
    Class<T> entityType();
    T loadById(Long id);
    List<T> loadByIds(List<Long> ids);
    Long idOf(T entity);

    /**
     * 按 @AutoCache.indexKeys 声明的列查主键列表，必须在调用时读取主库当前数据。
     * 只返回列值与 value 完全相同的行，避免数据库排序规则（如忽略大小写）让缓存 key 与内容不一致。
     */
    default List<Long> loadIdsByIndex(String column, String value) {
        throw new UnsupportedOperationException("No index loader for " + table() + "." + column);
    }
}
