package com.example.canalcache.model;

import com.example.canalcache.annotation.CacheStrategy;
import lombok.Data;

/**
 * 缓存配置信息
 */
@Data
public class AutoCacheConfig {

    private String table;
    private Class<?> entityClass;
    private String idField;
    private Class<?> idFieldType;
    private String keyTemplate;
    private CacheStrategy strategy;
    private int expireSeconds;
    private int maxSizeBytes;
    private boolean enableVersionControl;

    /**
     * 根据 keyTemplate 和 id 值生成缓存 Key，例如 "user:#id" + 42 → "user:42"
     */
    public String buildCacheKeySimple(Object idValue) {
        return keyTemplate.replace("#id", String.valueOf(idValue));
    }

    /** 配置注册时已校验：固定命名空间后只能是末尾的 #id。 */
    public String keyPrefix() {
        return keyTemplate.substring(0, keyTemplate.length() - "#id".length());
    }
}
