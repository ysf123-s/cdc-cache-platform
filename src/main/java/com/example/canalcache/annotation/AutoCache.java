package com.example.canalcache.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记需要自动同步缓存的实体类。
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface AutoCache {

    /**
     * 监听的数据库表名。
     */
    String table();

    /**
     * 固定命名空间后接 :#id，例如 user:#id；不同分组的命名空间不能重叠。
     */
    String keyTemplate();

    /**
     * 缓存同步策略。
     */
    CacheStrategy strategy() default CacheStrategy.HYBRID_MODE;

    /**
     * 缓存过期时间，单位秒。
     */
    int expireSeconds() default 1800;

    /**
     * 大 key 阈值，单位字节。超过后 HYBRID_MODE 会降级为删除。
     */
    int maxSizeBytes() default 10240;

    /**
     * 是否启用版本控制，降低旧事件覆盖新缓存的风险。
     */
    boolean enableVersionControl() default true;
}
