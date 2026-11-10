package com.example.canalcache.annotation;

/**
 * 缓存同步策略。
 */
public enum CacheStrategy {

    /**
     * 数据变更后直接删除缓存，读请求下次回源重建。
     */
    DELETE_MODE,

    /**
     * 数据变更后直接刷新缓存内容。
     */
    REFRESH_MODE,

    /**
     * 混合策略：大 key 删除；高频更新 key 更新；低频更新 key 删除。
     */
    HYBRID_MODE
}
