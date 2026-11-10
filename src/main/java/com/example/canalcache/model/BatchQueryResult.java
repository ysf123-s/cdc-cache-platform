package com.example.canalcache.model;

import java.util.List;

/** 按请求中 ID 的首次出现顺序返回；不存在的 ID 单独列出，重复 ID 合并。 */
public record BatchQueryResult<T>(List<T> items, List<Long> missingIds) {
}
