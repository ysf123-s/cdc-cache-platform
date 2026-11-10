package com.example.canalcache.model;

/** Canal column metadata. Values remain strings so BIGINT and DECIMAL retain their precision. */
public record BinlogColumn(int index, String name, int jdbcType, String mysqlType,
                           boolean primaryKey, boolean nullValue, boolean updated, String value) {
}
