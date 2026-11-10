package com.example.canalcache.platform.history;

import java.util.List;

/** A snapshot of current JDBC metadata, not proof of the schema of older Kafka messages. */
public record TableSchema(String snapshotId, String sourceId, String database, String table,
                          String primaryKey, List<ColumnDefinition> columns, String recoveryRestriction) {
    public TableSchema(String snapshotId, String sourceId, String database, String table,
                       String primaryKey, List<ColumnDefinition> columns) {
        this(snapshotId, sourceId, database, table, primaryKey, columns, null);
    }
    public record ColumnDefinition(String name, int ordinal, int jdbcType, String typeName,
                                   int size, int scale, boolean nullable,
                                   boolean autoIncrement, boolean generated, String characterSet, String collation) {
        public ColumnDefinition(String name, int ordinal, int jdbcType, String typeName, int size, int scale,
                                boolean nullable, boolean autoIncrement, boolean generated) {
            this(name, ordinal, jdbcType, typeName, size, scale, nullable, autoIncrement, generated, null, null);
        }
    }
}
