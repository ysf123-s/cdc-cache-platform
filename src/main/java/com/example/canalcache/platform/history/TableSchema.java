package com.example.canalcache.platform.history;

import com.fasterxml.jackson.annotation.JsonInclude;
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
                                   boolean autoIncrement, boolean generated, String characterSet, String collation,
                                   @JsonInclude(JsonInclude.Include.NON_DEFAULT) boolean foreignKey) {
        public ColumnDefinition(String name, int ordinal, int jdbcType, String typeName, int size, int scale,
                                boolean nullable, boolean autoIncrement, boolean generated, String characterSet, String collation) {
            this(name, ordinal, jdbcType, typeName, size, scale, nullable, autoIncrement, generated, characterSet, collation, false);
        }
        public ColumnDefinition(String name, int ordinal, int jdbcType, String typeName, int size, int scale,
                                boolean nullable, boolean autoIncrement, boolean generated) {
            this(name, ordinal, jdbcType, typeName, size, scale, nullable, autoIncrement, generated, null, null);
        }
    }
}
