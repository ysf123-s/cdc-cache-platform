package com.example.canalcache.platform.history;

import com.example.canalcache.platform.PlatformProperties;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TableRegistry {
    private final PlatformProperties properties;
    private final ArchiveStore store;
    private final Map<String, TableSchema> tables;

    public TableRegistry(PlatformProperties properties, ArchiveStore store, List<TableSchema> schemas) {
        this.properties = properties;
        this.store = store;
        this.tables = new java.util.concurrent.ConcurrentHashMap<>();
        schemas.forEach(schema -> tables.put(schema.table(), schema));
    }

    public static TableRegistry capture(PlatformProperties properties, ArchiveStore store, DataSource business,
                                        DataSource platform) throws SQLException {
        try (Connection source = business.getConnection(); Connection target = platform.getConnection()) {
            if (!properties.getDatabase().equals(source.getCatalog())) {
                throw new IllegalArgumentException("platform.database must match the business datasource catalog");
            }
            if (properties.getDatabase().equalsIgnoreCase(target.getCatalog())) {
                throw new IllegalArgumentException("Business and platform storage must use different database catalogs");
            }
            List<TableSchema> schemas = new ArrayList<>();
            for (String table : properties.getTables()) {
                TableSchema schema = inspect(properties, store, source, table);
                store.saveSchema(schema);
                schemas.add(schema);
            }
            return new TableRegistry(properties, store, schemas);
        }
    }

    public static TableSchema inspect(PlatformProperties properties, ArchiveStore store, Connection source, String table) throws SQLException {
        DatabaseMetaData metadata = source.getMetaData();
        List<TableSchema.ColumnDefinition> columns = columns(metadata, source.getCatalog(), table);
        List<String> keys = new ArrayList<>();
        try (ResultSet rs = metadata.getPrimaryKeys(source.getCatalog(), null, table)) {
            while (rs.next()) keys.add(rs.getString("COLUMN_NAME"));
        }
        if (columns.isEmpty() || keys.size() != 1) throw new IllegalArgumentException("Registered table must exist and have one primary key: " + table);
        TableSchema.ColumnDefinition key = columns.stream().filter(c -> c.name().equals(keys.get(0))).findFirst().orElseThrow();
        if (!List.of(Types.BIGINT, Types.INTEGER, Types.SMALLINT, Types.TINYINT, Types.VARCHAR, Types.CHAR).contains(key.jdbcType())
                || key.size() > 255) throw new IllegalArgumentException("Unsupported primary key type/size for table " + table);
        List<String> restrictions = new ArrayList<>();
        try (ResultSet rs = metadata.getImportedKeys(source.getCatalog(), null, table)) {
            if (rs.next()) restrictions.add("Table has foreign keys");
        }
        try (ResultSet rs = metadata.getExportedKeys(source.getCatalog(), null, table)) {
            if (rs.next()) restrictions.add("Table is referenced by foreign keys");
        }
        if ("MySQL".equals(metadata.getDatabaseProductName())) {
            String currentUser;
            try (var statement = source.createStatement(); var rs = statement.executeQuery("SELECT CURRENT_USER()")) {
                rs.next(); currentUser = rs.getString(1);
            }
            int at = currentUser.lastIndexOf('@');
            String grantee = "'" + currentUser.substring(0, at).replace("'", "''") + "'@'" + currentUser.substring(at + 1).replace("'", "''") + "'";
            String privilegeSql = "SELECT (SELECT COUNT(*) FROM information_schema.USER_PRIVILEGES WHERE GRANTEE=? AND PRIVILEGE_TYPE='TRIGGER')"
                    + "+(SELECT COUNT(*) FROM information_schema.SCHEMA_PRIVILEGES WHERE GRANTEE=? AND TABLE_SCHEMA=? AND PRIVILEGE_TYPE='TRIGGER')"
                    + "+(SELECT COUNT(*) FROM information_schema.TABLE_PRIVILEGES WHERE GRANTEE=? AND TABLE_SCHEMA=? AND TABLE_NAME=? AND PRIVILEGE_TYPE='TRIGGER')";
            try (var statement = source.prepareStatement(privilegeSql)) {
                statement.setString(1, grantee); statement.setString(2, grantee); statement.setString(3, source.getCatalog());
                statement.setString(4, grantee); statement.setString(5, source.getCatalog()); statement.setString(6, table);
                try (var rs = statement.executeQuery()) {
                    rs.next(); if (rs.getLong(1) == 0) restrictions.add("Direct TRIGGER privilege required to verify trigger visibility");
                }
            }
            Map<String, String[]> text = new LinkedHashMap<>();
            try (var stmt = source.prepareStatement("SELECT COLUMN_NAME,CHARACTER_SET_NAME,COLLATION_NAME,GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?")) {
                stmt.setString(1, source.getCatalog()); stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) text.put(rs.getString(1), new String[]{rs.getString(2), rs.getString(3), rs.getString(4)});
                }
            }
            columns = columns.stream().map(c -> {
                String[] info = text.get(c.name());
                // Connector/J 会把 DEFAULT_GENERATED 时间戳也标为生成列；实际生成列有表达式。
                boolean generated = info[2] != null && !info[2].isBlank();
                return new TableSchema.ColumnDefinition(c.name(), c.ordinal(), c.jdbcType(), c.typeName(), c.size(), c.scale(),
                        c.nullable(), c.autoIncrement(), generated, info[0], info[1]);
            }).toList();
            try (var stmt = source.prepareStatement("SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA=? AND EVENT_OBJECT_TABLE=?")) {
                stmt.setString(1, source.getCatalog()); stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) { rs.next(); if (rs.getLong(1) > 0) restrictions.add("Table has triggers"); }
            }
        }
        String restriction = restrictions.isEmpty() ? null : String.join("; ", restrictions);
        String id = EventIdentity.hash(store.encode(List.of("schema-v2", properties.getSourceId(), properties.getDatabase(),
                table, keys.get(0), columns, restrictions)));
        return new TableSchema(id, properties.getSourceId(), properties.getDatabase(), table, keys.get(0), columns, restriction);
    }

    public void refresh(TableSchema schema) { tables.put(schema.table(), schema); }

    private static List<TableSchema.ColumnDefinition> columns(DatabaseMetaData metadata, String catalog,
                                                              String table) throws SQLException {
        List<TableSchema.ColumnDefinition> result = new ArrayList<>();
        String escape = metadata.getSearchStringEscape();
        String pattern = table.replace(escape, escape + escape).replace("_", escape + "_");
        try (ResultSet rs = metadata.getColumns(catalog, null, pattern, null)) {
            while (rs.next()) {
                if (!table.equals(rs.getString("TABLE_NAME"))) { continue; }
                result.add(new TableSchema.ColumnDefinition(rs.getString("COLUMN_NAME"), rs.getInt("ORDINAL_POSITION"),
                        rs.getInt("DATA_TYPE"), rs.getString("TYPE_NAME"), rs.getInt("COLUMN_SIZE"), rs.getInt("DECIMAL_DIGITS"),
                        rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls,
                        "YES".equals(rs.getString("IS_AUTOINCREMENT")), "YES".equals(rs.getString("IS_GENERATEDCOLUMN"))));
            }
        }
        result.sort(java.util.Comparator.comparingInt(TableSchema.ColumnDefinition::ordinal));
        return result;
    }

    public TableSchema get(String database, String table) {
        return properties.getDatabase().equals(database) ? tables.get(table) : null;
    }

    public List<TableView> views() {
        return properties.getTables().stream().map(tables::get).map(schema -> {
            ArchivedChange.Recovery recovery = store.recoveryState(schema, null);
            return new TableView(schema, store.invalidationReason(schema.snapshotId()), recovery.available(), recovery.reason());
        }).toList();
    }

    public record TableView(TableSchema schema, String invalidatedReason, boolean recoveryAvailable, String reason) { }
}
