package com.example.canalcache.platform.recovery;

import com.example.canalcache.platform.PlatformProperties;
import com.example.canalcache.platform.history.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

public class RecoveryService {
    private final PlatformProperties properties;
    private final TableRegistry tables;
    private final ArchiveStore store;
    private final JdbcTemplate businessJdbc;

    public RecoveryService(PlatformProperties properties, TableRegistry tables, ArchiveStore store,
                           JdbcTemplate businessJdbc, ObjectMapper json) {
        this.properties = properties; this.tables = tables; this.store = store; this.businessJdbc = businessJdbc;
    }

    public RecoveryBaseline confirmBaseline(String table) {
        if (tables.get(properties.getDatabase(), table) == null) throw new NoSuchElementException("Unknown registered table");
        RecoveryBaseline baseline = businessJdbc.execute((ConnectionCallback<RecoveryBaseline>) connection -> {
            if (!"MySQL".equals(connection.getMetaData().getDatabaseProductName())) {
                throw new IllegalStateException("Recovery baseline must be verified against MySQL");
            }
            try (var statement = connection.createStatement()) {
                try (var rs = statement.executeQuery("SELECT @@global.binlog_format,@@global.binlog_row_image,@@server_uuid")) {
                    rs.next();
                    if (!"ROW".equalsIgnoreCase(rs.getString(1)) || !"FULL".equalsIgnoreCase(rs.getString(2))) {
                        throw new IllegalStateException("Recovery requires ROW binlog format and FULL row image");
                    }
                    String serverUuid = rs.getString(3);
                    TableSchema schema = TableRegistry.inspect(properties, store, connection, table);
                    var eligibility = SqlGenerator.checkEligibility(schema);
                    if (!eligibility.eligible()) throw new IllegalStateException(eligibility.reason());
                    try (var position = connection.createStatement(); var master = position.executeQuery(binlogStatusQuery(connection.getMetaData()))) {
                        if (!master.next()) throw new IllegalStateException("Binary logging is not enabled");
                        String file = master.getString("File"); long offset = master.getLong("Position");
                        TableSchema after = TableRegistry.inspect(properties, store, connection, table);
                        if (!schema.snapshotId().equals(after.snapshotId())) throw new IllegalStateException("Schema changed while confirming baseline; retry after DDL completes");
                        RecoveryBaseline result = new RecoveryBaseline(schema.sourceId(), schema.database(), table, schema.snapshotId(),
                                file, offset, serverUuid, Instant.now().toString());
                        store.saveBaseline(schema, result);
                        tables.refresh(schema);
                        return result;
                    }
                }
            }
        });
        return baseline;
    }

    static String binlogStatusQuery(DatabaseMetaData metadata) throws SQLException {
        int major = metadata.getDatabaseMajorVersion();
        // MySQL 8.4 删除了 SHOW MASTER STATUS；保留项目 MySQL 8.0 环境支持。
        return major > 8 || (major == 8 && metadata.getDatabaseMinorVersion() >= 4)
                ? "SHOW BINARY LOG STATUS" : "SHOW MASTER STATUS";
    }

    public RecoveryPlan createPlan(String eventId) {
        ArchivedChange archived = store.find(eventId, properties);
        if (archived == null) throw new NoSuchElementException("Archived change event not found: " + eventId);
        var summary = archived.summary();
        TableSchema schema = store.findSchema(summary.schemaSnapshotId());
        ArchivedChange.Recovery eligibility = store.recoveryState(schema, archived.event());
        if (!eligibility.available()) return save(archived, null, false, eligibility.reason(), Map.of("eligibility", eligibility.reason()));
        TableSchema current;
        try {
            current = businessJdbc.execute((ConnectionCallback<TableSchema>) connection -> {
                if ("MySQL".equals(connection.getMetaData().getDatabaseProductName())) {
                    try (var statement = connection.createStatement(); var rs = statement.executeQuery("SELECT @@server_uuid")) {
                        rs.next();
                        if (!store.baseline(schema).serverUuid().equals(rs.getString(1))) throw new IllegalStateException("Source server changed; establish a new source-id and baseline");
                    }
                }
                return TableRegistry.inspect(properties, store, connection, summary.table());
            });
        } catch (RuntimeException e) {
            return save(archived, null, false, "Current schema verification failed: " + e.getMessage(), Map.of("schemaCheck", false));
        }
        if (!schema.snapshotId().equals(current.snapshotId())) {
            return save(archived, null, false, "Current schema differs from the archived snapshot; establish a new baseline", Map.of("schemaCheck", false));
        }
        SqlGenerator.GeneratedSql generated;
        try { generated = SqlGenerator.generate(archived.event(), schema); }
        catch (IllegalArgumentException | java.time.DateTimeException e) {
            return save(archived, null, false, e.getMessage(), Map.of("generationCheck", false));
        }
        PreCheckResult check = preCheck(archived, schema);
        return save(archived, generated.sql(), check.matched(), check.reason(), check.details());
    }

    public RecoveryPlan getPlan(String id) { return store.findPlan(id); }

    private PreCheckResult preCheck(ArchivedChange archived, TableSchema schema) {
        String sql = "SELECT * FROM " + SqlGenerator.quoteIdentifier(schema.table()) + " WHERE "
                + SqlGenerator.quoteIdentifier(schema.primaryKey()) + " = ?";
        List<Map<String, Object>> rows = businessJdbc.queryForList(sql, archived.summary().primaryKeyValue());
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("checkedAt", Instant.now().toString());
        details.put("warning", "Read-only snapshot; later writes and business constraints still require review");
        if ("DELETE".equals(archived.summary().operation())) {
            return new PreCheckResult(rows.isEmpty(), rows.isEmpty() ? "Pre-check passed; review before execution" : "Primary key conflict: target row exists", details);
        }
        if (rows.isEmpty()) return new PreCheckResult(false, "Target row not found", details);
        Map<String, Object> row = rows.get(0);
        for (var col : schema.columns()) {
            Object actual = row.entrySet().stream().filter(e -> e.getKey().equalsIgnoreCase(col.name())).map(Map.Entry::getValue)
                    .filter(Objects::nonNull).findFirst().orElse(null);
            Object expected = archived.event().getAfterData().get(col.name());
            if (!SqlGenerator.valuesEqual(expected, actual, col)) {
                details.put("conflictColumn", col.name());
                return new PreCheckResult(false, "Row has been further modified: column " + col.name(), details);
            }
        }
        return new PreCheckResult(true, "Pre-check passed; review before execution", details);
    }

    private RecoveryPlan save(ArchivedChange archived, String sql, boolean executable, String reason, Map<String, Object> details) {
        var s = archived.summary();
        String reverse = switch (s.operation()) { case "INSERT" -> "DELETE"; case "DELETE" -> "INSERT"; default -> "UPDATE"; };
        RecoveryPlan plan = new RecoveryPlan(EventIdentity.hash("plan:" + s.eventId() + ":" + UUID.randomUUID()),
                s.eventId(), s.sourceId(), s.database(), s.table(), s.operation(), reverse, s.primaryKey(), s.primaryKeyValue(),
                sql == null ? "-- Recovery SQL unavailable. See the plan reason." : sql, store.encode(details),
                s.schemaSnapshotId(), executable, reason, Instant.now().toString());
        store.savePlan(plan);
        return plan;
    }

    public record PreCheckResult(boolean matched, String reason, Map<String, Object> details) { }
}
