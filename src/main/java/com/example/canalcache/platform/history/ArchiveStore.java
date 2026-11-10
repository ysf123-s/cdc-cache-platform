package com.example.canalcache.platform.history;

import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.model.KafkaCanalRecord;
import com.example.canalcache.platform.PlatformProperties;
import com.example.canalcache.platform.recovery.RecoveryPlan;
import com.example.canalcache.platform.recovery.RecoveryBaseline;
import com.example.canalcache.platform.recovery.SqlGenerator;
import com.example.canalcache.platform.replay.ReplayJob;
import com.example.canalcache.platform.subscription.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/** Explicit SQL over the platform datasource only; no business Mapper or transaction manager is reused. */
public class ArchiveStore {
    private static final String SUMMARY_COLUMNS = "sequence_id,event_id,source_id,database_name,table_name,"
            + "operation,primary_key_name,primary_key_value,before_primary_key_value,event_time_ms,archived_at,schema_snapshot_id";
    private final JdbcTemplate jdbc;
    private final NamedParameterJdbcTemplate named;
    private final ObjectMapper json;
    private final TransactionTemplate transaction;

    public ArchiveStore(JdbcTemplate jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.named = new NamedParameterJdbcTemplate(jdbc);
        this.json = json;
        this.transaction = new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
    }

    public void saveSchema(TableSchema schema) {
        if (jdbc.queryForObject("SELECT COUNT(*) FROM table_schema_snapshot WHERE snapshot_id=?", Long.class,
                schema.snapshotId()) > 0) {
            return;
        }
        try {
            jdbc.update("INSERT INTO table_schema_snapshot (snapshot_id,source_id,database_name,table_name,"
                            + "primary_key_name,columns_json,recovery_restriction) VALUES (?,?,?,?,?,?,?)", schema.snapshotId(), schema.sourceId(),
                    schema.database(), schema.table(), schema.primaryKey(), encode(schema.columns()), schema.recoveryRestriction());
        } catch (DuplicateKeyException e) {
            if (jdbc.queryForObject("SELECT COUNT(*) FROM table_schema_snapshot WHERE snapshot_id=?", Long.class,
                    schema.snapshotId()) != 1) {
                throw e;
            }
        }
    }

    public void invalidateSchemas(String sourceId, String database, String reason) {
        // RENAME and database-level DDL may affect more than the header's table. Recovery is disabled for the database.
        jdbc.update("UPDATE table_schema_snapshot SET invalidated_reason=? WHERE source_id=? AND database_name=?",
                reason, sourceId, database);
    }

    public void invalidateSchemasForDdl(String sourceId, String database, BinlogEvent ddl, String reason) {
        List<String> ids = jdbc.queryForList("SELECT snapshot_id FROM table_schema_snapshot WHERE source_id=? AND database_name=?",
                String.class, sourceId, database);
        for (String id : ids) {
            RecoveryBaseline baseline = baseline(findSchema(id));
            if (baseline == null || !baseline.snapshotId().equals(id) || !baseline.isBeforeBaseline(ddl)) {
                jdbc.update("UPDATE table_schema_snapshot SET invalidated_reason=? WHERE snapshot_id=?", reason, id);
            }
        }
    }

    public String invalidationReason(String snapshotId) {
        return jdbc.queryForObject("SELECT invalidated_reason FROM table_schema_snapshot WHERE snapshot_id=?",
                String.class, snapshotId);
    }

    public TableSchema findSchema(String snapshotId) {
        List<TableSchema> rows = jdbc.query("SELECT * FROM table_schema_snapshot WHERE snapshot_id=?", (rs, index) -> {
            TableSchema.ColumnDefinition[] columns = decode(rs.getString("columns_json"), TableSchema.ColumnDefinition[].class);
            return new TableSchema(rs.getString("snapshot_id"), rs.getString("source_id"), rs.getString("database_name"),
                    rs.getString("table_name"), rs.getString("primary_key_name"), List.of(columns), rs.getString("recovery_restriction"));
        }, snapshotId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public RecoveryBaseline baseline(TableSchema schema) {
        List<RecoveryBaseline> rows = jdbc.query("SELECT * FROM recovery_baseline WHERE source_id=? AND database_name=? AND table_name=?",
                (rs, index) -> new RecoveryBaseline(rs.getString("source_id"), rs.getString("database_name"), rs.getString("table_name"),
                        rs.getString("snapshot_id"), rs.getString("binlog_file"), rs.getLong("binlog_position"), rs.getString("server_uuid"),
                        rs.getTimestamp("verified_at").toInstant().toString()), schema.sourceId(), schema.database(), schema.table());
        return rows.isEmpty() ? null : rows.get(0);
    }

    public void saveBaseline(TableSchema schema, RecoveryBaseline baseline) {
        transaction.executeWithoutResult(status -> {
            saveSchema(schema);
            jdbc.update("DELETE FROM recovery_baseline WHERE source_id=? AND database_name=? AND table_name=?",
                    baseline.sourceId(), baseline.database(), baseline.table());
            jdbc.update("INSERT INTO recovery_baseline (source_id,database_name,table_name,snapshot_id,binlog_file,binlog_position,server_uuid,verified_at)"
                            + " VALUES (?,?,?,?,?,?,?,?)", baseline.sourceId(), baseline.database(), baseline.table(), baseline.snapshotId(),
                    baseline.binlogFile(), baseline.position(), baseline.serverUuid(), java.sql.Timestamp.from(java.time.Instant.parse(baseline.verifiedAt())));
            jdbc.update("UPDATE table_schema_snapshot SET invalidated_reason=NULL WHERE snapshot_id=?", schema.snapshotId());
        });
    }

    public ArchivedChange.Recovery recoveryState(TableSchema schema, BinlogEvent event) {
        if (schema == null) return new ArchivedChange.Recovery(false, "Schema snapshot not found");
        String invalidation = invalidationReason(schema.snapshotId());
        if (invalidation != null) return new ArchivedChange.Recovery(false, "Schema snapshot invalidated by DDL: " + invalidation);
        SqlGenerator.Eligibility eligibility = SqlGenerator.checkEligibility(schema);
        if (!eligibility.eligible()) return new ArchivedChange.Recovery(false, eligibility.reason());
        RecoveryBaseline baseline = baseline(schema);
        if (baseline == null || !baseline.snapshotId().equals(schema.snapshotId())) {
            return new ArchivedChange.Recovery(false, "Recovery baseline has not been verified for this schema");
        }
        if (event != null) {
            if (!baseline.covers(event)) return new ArchivedChange.Recovery(false, "Event precedes the verified recovery baseline");
            try { SqlGenerator.validateEvent(event, schema); }
            catch (IllegalArgumentException | java.time.DateTimeException e) { return new ArchivedChange.Recovery(false, e.getMessage()); }
        }
        return new ArchivedChange.Recovery(true, "Eligible for a current-state pre-check; not proof of safe execution");
    }

    public boolean insert(String eventId, TableSchema schema, KafkaCanalRecord record, BinlogEvent event,
                          String key, String beforeKey) {
        try {
            jdbc.update("INSERT INTO change_event (event_id,source_id,database_name,table_name,operation,"
                            + "primary_key_name,primary_key_value,before_primary_key_value,event_time_ms,schema_snapshot_id,"
                            + "kafka_topic,kafka_partition,kafka_offset,entry_index,row_index,event_json) "
                            + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", eventId, schema.sourceId(), schema.database(),
                    schema.table(), event.getEventType().name(), schema.primaryKey(), key, beforeKey,
                    event.getEventTimestamp(), schema.snapshotId(), record.topic(), record.partition(), record.offset(),
                    event.getEntryIndex(), event.getRowIndex(), encode(event));
            return true;
        } catch (DuplicateKeyException e) {
            if (jdbc.queryForObject("SELECT COUNT(*) FROM change_event WHERE event_id=?", Long.class, eventId) != 1) {
                throw e;
            }
            return false;
        }
    }

    public ArchivedChange.Page search(PlatformProperties properties, ArchivedChange.Filter filter) {
        MapSqlParameterSource parameters = new MapSqlParameterSource().addValue("source", properties.getSourceId())
                .addValue("database", properties.getDatabase()).addValue("tables", properties.getTables());
        StringBuilder sql = new StringBuilder("SELECT " + SUMMARY_COLUMNS + " FROM change_event WHERE source_id=:source"
                + " AND database_name=:database AND table_name IN (:tables)");
        if (filter.before() != null) { sql.append(" AND sequence_id<:before"); parameters.addValue("before", filter.before()); }
        if (filter.table() != null) { sql.append(" AND table_name=:table"); parameters.addValue("table", filter.table()); }
        if (filter.key() != null) {
            sql.append(" AND (primary_key_value=:key OR before_primary_key_value=:key)");
            parameters.addValue("key", filter.key());
        }
        if (filter.operation() != null) { sql.append(" AND operation=:operation"); parameters.addValue("operation", filter.operation()); }
        if (filter.from() != null) { sql.append(" AND event_time_ms>=:from"); parameters.addValue("from", filter.from()); }
        if (filter.to() != null) { sql.append(" AND event_time_ms<=:to"); parameters.addValue("to", filter.to()); }
        sql.append(" ORDER BY sequence_id DESC LIMIT :limit");
        parameters.addValue("limit", filter.limit() + 1);
        List<ArchivedChange.Summary> rows = named.query(sql.toString(), parameters, (rs, index) -> summary(rs));
        boolean more = rows.size() > filter.limit();
        List<ArchivedChange.Summary> items = more ? rows.subList(0, filter.limit()) : rows;
        return new ArchivedChange.Page(items, more ? items.get(items.size() - 1).sequence() : null);
    }

    public ArchivedChange find(String eventId, PlatformProperties properties) {
        List<ArchivedChange> rows = named.query("SELECT " + SUMMARY_COLUMNS + ",event_json,kafka_topic,kafka_partition,kafka_offset"
                        + " FROM change_event WHERE event_id=:id AND source_id=:source AND database_name=:database"
                        + " AND table_name IN (:tables)",
                new MapSqlParameterSource().addValue("id", eventId).addValue("source", properties.getSourceId())
                        .addValue("database", properties.getDatabase()).addValue("tables", properties.getTables()),
                (rs, index) -> {
                    BinlogEvent event = decode(rs.getString("event_json"), BinlogEvent.class);
                    return new ArchivedChange(summary(rs), event, rs.getString("kafka_topic"),
                            rs.getInt("kafka_partition"), Long.toString(rs.getLong("kafka_offset")),
                            ArchiveService.differences(event), recoveryState(findSchema(rs.getString("schema_snapshot_id")), event));
                });
        return rows.isEmpty() ? null : rows.get(0);
    }

    /** Replay needs the archived payload, not field differences or recovery eligibility. */
    public BinlogEvent findEvent(String eventId, PlatformProperties properties) {
        List<BinlogEvent> rows = named.query("SELECT event_json FROM change_event WHERE event_id=:id"
                        + " AND source_id=:source AND database_name=:database AND table_name IN (:tables)",
                new MapSqlParameterSource().addValue("id", eventId).addValue("source", properties.getSourceId())
                        .addValue("database", properties.getDatabase()).addValue("tables", properties.getTables()),
                (rs, index) -> decode(rs.getString("event_json"), BinlogEvent.class));
        return rows.isEmpty() ? null : rows.get(0);
    }

    private ArchivedChange.Summary summary(ResultSet rs) throws SQLException {
        return new ArchivedChange.Summary(Long.toString(rs.getLong("sequence_id")), rs.getString("event_id"),
                rs.getString("source_id"), rs.getString("database_name"), rs.getString("table_name"),
                rs.getString("operation"), rs.getString("primary_key_name"), rs.getString("primary_key_value"),
                rs.getString("before_primary_key_value"), rs.getLong("event_time_ms"),
                rs.getTimestamp("archived_at").toInstant().toString(), rs.getString("schema_snapshot_id"));
    }

    public String encode(Object value) {
        try { return json.writeValueAsString(value); }
        catch (JsonProcessingException e) { throw new IllegalArgumentException("Cannot encode archive data", e); }
    }

    public <T> T decode(String text, Class<T> type) {
        try { return json.readValue(text, type); }
        catch (JsonProcessingException e) { throw new IllegalStateException("Stored archive JSON is invalid", e); }
    }

    public void savePlan(RecoveryPlan plan) {
        jdbc.update("INSERT INTO recovery_plan (plan_id,event_id,source_id,database_name,table_name,"
                        + "operation,reverse_operation,primary_key_name,primary_key_value,generated_sql,"
                        + "pre_check_summary,schema_snapshot_id,executable,reason,created_at) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                plan.planId(), plan.eventId(), plan.sourceId(), plan.database(), plan.table(),
                plan.eventType(), plan.reverseType(), plan.primaryKeyColumn(), plan.primaryKeyValue(),
                plan.generatedSql(), plan.checkSummary(), plan.schemaSnapshotId(),
                plan.executable(), plan.reason(),
                java.sql.Timestamp.from(java.time.Instant.parse(plan.createdAt())));
    }

    public RecoveryPlan findPlan(String planId) {
        List<RecoveryPlan> rows = jdbc.query(
                "SELECT plan_id,event_id,source_id,database_name,table_name,operation,reverse_operation,"
                        + "primary_key_name,primary_key_value,generated_sql,pre_check_summary,schema_snapshot_id,"
                        + "executable,reason,created_at FROM recovery_plan WHERE plan_id = ?",
                (rs, idx) -> new RecoveryPlan(
                        rs.getString("plan_id"),
                        rs.getString("event_id"),
                        rs.getString("source_id"),
                        rs.getString("database_name"),
                        rs.getString("table_name"),
                        rs.getString("operation"),
                        rs.getString("reverse_operation"),
                        rs.getString("primary_key_name"),
                        rs.getString("primary_key_value"),
                        rs.getString("generated_sql"),
                        rs.getString("pre_check_summary"),
                        rs.getString("schema_snapshot_id"),
                        rs.getBoolean("executable"),
                        rs.getString("reason"),
                        rs.getTimestamp("created_at").toInstant().toString()
                ),
                planId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    // --- Subscription Management ---

    public void saveSubscription(EventSubscription sub) {
        jdbc.update("INSERT INTO event_subscription (subscription_id,source_id,database_name,table_name,"
                        + "target_name,operations,changed_fields,filter_field,filter_value,enabled,rule_version) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                sub.subscriptionId(), sub.sourceId(), sub.database(), sub.table(),
                sub.targetName(), sub.operations(), sub.changedFields(), sub.filterField(), sub.filterValue(),
                sub.enabled(), sub.ruleVersion());
    }

    public boolean updateSubscription(EventSubscription sub) {
        int updated = jdbc.update("UPDATE event_subscription SET table_name=?,target_name=?,operations=?, changed_fields=?, filter_field=?, "
                        + "filter_value=?, enabled=?, rule_version=rule_version+1,updated_at=CURRENT_TIMESTAMP(3) WHERE subscription_id=? AND rule_version=?",
                sub.table(), sub.targetName(), sub.operations(), sub.changedFields(), sub.filterField(), sub.filterValue(), sub.enabled(),
                sub.subscriptionId(), sub.ruleVersion());
        return updated > 0;
    }

    public EventSubscription findSubscription(String id) {
        List<EventSubscription> rows = jdbc.query(
                "SELECT subscription_id,source_id,database_name,table_name,target_name,operations,changed_fields,"
                        + "filter_field,filter_value,enabled,rule_version,created_at,updated_at "
                        + "FROM event_subscription WHERE subscription_id = ?",
                (rs, idx) -> new EventSubscription(
                        rs.getString("subscription_id"),
                        rs.getString("source_id"),
                        rs.getString("database_name"),
                        rs.getString("table_name"),
                        rs.getString("target_name"),
                        rs.getString("operations"),
                        rs.getString("changed_fields"),
                        rs.getString("filter_field"),
                        rs.getString("filter_value"),
                        rs.getBoolean("enabled"),
                        rs.getInt("rule_version"),
                        rs.getTimestamp("created_at").toInstant().toString(),
                        rs.getTimestamp("updated_at").toInstant().toString()
                ), id);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public List<EventSubscription> listSubscriptions(String sourceId, String database) {
        return jdbc.query(
                "SELECT subscription_id,source_id,database_name,table_name,target_name,operations,changed_fields,"
                        + "filter_field,filter_value,enabled,rule_version,created_at,updated_at "
                        + "FROM event_subscription WHERE source_id=? AND database_name=? ORDER BY created_at DESC",
                (rs, idx) -> new EventSubscription(
                        rs.getString("subscription_id"),
                        rs.getString("source_id"),
                        rs.getString("database_name"),
                        rs.getString("table_name"),
                        rs.getString("target_name"),
                        rs.getString("operations"),
                        rs.getString("changed_fields"),
                        rs.getString("filter_field"),
                        rs.getString("filter_value"),
                        rs.getBoolean("enabled"),
                        rs.getInt("rule_version"),
                        rs.getTimestamp("created_at").toInstant().toString(),
                        rs.getTimestamp("updated_at").toInstant().toString()
                ), sourceId, database);
    }

    public List<EventSubscription> findEnabledSubscriptions(String sourceId, String database, String table) {
        return jdbc.query(
                "SELECT subscription_id,source_id,database_name,table_name,target_name,operations,changed_fields,"
                        + "filter_field,filter_value,enabled,rule_version,created_at,updated_at "
                        + "FROM event_subscription WHERE source_id=? AND database_name=? AND table_name=? AND enabled=true",
                (rs, idx) -> new EventSubscription(
                        rs.getString("subscription_id"),
                        rs.getString("source_id"),
                        rs.getString("database_name"),
                        rs.getString("table_name"),
                        rs.getString("target_name"),
                        rs.getString("operations"),
                        rs.getString("changed_fields"),
                        rs.getString("filter_field"),
                        rs.getString("filter_value"),
                        rs.getBoolean("enabled"),
                        rs.getInt("rule_version"),
                        rs.getTimestamp("created_at").toInstant().toString(),
                        rs.getTimestamp("updated_at").toInstant().toString()
                ), sourceId, database, table);
    }

    // --- Delivery Task Management ---

    public boolean createDeliveryTask(DeliveryTask task) {
        try {
            jdbc.update("INSERT INTO delivery_task (delivery_id,event_id,subscription_id,round_type,round_id,"
                            + "target_name,target_url,status,attempt_count,max_attempts,next_retry_at,"
                            + "event_payload,subscription_snapshot) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    task.deliveryId(), task.eventId(), task.subscriptionId(), task.roundType(), task.roundId(),
                    task.targetName(), task.targetUrl(), task.status(), task.attemptCount(), task.maxAttempts(),
                    new java.sql.Timestamp(java.time.Instant.parse(task.nextRetryAt()).toEpochMilli()),
                    task.eventPayload(), task.subscriptionSnapshot());
            return true;
        } catch (DuplicateKeyException e) {
            // Already created for this event+subscription+round
            return false;
        }
    }

    public DeliveryTask findDeliveryTask(long taskId) {
        List<DeliveryTask> rows = jdbc.query(
                "SELECT task_id,delivery_id,event_id,subscription_id,round_type,round_id,target_name,target_url,"
                        + "status,attempt_count,max_attempts,lease_until,lease_owner,next_retry_at,last_error,"
                        + "event_payload,subscription_snapshot,created_at,updated_at FROM delivery_task WHERE task_id=?",
                (rs, idx) -> mapDeliveryTask(rs), taskId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public List<DeliveryTask> listDeliveryTasks(String status, String subscriptionId, String eventId, int limit) {
        return listDeliveryTasks(status, subscriptionId, eventId, null, null, limit);
    }

    public List<DeliveryTask> listDeliveryTasks(String status, String subscriptionId, String eventId, String roundId, Long before, int limit) {
        StringBuilder sql = new StringBuilder("SELECT task_id,delivery_id,event_id,subscription_id,round_type,round_id,"
                + "target_name,target_url,status,attempt_count,max_attempts,lease_until,lease_owner,next_retry_at,last_error,"
                + "event_payload,subscription_snapshot,created_at,updated_at FROM delivery_task WHERE 1=1");
        List<Object> args = new java.util.ArrayList<>();
        if (status != null && !status.isBlank()) { sql.append(" AND status=?"); args.add(status); }
        if (subscriptionId != null && !subscriptionId.isBlank()) { sql.append(" AND subscription_id=?"); args.add(subscriptionId); }
        if (eventId != null && !eventId.isBlank()) { sql.append(" AND event_id=?"); args.add(eventId); }
        if (roundId != null && !roundId.isBlank()) { sql.append(" AND round_id=?"); args.add(roundId); }
        if (before != null) { sql.append(" AND task_id<?"); args.add(before); }
        sql.append(" ORDER BY task_id DESC LIMIT ?");
        args.add(limit);
        return jdbc.query(sql.toString(), (rs, idx) -> mapDeliveryTask(rs), args.toArray());
    }

    public List<DeliveryTask> claimTasks(String owner, int limit, int leaseSeconds) {
        java.sql.Timestamp now = new java.sql.Timestamp(System.currentTimeMillis());
        java.sql.Timestamp leaseUntil = new java.sql.Timestamp(System.currentTimeMillis() + leaseSeconds * 1000L);

        List<Long> taskIds = jdbc.queryForList(
                "SELECT task_id FROM delivery_task WHERE "
                        + "(status IN ('PENDING', 'RETRY_WAIT') AND next_retry_at <= ?) "
                        + "OR (status = 'IN_FLIGHT' AND lease_until < ?) "
                        + "ORDER BY task_id ASC LIMIT ?",
                Long.class, now, now, limit);

        if (taskIds.isEmpty()) return List.of();

        List<DeliveryTask> claimed = new java.util.ArrayList<>();
        for (Long id : taskIds) {
            String claimToken = owner + ":" + java.util.UUID.randomUUID();
            int updated = jdbc.update(
                    "UPDATE delivery_task SET status='IN_FLIGHT', lease_owner=?, lease_until=?, updated_at=? "
                            + "WHERE task_id=? AND ((status IN ('PENDING', 'RETRY_WAIT') AND next_retry_at <= ?) "
                            + "OR (status = 'IN_FLIGHT' AND lease_until < ?))",
                    claimToken, leaseUntil, now, id, now, now);
            if (updated > 0) {
                DeliveryTask task = findDeliveryTask(id);
                if (task != null) claimed.add(task);
            }
        }
        return claimed;
    }

    public boolean finishDelivery(DeliveryTask claimed, DeliveryAttempt attempt) {
        return transaction.execute(status -> {
            List<DeliveryTask> rows = jdbc.query("SELECT * FROM delivery_task WHERE task_id=? FOR UPDATE",
                    (rs, index) -> mapDeliveryTask(rs), claimed.taskId());
            if (rows.isEmpty()) return false;
            DeliveryTask current = rows.get(0);
            java.time.Instant now = java.time.Instant.now();
            if (!"IN_FLIGHT".equals(current.status()) || !claimed.leaseOwner().equals(current.leaseOwner())
                    || !java.time.Instant.parse(current.leaseUntil()).isAfter(now)) return false;
            saveAttempt(attempt);
            jdbc.update("UPDATE delivery_task SET status=?,attempt_count=?,next_retry_at=?,last_error=?,"
                            + "lease_owner=NULL,lease_until=NULL,updated_at=? WHERE task_id=?",
                    attempt.status(), attempt.attemptNumber(),
                    java.sql.Timestamp.from(attempt.nextRetryAt() == null ? now : java.time.Instant.parse(attempt.nextRetryAt())),
                    attempt.errorMessage(), java.sql.Timestamp.from(now), claimed.taskId());
            return true;
        });
    }

    public boolean resetTaskForRetry(long taskId) {
        java.sql.Timestamp now = new java.sql.Timestamp(System.currentTimeMillis());
        int updated = jdbc.update(
                "UPDATE delivery_task SET status='PENDING', next_retry_at=?, lease_owner=NULL, lease_until=NULL, "
                        + "max_attempts=attempt_count+1, updated_at=? WHERE task_id=? AND status='FAILED'",
                now, now, taskId);
        return updated > 0;
    }

    private void saveAttempt(DeliveryAttempt attempt) {
        java.sql.Timestamp now = new java.sql.Timestamp(System.currentTimeMillis());
        java.sql.Timestamp nextRetry = attempt.nextRetryAt() != null
                ? java.sql.Timestamp.from(java.time.Instant.parse(attempt.nextRetryAt())) : null;
        jdbc.update("INSERT INTO delivery_attempt (task_id,attempt_number,status,http_status,duration_ms,"
                        + "error_message,response_summary,next_retry_at,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                attempt.taskId(), attempt.attemptNumber(), attempt.status(), attempt.httpStatus(),
                attempt.durationMs(), attempt.errorMessage(), attempt.responseSummary(), nextRetry, now);
    }

    public List<DeliveryAttempt> listAttempts(long taskId) {
        return jdbc.query("SELECT attempt_id,task_id,attempt_number,status,http_status,duration_ms,"
                        + "error_message,response_summary,next_retry_at,created_at FROM delivery_attempt "
                        + "WHERE task_id=? ORDER BY attempt_number ASC",
                (rs, idx) -> new DeliveryAttempt(
                        rs.getLong("attempt_id"),
                        rs.getLong("task_id"),
                        rs.getInt("attempt_number"),
                        rs.getString("status"),
                        rs.getObject("http_status") != null ? rs.getInt("http_status") : null,
                        rs.getLong("duration_ms"),
                        rs.getString("error_message"),
                        rs.getString("response_summary"),
                        rs.getTimestamp("next_retry_at") != null ? rs.getTimestamp("next_retry_at").toInstant().toString() : null,
                        rs.getTimestamp("created_at").toInstant().toString()
                ), taskId);
    }

    private DeliveryTask mapDeliveryTask(ResultSet rs) throws SQLException {
        return new DeliveryTask(
                rs.getLong("task_id"),
                rs.getString("delivery_id"),
                rs.getString("event_id"),
                rs.getString("subscription_id"),
                rs.getString("round_type"),
                rs.getString("round_id"),
                rs.getString("target_name"),
                rs.getString("target_url"),
                rs.getString("status"),
                rs.getInt("attempt_count"),
                rs.getInt("max_attempts"),
                rs.getTimestamp("lease_until") != null ? rs.getTimestamp("lease_until").toInstant().toString() : null,
                rs.getString("lease_owner"),
                rs.getTimestamp("next_retry_at") != null ? rs.getTimestamp("next_retry_at").toInstant().toString() : null,
                rs.getString("last_error"),
                rs.getString("event_payload"),
                rs.getString("subscription_snapshot"),
                rs.getTimestamp("created_at").toInstant().toString(),
                rs.getTimestamp("updated_at").toInstant().toString()
        );
    }

    // --- Replay Job Management ---

    public boolean saveReplayJob(ReplayJob job) {
        try {
            jdbc.update("INSERT INTO replay_job (job_id,request_key,source_id,database_name,table_name,"
                            + "subscription_id,status,total_events,matched_events,skipped_events,failed_events,"
                            + "event_ids_json,subscription_snapshot,target_url,last_error) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    job.jobId(), job.requestKey(), job.sourceId(), job.database(), job.table(),
                    job.subscriptionId(), job.status(), job.totalEvents(), job.matchedEvents(),
                    job.skippedEvents(), job.failedEvents(), encode(job.eventIds()), job.subscriptionSnapshot(), job.targetUrl(), job.lastError());
            return true;
        } catch (DuplicateKeyException e) {
            if (findReplayJobByRequestKey(job.requestKey()) == null) throw e;
            return false;
        }
    }

    public ReplayJob findReplayJob(String jobId) {
        List<ReplayJob> rows = jdbc.query(
                "SELECT job_id,request_key,source_id,database_name,table_name,subscription_id,status,"
                        + "total_events,matched_events,skipped_events,failed_events,event_ids_json,"
                        + "subscription_snapshot,target_url,last_error,created_at,updated_at FROM replay_job WHERE job_id = ?",
                (rs, idx) -> mapReplayJob(rs), jobId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public ReplayJob findReplayJobByRequestKey(String requestKey) {
        List<ReplayJob> rows = jdbc.query(
                "SELECT job_id,request_key,source_id,database_name,table_name,subscription_id,status,"
                        + "total_events,matched_events,skipped_events,failed_events,event_ids_json,"
                        + "subscription_snapshot,target_url,last_error,created_at,updated_at FROM replay_job WHERE request_key = ?",
                (rs, idx) -> mapReplayJob(rs), requestKey);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public void updateReplayJobProgress(String jobId, String status, int matched, int skipped, int failed) {
        java.sql.Timestamp now = new java.sql.Timestamp(System.currentTimeMillis());
        jdbc.update("UPDATE replay_job SET status=?, matched_events=?, skipped_events=?, failed_events=?, last_error=NULL,updated_at=? WHERE job_id=?",
                status, matched, skipped, failed, now, jobId);
    }

    @SuppressWarnings("unchecked")
    private ReplayJob mapReplayJob(ResultSet rs) throws SQLException {
        List<String> eventIds = decode(rs.getString("event_ids_json"), List.class);
        return new ReplayJob(
                rs.getString("job_id"),
                rs.getString("request_key"),
                rs.getString("source_id"),
                rs.getString("database_name"),
                rs.getString("table_name"),
                rs.getString("subscription_id"),
                rs.getString("status"),
                rs.getInt("total_events"),
                rs.getInt("matched_events"),
                rs.getInt("skipped_events"),
                rs.getInt("failed_events"),
                eventIds,
                rs.getString("subscription_snapshot"),
                rs.getString("target_url"),
                rs.getString("last_error"),
                rs.getTimestamp("created_at").toInstant().toString(),
                rs.getTimestamp("updated_at").toInstant().toString()
        );
    }

    public ReplayJob withReplayLock(String jobId, java.util.function.Function<ReplayJob, ReplayJob> work) {
        return transaction.execute(status -> {
            List<ReplayJob> rows = jdbc.query("SELECT * FROM replay_job WHERE job_id=? FOR UPDATE", (rs, index) -> mapReplayJob(rs), jobId);
            if (rows.isEmpty()) throw new java.util.NoSuchElementException("Replay job not found");
            return work.apply(rows.get(0));
        });
    }

    public List<String> pendingReplayIds() {
        return jdbc.queryForList("SELECT job_id FROM replay_job WHERE status IN ('PENDING','RUNNING') ORDER BY created_at LIMIT 10", String.class);
    }

    public void recordReplayFailure(String jobId, String reason, boolean permanent) {
        jdbc.update("UPDATE replay_job SET status=?,last_error=?,updated_at=? WHERE job_id=? AND status IN ('PENDING','RUNNING')",
                permanent ? "FAILED" : "PENDING", reason.substring(0, Math.min(reason.length(), 1000)),
                new java.sql.Timestamp(System.currentTimeMillis()), jobId);
    }

    public java.util.Map<String, Long> replayDeliveryStats(String jobId) {
        java.util.Map<String, Long> counts = new java.util.LinkedHashMap<>();
        jdbc.query("SELECT status,COUNT(*) AS amount FROM delivery_task WHERE round_id=? GROUP BY status", rs -> {
            counts.put(rs.getString("status"), rs.getLong("amount"));
        }, jobId);
        return counts;
    }
}
