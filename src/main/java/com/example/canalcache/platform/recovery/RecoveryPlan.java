package com.example.canalcache.platform.recovery;

/**
 * A recovery plan encapsulates one reverse SQL statement for one row change event.
 * The plan includes a pre-check summary (JSON) that records whether the current
 * database state matches the expected state — if it does not, the plan is marked
 * non-executable and the {@link #reason()} describes why.
 */
public record RecoveryPlan(
        String planId,
        String eventId,
        String sourceId,
        String database,
        String table,
        String eventType,
        String reverseType,
        String primaryKeyColumn,
        String primaryKeyValue,
        String generatedSql,
        String checkSummary,
        String schemaSnapshotId,
        boolean executable,
        String reason,
        String createdAt) {
}
