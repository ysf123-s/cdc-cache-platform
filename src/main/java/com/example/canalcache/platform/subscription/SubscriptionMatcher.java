package com.example.canalcache.platform.subscription;

import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.platform.PlatformProperties;
import com.example.canalcache.platform.history.ArchiveService;
import com.example.canalcache.platform.history.ArchivedChange;
import com.example.canalcache.platform.history.TableRegistry;
import com.example.canalcache.platform.history.TableSchema;

import java.util.*;

public class SubscriptionMatcher {

    /**
     * Validates subscription definition against platform configuration and registered table schemas.
     */
    public static void validate(EventSubscription.Request req, PlatformProperties properties, TableRegistry tables) {
        if (req == null) {
            throw new IllegalArgumentException("Subscription request cannot be null");
        }
        if (req.table() == null || req.table().isBlank()) {
            throw new IllegalArgumentException("Table name is required");
        }
        if (!properties.getTables().contains(req.table())) {
            throw new IllegalArgumentException("Table '" + req.table() + "' is not registered for platform");
        }
        TableSchema schema = tables.get(properties.getDatabase(), req.table());
        if (schema == null) {
            throw new IllegalArgumentException("Table schema for '" + req.table() + "' not found");
        }

        if (req.targetName() == null || req.targetName().isBlank()) {
            throw new IllegalArgumentException("Target name is required");
        }
        if (!properties.getTargets().containsKey(req.targetName())) {
            throw new IllegalArgumentException("Target alias '" + req.targetName() + "' is not registered in platform configuration");
        }

        if (req.operations() == null || req.operations().isEmpty()) {
            throw new IllegalArgumentException("At least one operation must be specified");
        }
        Set<String> validOps = Set.of("INSERT", "UPDATE", "DELETE");
        for (String op : req.operations()) {
            if (op == null || !validOps.contains(op.toUpperCase(Locale.ROOT))) {
                throw new IllegalArgumentException("Invalid operation '" + op + "'. Must be INSERT, UPDATE, or DELETE");
            }
        }
        if (req.operations().stream().map(s -> s.toUpperCase(Locale.ROOT)).distinct().count() != req.operations().size()) {
            throw new IllegalArgumentException("Duplicate operations are not allowed");
        }
        if (req.sourceId() != null && !req.sourceId().equals(properties.getSourceId())
                || req.database() != null && !req.database().equals(properties.getDatabase())) {
            throw new IllegalArgumentException("Subscription must use the configured source and database");
        }

        Set<String> tableCols = new HashSet<>();
        schema.columns().forEach(c -> tableCols.add(c.name()));

        // Rule constraint: changed_fields is only valid for UPDATE
        if (req.changedFields() != null && !req.changedFields().isEmpty()) {
            if (req.operations().size() != 1 || !"UPDATE".equalsIgnoreCase(req.operations().get(0))) {
                throw new IllegalArgumentException("Changed-fields condition is only valid when operation is exclusively UPDATE");
            }
            for (String field : req.changedFields()) {
                if (!tableCols.contains(field)) {
                    throw new IllegalArgumentException("Field '" + field + "' does not exist in table '" + req.table() + "'");
                }
            }
            if (String.join(",", req.changedFields()).length() > 512
                    || req.changedFields().stream().distinct().count() != req.changedFields().size()) {
                throw new IllegalArgumentException("Changed fields are duplicated or exceed 512 characters");
            }
        }

        // Rule constraint: filterField/filterValue cannot be applied to DELETE
        if (req.filterField() != null && !req.filterField().isBlank()) {
            if (req.operations().stream().anyMatch("DELETE"::equalsIgnoreCase)) {
                throw new IllegalArgumentException("Filter-field condition cannot be configured for DELETE operation (no after-data)");
            }
            if (!tableCols.contains(req.filterField())) {
                throw new IllegalArgumentException("Filter field does not exist in table: " + req.filterField());
            }
            var column = schema.columns().stream().filter(c -> c.name().equals(req.filterField())).findFirst().orElseThrow();
            if (req.filterValue() != null) {
                if (req.filterValue().length() > 255) throw new IllegalArgumentException("filterValue exceeds 255 characters");
                try { com.example.canalcache.platform.recovery.SqlGenerator.formatValue(req.filterValue(), column); }
                catch (RuntimeException e) { throw new IllegalArgumentException("Invalid filter value for column " + column.name() + " (" + column.typeName() + ")", e); }
            }
        } else if (req.filterField() != null || req.filterValue() != null) {
            throw new IllegalArgumentException("filterField must name a column when a filter is provided");
        }
    }

    /**
     * Determines whether a given binlog event matches a subscription rule.
     */
    public static boolean matches(BinlogEvent event, EventSubscription sub) {
        if (!sub.enabled()) {
            return false;
        }
        if (!sub.table().equalsIgnoreCase(event.getTableName())) {
            return false;
        }

        String eventOp = event.getEventType() != null ? event.getEventType().name() : "";
        List<String> allowedOps = Arrays.stream(sub.operations().split(","))
                .map(String::trim)
                .map(String::toUpperCase)
                .toList();

        if (!allowedOps.contains(eventOp.toUpperCase())) {
            return false;
        }

        // Check changed_fields condition for UPDATE
        if ("UPDATE".equalsIgnoreCase(eventOp) && sub.changedFields() != null && !sub.changedFields().isBlank()) {
            List<String> requiredFields = Arrays.stream(sub.changedFields().split(","))
                    .map(String::trim)
                    .toList();

            List<ArchivedChange.FieldChange> actualChanges = ArchiveService.differences(event);
            Set<String> changedFieldNames = new HashSet<>();
            for (ArchivedChange.FieldChange fc : actualChanges) {
                changedFieldNames.add(fc.field());
            }

            if (!changedFieldNames.containsAll(requiredFields)) return false;
        }

        // Check filter_field on afterData
        if (sub.filterField() != null && !sub.filterField().isBlank()) {
            Map<String, Object> after = event.getAfterData();
            if (after == null || !after.containsKey(sub.filterField())) {
                return false;
            }
            Object actualVal = after.get(sub.filterField());
            String expectedVal = sub.filterValue();
            if (expectedVal == null && actualVal != null) {
                return false;
            }
            if (expectedVal != null && actualVal == null) return false;
            if (expectedVal != null) {
                var columns = event.getAfterColumns();
                var column = columns == null ? null : columns.stream().filter(c -> c.name().equals(sub.filterField())).findFirst().orElse(null);
                if (column != null) {
                    var definition = new TableSchema.ColumnDefinition(column.name(), column.index(), column.jdbcType(), column.mysqlType(),
                            0, 0, true, false, false);
                    if (!com.example.canalcache.platform.recovery.SqlGenerator.valuesEqual(expectedVal, actualVal, definition)) return false;
                } else if (!expectedVal.equals(actualVal.toString())) return false;
            }
        }

        return true;
    }
}
