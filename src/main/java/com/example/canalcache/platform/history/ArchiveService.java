package com.example.canalcache.platform.history;

import com.example.canalcache.model.BinlogColumn;
import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.model.KafkaCanalRecord;
import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.platform.PlatformProperties;
import com.example.canalcache.platform.subscription.DeliveryTask;
import com.example.canalcache.platform.subscription.EventSubscription;
import com.example.canalcache.platform.subscription.SubscriptionMatcher;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ArchiveService {
    private final PlatformProperties properties;
    private final TableRegistry tables;
    private final BinlogParser parser;
    private final ArchiveStore store;
    private final TransactionTemplate transaction;

    public ArchiveService(PlatformProperties properties, TableRegistry tables, BinlogParser parser,
                           ArchiveStore store, TransactionTemplate transaction) {
        this.properties = properties;
        this.tables = tables;
        this.parser = parser;
        this.store = store;
        this.transaction = transaction;
    }

    public Result archive(KafkaCanalRecord record) {
        return archiveBatch(List.of(record));
    }

    /** One Kafka poll shares a local archive transaction, independent of source MySQL transaction boundaries. */
    public Result archiveBatch(List<KafkaCanalRecord> records) {
        return transaction.execute(status -> {
            int inserted = 0, duplicate = 0, filtered = 0, ddl = 0;
            for (KafkaCanalRecord record : records) {
                if (record.message() == null || record.message().isRaw()) {
                    throw new IllegalArgumentException("Expected a decoded Canal Message at " + record.topic() + ":"
                            + record.partition() + ":" + record.offset());
                }
                List<BinlogEvent> events = parser.parseIncludingDdl(record.message().getEntries());
                for (BinlogEvent event : events) {
                    if (event.isDdl() && properties.getDatabase().equals(event.getDatabaseName())) {
                        store.invalidateSchemasForDdl(properties.getSourceId(), properties.getDatabase(), event,
                                event.getEventType() + " at " + record.topic() + ":" + record.partition() + ":" + record.offset());
                        ddl++;
                        continue;
                    }
                    TableSchema schema = tables.get(event.getDatabaseName(), event.getTableName());
                    if (schema == null) { filtered++; continue; }
                    Map<String, Object> current = event.getAfterData() == null ? event.getBeforeData() : event.getAfterData();
                    String key = primaryKey(current, schema);
                    String beforeKey = event.getBeforeData() == null ? null : primaryKey(event.getBeforeData(), schema);
                    String eventId = EventIdentity.of(properties.getSourceId(), record, event);
                    if (store.insert(eventId, schema, record, event, key, beforeKey)) {
                        inserted++;
                        List<EventSubscription> subs = store.findEnabledSubscriptions(
                                properties.getSourceId(), properties.getDatabase(), schema.table());
                        for (EventSubscription sub : subs) {
                            if (SubscriptionMatcher.matches(event, sub)) {
                                PlatformProperties.TargetConfig target = properties.getTargets().get(sub.targetName());
                                if (target == null) throw new IllegalStateException("Subscription target is not registered: " + sub.targetName());
                                String url = target.getUrl();
                                String deliveryId = EventIdentity.hash("delivery:" + eventId + ":" + sub.subscriptionId() + ":INITIAL");
                                DeliveryTask task = new DeliveryTask(
                                        0L,
                                        deliveryId,
                                        eventId,
                                        sub.subscriptionId(),
                                        "INITIAL",
                                        "INITIAL",
                                        sub.targetName(),
                                        url,
                                        "PENDING",
                                        0,
                                        properties.getMaxAttempts(),
                                        null,
                                        null,
                                        java.time.Instant.now().toString(),
                                        null,
                                        store.encode(event),
                                        store.encode(sub),
                                        java.time.Instant.now().toString(),
                                        java.time.Instant.now().toString()
                                );
                                store.createDeliveryTask(task);
                            }
                        }
                    } else { duplicate++; }
                }
            }
            return new Result(inserted, duplicate, filtered, ddl);
        });
    }

    private String primaryKey(Map<String, Object> values, TableSchema schema) {
        Object value = values == null ? null : values.get(schema.primaryKey());
        if (value == null || value.toString().length() > 255) {
            throw new IllegalArgumentException("Missing or oversized primary key " + schema.table() + "." + schema.primaryKey());
        }
        return value.toString();
    }

    public static List<ArchivedChange.FieldChange> differences(BinlogEvent event) {
        Map<String, Object> before = event.getBeforeData() == null ? Map.of() : event.getBeforeData();
        Map<String, Object> after = event.getAfterData() == null ? Map.of() : event.getAfterData();
        Set<String> fields = new LinkedHashSet<>(before.keySet());
        fields.addAll(after.keySet());
        List<ArchivedChange.FieldChange> changes = new ArrayList<>();
        for (String field : fields) {
            boolean had = before.containsKey(field), has = after.containsKey(field);
            Object oldValue = before.get(field), newValue = after.get(field);
            if (had != has || !sameValue(field, oldValue, newValue, event)) {
                changes.add(new ArchivedChange.FieldChange(field, had, oldValue, has, newValue));
            }
        }
        return changes;
    }

    private static boolean sameValue(String field, Object before, Object after, BinlogEvent event) {
        if (before == null || after == null) { return Objects.equals(before, after); }
        List<BinlogColumn> columns = event.getAfterColumns() == null ? event.getBeforeColumns() : event.getAfterColumns();
        if (columns != null && columns.stream().anyMatch(column -> column.name().equals(field)
                && List.of(Types.INTEGER, Types.BIGINT, Types.SMALLINT, Types.TINYINT, Types.DECIMAL, Types.NUMERIC)
                .contains(column.jdbcType()))) {
            return new BigDecimal(before.toString()).compareTo(new BigDecimal(after.toString())) == 0;
        }
        return Objects.equals(before, after);
    }

    public record Result(int inserted, int duplicates, int filtered, int ddl) { }
}
