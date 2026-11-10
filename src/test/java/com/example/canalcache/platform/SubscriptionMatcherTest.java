package com.example.canalcache.platform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.platform.history.ArchiveStore;
import com.example.canalcache.platform.history.TableRegistry;
import com.example.canalcache.platform.history.TableSchema;
import com.example.canalcache.platform.subscription.EventSubscription;
import com.example.canalcache.platform.subscription.SubscriptionMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class SubscriptionMatcherTest {

    private PlatformProperties properties;
    private TableRegistry tables;

    @BeforeEach
    void setUp() {
        properties = new PlatformProperties();
        properties.setSourceId("src-1");
        properties.setDatabase("demo_db");
        properties.setTables(List.of("inventory"));

        PlatformProperties.TargetConfig target = new PlatformProperties.TargetConfig();
        target.setUrl("http://localhost:8080/webhook");
        properties.getTargets().put("inventory-hook", target);

        List<TableSchema.ColumnDefinition> cols = List.of(
                new TableSchema.ColumnDefinition("id", 1, Types.BIGINT, "BIGINT", 20, 0, false, true, false),
                new TableSchema.ColumnDefinition("name", 2, Types.VARCHAR, "VARCHAR", 64, 0, false, false, false),
                new TableSchema.ColumnDefinition("available", 3, Types.INTEGER, "INT", 11, 0, true, false, false)
        );
        TableSchema schema = new TableSchema("snap-1", "src-1", "demo_db", "inventory", "id", cols);
        tables = new TableRegistry(properties, null, List.of(schema));
    }

    @Test
    @DisplayName("Reject subscription for unregistered table")
    void rejectUnregisteredTable() {
        EventSubscription.Request req = new EventSubscription.Request(
                "src-1", "demo_db", "unknown_table", "inventory-hook",
                List.of("INSERT"), null, null, null, true);

        assertThatThrownBy(() -> SubscriptionMatcher.validate(req, properties, tables))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not registered for platform");
    }

    @Test
    @DisplayName("Reject subscription for unregistered target alias")
    void rejectUnregisteredTarget() {
        EventSubscription.Request req = new EventSubscription.Request(
                "src-1", "demo_db", "inventory", "unknown-hook",
                List.of("INSERT"), null, null, null, true);

        assertThatThrownBy(() -> SubscriptionMatcher.validate(req, properties, tables))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not registered in platform configuration");
    }

    @Test
    @DisplayName("Reject changed_fields on INSERT or DELETE")
    void rejectChangedFieldsOnNonUpdate() {
        EventSubscription.Request req = new EventSubscription.Request(
                "src-1", "demo_db", "inventory", "inventory-hook",
                List.of("INSERT"), List.of("available"), null, null, true);

        assertThatThrownBy(() -> SubscriptionMatcher.validate(req, properties, tables))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exclusively UPDATE");
    }

    @Test
    @DisplayName("Reject filter_field on DELETE operation")
    void rejectFilterFieldOnDelete() {
        EventSubscription.Request req = new EventSubscription.Request(
                "src-1", "demo_db", "inventory", "inventory-hook",
                List.of("DELETE"), null, "available", "0", true);

        assertThatThrownBy(() -> SubscriptionMatcher.validate(req, properties, tables))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be configured for DELETE");
    }

    @Test
    @DisplayName("Match UPDATE when changed_fields and filter_value both match")
    void matchUpdateWhenConditionsMet() {
        EventSubscription sub = new EventSubscription(
                "sub-1", "src-1", "demo_db", "inventory", "inventory-hook",
                "UPDATE", "available", "available", "0", true, 1, null, null);

        // Case 1: available changed from 5 to 0 -> should match
        BinlogEvent matched = BinlogEvent.builder()
                .databaseName("demo_db")
                .tableName("inventory")
                .eventType(CanalEntry.EventType.UPDATE)
                .beforeData(Map.of("id", 1, "available", 5))
                .afterData(Map.of("id", 1, "available", 0))
                .build();
        assertThat(SubscriptionMatcher.matches(matched, sub)).isTrue();

        // Case 2: available changed from 5 to 10 -> filterField not matching 0
        BinlogEvent notMatchedValue = BinlogEvent.builder()
                .databaseName("demo_db")
                .tableName("inventory")
                .eventType(CanalEntry.EventType.UPDATE)
                .beforeData(Map.of("id", 1, "available", 5))
                .afterData(Map.of("id", 1, "available", 10))
                .build();
        assertThat(SubscriptionMatcher.matches(notMatchedValue, sub)).isFalse();

        // Case 3: name changed, available unchanged -> changed_fields not matching
        BinlogEvent nameOnly = BinlogEvent.builder()
                .databaseName("demo_db")
                .tableName("inventory")
                .eventType(CanalEntry.EventType.UPDATE)
                .beforeData(Map.of("id", 1, "name", "old", "available", 0))
                .afterData(Map.of("id", 1, "name", "new", "available", 0))
                .build();
        assertThat(SubscriptionMatcher.matches(nameOnly, sub)).isFalse();
    }

    @Test
    @DisplayName("Disabled subscription never matches")
    void disabledSubscriptionNeverMatches() {
        EventSubscription sub = new EventSubscription(
                "sub-1", "src-1", "demo_db", "inventory", "inventory-hook",
                "INSERT", null, null, null, false, 1, null, null);

        BinlogEvent event = BinlogEvent.builder()
                .databaseName("demo_db")
                .tableName("inventory")
                .eventType(CanalEntry.EventType.INSERT)
                .afterData(Map.of("id", 1))
                .build();

        assertThat(SubscriptionMatcher.matches(event, sub)).isFalse();
    }
}
