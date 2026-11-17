package com.example.canalcache.platform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.platform.history.TableRegistry;
import com.example.canalcache.platform.history.TableSchema;
import com.example.canalcache.platform.recovery.SqlGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Types;
import java.util.List;

import static com.example.canalcache.platform.ArchiveTestFixture.column;
import static org.assertj.core.api.Assertions.assertThat;

class ForeignKeyRecoveryTest {
    private ArchiveTestFixture f;
    private JdbcTemplate db;
    private TableSchema schema;

    @BeforeEach void setup() throws Exception {
        f = new ArchiveTestFixture();
        db = new JdbcTemplate(f.business);
        db.execute("CREATE TABLE parent_record (id INT, code VARCHAR(255), PRIMARY KEY(id, code))");
        db.execute("ALTER TABLE inventory ADD CONSTRAINT fk_parent FOREIGN KEY(available, remark) REFERENCES parent_record(id, code)");
        db.execute("CREATE TABLE child_record (id INT PRIMARY KEY, inventory_id BIGINT REFERENCES inventory(id))");
        try (var connection = f.business.getConnection()) {
            schema = TableRegistry.inspect(f.properties, f.store, connection, "inventory");
        }
        f.tables.refresh(schema);
        f.assumeVerifiedBaselineForUnitTest();
        db.update("INSERT INTO parent_record VALUES(10, 'R'), (10, 'S'), (20, 'R')");
        db.update("INSERT INTO inventory VALUES(1, 'B', 10, 'R')");
        db.update("INSERT INTO child_record VALUES(1, 1)");
    }

    private List<CanalEntry.Column> columns(String name, String available, String remark) {
        return List.of(column("id", "1", 0, Types.BIGINT, "bigint", true),
                column("name", name, 1, Types.VARCHAR, "varchar(64)", false),
                column("available", available, 2, Types.INTEGER, "int", false),
                column("remark", remark, 3, Types.VARCHAR, "varchar(255)", false));
    }

    private String archiveUpdate(String beforeName, String available, String remark) {
        var row = CanalEntry.RowData.newBuilder().addAllBeforeColumns(columns(beforeName, "10", "R"))
                .addAllAfterColumns(columns("B", available, remark)).build();
        f.service.archive(f.record(1, f.rows(EventType.UPDATE, row)));
        return f.store.search(f.properties, new com.example.canalcache.platform.history.ArchivedChange.Filter(
                null, null, null, null, null, null, 1)).items().get(0).eventId();
    }

    @Test void capturesBothDirectionsAndAllCompositeKeyColumnsInPersistedSnapshot() {
        var persisted = f.store.findSchema(schema.snapshotId());
        assertThat(persisted.recoveryRestriction()).isNull();
        assertThat(persisted.columns().stream().filter(TableSchema.ColumnDefinition::foreignKey).map(TableSchema.ColumnDefinition::name))
                .containsExactly("id", "available", "remark");
        assertThat(SqlGenerator.checkEligibility(persisted).eligible()).isTrue();
        assertThat(f.tables.views().get(0).recoveryAvailable()).isTrue();
    }

    @Test void ordinaryUpdateProducesPlanButStillRejectsLaterDataChanges() {
        String id = archiveUpdate("A", "10", "R");
        var plan = f.recoveryService.createPlan(id);
        assertThat(plan.executable()).isTrue();
        assertThat(plan.generatedSql()).contains(" SET `name` = CONVERT(X'41' USING utf8mb4)");
        assertThat(db.queryForObject("SELECT name FROM inventory WHERE id=1", String.class)).isEqualTo("B");
        db.update("UPDATE inventory SET name='C' WHERE id=1");
        assertThat(f.recoveryService.createPlan(id).reason()).contains("further modified");
    }

    @Test void importedCompositeKeyChangeIsRejectedBeforeSqlGeneration() {
        String id = archiveUpdate("B", "10", "S");
        var plan = f.recoveryService.createPlan(id);
        assertThat(plan.executable()).isFalse();
        assertThat(plan.reason()).contains("Foreign key column modification", "remark");
        assertThat(plan.generatedSql()).doesNotContain("UPDATE ");
    }

    @Test void referencedNonPrimaryKeyColumnIsAlsoProtected() throws Exception {
        db.execute("ALTER TABLE inventory ADD CONSTRAINT uq_name UNIQUE(name)");
        db.execute("CREATE TABLE name_reference (name VARCHAR(64) REFERENCES inventory(name))");
        try (var connection = f.business.getConnection()) {
            schema = TableRegistry.inspect(f.properties, f.store, connection, "inventory");
        }
        f.tables.refresh(schema);
        f.assumeVerifiedBaselineForUnitTest();
        var plan = f.recoveryService.createPlan(archiveUpdate("A", "10", "R"));
        assertThat(plan.executable()).isFalse();
        assertThat(plan.reason()).contains("Foreign key column modification", "name");
    }

    @Test void foreignKeyFreeSnapshotEncodingKeepsExistingHashInput() throws Exception {
        var json = new ObjectMapper();
        String previous = "{\"name\":\"id\",\"ordinal\":1,\"jdbcType\":-5,\"typeName\":\"BIGINT\",\"size\":20,\"scale\":0,\"nullable\":false,\"autoIncrement\":true,\"generated\":false,\"characterSet\":null,\"collation\":null}";
        var column = json.readValue(previous, TableSchema.ColumnDefinition.class);
        assertThat(column.foreignKey()).isFalse();
        assertThat(json.writeValueAsString(column)).isEqualTo(previous);
    }
}
