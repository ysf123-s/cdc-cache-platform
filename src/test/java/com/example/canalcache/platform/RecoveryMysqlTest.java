package com.example.canalcache.platform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.Message;
import com.example.canalcache.model.KafkaCanalRecord;
import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.platform.history.*;
import com.example.canalcache.platform.recovery.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.utility.DockerImageName;
import java.sql.Types;
import java.util.*;
import static com.example.canalcache.platform.ArchiveTestFixture.column;
import static org.assertj.core.api.Assertions.*;

/** Real MySQL SQL modes, collation, metadata and recovery execution. Requires Docker. */
@Testcontainers(disabledWithoutDocker = true)
class RecoveryMysqlTest {
    @Container static MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse(System.getProperty("test.mysql.image", "mysql:8.0")))
            .withUsername("root").withPassword("test-password").withEnv("MYSQL_ROOT_HOST","%")
            .withDatabaseName("demo_db")
            .withConfigurationOverride("mysql-recovery")
            .withCommand("--server-id=11","--log-bin=mysql-bin","--binlog-format=ROW","--binlog-row-image=FULL");
    JdbcTemplate db;
    DriverManagerDataSource business;
    PlatformProperties properties;
    ArchiveStore store;
    ArchiveService archive;
    RecoveryService recovery;
    RecoveryBaseline baseline;

    @BeforeEach void setup() throws Exception {
        String url="jdbc:mysql://"+mysql.getHost()+":"+mysql.getMappedPort(3306)+"/";
        business=new DriverManagerDataSource(url+"demo_db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai","root","test-password");
        db=new JdbcTemplate(business);
        db.execute("DROP DATABASE IF EXISTS cdc_platform");db.execute("DROP TABLE IF EXISTS inventory");
        db.execute("CREATE TABLE inventory(id BIGINT PRIMARY KEY,name VARCHAR(64),price DECIMAL(30,4),remark TEXT,created_at DATETIME(6)) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
        try(var connection=business.getConnection()){ScriptUtils.executeSqlScript(connection,new FileSystemResource("docker/mysql/init/03-platform.sql"));}
        var platform=new DriverManagerDataSource(url+"cdc_platform?useSSL=false&allowPublicKeyRetrieval=true","root","test-password");
        properties=new PlatformProperties();properties.setDatabase("demo_db");properties.setTables(List.of("inventory"));
        store=new ArchiveStore(new JdbcTemplate(platform),new ObjectMapper());
        var tables=TableRegistry.capture(properties,store,business,platform);
        archive=new ArchiveService(properties,tables,new BinlogParser(),store,new TransactionTemplate(new DataSourceTransactionManager(platform)));
        recovery=new RecoveryService(properties,tables,store,db,new ObjectMapper());
    }
    String event(EventType type,String beforeName,String afterName) {
        var row=CanalEntry.RowData.newBuilder();
        if(type!=EventType.INSERT) row.addAllBeforeColumns(columns(beforeName));
        if(type!=EventType.DELETE) row.addAllAfterColumns(columns(afterName));
        var change=CanalEntry.RowChange.newBuilder().setEventType(type).addRowDatas(row).build();
        var entry=CanalEntry.Entry.newBuilder().setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder().setSchemaName("demo_db").setTableName("inventory")
                        .setLogfileName(baseline.binlogFile()).setLogfileOffset(baseline.position()).setExecuteTime(System.currentTimeMillis()))
                .setStoreValue(change.toByteString()).build();
        archive.archive(new KafkaCanalRecord("example",0,1,new Message(1,List.of(entry))));
        return store.search(properties,new ArchivedChange.Filter(null,null,null,null,null,null,1)).items().get(0).eventId();
    }
    List<CanalEntry.Column> columns(String name) {
        return List.of(column("id","9007199254740993",0,Types.BIGINT,"bigint",true),
                column("name",name,1,Types.VARCHAR,"varchar(64)",false),column("price","123456789012345.6789",2,Types.DECIMAL,"decimal(30,4)",false),
                column("remark",null,3,Types.LONGVARCHAR,"text",false),column("created_at","2026-10-18 12:00:00.123456",4,Types.TIMESTAMP,"datetime(6)",false));
    }
    void insert(String name) { db.update("INSERT INTO inventory VALUES(9007199254740993,?,123456789012345.6789,NULL,'2026-10-18 12:00:00.123456')",name); }
    int execute(String mode,String sql) throws Exception {
        try(var connection=business.getConnection();var statement=connection.createStatement()) {
            statement.execute("SET SESSION sql_mode='"+mode+"'");return statement.executeUpdate(sql);
        }
    }
    @ParameterizedTest @ValueSource(strings={"","NO_BACKSLASH_ESCAPES"})
    void deleteRecoveryRoundTripsUnicodeControlsPrecisionAndNullUnderBothSqlModes(String mode) throws Exception {
        baseline=recovery.confirmBaseline("inventory");
        String name="  中文 'quote' \\ slash\n\t\0 ";
        var plan=recovery.createPlan(event(EventType.DELETE,name,null));
        assertThat(plan.executable()).isTrue();assertThat(execute(mode,plan.generatedSql())).isEqualTo(1);
        var row=db.queryForMap("SELECT * FROM inventory");
        assertThat(row.get("name")).isEqualTo(name);
        assertThat(row.get("price").toString()).isEqualTo("123456789012345.6789");
        assertThat(row.get("created_at").toString()).endsWith("12:00:00.123456");assertThat(row.get("remark")).isNull();
    }
    @ParameterizedTest @ValueSource(strings={"","NO_BACKSLASH_ESCAPES"})
    void insertRecoveryGuardRejectsSubsequentCaseAndSpaceChanges(String mode) throws Exception {
        baseline=recovery.confirmBaseline("inventory");insert("Alice");
        var plan=recovery.createPlan(event(EventType.INSERT,null,"Alice"));assertThat(plan.executable()).isTrue();
        db.update("UPDATE inventory SET name='ALICE'");assertThat(execute(mode,plan.generatedSql())).isZero();
        db.update("UPDATE inventory SET name='Alice '");assertThat(execute(mode,plan.generatedSql())).isZero();
        db.update("UPDATE inventory SET name='Alice'");assertThat(execute(mode,plan.generatedSql())).isEqualTo(1);
    }
    @Test void updateRecoveryExecutesAndCurrentSchemaChangesAreRejected() throws Exception {
        baseline=recovery.confirmBaseline("inventory");insert("Bob");String id=event(EventType.UPDATE,"Alice","Bob");
        var plan=recovery.createPlan(id);assertThat(plan.executable()).isTrue();assertThat(execute("NO_BACKSLASH_ESCAPES",plan.generatedSql())).isEqualTo(1);
        assertThat(db.queryForObject("SELECT name FROM inventory",String.class)).isEqualTo("Alice");
        db.execute("ALTER TABLE inventory ADD extra INT");assertThat(recovery.createPlan(id).executable()).isFalse();
    }
    @Test void baselineRejectsMinimalImagesAndTriggers() {
        db.execute("SET GLOBAL binlog_row_image='MINIMAL'");
        try { assertThatThrownBy(()->recovery.confirmBaseline("inventory")).hasMessageContaining("FULL"); }
        finally {db.execute("SET GLOBAL binlog_row_image='FULL'");}
        db.execute("CREATE TRIGGER inventory_trigger BEFORE INSERT ON inventory FOR EACH ROW SET NEW.price=1");
        assertThatThrownBy(()->recovery.confirmBaseline("inventory")).hasMessageContaining("triggers");
    }
    record TypeCase(String type, String beforeSql, String afterSql, String beforeCanal, String afterCanal) {
        @Override public String toString() { return type + " " + beforeCanal; }
    }
    static java.util.stream.Stream<TypeCase> recoveryTypes() {
        return java.util.stream.Stream.of(
                new TypeCase("BIT(1)", "0", "1", "0", "1"),
                new TypeCase("BIT(8)", "255", "0", "255", "0"),
                new TypeCase("BIT(64)", "18446744073709551615", "0", "-1", "0"),
                new TypeCase("TINYINT(1)", "2", "3", "2", "3"),
                new TypeCase("FLOAT", "CAST(0.1 AS FLOAT)", "CAST(0.2 AS FLOAT)", "0.1", "0.2"),
                new TypeCase("DOUBLE", "CAST(0.10000000000000002 AS DOUBLE)", "CAST(0.2 AS DOUBLE)", "0.10000000000000002", "0.2"),
                new TypeCase("JSON", "CAST('{\"a\":1,\"s\":\"中文\"}' AS JSON)", "CAST('{\"b\":[2,3],\"a\":2}' AS JSON)",
                        "{\"s\":\"中文\",\"a\":1}", "{ \"a\":2, \"b\":[2,3] }"),
                new TypeCase("JSON", "CAST('null' AS JSON)", "CAST('[1,true]' AS JSON)", "null", "[1,true]"),
                new TypeCase("ENUM('新建','完成')", "'新建'", "'完成'", "1", "2"),
                new TypeCase("SET('red','blue','green')", "'red,blue'", "'green'", "3", "4"),
                new TypeCase("YEAR", "0", "2026", "0000", "2026"),
                new TypeCase("TIME(6)", "'-25:03:04.123456'", "'25:03:04.654321'", "-25:03:04.123456", "25:03:04.654321"),
                new TypeCase("TIME(6)", "'838:59:59'", "'-838:59:59'", "838:59:59", "-838:59:59"),
                new TypeCase("BINARY(4)", "X'00ff275c'", "X'01020304'", "\0\u00ff'\\", "\u0001\u0002\u0003\u0004"),
                new TypeCase("VARBINARY(10)", "X'00ff275c'", "X'01020304'", "\0\u00ff'\\", "\u0001\u0002\u0003\u0004"),
                new TypeCase("BLOB", "X'00ff275c'", "X'01020304'", "\0\u00ff'\\", "\u0001\u0002\u0003\u0004"),
                new TypeCase("TIMESTAMP(6)", "'2026-10-18 12:00:00.123456+08:00'", "'2026-10-19 13:14:15.654321+08:00'",
                        "2026-10-18 12:00:00.123456", "2026-10-19 13:14:15.654321"));
    }

    private void configureTypesTable(String columns) throws Exception {
        db.execute("DROP TABLE inventory");
        db.execute("CREATE TABLE inventory(id BIGINT PRIMARY KEY," + columns + ") CHARACTER SET utf8mb4");
        var platform = new DriverManagerDataSource(business.getUrl().replace("demo_db", "cdc_platform"), "root", "test-password");
        var tables = TableRegistry.capture(properties, store, business, platform);
        archive = new ArchiveService(properties, tables, new BinlogParser(), store, new TransactionTemplate(new DataSourceTransactionManager(platform)));
        recovery = new RecoveryService(properties, tables, store, db, new ObjectMapper());
        baseline = recovery.confirmBaseline("inventory");
    }

    private String typeEvent(EventType type, String before, String after, long offset) {
        var schema = store.findSchema(baseline.snapshotId());
        var definition = schema.columns().get(1);
        var row = CanalEntry.RowData.newBuilder();
        if (type != EventType.INSERT) row.addBeforeColumns(column("id", "1", 0, Types.BIGINT, "bigint", true))
                .addBeforeColumns(column("v", before, 1, definition.jdbcType(), definition.typeName(), false));
        if (type != EventType.DELETE) row.addAfterColumns(column("id", "1", 0, Types.BIGINT, "bigint", true))
                .addAfterColumns(column("v", after, 1, definition.jdbcType(), definition.typeName(), false));
        var entry = CanalEntry.Entry.newBuilder().setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder().setSchemaName("demo_db").setTableName("inventory")
                        .setLogfileName(baseline.binlogFile()).setLogfileOffset(baseline.position() + offset))
                .setStoreValue(CanalEntry.RowChange.newBuilder().setEventType(type).addRowDatas(row).build().toByteString()).build();
        archive.archive(new KafkaCanalRecord("example", 0, offset, new Message(offset, List.of(entry))));
        return store.search(properties, new ArchivedChange.Filter(null, null, null, null, null, null, 1)).items().get(0).eventId();
    }

    @ParameterizedTest(name="{0}") @MethodSource("recoveryTypes")
    void supportedTypesGenerateExecutableUpdateInsertAndDeleteRecovery(TypeCase sample) throws Exception {
        configureTypesTable("v " + sample.type());
        db.update("INSERT INTO inventory VALUES(1," + sample.afterSql() + ")");
        var update = recovery.createPlan(typeEvent(EventType.UPDATE, sample.beforeCanal(), sample.afterCanal(), 1));
        assertThat(update.executable()).describedAs(update.reason()).isTrue();
        assertThat(execute("NO_BACKSLASH_ESCAPES", update.generatedSql())).isEqualTo(1);
        assertRestoredValue(sample);
        assertThat(execute("", update.generatedSql())).isZero();
        db.update("DELETE FROM inventory");
        var insert = recovery.createPlan(typeEvent(EventType.DELETE, sample.beforeCanal(), null, 2));
        assertThat(insert.executable()).describedAs(insert.reason()).isTrue();
        assertThat(execute("NO_BACKSLASH_ESCAPES", insert.generatedSql())).isEqualTo(1);
        assertRestoredValue(sample);
        var delete = recovery.createPlan(typeEvent(EventType.INSERT, null, sample.beforeCanal(), 3));
        assertThat(delete.executable()).describedAs(delete.reason()).isTrue();
        assertThat(execute("", delete.generatedSql())).isEqualTo(1);
    }

    private void assertRestoredValue(TypeCase sample) {
        if (sample.type().startsWith("TIMESTAMP")) {
            var expected = java.math.BigDecimal.valueOf(java.time.Instant.parse("2026-10-18T04:00:00Z").getEpochSecond())
                    .add(new java.math.BigDecimal("0.123456"));
            assertThat(db.queryForObject("SELECT UNIX_TIMESTAMP(v) FROM inventory", java.math.BigDecimal.class)).isEqualByComparingTo(expected);
        } else {
            assertThat(db.queryForObject("SELECT v <=> " + sample.beforeSql() + " FROM inventory", Integer.class)).isEqualTo(1);
        }
    }

    @ParameterizedTest @ValueSource(strings={"+00:00", "+08:00", "-05:00"})
    void timestampRecoveryDoesNotDependOnExecutionSessionTimeZone(String targetZone) throws Exception {
        configureTypesTable("v TIMESTAMP(6) NULL");
        db.execute("INSERT INTO inventory VALUES(1,'2026-10-19 13:14:15.654321+08:00')");
        var plan = recovery.createPlan(typeEvent(EventType.UPDATE, "2026-10-18 12:00:00.123456", "2026-10-19 13:14:15.654321", 1));
        assertThat(plan.executable()).describedAs(plan.reason()).isTrue();
        try (var connection = business.getConnection(); var statement = connection.createStatement()) {
            statement.execute("SET time_zone='" + targetZone + "'");
            assertThat(statement.executeUpdate(plan.generatedSql())).isEqualTo(1);
            try (var result = statement.executeQuery("SELECT UNIX_TIMESTAMP(v) FROM inventory")) {
                result.next();
                var expected = java.math.BigDecimal.valueOf(java.time.Instant.parse("2026-10-18T04:00:00Z").getEpochSecond())
                        .add(new java.math.BigDecimal("0.123456"));
                assertThat(result.getBigDecimal(1)).isEqualByComparingTo(expected);
            }
        }
    }

    @Test void ordinaryFieldRecoveryPreservesAnUnchangedAutoUpdateTimestamp() throws Exception {
        configureTypesTable("v TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), note VARCHAR(20)");
        db.execute("INSERT INTO inventory VALUES(1,'2026-10-18 12:00:00.123456+08:00','new')");
        var row = CanalEntry.RowData.newBuilder();
        for (boolean after : List.of(false, true)) {
            var values = List.of(column("id", "1", 0, Types.BIGINT, "bigint", true),
                    column("v", "2026-10-18 12:00:00.123456", 1, Types.TIMESTAMP, "timestamp(6)", false),
                    column("note", after ? "new" : "old", 2, Types.VARCHAR, "varchar(20)", false));
            if (after) row.addAllAfterColumns(values); else row.addAllBeforeColumns(values);
        }
        var entry = CanalEntry.Entry.newBuilder().setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder().setSchemaName("demo_db").setTableName("inventory")
                        .setLogfileName(baseline.binlogFile()).setLogfileOffset(baseline.position()))
                .setStoreValue(CanalEntry.RowChange.newBuilder().setEventType(EventType.UPDATE).addRowDatas(row).build().toByteString()).build();
        archive.archive(new KafkaCanalRecord("example", 0, 1, new Message(1, List.of(entry))));
        String id = store.search(properties, new ArchivedChange.Filter(null,null,null,null,null,null,1)).items().get(0).eventId();
        var plan = recovery.createPlan(id);
        assertThat(plan.executable()).describedAs(plan.reason()).isTrue();
        assertThat(execute("", plan.generatedSql())).isEqualTo(1);
        assertThat(db.queryForObject("SELECT note FROM inventory", String.class)).isEqualTo("old");
        assertThat(db.queryForObject("SELECT UNIX_TIMESTAMP(v) FROM inventory", java.math.BigDecimal.class))
                .isEqualByComparingTo(java.math.BigDecimal.valueOf(java.time.Instant.parse("2026-10-18T04:00:00Z").getEpochSecond())
                        .add(new java.math.BigDecimal("0.123456")));
    }
    @ParameterizedTest @ValueSource(strings={"imported", "exported"})
    void foreignKeyBaselineAllowsOrdinaryFieldRecovery(String direction) throws Exception {
        if ("imported".equals(direction)) {
            db.execute("CREATE TABLE price_parent(price DECIMAL(30,4) PRIMARY KEY)");
            db.update("INSERT INTO price_parent VALUES(123456789012345.6789)");
            db.execute("ALTER TABLE inventory ADD CONSTRAINT fk_inventory_price FOREIGN KEY(price) REFERENCES price_parent(price)");
        } else {
            db.execute("ALTER TABLE inventory ADD CONSTRAINT uq_price UNIQUE(price)");
            db.execute("CREATE TABLE price_child(price DECIMAL(30,4), FOREIGN KEY(price) REFERENCES inventory(price))");
        }
        try {
            baseline=recovery.confirmBaseline("inventory");
            assertThat(store.findSchema(baseline.snapshotId()).columns().stream().filter(TableSchema.ColumnDefinition::foreignKey)
                    .map(TableSchema.ColumnDefinition::name)).containsExactly("price");
            insert("Bob");
            if ("exported".equals(direction)) db.update("INSERT INTO price_child VALUES(123456789012345.6789)");
            var plan=recovery.createPlan(event(EventType.UPDATE,"Alice","Bob"));
            assertThat(plan.executable()).isTrue();
            assertThat(execute("",plan.generatedSql())).isEqualTo(1);
            assertThat(db.queryForObject("SELECT name FROM inventory",String.class)).isEqualTo("Alice");
            db.update("UPDATE inventory SET name='Charlie'");
            assertThat(execute("",plan.generatedSql())).isZero();
        } finally {
            if ("imported".equals(direction)) {
                db.execute("ALTER TABLE inventory DROP FOREIGN KEY fk_inventory_price");
                db.execute("DROP TABLE price_parent");
            } else {
                db.execute("DROP TABLE price_child");
            }
        }
    }
}
