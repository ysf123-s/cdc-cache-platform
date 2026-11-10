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
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.utility.DockerImageName;
import java.sql.Types;
import java.util.*;
import static com.example.canalcache.platform.ArchiveTestFixture.column;
import static org.assertj.core.api.Assertions.*;

/** Real MySQL SQL modes, collation, metadata and recovery execution. Requires Docker. */
@Testcontainers(disabledWithoutDocker = true)
class RecoveryMysqlTest {
    @Container static GenericContainer<?> mysql = new GenericContainer<>(DockerImageName.parse("mysql:8.0"))
            .withEnv("MYSQL_ROOT_PASSWORD","test-password").withEnv("MYSQL_ROOT_HOST","%")
            .withEnv("MYSQL_DATABASE","demo_db").withExposedPorts(3306)
            .withCommand("--server-id=11","--log-bin=mysql-bin","--binlog-format=ROW","--binlog-row-image=FULL")
            .waitingFor(Wait.forLogMessage(".*ready for connections.*",2));
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
}
