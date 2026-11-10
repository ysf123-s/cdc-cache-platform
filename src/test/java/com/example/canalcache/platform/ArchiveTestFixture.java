package com.example.canalcache.platform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.example.canalcache.model.KafkaCanalRecord;
import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.platform.history.*;
import com.example.canalcache.platform.recovery.RecoveryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.h2.jdbcx.JdbcDataSource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Types;
import java.util.List;
import java.util.UUID;

/** Real relational storage/transactions, with H2's MySQL mode; this does not certify MySQL compatibility. */
public class ArchiveTestFixture {
    public final JdbcDataSource business = database("business");
    public final JdbcDataSource platform = database("platform");
    public final JdbcTemplate jdbc = new JdbcTemplate(platform);
    public final PlatformProperties properties = new PlatformProperties();
    public final ArchiveStore store = new ArchiveStore(jdbc, new ObjectMapper());
    public final TableRegistry tables;
    public final ArchiveService service;
    public final RecoveryService recoveryService;

    public ArchiveTestFixture() throws Exception {
        new JdbcTemplate(business).execute("CREATE TABLE inventory (id BIGINT PRIMARY KEY, name VARCHAR(64), available INT, remark VARCHAR(255))");
        initializePlatform(platform);
        try (Connection connection = business.getConnection()) { properties.setDatabase(connection.getCatalog()); }
        properties.setTables(List.of("inventory"));
        tables = TableRegistry.capture(properties, store, business, platform);
        service = new ArchiveService(properties, tables, new BinlogParser(), store,
                new TransactionTemplate(new DataSourceTransactionManager(platform)));
        recoveryService = new RecoveryService(properties, tables, store, new JdbcTemplate(business), new ObjectMapper());
    }

    public static JdbcDataSource database(String prefix) {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + prefix + "_" + UUID.randomUUID().toString().replace("-", "")
                + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        return dataSource;
    }

    public static void initializePlatform(javax.sql.DataSource target) throws Exception {
        String sql = Files.readString(Path.of("docker/mysql/init/03-platform.sql"), StandardCharsets.UTF_8);
        sql = sql.substring(sql.indexOf("CREATE TABLE"))
                .replace(" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin", "");
        try (Connection connection = target.getConnection()) {
            ScriptUtils.executeSqlScript(connection, new ByteArrayResource(sql.getBytes(StandardCharsets.UTF_8)));
        }
    }

    public static void initializeStockout(javax.sql.DataSource target) throws Exception {
        String sql = Files.readString(Path.of("examples/stockout/schema.sql"), StandardCharsets.UTF_8);
        sql = sql.substring(sql.indexOf("CREATE TABLE"))
                .replace(" CHARACTER SET ascii COLLATE ascii_bin", "")
                .replace(" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", "");
        try (Connection connection = target.getConnection()) {
            ScriptUtils.executeSqlScript(connection, new ByteArrayResource(sql.getBytes(StandardCharsets.UTF_8)));
        }
    }

    public KafkaCanalRecord record(long offset, CanalEntry.Entry... entries) {
        return new KafkaCanalRecord("example", 2, offset, new Message(99L, List.of(entries)));
    }

    public CanalEntry.Entry rows(CanalEntry.EventType operation, CanalEntry.RowData... rows) {
        return entry("inventory", CanalEntry.RowChange.newBuilder().setEventType(operation).addAllRowDatas(List.of(rows)).build());
    }

    public CanalEntry.Entry entry(String table, CanalEntry.RowChange change) {
        return CanalEntry.Entry.newBuilder().setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder().setSchemaName(properties.getDatabase()).setTableName(table)
                        .setExecuteTime(1700000000000L).setLogfileName("mysql-bin.000001").setLogfileOffset(500))
                .setStoreValue(change.toByteString()).build();
    }

    public static CanalEntry.RowData inserted(String id, String name) {
        return CanalEntry.RowData.newBuilder().addAfterColumns(column("id", id, 0, Types.BIGINT, "bigint", true))
                .addAfterColumns(column("name", name, 1, Types.VARCHAR, "varchar(64)", false)).build();
    }

    public static CanalEntry.Column column(String name, String value, int index, int type, String mysqlType, boolean key) {
        CanalEntry.Column.Builder column = CanalEntry.Column.newBuilder().setName(name).setIndex(index).setSqlType(type)
                .setMysqlType(mysqlType).setIsKey(key).setIsNull(value == null).setUpdated(true);
        if (value != null) { column.setValue(value); }
        return column.build();
    }

    public long count() { return jdbc.queryForObject("SELECT COUNT(*) FROM change_event", Long.class); }

    /** Synthetic unit-test premise only; no claim that H2 verifies MySQL ROW/FULL. */
    public void assumeVerifiedBaselineForUnitTest() {
        var schema = tables.get(properties.getDatabase(), "inventory");
        store.saveBaseline(schema, new com.example.canalcache.platform.recovery.RecoveryBaseline(properties.getSourceId(),
                properties.getDatabase(), "inventory", schema.snapshotId(), "mysql-bin.000001", 500, "test-server", java.time.Instant.now().toString()));
    }
}
