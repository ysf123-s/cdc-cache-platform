package com.example.canalcache.parser;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.example.canalcache.model.BinlogEvent;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BinlogParserTest {

    private final BinlogParser parser = new BinlogParser();

    @Test
    void parsesInsertRowDataForNegativeCacheInvalidation() {
        CanalEntry.Entry entry = buildRowEntry(
                CanalEntry.EventType.INSERT,
                List.of(),
                List.of(
                        column("id", "bigint", "100", true, false),
                        column("name", "varchar(64)", "alice", false, false),
                        column("age", "int", "18", false, false)
                )
        );

        List<BinlogEvent> events = parser.parse(List.of(entry));

        assertThat(events).hasSize(1);
        assertThat(events.get(0).getEventType()).isEqualTo(CanalEntry.EventType.INSERT);
        assertThat(events.get(0).getAfterData()).containsEntry("id", "100").containsEntry("name", "alice");
        assertThat(events.get(0).getBeforeData()).isNull();
    }

    @Test
    void parsesUpdateRowDataWithoutDroppingUnchangedColumns() {
        CanalEntry.Entry entry = buildRowEntry(
                CanalEntry.EventType.UPDATE,
                List.of(
                        column("id", "bigint", "100", false, false),
                        column("name", "varchar(64)", "alice", false, false)
                ),
                List.of(
                        column("id", "bigint", "100", false, false),
                        column("name", "varchar(64)", "alice-new", true, false)
                )
        );

        BinlogEvent event = parser.parse(List.of(entry)).get(0);

        assertThat(event.getAfterData())
                .containsEntry("id", "100")
                .containsEntry("name", "alice-new");
    }

    private CanalEntry.Entry buildRowEntry(
            CanalEntry.EventType eventType,
            List<CanalEntry.Column> beforeColumns,
            List<CanalEntry.Column> afterColumns) {

        CanalEntry.RowData rowData = CanalEntry.RowData.newBuilder()
                .addAllBeforeColumns(beforeColumns)
                .addAllAfterColumns(afterColumns)
                .build();

        CanalEntry.RowChange rowChange = CanalEntry.RowChange.newBuilder()
                .setEventType(eventType)
                .addRowDatas(rowData)
                .build();

        CanalEntry.Header header = CanalEntry.Header.newBuilder()
                .setSchemaName("demo_db")
                .setTableName("users")
                .setEventType(eventType)
                .setLogfileName("mysql-bin.000001")
                .setLogfileOffset(12345L)
                .setExecuteTime(1700000000000L)
                .build();

        return CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(header)
                .setStoreValue(rowChange.toByteString())
                .build();
    }

    private CanalEntry.Column column(
            String name,
            String mysqlType,
            String value,
            boolean updated,
            boolean isNull) {

        return CanalEntry.Column.newBuilder()
                .setName(name)
                .setMysqlType(mysqlType)
                .setValue(value)
                .setUpdated(updated)
                .setIsNull(isNull)
                .build();
    }
}
