package com.example.canalcache.parser;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.model.BinlogColumn;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts Canal row change entries into internal BinlogEvent objects.
 */
@Slf4j
@Component
public class BinlogParser {

    public List<BinlogEvent> parse(List<Entry> entries) {
        return parseIncludingDdl(entries).stream().filter(event -> !event.isDdl()).toList();
    }

    /** Platform entry point. Cache consumers continue to use parse(), which filters DDL. */
    public List<BinlogEvent> parseIncludingDdl(List<Entry> entries) {
        List<BinlogEvent> events = new ArrayList<>();

        for (int entryIndex = 0; entryIndex < entries.size(); entryIndex++) {
            Entry entry = entries.get(entryIndex);
            if (entry.getEntryType() != EntryType.ROWDATA) {
                continue;
            }

            try {
                RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
                if (rowChange.getIsDdl()) {
                    events.add(baseEvent(entry, entryIndex, -1, rowChange.getEventType())
                            .ddl(true).ddlSql(rowChange.getSql()).build());
                } else {
                    events.addAll(parseRowChange(entry, entryIndex, rowChange));
                }
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException("Failed to parse RowChange at "
                        + entry.getHeader().getLogfileName() + ":"
                        + entry.getHeader().getLogfileOffset(), e);
            }
        }

        return events;
    }

    private List<BinlogEvent> parseRowChange(Entry entry, int entryIndex, RowChange rowChange) {
        List<BinlogEvent> events = new ArrayList<>();
        EventType eventType = rowChange.getEventType();
        for (int rowIndex = 0; rowIndex < rowChange.getRowDatasCount(); rowIndex++) {
            RowData rowData = rowChange.getRowDatas(rowIndex);
            BinlogEvent event = baseEvent(entry, entryIndex, rowIndex, eventType)
                    .beforeColumns(columnDetails(rowData.getBeforeColumnsList()))
                    .afterColumns(columnDetails(rowData.getAfterColumnsList())).build();

            switch (eventType) {
                case INSERT:
                    event.setAfterData(parseColumns(rowData.getAfterColumnsList()));
                    break;
                case UPDATE:
                    event.setBeforeData(parseColumns(rowData.getBeforeColumnsList()));
                    event.setAfterData(parseColumns(rowData.getAfterColumnsList()));
                    break;
                case DELETE:
                    event.setBeforeData(parseColumns(rowData.getBeforeColumnsList()));
                    break;
                default:
                    continue;
            }

            events.add(event);
        }

        return events;
    }

    private BinlogEvent.BinlogEventBuilder baseEvent(Entry entry, int entryIndex, int rowIndex,
                                                     EventType eventType) {
        return BinlogEvent.builder().databaseName(entry.getHeader().getSchemaName())
                .tableName(entry.getHeader().getTableName()).entryIndex(entryIndex).rowIndex(rowIndex)
                .eventType(eventType).binlogFile(entry.getHeader().getLogfileName())
                .binlogPosition(entry.getHeader().getLogfileOffset())
                .eventTimestamp(entry.getHeader().getExecuteTime());
    }

    private List<BinlogColumn> columnDetails(List<Column> columns) {
        return columns.stream().map(column -> new BinlogColumn(column.getIndex(), column.getName(),
                column.getSqlType(), column.getMysqlType(), column.getIsKey(), column.getIsNull(),
                column.getUpdated(), column.getIsNull() ? null : column.getValue())).toList();
    }

    private Map<String, Object> parseColumns(List<Column> columns) {
        Map<String, Object> data = new LinkedHashMap<>();
        for (Column column : columns) {
            data.put(column.getName(), column.getIsNull() ? null : column.getValue());
        }
        return data;
    }
}
