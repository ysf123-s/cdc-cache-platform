package com.example.canalcache.parser;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.example.canalcache.model.BinlogEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts Canal MQ FlatMessage records into internal BinlogEvent objects.
 */
@Component
public class FlatMessageBinlogParser {

    public List<BinlogEvent> parse(List<FlatMessage> flatMessages) {
        List<BinlogEvent> events = new ArrayList<>();
        for (FlatMessage flatMessage : flatMessages) {
            if (flatMessage == null || Boolean.TRUE.equals(flatMessage.getIsDdl())) {
                continue;
            }
            events.addAll(parseFlatMessage(flatMessage));
        }
        return events;
    }

    private List<BinlogEvent> parseFlatMessage(FlatMessage flatMessage) {
        List<BinlogEvent> events = new ArrayList<>();
        EventType eventType = EventType.valueOf(flatMessage.getType());
        List<Map<String, String>> dataRows = safeRows(flatMessage.getData());
        List<Map<String, String>> oldRows = safeRows(flatMessage.getOld());

        for (int index = 0; index < dataRows.size(); index++) {
            Map<String, Object> currentRow = copyRow(dataRows.get(index));
            Map<String, Object> beforeData = null;
            Map<String, Object> afterData = null;

            switch (eventType) {
                case INSERT:
                    afterData = currentRow;
                    break;
                case UPDATE:
                    afterData = currentRow;
                    beforeData = new LinkedHashMap<>(currentRow);
                    if (index < oldRows.size()) {
                        beforeData.putAll(copyRow(oldRows.get(index)));
                    }
                    break;
                case DELETE:
                    beforeData = currentRow;
                    break;
                default:
                    continue;
            }

            BinlogEvent event = BinlogEvent.builder()
                    .tableName(flatMessage.getTable())
                    .eventType(eventType)
                    .beforeData(beforeData)
                    .afterData(afterData)
                    .binlogFile("mq")
                    .binlogPosition(0L)
                    .eventTimestamp(flatMessage.getEs() == null ? 0L : flatMessage.getEs())
                    .build();
            events.add(event);
        }

        return events;
    }

    private List<Map<String, String>> safeRows(List<Map<String, String>> rows) {
        return rows == null ? List.of() : rows;
    }

    private Map<String, Object> copyRow(Map<String, String> row) {
        Map<String, Object> data = new LinkedHashMap<>();
        if (row != null) {
            data.putAll(row);
        }
        return data;
    }
}
