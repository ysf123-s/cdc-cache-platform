package com.example.canalcache.parser;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.model.BinlogEvent;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FlatMessageBinlogParserTest {

    private final FlatMessageBinlogParser parser = new FlatMessageBinlogParser();

    @Test
    void parsesInsertFlatMessageForNegativeCacheInvalidation() {
        FlatMessage flatMessage = new FlatMessage();
        flatMessage.setTable("users");
        flatMessage.setType("INSERT");
        flatMessage.setIsDdl(false);
        flatMessage.setData(List.of(Map.of("id", "100", "name", "alice")));

        List<BinlogEvent> events = parser.parse(List.of(flatMessage));

        assertThat(events).hasSize(1);
        assertThat(events.get(0).getEventType()).isEqualTo(EventType.INSERT);
        assertThat(events.get(0).getAfterData()).containsEntry("id", "100");
        assertThat(events.get(0).getBeforeData()).isNull();
    }

    @Test
    void ddlIsStillFiltered() {
        FlatMessage ddl = new FlatMessage();
        ddl.setType("CREATE");
        ddl.setIsDdl(true);

        assertThat(parser.parse(List.of(ddl))).isEmpty();
    }
}
