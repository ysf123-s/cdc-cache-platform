package com.example.canalcache.parser;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.model.UnprocessableEventException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void singleMessageParsingKeepsDdlForCacheConsumer() {
        FlatMessage truncate = new FlatMessage();
        truncate.setDatabase("demo_db");
        truncate.setTable("users");
        truncate.setType("TRUNCATE");
        truncate.setIsDdl(true);
        truncate.setSql("TRUNCATE TABLE users");

        List<BinlogEvent> events = parser.parseMessage(truncate);

        assertThat(events).singleElement().satisfies(event -> {
            assertThat(event.isDdl()).isTrue();
            assertThat(event.getEventType()).isEqualTo(EventType.TRUNCATE);
            assertThat(event.getTableName()).isEqualTo("users");
        });
    }

    @Test
    void unknownTypeIsUnprocessable() {
        FlatMessage unknown = new FlatMessage();
        unknown.setTable("users");
        unknown.setType("UPSERT");
        unknown.setIsDdl(false);

        assertThatThrownBy(() -> parser.parseMessage(unknown))
                .isInstanceOf(UnprocessableEventException.class)
                .hasMessageContaining("UPSERT");
    }
}
