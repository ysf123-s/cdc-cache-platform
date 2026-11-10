package com.example.canalcache.platform;

import com.alibaba.otter.canal.client.kafka.MessageDeserializer;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.example.canalcache.model.KafkaCanalRecord;
import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.platform.history.*;
import com.example.canalcache.service.CanalCacheSyncService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.sql.Types;
import java.util.List;

import static com.example.canalcache.platform.ArchiveTestFixture.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class ArchivePipelineTest {
    private ArchiveTestFixture fixture;

    @BeforeEach
    void setUp() throws Exception { fixture = new ArchiveTestFixture(); }

    @Test
    void actualCanalSerializationStoresAllRowsAndReconsumptionDoesNotDuplicateThem() {
        KafkaCanalRecord original = fixture.record(20, fixture.rows(CanalEntry.EventType.INSERT,
                inserted("9223372036854775807", "中文'\\"), inserted("2", null)),
                fixture.rows(CanalEntry.EventType.INSERT, inserted("3", "third")));
        CanalPacket.Messages.Builder messages = CanalPacket.Messages.newBuilder().setBatchId(original.message().getId());
        original.message().getEntries().forEach(entry -> messages.addMessages(entry.toByteString()));
        byte[] bytes = CanalPacket.Packet.newBuilder().setType(CanalPacket.PacketType.MESSAGES)
                .setCompression(CanalPacket.Compression.NONE).setBody(messages.build().toByteString()).build().toByteArray();
        KafkaCanalRecord decoded = new KafkaCanalRecord(original.topic(), original.partition(), original.offset(),
                new MessageDeserializer().deserialize(original.topic(), bytes));

        assertThat(fixture.service.archive(decoded).inserted()).isEqualTo(3);
        assertThat(fixture.service.archive(decoded).duplicates()).isEqualTo(3);
        assertThat(fixture.count()).isEqualTo(3);
        ArchivedChange.Page page = fixture.store.search(fixture.properties, filter(null, null, 20));
        assertThat(page.items()).extracting(ArchivedChange.Summary::eventId).doesNotHaveDuplicates();
        ArchivedChange detail = fixture.store.find(page.items().get(2).eventId(), fixture.properties);
        assertThat(detail.event().getAfterData()).containsEntry("id", "9223372036854775807").containsEntry("name", "中文'\\");
        assertThat(detail.event().getAfterColumns().get(0).jdbcType()).isEqualTo(Types.BIGINT);
        assertThat(detail.offset()).isEqualTo("20");
        assertThat(detail.recovery().available()).isFalse();
    }

    @Test
    void failingLaterRowRollsBackEarlierRowsInTheSameKafkaRecord() {
        CanalEntry.RowData missingKey = CanalEntry.RowData.newBuilder()
                .addAfterColumns(column("name", "broken", 1, Types.VARCHAR, "varchar(64)", false)).build();
        assertThatThrownBy(() -> fixture.service.archive(fixture.record(21,
                fixture.rows(CanalEntry.EventType.INSERT, inserted("1", "valid"), missingKey))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("primary key");
        assertThat(fixture.count()).isZero();
    }

    @Test
    void recordsWithoutCacheAnnotationsAreArchivedAndOtherTablesAreFiltered() {
        CanalEntry.RowChange change = CanalEntry.RowChange.newBuilder().setEventType(CanalEntry.EventType.INSERT)
                .addRowDatas(inserted("1", "registered only in platform")).build();
        ArchiveService.Result result = fixture.service.archive(fixture.record(22,
                fixture.entry("inventory", change), fixture.entry("not_registered", change)));
        assertThat(result.inserted()).isEqualTo(1);
        assertThat(result.filtered()).isEqualTo(1);
    }

    @Test
    void ddlIsVisibleToPlatformAndInvalidatesSnapshotsButRemainsFilteredForCache() {
        CanalEntry.Entry ddl = fixture.entry("inventory", CanalEntry.RowChange.newBuilder()
                .setEventType(CanalEntry.EventType.ALTER).setIsDdl(true).setSql("ALTER TABLE inventory ADD label INT").build());
        assertThat(new BinlogParser().parse(List.of(ddl))).isEmpty();
        assertThat(fixture.service.archive(fixture.record(23, ddl)).ddl()).isEqualTo(1);
        assertThat(fixture.tables.views().get(0).invalidatedReason()).contains("ALTER", "example:2:23");
        assertThat(fixture.count()).isZero();
    }

    @Test
    void updateDistinguishesNullFromMissingAndComparesExactNumbersWithoutDoubleConversion() {
        CanalEntry.RowData row = CanalEntry.RowData.newBuilder()
                .addBeforeColumns(column("id", "1", 0, Types.BIGINT, "bigint", true))
                .addBeforeColumns(column("available", "9007199254740993.00", 1, Types.DECIMAL, "decimal(20,2)", false))
                .addBeforeColumns(column("remark", null, 2, Types.VARCHAR, "varchar(255)", false))
                .addAfterColumns(column("id", "1", 0, Types.BIGINT, "bigint", true))
                .addAfterColumns(column("available", "9007199254740993.0", 1, Types.DECIMAL, "decimal(20,2)", false))
                .addAfterColumns(column("remark", "changed", 2, Types.VARCHAR, "varchar(255)", false))
                .addAfterColumns(column("name", null, 3, Types.VARCHAR, "varchar(64)", false)).build();
        fixture.service.archive(fixture.record(24, fixture.rows(CanalEntry.EventType.UPDATE, row)));
        String id = fixture.store.search(fixture.properties, filter(null, null, 20)).items().get(0).eventId();
        List<ArchivedChange.FieldChange> changes = fixture.store.find(id, fixture.properties).changes();
        assertThat(changes).extracting(ArchivedChange.FieldChange::field).containsExactly("remark", "name");
        assertThat(changes.get(1).beforePresent()).isFalse();
        assertThat(changes.get(1).afterPresent()).isTrue();
    }

    @Test
    void cursorPaginationDoesNotLoseRowsWithIdenticalSourceTimestamps() {
        for (int index = 0; index < 5; index++) {
            fixture.service.archive(fixture.record(30 + index, fixture.rows(CanalEntry.EventType.INSERT, inserted("" + index, "same time"))));
        }
        ArchivedChange.Page first = fixture.store.search(fixture.properties, filter(null, null, 2));
        ArchivedChange.Page second = fixture.store.search(fixture.properties, filter(Long.valueOf(first.nextCursor()), null, 2));
        ArchivedChange.Page third = fixture.store.search(fixture.properties, filter(Long.valueOf(second.nextCursor()), null, 2));
        assertThat(List.of(first.items().get(0).eventId(), first.items().get(1).eventId(), second.items().get(0).eventId(),
                second.items().get(1).eventId(), third.items().get(0).eventId())).doesNotHaveDuplicates();
        assertThat(third.nextCursor()).isNull();
        assertThat(fixture.store.search(fixture.properties, filter(null, "' OR 1=1 --", 20)).items()).isEmpty();
    }

    @Test
    void actualHttpControllerReadsPersistedEventsAndRejectsInvalidFilters() throws Exception {
        fixture.service.archive(fixture.record(40, fixture.rows(CanalEntry.EventType.INSERT, inserted("9007199254740993", "web path"))));
        String eventId = fixture.store.search(fixture.properties, filter(null, null, 20)).items().get(0).eventId();
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new HistoryController(fixture.properties, fixture.tables, fixture.store,
                mock(ArchiveConsumer.class), mock(CanalCacheSyncService.class))).build();
        mvc.perform(get("/platform/events").param("table", "inventory").param("key", "9007199254740993"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items[0].primaryKeyValue").value("9007199254740993"));
        mvc.perform(get("/platform/events/{id}", eventId)).andExpect(status().isOk())
                .andExpect(jsonPath("$.event.afterData.name").value("web path")).andExpect(jsonPath("$.offset").value("40"));
        mvc.perform(get("/platform/events").param("cursor", "")).andExpect(status().isBadRequest());
        mvc.perform(get("/platform/events").param("cursor", "9223372036854775808")).andExpect(status().isBadRequest());
        mvc.perform(get("/platform/events").param("table", "not_registered")).andExpect(status().isNotFound());
        mvc.perform(get("/platform/events").param("operation", "TRUNCATE")).andExpect(status().isBadRequest());
        mvc.perform(get("/platform/events").param("limit", "101")).andExpect(status().isBadRequest());
    }

    private ArchivedChange.Filter filter(Long before, String key, int limit) {
        return new ArchivedChange.Filter(before, null, key, null, null, null, limit);
    }
}
