package com.example.canalcache.platform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.platform.history.*;
import com.example.canalcache.platform.recovery.*;
import com.example.canalcache.service.CanalCacheSyncService;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.http.MediaType;
import java.sql.Types;
import java.time.Instant;
import static com.example.canalcache.platform.ArchiveTestFixture.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** H2 verifies persistence and pre-checks. Exported MySQL statements are executed in RecoveryMysqlTest. */
class RecoveryPipelineTest {
    private ArchiveTestFixture f;
    private JdbcTemplate db;
    @BeforeEach void setup() throws Exception {f=new ArchiveTestFixture();db=new JdbcTemplate(f.business);}
    private CanalEntry.RowData image(EventType op,String beforeName,String afterName) {
        var row=CanalEntry.RowData.newBuilder();
        if(op!=EventType.INSERT) row.addBeforeColumns(column("id","1",0,Types.BIGINT,"bigint",true))
                .addBeforeColumns(column("name",beforeName,1,Types.VARCHAR,"varchar(64)",false))
                .addBeforeColumns(column("available","10",2,Types.INTEGER,"int",false))
                .addBeforeColumns(column("remark",null,3,Types.VARCHAR,"varchar(255)",false));
        if(op!=EventType.DELETE) row.addAfterColumns(column("id","1",0,Types.BIGINT,"bigint",true))
                .addAfterColumns(column("name",afterName,1,Types.VARCHAR,"varchar(64)",false))
                .addAfterColumns(column("available","10",2,Types.INTEGER,"int",false))
                .addAfterColumns(column("remark",null,3,Types.VARCHAR,"varchar(255)",false));
        return row.build();
    }
    private String archive(EventType type,CanalEntry.RowData row) {
        f.service.archive(f.record(1,f.rows(type,row)));
        return f.store.search(f.properties,new ArchivedChange.Filter(null,null,null,null,null,null,1)).items().get(0).eventId();
    }
    @Test void unverifiedBaselineCannotGenerateSql() {
        db.update("INSERT INTO inventory VALUES(1,'A',10,NULL)");
        String id=archive(EventType.INSERT,image(EventType.INSERT,null,"A"));
        assertThat(f.recoveryService.createPlan(id).executable()).isFalse();
        assertThat(f.store.find(id,f.properties).recovery().available()).isFalse();
        assertThat(f.tables.views().get(0).recoveryAvailable()).isFalse();
    }
    @Test void verifiedInsertPreviewDoesNotExecuteItself() {
        f.assumeVerifiedBaselineForUnitTest();db.update("INSERT INTO inventory VALUES(1,'A',10,NULL)");
        var plan=f.recoveryService.createPlan(archive(EventType.INSERT,image(EventType.INSERT,null,"A")));
        assertThat(plan.executable()).isTrue();assertThat(plan.reverseType()).isEqualTo("DELETE");
        assertThat(plan.generatedSql()).contains("AS BINARY","LIMIT 1;");
        assertThat(db.queryForObject("SELECT COUNT(*) FROM inventory",Long.class)).isEqualTo(1L);
    }
    @Test void verifiedUpdatePreviewRestoresChangedFieldsAndIncludesGuard() {
        f.assumeVerifiedBaselineForUnitTest();db.update("INSERT INTO inventory VALUES(1,'B',10,NULL)");
        var plan=f.recoveryService.createPlan(archive(EventType.UPDATE,image(EventType.UPDATE,"A","B")));
        assertThat(plan.executable()).isTrue();assertThat(plan.generatedSql()).contains(" SET `name` = CONVERT(X'41' USING utf8mb4)","X'42'");
    }
    @Test void verifiedDeletePreviewPreservesKeyAndChecksConflict() {
        f.assumeVerifiedBaselineForUnitTest();String id=archive(EventType.DELETE,image(EventType.DELETE,"A",null));
        assertThat(f.recoveryService.createPlan(id).executable()).isTrue();
        db.update("INSERT INTO inventory VALUES(1,'other',10,NULL)");
        assertThat(f.recoveryService.createPlan(id).reason()).contains("Primary key conflict");
    }
    @Test void fullRowPrecheckRejectsCaseAndWhitespaceChanges() {
        f.assumeVerifiedBaselineForUnitTest();db.update("INSERT INTO inventory VALUES(1,'A ',10,NULL)");
        String id=archive(EventType.INSERT,image(EventType.INSERT,null,"A"));
        assertThat(f.recoveryService.createPlan(id).reason()).contains("further modified");
        db.update("UPDATE inventory SET name='a'");
        assertThat(f.recoveryService.createPlan(id).executable()).isFalse();
    }
    @Test void incompleteOldImageCannotBecomeNullAssignment() {
        f.assumeVerifiedBaselineForUnitTest();db.update("INSERT INTO inventory VALUES(1,'A',10,NULL)");
        var row=CanalEntry.RowData.newBuilder().addBeforeColumns(column("id","1",0,Types.BIGINT,"bigint",true))
                .addAllAfterColumns(image(EventType.INSERT,null,"A").getAfterColumnsList()).build();
        var plan=f.recoveryService.createPlan(archive(EventType.UPDATE,row));
        assertThat(plan.executable()).isFalse();assertThat(plan.reason()).contains("Incomplete before");
        assertThat(plan.generatedSql()).doesNotContain(" SET ");
    }
    @Test void currentSchemaAndDdlAreCheckedBeforeGeneration() {
        f.assumeVerifiedBaselineForUnitTest();db.update("INSERT INTO inventory VALUES(1,'A',10,NULL)");
        String id=archive(EventType.INSERT,image(EventType.INSERT,null,"A"));
        db.execute("ALTER TABLE inventory ADD COLUMN extra INT");
        assertThat(f.recoveryService.createPlan(id).reason()).contains("schema differs");
        f.store.invalidateSchemas(f.properties.getSourceId(),f.properties.getDatabase(),"ALTER");
        assertThat(f.recoveryService.createPlan(id).reason()).contains("DDL");
    }
    @Test void olderBinlogEventsAreExcludedByBaselinePosition() {
        var schema=f.tables.get(f.properties.getDatabase(),"inventory");
        f.store.saveBaseline(schema,new RecoveryBaseline(f.properties.getSourceId(),f.properties.getDatabase(),"inventory",schema.snapshotId(),
                "mysql-bin.000001",501,"test-server",Instant.now().toString()));
        var plan=f.recoveryService.createPlan(archive(EventType.INSERT,image(EventType.INSERT,null,"A")));
        assertThat(plan.reason()).contains("precedes");
    }
    @Test void endpointsPersistPreviewAndDownloadTheSameSql() throws Exception {
        f.assumeVerifiedBaselineForUnitTest();db.update("INSERT INTO inventory VALUES(1,'A',10,NULL)");
        String id=archive(EventType.INSERT,image(EventType.INSERT,null,"A"));
        var mvc=MockMvcBuilders.standaloneSetup(new HistoryController(f.properties,f.tables,f.store,mock(ArchiveConsumer.class),
                mock(CanalCacheSyncService.class),f.recoveryService)).setControllerAdvice(new PlatformExceptionHandler()).build();
        String body=mvc.perform(post("/platform/recovery-plans").contentType(MediaType.APPLICATION_JSON).content("{\"eventId\":\""+id+"\"}"))
                .andExpect(status().isCreated()).andExpect(jsonPath("$.executable").value(true)).andReturn().getResponse().getContentAsString();
        String planId=new com.fasterxml.jackson.databind.ObjectMapper().readTree(body).get("planId").asText();
        mvc.perform(get("/platform/recovery-plans/"+planId+"/sql")).andExpect(status().isOk())
                .andExpect(content().string(f.recoveryService.getPlan(planId).generatedSql()));
        mvc.perform(post("/platform/tables/inventory/recovery-baseline")).andExpect(status().isConflict());
    }
    @Test void backloggedDdlDoesNotInvalidateANewerBaselineButLaterDdlDoes() {
        f.assumeVerifiedBaselineForUnitTest();
        var ddl=f.entry("inventory",CanalEntry.RowChange.newBuilder().setEventType(EventType.ALTER).setIsDdl(true)
                .setSql("ALTER TABLE inventory ADD extra INT").build());
        f.service.archive(f.record(2,ddl.toBuilder().setHeader(ddl.getHeader().toBuilder().setLogfileOffset(499)).build()));
        assertThat(f.tables.views().get(0).recoveryAvailable()).isTrue();
        f.service.archive(f.record(3,ddl.toBuilder().setHeader(ddl.getHeader().toBuilder().setLogfileOffset(501)).build()));
        assertThat(f.tables.views().get(0).recoveryAvailable()).isFalse();
        assertThat(f.tables.views().get(0).reason()).contains("DDL");
    }
    @Test void baselineComparesBinlogNumbersAndRejectsUnknownNamespaces() {
        var baseline=new RecoveryBaseline("s","d","t","snap","mysql-bin.999999",500,"uuid",Instant.now().toString());
        var event=com.example.canalcache.model.BinlogEvent.builder().binlogFile("mysql-bin.1000000").binlogPosition(4).build();
        assertThat(baseline.covers(event)).isTrue();
        event.setBinlogFile("other-bin.1000000");
        assertThat(baseline.covers(event)).isFalse();
        assertThat(baseline.isBeforeBaseline(event)).isFalse();
    }
}
