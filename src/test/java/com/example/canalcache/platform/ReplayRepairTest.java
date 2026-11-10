package com.example.canalcache.platform;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.platform.history.*;
import com.example.canalcache.platform.replay.*;
import com.example.canalcache.platform.subscription.*;
import org.junit.jupiter.api.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import static com.example.canalcache.platform.ArchiveTestFixture.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;

class ReplayRepairTest {
    private ArchiveTestFixture f;
    private EventSubscription sub;
    private List<String> ids;
    @BeforeEach void setup() throws Exception {
        f=new ArchiveTestFixture();
        var target=new PlatformProperties.TargetConfig();target.setUrl("http://original/hook");f.properties.getTargets().put("hook",target);
        sub=new EventSubscription("sub",f.properties.getSourceId(),f.properties.getDatabase(),"inventory","hook","INSERT",null,null,null,true,1,null,null);
        f.store.saveSubscription(sub);
        for(int i=1;i<=2;i++) f.service.archive(f.record(i,f.rows(EventType.INSERT,inserted(""+i,"A"))));
        ids=f.store.search(f.properties,new ArchivedChange.Filter(null,null,null,null,null,null,10)).items().stream().map(ArchivedChange.Summary::eventId).sorted().toList();
    }
    private ReplayJob pending() {
        var job=new ReplayJob("job","request",f.properties.getSourceId(),f.properties.getDatabase(),"inventory","sub","PENDING",2,0,0,0,
                ids,f.store.encode(sub),"http://original/hook",null,Instant.now().toString(),Instant.now().toString());
        f.store.saveReplayJob(job);return job;
    }
    @Test void failedRoundRollsBackAllTasksThenRestartUsesFrozenRuleAndTarget() {
        pending();
        var spyStore=spy(f.store);
        AtomicInteger calls=new AtomicInteger();
        doAnswer(call->{if(calls.incrementAndGet()==2) throw new org.springframework.dao.DataAccessResourceFailureException("injected interruption");return call.callRealMethod();})
                .when(spyStore).createDeliveryTask(any(DeliveryTask.class));
        assertThatThrownBy(()->new ReplayService(f.properties,spyStore).processJob("job")).hasMessageContaining("injected interruption");
        assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM delivery_task WHERE round_id='job'",Long.class)).isZero();
        assertThat(f.store.findReplayJob("job").status()).isEqualTo("PENDING");
        f.properties.getTargets().get("hook").setUrl("http://new/hook");
        f.store.updateSubscription(new EventSubscription("sub",sub.sourceId(),sub.database(),sub.table(),"hook","DELETE",null,null,null,false,1,null,null));
        new ReplayService(f.properties,f.store).resumePending();
        assertThat(f.store.findReplayJob("job").status()).isEqualTo("COMPLETED");
        assertThat(f.store.findReplayJob("job").matchedEvents()).isEqualTo(2);
        assertThat(f.store.listDeliveryTasks(null,"sub",null,10).stream().filter(t->t.roundId().equals("job")))
                .hasSize(2).allSatisfy(t->assertThat(t.targetUrl()).isEqualTo("http://original/hook"));
    }
    @Test void duplicateRequestCannotChangeItsMeaning() {
        var service=new ReplayService(f.properties,f.store);
        var original=service.createReplayJob(new ReplayJob.Request("same","sub",ids));
        assertThat(service.createReplayJob(new ReplayJob.Request("same","sub",ids)).jobId()).isEqualTo(original.jobId());
        assertThatThrownBy(()->service.createReplayJob(new ReplayJob.Request("same","sub",List.of(ids.get(0)))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("different replay request");
    }

    @Test void replayDoesNotReadRecoveryMetadata() {
        pending();
        var spyStore = spy(f.store);
        doThrow(new AssertionError("Replay must not read recovery schema")).when(spyStore).findSchema(anyString());
        var job = new ReplayService(f.properties, spyStore).processJob("job");
        assertThat(job.status()).isEqualTo("COMPLETED");
        assertThat(job.matchedEvents()).isEqualTo(2);
        assertThat(f.store.replayDeliveryStats("job")).containsEntry("PENDING", 2L);
    }
}
