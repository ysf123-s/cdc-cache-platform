package com.example.canalcache.platform;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.platform.subscription.*;
import org.junit.jupiter.api.*;
import java.time.Instant;
import java.util.List;
import static com.example.canalcache.platform.ArchiveTestFixture.*;
import static org.assertj.core.api.Assertions.*;

class DeliveryRepairTest {
    private ArchiveTestFixture f;
    @BeforeEach void setup() throws Exception {
        f=new ArchiveTestFixture();
        var target=new PlatformProperties.TargetConfig();target.setUrl("http://localhost/hook");f.properties.getTargets().put("hook",target);
        f.store.saveSubscription(new EventSubscription("sub",f.properties.getSourceId(),f.properties.getDatabase(),"inventory","hook","INSERT",
                null,null,null,true,1,null,null));
        f.service.archive(f.record(1,f.rows(EventType.INSERT,inserted("1","A"))));
    }
    private DeliveryAttempt success(DeliveryTask task,String response) {
        return new DeliveryAttempt(0,task.taskId(),task.attemptCount()+1,"SUCCEEDED",200,1,null,response,null,Instant.now().toString());
    }
    @Test void resultAndAttemptRollbackTogetherAndCanBeRetried() {
        var task=f.store.claimTasks("worker",1,30).get(0);
        assertThatThrownBy(()->f.store.finishDelivery(task,success(task,"x".repeat(2000)))).isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThat(f.store.listAttempts(task.taskId())).isEmpty();
        assertThat(f.store.findDeliveryTask(task.taskId()).status()).isEqualTo("IN_FLIGHT");
        assertThat(f.store.finishDelivery(task,success(task,"OK"))).isTrue();
        assertThat(f.store.findDeliveryTask(task.taskId()).attemptCount()).isEqualTo(1);
        assertThat(f.store.listAttempts(task.taskId())).hasSize(1);
    }
    @Test void leaseTakeoverRejectsOldAttemptBeforeItCanInsertAnything() {
        var old=f.store.claimTasks("same-instance",1,30).get(0);
        f.jdbc.update("UPDATE delivery_task SET lease_until=? WHERE task_id=?",new java.sql.Timestamp(1),old.taskId());
        var replacement=f.store.claimTasks("same-instance",1,30).get(0);
        assertThat(replacement.leaseOwner()).isNotEqualTo(old.leaseOwner());
        assertThat(f.store.finishDelivery(old,success(old,"late"))).isFalse();
        assertThat(f.store.listAttempts(old.taskId())).isEmpty();
        assertThat(f.store.finishDelivery(replacement,success(replacement,"OK"))).isTrue();
        assertThat(f.store.findDeliveryTask(old.taskId()).status()).isEqualTo("SUCCEEDED");
    }
    @Test void expiredLeaseCannotWriteAResultEvenBeforeTakeover() {
        var task=f.store.claimTasks("worker",1,30).get(0);
        f.jdbc.update("UPDATE delivery_task SET lease_until=? WHERE task_id=?",new java.sql.Timestamp(1),task.taskId());
        assertThat(f.store.finishDelivery(task,success(task,"late"))).isFalse();
        assertThat(f.store.listAttempts(task.taskId())).isEmpty();
    }
    @Test void concurrentClaimersCannotBothOwnTheSameTask() throws Exception {
        var pool=java.util.concurrent.Executors.newFixedThreadPool(2);
        var start=new java.util.concurrent.CountDownLatch(1);
        try {
            var a=pool.submit(()->{start.await();return f.store.claimTasks("a",1,30);});
            var b=pool.submit(()->{start.await();return f.store.claimTasks("b",1,30);});
            start.countDown();
            assertThat(a.get().size()+b.get().size()).isEqualTo(1);
        } finally {pool.shutdownNow();}
    }
}
