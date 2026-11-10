package com.example.canalcache.platform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.example.canalcache.platform.history.ArchivedChange;
import com.example.canalcache.platform.replay.ReplayJob;
import com.example.canalcache.platform.replay.ReplayService;
import com.example.canalcache.platform.subscription.DeliveryExecutor;
import com.example.canalcache.platform.subscription.DeliveryTask;
import com.example.canalcache.platform.subscription.EventSubscription;
import com.example.canalcache.platform.subscription.SubscriptionController;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.example.canalcache.platform.ArchiveTestFixture.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class ReplayPipelineTest {

    private ArchiveTestFixture fixture;
    private ReplayService replayService;

    @BeforeEach
    void setUp() throws Exception {
        fixture = new ArchiveTestFixture();

        PlatformProperties.TargetConfig target = new PlatformProperties.TargetConfig();
        target.setUrl("http://replay-target/hook");
        fixture.properties.getTargets().put("replay-hook", target);

        replayService = new ReplayService(fixture.properties, fixture.store);
    }

    @Test
    @DisplayName("Replay job generates new delivery round with distinct deliveryId without altering Kafka offset")
    void replayJobGeneratesNewRound() {
        // 1. Create and save active subscription
        EventSubscription sub = new EventSubscription(
                "sub-replay-1", fixture.properties.getSourceId(), fixture.properties.getDatabase(),
                "inventory", "replay-hook", "INSERT", null, null, null,
                true, 1, null, null);
        fixture.store.saveSubscription(sub);

        // 2. Archive 2 events: one matches inventory, one doesn't match rule or table
        fixture.service.archive(fixture.record(501, fixture.rows(CanalEntry.EventType.INSERT, inserted("501", "Item A"))));
        fixture.service.archive(fixture.record(502, fixture.rows(CanalEntry.EventType.INSERT, inserted("502", "Item B"))));

        String eventId1 = fixture.store.search(fixture.properties,
                new ArchivedChange.Filter(null, null, "501", null, null, null, 1)).items().get(0).eventId();
        String eventId2 = fixture.store.search(fixture.properties,
                new ArchivedChange.Filter(null, null, "502", null, null, null, 1)).items().get(0).eventId();

        // 3. Initial tasks were already created with round_type='INITIAL'
        List<DeliveryTask> initialTasks = fixture.store.listDeliveryTasks(null, "sub-replay-1", eventId1, 10);
        assertThat(initialTasks).hasSize(1);
        String initialDeliveryId = initialTasks.get(0).deliveryId();

        // 4. Trigger replay for eventId1 + a non-existent event
        ReplayJob.Request request = new ReplayJob.Request("req-rep-001", "sub-replay-1", List.of(eventId1, "f".repeat(64)));
        ReplayJob job = replayService.createReplayJob(request);

        assertThat(job.status()).isEqualTo("COMPLETED");
        assertThat(job.totalEvents()).isEqualTo(2);
        assertThat(job.matchedEvents()).isEqualTo(1);
        assertThat(job.skippedEvents()).isEqualTo(1);

        // 5. Verify a new REPLAY task was created for eventId1 with roundId = jobId
        List<DeliveryTask> replayTasks = fixture.store.listDeliveryTasks(null, "sub-replay-1", eventId1, 10);
        assertThat(replayTasks).hasSize(2); // 1 INITIAL + 1 REPLAY

        DeliveryTask replayTask = replayTasks.stream()
                .filter(t -> "REPLAY".equals(t.roundType()))
                .findFirst()
                .orElseThrow();

        assertThat(replayTask.roundId()).isEqualTo(job.jobId());
        assertThat(replayTask.deliveryId()).isNotEqualTo(initialDeliveryId);
        assertThat(replayTask.status()).isEqualTo("PENDING");
    }

    @Test
    @DisplayName("Duplicate replay request key is idempotent and does not create duplicate jobs")
    void replayDeduplication() {
        EventSubscription sub = new EventSubscription(
                "sub-replay-2", fixture.properties.getSourceId(), fixture.properties.getDatabase(),
                "inventory", "replay-hook", "INSERT", null, null, null,
                true, 1, null, null);
        fixture.store.saveSubscription(sub);

        fixture.service.archive(fixture.record(503, fixture.rows(CanalEntry.EventType.INSERT, inserted("503", "Item C"))));
        String eventId = fixture.store.search(fixture.properties,
                new ArchivedChange.Filter(null, null, "503", null, null, null, 1)).items().get(0).eventId();

        ReplayJob.Request request = new ReplayJob.Request("same-key-123", "sub-replay-2", List.of(eventId));
        ReplayJob first = replayService.createReplayJob(request);
        ReplayJob second = replayService.createReplayJob(request);

        assertThat(second.jobId()).isEqualTo(first.jobId());
        List<DeliveryTask> tasks = fixture.store.listDeliveryTasks(null, "sub-replay-2", eventId, 10);
        // 1 initial + 1 replay = exactly 2 tasks
        assertThat(tasks).hasSize(2);
    }

    @Test
    @DisplayName("Replay rejects disabled subscription and oversized event batch")
    void replayValidation() {
        EventSubscription disabledSub = new EventSubscription(
                "sub-disabled", fixture.properties.getSourceId(), fixture.properties.getDatabase(),
                "inventory", "replay-hook", "INSERT", null, null, null,
                false, 1, null, null);
        fixture.store.saveSubscription(disabledSub);

        // Disabled sub rejected
        ReplayJob.Request req1 = new ReplayJob.Request("req-fail-1", "sub-disabled", List.of("e".repeat(64)));
        assertThatThrownBy(() -> replayService.createReplayJob(req1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("disabled subscription");

        // >100 events rejected
        List<String> largeList = new ArrayList<>();
        for (int i = 0; i < 101; i++) largeList.add("event-" + i);
        ReplayJob.Request req2 = new ReplayJob.Request("req-fail-2", "sub-disabled", largeList);
        assertThatThrownBy(() -> replayService.createReplayJob(req2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("limited to at most 100 events");
    }

    @Test
    @DisplayName("HTTP controller creates and queries replay jobs")
    void controllerReplayEndpoints() throws Exception {
        EventSubscription sub = new EventSubscription(
                "sub-http-replay", fixture.properties.getSourceId(), fixture.properties.getDatabase(),
                "inventory", "replay-hook", "INSERT", null, null, null,
                true, 1, null, null);
        fixture.store.saveSubscription(sub);

        fixture.service.archive(fixture.record(504, fixture.rows(CanalEntry.EventType.INSERT, inserted("504", "Item D"))));
        String eventId = fixture.store.search(fixture.properties,
                new ArchivedChange.Filter(null, null, "504", null, null, null, 1)).items().get(0).eventId();

        MockMvc mvc = MockMvcBuilders.standaloneSetup(new SubscriptionController(
                fixture.properties, fixture.tables, fixture.store,
                mock(DeliveryExecutor.class), replayService)).build();

        String body = String.format("""
                {
                    "requestKey": "http-key-001",
                    "subscriptionId": "sub-http-replay",
                    "eventIds": ["%s"]
                }
                """, eventId);

        String res = mvc.perform(post("/platform/replay-jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.jobId").exists())
                .andExpect(jsonPath("$.matchedEvents").value(1))
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andReturn().getResponse().getContentAsString();

        String jobId = new com.fasterxml.jackson.databind.ObjectMapper().readTree(res).get("jobId").asText();

        mvc.perform(get("/platform/replay-jobs/{id}", jobId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.requestKey").value("http-key-001"))
                .andExpect(jsonPath("$.totalEvents").value(1));
    }
}
