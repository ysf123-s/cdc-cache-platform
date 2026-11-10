package com.example.canalcache.platform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.example.canalcache.platform.history.ArchivedChange;
import com.example.canalcache.platform.subscription.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import static com.example.canalcache.platform.ArchiveTestFixture.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class SubscriptionPipelineTest {

    private ArchiveTestFixture fixture;
    private HttpClient mockHttp;
    private DeliveryExecutor executor;

    @BeforeEach
    void setUp() throws Exception {
        fixture = new ArchiveTestFixture();

        // Register a target alias
        PlatformProperties.TargetConfig target = new PlatformProperties.TargetConfig();
        target.setUrl("http://internal-service/webhook");
        fixture.properties.getTargets().put("stock-hook", target);

        mockHttp = mock(HttpClient.class);
        executor = new DeliveryExecutor(fixture.properties, fixture.store, mockHttp, new ObjectMapper());
    }

    @Test
    @DisplayName("Archiving event automatically creates delivery task in same transaction when rule matches")
    void archivingCreatesDeliveryTaskAtomically() {
        // Save an active subscription: inventory UPDATE where available changes and becomes 0
        EventSubscription sub = new EventSubscription(
                "sub-stock-0", fixture.properties.getSourceId(), fixture.properties.getDatabase(),
                "inventory", "stock-hook", "UPDATE", "available", "available", "0",
                true, 1, null, null);
        fixture.store.saveSubscription(sub);

        // Archive UPDATE: available 10 -> 0
        CanalEntry.RowData row = CanalEntry.RowData.newBuilder()
                .addBeforeColumns(column("id", "301", 0, Types.BIGINT, "bigint", true))
                .addBeforeColumns(column("name", "Product A", 1, Types.VARCHAR, "varchar(64)", false))
                .addBeforeColumns(column("available", "10", 2, Types.INTEGER, "int", false))
                .addAfterColumns(column("id", "301", 0, Types.BIGINT, "bigint", true))
                .addAfterColumns(column("name", "Product A", 1, Types.VARCHAR, "varchar(64)", false))
                .addAfterColumns(column("available", "0", 2, Types.INTEGER, "int", false))
                .build();

        fixture.service.archive(fixture.record(201, fixture.rows(CanalEntry.EventType.UPDATE, row)));

        // Delivery task should exist in PENDING status
        List<DeliveryTask> tasks = fixture.store.listDeliveryTasks("PENDING", "sub-stock-0", null, 10);
        assertThat(tasks).hasSize(1);
        DeliveryTask task = tasks.get(0);
        assertThat(task.targetName()).isEqualTo("stock-hook");
        assertThat(task.targetUrl()).isEqualTo("http://internal-service/webhook");
        assertThat(task.attemptCount()).isZero();

        // Re-consume same Kafka record -> should NOT duplicate delivery task (uk_event_sub_round constraint)
        fixture.service.archive(fixture.record(201, fixture.rows(CanalEntry.EventType.UPDATE, row)));
        List<DeliveryTask> allTasks = fixture.store.listDeliveryTasks(null, "sub-stock-0", null, 10);
        assertThat(allTasks).hasSize(1);
    }

    @Test
    @DisplayName("Successful 2xx delivery transitions task to SUCCEEDED and logs attempt")
    @SuppressWarnings("unchecked")
    void successfulDeliveryMarksSucceeded() throws Exception {
        EventSubscription sub = new EventSubscription(
                "sub-success", fixture.properties.getSourceId(), fixture.properties.getDatabase(),
                "inventory", "stock-hook", "INSERT", null, null, null,
                true, 1, null, null);
        fixture.store.saveSubscription(sub);

        fixture.service.archive(fixture.record(202, fixture.rows(CanalEntry.EventType.INSERT, inserted("302", "item"))));

        HttpResponse<String> mockResp = mock(HttpResponse.class);
        when(mockResp.statusCode()).thenReturn(200);
        when(mockResp.body()).thenReturn("{\"status\":\"ok\"}");
        when(mockHttp.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(mockResp);

        int processed = executor.dispatch(10);
        assertThat(processed).isEqualTo(1);

        List<DeliveryTask> succeeded = fixture.store.listDeliveryTasks("SUCCEEDED", "sub-success", null, 10);
        assertThat(succeeded).hasSize(1);
        assertThat(succeeded.get(0).attemptCount()).isEqualTo(1);

        List<DeliveryAttempt> attempts = fixture.store.listAttempts(succeeded.get(0).taskId());
        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0).status()).isEqualTo("SUCCEEDED");
        assertThat(attempts.get(0).httpStatus()).isEqualTo(200);
        assertThat(attempts.get(0).responseSummary()).contains("{\"status\":\"ok\"}");
    }

    @Test
    @DisplayName("Temporary 500 error causes RETRY_WAIT, and max attempts exhaustion leads to FAILED")
    @SuppressWarnings("unchecked")
    void retryWaitThenFinalFailure() throws Exception {
        EventSubscription sub = new EventSubscription(
                "sub-retry", fixture.properties.getSourceId(), fixture.properties.getDatabase(),
                "inventory", "stock-hook", "INSERT", null, null, null,
                true, 1, null, null);
        fixture.store.saveSubscription(sub);

        fixture.service.archive(fixture.record(203, fixture.rows(CanalEntry.EventType.INSERT, inserted("303", "retry-item"))));

        HttpResponse<String> mockResp = mock(HttpResponse.class);
        when(mockResp.statusCode()).thenReturn(500);
        when(mockResp.body()).thenReturn("Internal Server Error");
        when(mockHttp.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(mockResp);

        // Attempt 1 -> RETRY_WAIT
        executor.dispatch(10);
        List<DeliveryTask> retryTasks = fixture.store.listDeliveryTasks("RETRY_WAIT", "sub-retry", null, 10);
        assertThat(retryTasks).hasSize(1);
        assertThat(retryTasks.get(0).attemptCount()).isEqualTo(1);

        // Force next_retry_at to past so it can be claimed again
        java.sql.Timestamp past = new java.sql.Timestamp(System.currentTimeMillis() - 5000L);
        fixture.jdbc.update("UPDATE delivery_task SET next_retry_at = ? WHERE task_id = ?",
                past, retryTasks.get(0).taskId());

        // Attempt 2 -> RETRY_WAIT
        executor.dispatch(10);
        DeliveryTask task2 = fixture.store.findDeliveryTask(retryTasks.get(0).taskId());
        assertThat(task2.attemptCount()).isEqualTo(2);
        assertThat(task2.status()).isEqualTo("RETRY_WAIT");

        fixture.jdbc.update("UPDATE delivery_task SET next_retry_at = ? WHERE task_id = ?",
                past, task2.taskId());

        // Attempt 3 (maxAttempts = 3) -> FAILED
        executor.dispatch(10);
        DeliveryTask finalTask = fixture.store.findDeliveryTask(retryTasks.get(0).taskId());
        assertThat(finalTask.attemptCount()).isEqualTo(3);
        assertThat(finalTask.status()).isEqualTo("FAILED");
        assertThat(finalTask.lastError()).contains("HTTP 500");

        List<DeliveryAttempt> attempts = fixture.store.listAttempts(finalTask.taskId());
        assertThat(attempts).hasSize(3);

        DeliveryTask manual = executor.manualRetry(finalTask.taskId());
        assertThat(manual.attemptCount()).isEqualTo(3);
        assertThat(manual.maxAttempts()).isEqualTo(4);
        executor.dispatch(10);
        assertThat(fixture.store.findDeliveryTask(manual.taskId()).status()).isEqualTo("FAILED");
        assertThat(fixture.store.listAttempts(manual.taskId())).extracting(DeliveryAttempt::attemptNumber)
                .containsExactly(1, 2, 3, 4);
    }

    @Test
    @DisplayName("Manual retry of FAILED task resets status to PENDING")
    @SuppressWarnings("unchecked")
    void manualRetryResetsFailedTask() throws Exception {
        EventSubscription sub = new EventSubscription(
                "sub-fail", fixture.properties.getSourceId(), fixture.properties.getDatabase(),
                "inventory", "stock-hook", "INSERT", null, null, null,
                true, 1, null, null);
        fixture.store.saveSubscription(sub);

        fixture.service.archive(fixture.record(204, fixture.rows(CanalEntry.EventType.INSERT, inserted("304", "fail-item"))));

        // 400 Bad Request: non-retryable client error, goes straight to FAILED
        HttpResponse<String> mockResp = mock(HttpResponse.class);
        when(mockResp.statusCode()).thenReturn(400);
        when(mockResp.body()).thenReturn("Bad Request");
        when(mockHttp.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(mockResp);

        executor.dispatch(10);

        List<DeliveryTask> failed = fixture.store.listDeliveryTasks("FAILED", "sub-fail", null, 10);
        assertThat(failed).hasSize(1);
        long taskId = failed.get(0).taskId();

        // Perform manual retry
        DeliveryTask retried = executor.manualRetry(taskId);
        assertThat(retried.status()).isEqualTo("PENDING");

        // Retrying a non-failed task throws IllegalStateException
        assertThatThrownBy(() -> executor.manualRetry(taskId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only FAILED tasks can be manually retried");

        // A manual retry is one additional attempt, even when the first 400 left unused automatic attempts.
        when(mockResp.statusCode()).thenReturn(500);
        executor.dispatch(10);
        DeliveryTask afterManual = fixture.store.findDeliveryTask(taskId);
        assertThat(afterManual.status()).isEqualTo("FAILED");
        assertThat(afterManual.attemptCount()).isEqualTo(2);
        assertThat(fixture.store.listAttempts(taskId)).extracting(DeliveryAttempt::attemptNumber).containsExactly(1, 2);
        executor.manualRetry(taskId);
        when(mockResp.statusCode()).thenReturn(200);
        executor.dispatch(10);
        assertThat(fixture.store.findDeliveryTask(taskId).status()).isEqualTo("SUCCEEDED");
        assertThat(fixture.store.listAttempts(taskId)).extracting(DeliveryAttempt::attemptNumber).containsExactly(1, 2, 3);
    }

    @Test
    void deliveryCursorCanReachTasksOlderThanTheLimitAndKeepsFilters() throws Exception {
        fixture.store.saveSubscription(new EventSubscription("sub-pages", fixture.properties.getSourceId(),
                fixture.properties.getDatabase(), "inventory", "stock-hook", "INSERT", null, null, null, true, 1, null, null));
        for (int i = 1; i <= 205; i++) {
            fixture.service.archive(fixture.record(i, fixture.rows(CanalEntry.EventType.INSERT, inserted("" + i, "item"))));
        }
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new SubscriptionController(
                fixture.properties, fixture.tables, fixture.store, executor)).build();
        String first = mvc.perform(get("/platform/deliveries").param("limit", "200")
                        .param("status", "PENDING").param("subscriptionId", "sub-pages"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(200))
                .andReturn().getResponse().getContentAsString();
        var page = new ObjectMapper().readTree(first);
        String cursor = page.get("nextCursor").asText();
        assertThat(cursor).isEqualTo(page.get("items").get(199).get("taskId").asText());
        mvc.perform(get("/platform/deliveries").param("limit", "200").param("cursor", cursor)
                        .param("status", "PENDING").param("subscriptionId", "sub-pages"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(5))
                .andExpect(jsonPath("$.nextCursor").isEmpty());
        mvc.perform(get("/platform/deliveries").param("cursor", cursor).param("status", "SUCCEEDED"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items").isEmpty());
        mvc.perform(get("/platform/deliveries").param("eventId", page.get("items").get(0).get("eventId").asText()))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(1));
        mvc.perform(get("/platform/deliveries").param("cursor", "0")).andExpect(status().isBadRequest());
        mvc.perform(get("/platform/deliveries").param("roundId", "INITIAL"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(50));
        mvc.perform(get("/platform/deliveries").param("roundId", "unknown-replay"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items").isEmpty());
    }

    @Test
    void removedExampleTargetCannotBeReenabledUntilRegisteredAgain() throws Exception {
        fixture.store.saveSubscription(new EventSubscription("removed-target", fixture.properties.getSourceId(),
                fixture.properties.getDatabase(), "inventory", "retired-hook", "INSERT", null, null, null, false, 1, null, null));
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new SubscriptionController(
                fixture.properties, fixture.tables, fixture.store, executor)).build();
        mvc.perform(put("/platform/subscriptions/removed-target/enabled").param("expectedVersion", "1")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"enabled\":true}"))
                .andExpect(status().isBadRequest());
        assertThat(fixture.store.findSubscription("removed-target").enabled()).isFalse();
        mvc.perform(put("/platform/subscriptions/removed-target/enabled").param("expectedVersion", "1")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"enabled\":false}"))
                .andExpect(status().isOk());
        fixture.properties.getTargets().put("retired-hook", fixture.properties.getTargets().get("stock-hook"));
        mvc.perform(put("/platform/subscriptions/removed-target/enabled").param("expectedVersion", "2")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"enabled\":true}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.enabled").value(true));
    }

    @Test
    @DisplayName("Subscription REST controller CRUD and conflict handling")
    void controllerCrudAndVersionConflict() throws Exception {
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new SubscriptionController(
                fixture.properties, fixture.tables, fixture.store, executor)).build();

        // 1. POST /platform/subscriptions
        String createJson = """
                {
                    "table": "inventory",
                    "targetName": "stock-hook",
                    "operations": ["UPDATE"],
                    "changedFields": ["available"],
                    "filterField": "available",
                    "filterValue": "0"
                }
                """;

        String resp = mvc.perform(post("/platform/subscriptions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(createJson))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.subscriptionId").exists())
                .andExpect(jsonPath("$.ruleVersion").value(1))
                .andReturn().getResponse().getContentAsString();

        String subId = new ObjectMapper().readTree(resp).get("subscriptionId").asText();

        // 2. GET /platform/subscriptions
        mvc.perform(get("/platform/subscriptions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].subscriptionId").value(subId));

        // 3. PUT /platform/subscriptions/{id} with wrong expectedVersion -> 409 Conflict
        String updateJson = """
                {
                    "table": "inventory",
                    "targetName": "stock-hook",
                    "operations": ["UPDATE"],
                    "enabled": false
                }
                """;
        mvc.perform(put("/platform/subscriptions/{id}", subId)
                        .param("expectedVersion", "99")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(updateJson))
                .andExpect(status().isConflict());

        // 4. PUT with correct expectedVersion=1 -> 200 OK and version becomes 2
        mvc.perform(put("/platform/subscriptions/{id}", subId)
                        .param("expectedVersion", "1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(updateJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ruleVersion").value(2))
                .andExpect(jsonPath("$.enabled").value(false));
    }
}
