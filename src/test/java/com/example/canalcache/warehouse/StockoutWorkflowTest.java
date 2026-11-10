package com.example.canalcache.warehouse;

import com.example.canalcache.CanalCacheSyncApplication;
import com.example.canalcache.platform.ArchiveTestFixture;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class StockoutWorkflowTest {
    private JdbcDataSource db;
    private StockoutStore store;
    private MockMvc mvc;
    private final ObjectMapper json = new CanalCacheSyncApplication().objectMapper();
    private final String eventId = "a".repeat(64);

    @BeforeEach void setup() throws Exception {
        db = ArchiveTestFixture.database("stockout");
        ArchiveTestFixture.initializeStockout(db);
        store = new StockoutStore(db);
        mvc = MockMvcBuilders.standaloneSetup(new StockoutController(store, json)).build();
    }

    private String notification(String deliveryId, String id, int before, int after) throws Exception {
        return json.writeValueAsString(Map.of("deliveryId", deliveryId, "eventId", id, "attempt", 1,
                "event", Map.of("tableName", "inventory", "eventType", "UPDATE", "eventTimestamp", 1700000000000L,
                        "beforeData", Map.of("id", "3", "sku", "SKU-3", "available_qty", "" + before),
                        "afterData", Map.of("id", "3", "sku", "SKU-3", "available_qty", "" + after))));
    }

    @Test void receiveCompleteAndReplayKeepsOnePersistentHandledTask() throws Exception {
        String response = mvc.perform(post("/warehouse/stockout-events").contentType(MediaType.APPLICATION_JSON)
                        .content(notification("initial", eventId, 5, 0)))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.sku").value("SKU-3")).andReturn().getResponse().getContentAsString();
        String id = json.readTree(response).get("id").asText();
        String handled = mvc.perform(post("/warehouse/stockout-tasks/{id}/complete", id))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("HANDLED"))
                .andReturn().getResponse().getContentAsString();
        // New receiver instance reads the same business DB, rather than an in-memory dedup map.
        mvc = MockMvcBuilders.standaloneSetup(new StockoutController(new StockoutStore(db), json)).build();
        mvc.perform(post("/warehouse/stockout-events").contentType(MediaType.APPLICATION_JSON)
                        .content(notification("new-replay-delivery", eventId, 5, 0)))
                .andExpect(status().isOk()).andExpect(content().json(handled));
        mvc.perform(get("/warehouse/stockout-tasks").param("eventId", eventId))
                .andExpect(jsonPath("$.items.length()").value(1));
        mvc.perform(post("/warehouse/stockout-tasks/{id}/complete", id))
                .andExpect(content().json(handled));
    }

    @Test void concurrentDuplicateDeliveriesCreateOneTask() throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        var start = new CountDownLatch(1);
        try {
            var a = pool.submit(() -> { start.await(); return store.receive(eventId, 3, "SKU-3", 1700000000000L); });
            var b = pool.submit(() -> { start.await(); return store.receive(eventId, 3, "SKU-3", 1700000000000L); });
            start.countDown();
            assertThat(a.get().id()).isEqualTo(b.get().id());
            assertThat(store.list(null, null, null, 20).items()).hasSize(1);
        } finally { pool.shutdownNow(); }
    }

    @Test void listPagesAndFiltersAndCompletionOfMissingTask() throws Exception {
        for (int i = 1; i <= 21; i++) store.receive(String.format("%064x", i), i, "SKU-" + i, 1700000000000L);
        var first = store.list("PENDING", null, null, 20);
        assertThat(first.items()).hasSize(20);
        var second = store.list("PENDING", null, Long.valueOf(first.nextCursor()), 20);
        assertThat(second.items()).hasSize(1); assertThat(second.nextCursor()).isNull();
        store.complete(second.items().get(0).id());
        assertThat(store.list("HANDLED", null, null, 20).items()).hasSize(1);
        mvc.perform(post("/warehouse/stockout-tasks/99999/complete")).andExpect(status().isNotFound());
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"eventId\":\"invalid\"}", "{\"eventId\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"event\":{\"tableName\":\"users\"}}"})
    void invalidNotificationsDoNotCreateTasks(String body) throws Exception {
        mvc.perform(post("/warehouse/stockout-events").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());
        assertThat(store.list(null, null, null, 20).items()).isEmpty();
    }

    @Test void onlyPositiveToZeroCreatesAStockoutTask() throws Exception {
        for (var quantities : List.of(new int[]{5, 1}, new int[]{0, 0}, new int[]{-1, 0})) {
            mvc.perform(post("/warehouse/stockout-events").contentType(MediaType.APPLICATION_JSON)
                    .content(notification("d", eventId, quantities[0], quantities[1]))).andExpect(status().isBadRequest());
        }
        assertThat(store.list(null, null, null, 20).items()).isEmpty();
    }

    @Test void databaseFailureIsNotAcknowledgedAsSuccess() {
        new JdbcTemplate(db).execute("DROP TABLE stockout_task");
        assertThatThrownBy(() -> mvc.perform(post("/warehouse/stockout-events").contentType(MediaType.APPLICATION_JSON)
                .content(notification("d", eventId, 5, 0))))
                .hasCauseInstanceOf(org.springframework.dao.DataAccessException.class);
    }
}
