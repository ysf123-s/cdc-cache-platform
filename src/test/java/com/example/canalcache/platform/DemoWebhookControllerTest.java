package com.example.canalcache.platform.subscription;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DemoWebhookControllerTest {

    private DemoWebhookController controller;

    @BeforeEach
    void setUp() {
        controller = new DemoWebhookController();
    }

    @Test
    void normalDeliveryAcceptedAndDuplicatesRecognized() {
        ResponseEntity<Map<String, Object>> resp1 = controller.receiveWebhook(
                "del-101", "evt-201", Map.of("op", "UPDATE", "row", Map.of("id", 1))
        );
        assertThat(resp1.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp1.getBody()).containsEntry("status", "RECEIVED");
        assertThat(resp1.getBody()).containsEntry("isDuplicate", false);
        assertThat(resp1.getBody()).containsEntry("deliverCount", 1);

        // 重复投递同一 deliveryId
        ResponseEntity<Map<String, Object>> resp2 = controller.receiveWebhook(
                "del-101", "evt-201", Map.of("op", "UPDATE", "row", Map.of("id", 1))
        );
        assertThat(resp2.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp2.getBody()).containsEntry("isDuplicate", true);
        assertThat(resp2.getBody()).containsEntry("deliverCount", 2);

        Map<String, Object> records = controller.listRecords();
        assertThat(records.get("distinctDeliveries")).isEqualTo(1);
        assertThat(records.get("totalReceived")).isEqualTo(2);
    }

    @Test
    void chaosModesTriggerExpectedStatuses() {
        // 模拟 500
        controller.setChaosMode(new DemoWebhookController.ChaosConfig("500"));
        ResponseEntity<Map<String, Object>> resp500 = controller.receiveWebhook("del-1", "evt-1", Map.of());
        assertThat(resp500.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);

        // 模拟 429
        controller.setChaosMode(new DemoWebhookController.ChaosConfig("429"));
        ResponseEntity<Map<String, Object>> resp429 = controller.receiveWebhook("del-2", "evt-2", Map.of());
        assertThat(resp429.getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);

        // 模拟 FAIL_ONCE (首发 503，再次 200)
        controller.setChaosMode(new DemoWebhookController.ChaosConfig("FAIL_ONCE"));
        ResponseEntity<Map<String, Object>> respFail = controller.receiveWebhook("del-3", "evt-3", Map.of());
        assertThat(respFail.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);

        ResponseEntity<Map<String, Object>> respRecover = controller.receiveWebhook("del-3", "evt-3", Map.of());
        assertThat(respRecover.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(respRecover.getBody()).containsEntry("deliverCount", 1);
    }

    @Test
    void resetClearsAllState() {
        controller.receiveWebhook("del-99", "evt-99", Map.of());
        controller.reset();
        Map<String, Object> records = controller.listRecords();
        assertThat(records.get("totalReceived")).isEqualTo(0);
        assertThat(records.get("distinctDeliveries")).isEqualTo(0);
        assertThat(records.get("currentChaosMode")).isEqualTo("NORMAL");
    }
}
