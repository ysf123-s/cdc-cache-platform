package com.example.canalcache.service;

import com.example.canalcache.controller.SyncStatusController;
import com.example.canalcache.detector.BigKeyDetector;
import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.parser.FlatMessageBinlogParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class SyncStatusTest {
    private final CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
    private final CanalConnectorWrapper.PartitionConsumer consumer = mock(CanalConnectorWrapper.PartitionConsumer.class);
    private final CanalCacheSyncService service = new CanalCacheSyncService(connector,
            new BinlogParser(), new FlatMessageBinlogParser(), mock(CacheAnnotationProcessor.class),
            mock(VersionControllerWithLua.class), mock(BigKeyDetector.class), mock(RedisTemplate.class),
            new ObjectMapper(), new SimpleMeterRegistry());

    @Test
    void startupFailureReportsNoActiveConsumerEvenWhenServiceWasStarted() throws Exception {
        when(connector.getConsumerThreadCount()).thenReturn(2);
        when(connector.createPartitionConsumer()).thenThrow(new IllegalStateException("invalid Kafka configuration"));
        ReflectionTestUtils.setField(service, "running", true);

        ReflectionTestUtils.invokeMethod(service, "syncLoop");

        var mvc = MockMvcBuilders.standaloneSetup(new SyncStatusController(service)).build();
        mvc.perform(get("/sync/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.serviceStarted").value(true))
                .andExpect(jsonPath("$.configuredConsumers").value(2))
                .andExpect(jsonPath("$.activeConsumers").value(0))
                .andExpect(jsonPath("$.lastSuccessAt").doesNotExist())
                .andExpect(jsonPath("$.lastError.type").value("IllegalStateException"))
                .andExpect(jsonPath("$.lastError.message").value("invalid Kafka configuration"))
                .andExpect(jsonPath("$.lastError.occurredAt").exists());
    }

    @Test
    @Timeout(10)
    void activeConsumerCountTracksActualLoopAndIdleDoesNotBecomeFailure() {
        assertThat(service.getStatus().serviceStarted()).isFalse();
        assertThat(service.getStatus().activeConsumers()).isZero();
        when(connector.createPartitionConsumer()).thenReturn(consumer);
        when(consumer.getRecordsWithoutAck()).thenAnswer(invocation -> {
            assertThat(service.getStatus().activeConsumers()).isEqualTo(1);
            service.stop();
            return List.of();
        });
        ReflectionTestUtils.setField(service, "running", true);

        ReflectionTestUtils.invokeMethod(service, "syncLoop");

        assertThat(service.getStatus().activeConsumers()).isZero();
        assertThat(service.getStatus().lastError()).isNull();
        assertThat(service.getStatus().lastSuccessAt()).isNull();
        verify(consumer).close();
    }
}
