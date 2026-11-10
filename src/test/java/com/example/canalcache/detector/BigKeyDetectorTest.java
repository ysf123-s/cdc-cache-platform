package com.example.canalcache.detector;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BigKeyDetectorTest {

    private final BigKeyDetector detector = new BigKeyDetector(new ObjectMapper());

    @Test
    void detectsPayloadLargerThanThreshold() {
        Map<String, Object> data = Map.of("name", "zhangsan", "bio", "x".repeat(128));

        assertThat(detector.isBigKey(data, 32)).isTrue();
        assertThat(detector.isBigKey(data, 1024)).isFalse();
    }

    @Test
    void returnsZeroSizeForNullPayload() {
        assertThat(detector.estimateSizeBytes(null)).isZero();
    }
}
