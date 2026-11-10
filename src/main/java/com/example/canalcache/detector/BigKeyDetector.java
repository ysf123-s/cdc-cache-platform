package com.example.canalcache.detector;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 大 Key 检测器
 */
@Slf4j
@Component
public class BigKeyDetector {

    private final ObjectMapper objectMapper;

    public BigKeyDetector(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * 检查数据是否为大 Key
     * @param data 数据对象
     * @param thresholdBytes 阈值（字节）
     * @return true=是大 Key
     */
    public boolean isBigKey(Map<String, Object> data, int thresholdBytes) {
        if (data == null) {
            return false;
        }

        try {
            byte[] bytes = objectMapper.writeValueAsBytes(data);
            // 算字节数
            return bytes.length > thresholdBytes;
        } catch (Exception e) {
            log.warn("Failed to serialize data for big key check", e);
            return false;
        }
    }

    /**
     * 估算对象的字节大小
     */
    public int estimateSizeBytes(Map<String, Object> data) {
        if (data == null) {
            return 0;
        }

        try {
            byte[] bytes = objectMapper.writeValueAsBytes(data);
            return bytes.length;
        } catch (Exception e) {
            log.warn("Failed to estimate size for data", e);
            return -1;
        }
    }

    /**
     * 获取人类可读的大小描述
     */
    public String humanReadableSize(int bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        } else {
            return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        }
    }
}
