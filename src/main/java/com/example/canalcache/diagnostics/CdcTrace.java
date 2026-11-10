package com.example.canalcache.diagnostics;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * CDC 端到端链路分段耗时跟踪记录。
 *
 * 计时规范：
 * 1. 跨进程时间点统一采用 System.currentTimeMillis()（毫秒时间戳），避免跨进程单调时钟基准不一致；
 * 2. 同一 JVM 内部的分段间隔严格采用 System.nanoTime() 差值纳秒计时，避免系统时钟回拨或跳变；
 * 3. 压测中以 code（如 cdc-load-xxxx-w00-000012）为唯一关联主键，并记录 Binlog 位点与 Kafka 分区/位移。
 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CdcTrace {
    private String traceKey;
    private String compositeKey;
    private String code;
    private Long departmentId;
    private String binlogFile;
    private Long binlogPosition;
    private Long encodedVersion;
    private Integer kafkaPartition;
    private Long kafkaOffset;

    // 跨进程基准时间戳（Epoch ms）
    private Long requestStartEpochMs;
    private Long requestEndEpochMs;
    private Long binlogExecuteEpochMs;
    private Long consumerReceiveEpochMs;
    private Long redisLuaEndEpochMs;

    // JVM 内部精确耗时（纳秒 ns）
    private Long controllerCostNs;
    private Long sqlCostNs;
    private Long commitCostNs;
    private Long fetchCostNs;
    private Long protobufParseCostNs;
    private Long redisLuaCostNs;
    private Long canalAckCostNs;

    private boolean writeCompleted;
    private boolean consumerCompleted;

    public CdcTrace(String traceKey) {
        this.traceKey = traceKey;
        this.code = traceKey;
    }

    public void updateCompositeKey() {
        this.compositeKey = String.format("%s:%s:%s",
                code != null ? code : "unknown",
                departmentId != null ? departmentId : 0,
                encodedVersion != null ? encodedVersion : 0);
    }

    public Double getControllerCostMs() {
        return controllerCostNs != null ? round3(controllerCostNs / 1_000_000.0) : null;
    }

    public Double getSqlCostMs() {
        return sqlCostNs != null ? round3(sqlCostNs / 1_000_000.0) : null;
    }

    public Double getCommitCostMs() {
        return commitCostNs != null ? round3(commitCostNs / 1_000_000.0) : null;
    }

    public Double getFetchCostMs() {
        return fetchCostNs != null ? round3(fetchCostNs / 1_000_000.0) : null;
    }

    public Double getProtobufParseCostMs() {
        return protobufParseCostNs != null ? round3(protobufParseCostNs / 1_000_000.0) : null;
    }

    public Double getRedisLuaCostMs() {
        return redisLuaCostNs != null ? round3(redisLuaCostNs / 1_000_000.0) : null;
    }

    public Double getCanalAckCostMs() {
        return canalAckCostNs != null ? round3(canalAckCostNs / 1_000_000.0) : null;
    }

    public Double getAsyncTransitMs() {
        if (consumerReceiveEpochMs != null && requestEndEpochMs != null) {
            return round3((double) Math.max(0, consumerReceiveEpochMs - requestEndEpochMs));
        }
        return null;
    }

    public Double getTotalPipelineMs() {
        if (redisLuaEndEpochMs != null && requestStartEpochMs != null) {
            return round3((double) Math.max(0, redisLuaEndEpochMs - requestStartEpochMs));
        }
        return null;
    }

    private static double round3(double val) {
        return Math.round(val * 1000.0) / 1000.0;
    }
}
