package com.example.canalcache.diagnostics;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 链路分段耗时收集器。
 *
 * 1. 严格使用有界内存（最大 5000 条），淘汰最老记录；
 * 2. 仅对压测标识（如 cdc-load- 开头）生效，避免无关生产业务内存开销；
 * 3. 线程安全，统一聚合写入端与消费端跨进程关联信息。
 */
@Slf4j
@Component
public class TraceCollector {
    private static final int MAX_TRACES = 5000;
    private static final String DEFAULT_PREFIX = "cdc-load-";

    private final Map<String, CdcTrace> traces = new LinkedHashMap<>(MAX_TRACES, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, CdcTrace> eldest) {
            return size() > MAX_TRACES;
        }
    };

    private boolean isTarget(String code) {
        return code != null && (code.startsWith(DEFAULT_PREFIX) || code.startsWith("cdc-trace-"));
    }

    public synchronized void recordWrite(String code, Long departmentId, long startEpochMs,
                                        long endEpochMs, long controllerCostNs, long sqlCostNs) {
        if (!isTarget(code)) {
            return;
        }
        CdcTrace trace = traces.computeIfAbsent(code, CdcTrace::new);
        if (trace.isConsumerCompleted()) {
            return;
        }
        trace.setCode(code);
        trace.setDepartmentId(departmentId);
        trace.setRequestStartEpochMs(startEpochMs);
        trace.setRequestEndEpochMs(endEpochMs);
        trace.setControllerCostNs(controllerCostNs);
        trace.setSqlCostNs(sqlCostNs);
        trace.setCommitCostNs(0L); // MySQL autocommit included in sqlCostNs
        trace.setWriteCompleted(true);
        trace.updateCompositeKey();
    }

    public synchronized void recordConsumerLua(String code, Long encodedVersion, String binlogFile,
                                              Long binlogPosition, Long binlogExecuteEpochMs,
                                              Integer kafkaPartition, Long kafkaOffset,
                                              Long fetchCostNs, Long protobufParseCostNs,
                                              Long consumerReceiveEpochMs,
                                              long redisLuaCostNs, long redisLuaEndEpochMs) {
        if (!isTarget(code)) {
            return;
        }
        CdcTrace trace = traces.computeIfAbsent(code, CdcTrace::new);
        if (trace.isConsumerCompleted()) {
            return;
        }
        trace.setEncodedVersion(encodedVersion);
        trace.setBinlogFile(binlogFile);
        trace.setBinlogPosition(binlogPosition);
        trace.setBinlogExecuteEpochMs(binlogExecuteEpochMs);
        trace.setKafkaPartition(kafkaPartition);
        trace.setKafkaOffset(kafkaOffset);
        trace.setFetchCostNs(fetchCostNs);
        trace.setProtobufParseCostNs(protobufParseCostNs);
        trace.setConsumerReceiveEpochMs(consumerReceiveEpochMs);
        trace.setRedisLuaCostNs(redisLuaCostNs);
        trace.setRedisLuaEndEpochMs(redisLuaEndEpochMs);
        trace.updateCompositeKey();
    }

    public synchronized void recordConsumerAck(List<String> codes, long canalAckCostNs) {
        if (codes == null || codes.isEmpty()) {
            return;
        }
        for (String code : codes) {
            if (isTarget(code)) {
                CdcTrace trace = traces.get(code);
                if (trace != null) {
                    trace.setCanalAckCostNs(canalAckCostNs);
                    trace.setConsumerCompleted(true);
                }
            }
        }
    }

    public synchronized List<CdcTrace> getTraces(String prefix, int limit) {
        List<CdcTrace> list = new ArrayList<>();
        for (CdcTrace trace : traces.values()) {
            if (prefix == null || prefix.isBlank() || trace.getCode() != null && trace.getCode().startsWith(prefix)) {
                list.add(trace);
                if (limit > 0 && list.size() >= limit) {
                    break;
                }
            }
        }
        return list;
    }

    public synchronized TraceSummaryResponse getSummary(String prefix) {
        TraceSummaryResponse resp = new TraceSummaryResponse();
        List<Double> controllerCosts = new ArrayList<>();
        List<Double> sqlCosts = new ArrayList<>();
        List<Double> commitCosts = new ArrayList<>();
        List<Double> fetchCosts = new ArrayList<>();
        List<Double> parseCosts = new ArrayList<>();
        List<Double> luaCosts = new ArrayList<>();
        List<Double> ackCosts = new ArrayList<>();
        List<Double> asyncTransits = new ArrayList<>();
        List<Double> totalPipelines = new ArrayList<>();

        int total = 0;
        int completed = 0;

        for (CdcTrace trace : traces.values()) {
            if (prefix != null && !prefix.isBlank() && (trace.getCode() == null || !trace.getCode().startsWith(prefix))) {
                continue;
            }
            total++;
            if (trace.isWriteCompleted() && trace.isConsumerCompleted()) {
                completed++;
            }
            if (trace.getControllerCostMs() != null) controllerCosts.add(trace.getControllerCostMs());
            if (trace.getSqlCostMs() != null) sqlCosts.add(trace.getSqlCostMs());
            if (trace.getCommitCostMs() != null) commitCosts.add(trace.getCommitCostMs());
            if (trace.getFetchCostMs() != null) fetchCosts.add(trace.getFetchCostMs());
            if (trace.getProtobufParseCostMs() != null) parseCosts.add(trace.getProtobufParseCostMs());
            if (trace.getRedisLuaCostMs() != null) luaCosts.add(trace.getRedisLuaCostMs());
            if (trace.getCanalAckCostMs() != null) ackCosts.add(trace.getCanalAckCostMs());
            if (trace.getAsyncTransitMs() != null) asyncTransits.add(trace.getAsyncTransitMs());
            if (trace.getTotalPipelineMs() != null) totalPipelines.add(trace.getTotalPipelineMs());
        }

        resp.setTotalTraces(total);
        resp.setCompletedTraces(completed);
        resp.setControllerCost(SegmentSummary.from(controllerCosts));
        resp.setSqlCost(SegmentSummary.from(sqlCosts));
        resp.setCommitCost(SegmentSummary.from(commitCosts));
        resp.setFetchCost(SegmentSummary.from(fetchCosts));
        resp.setProtobufParseCost(SegmentSummary.from(parseCosts));
        resp.setRedisLuaCost(SegmentSummary.from(luaCosts));
        resp.setCanalAckCost(SegmentSummary.from(ackCosts));
        resp.setAsyncTransit(SegmentSummary.from(asyncTransits));
        resp.setTotalPipeline(SegmentSummary.from(totalPipelines));

        return resp;
    }

    public synchronized void clear() {
        traces.clear();
        log.info("Cleared all recorded CDC diagnostic traces");
    }
}
