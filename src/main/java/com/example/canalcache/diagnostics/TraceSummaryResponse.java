package com.example.canalcache.diagnostics;

import lombok.Data;

@Data
public class TraceSummaryResponse {
    private int totalTraces;
    private int completedTraces;
    private SegmentSummary controllerCost;
    private SegmentSummary sqlCost;
    private SegmentSummary commitCost;
    private SegmentSummary fetchCost;
    private SegmentSummary protobufParseCost;
    private SegmentSummary redisLuaCost;
    private SegmentSummary canalAckCost;
    private SegmentSummary asyncTransit;
    private SegmentSummary totalPipeline;
}
