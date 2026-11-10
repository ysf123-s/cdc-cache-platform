package com.example.canalcache.controller;

import com.example.canalcache.diagnostics.CdcTrace;
import com.example.canalcache.diagnostics.TraceCollector;
import com.example.canalcache.diagnostics.TraceSummaryResponse;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/diagnostics/traces")
public class CdcDiagnosticsController {

    private final TraceCollector traceCollector;

    public CdcDiagnosticsController(TraceCollector traceCollector) {
        this.traceCollector = traceCollector;
    }

    @GetMapping
    public List<CdcTrace> getTraces(
            @RequestParam(required = false, defaultValue = "") String prefix,
            @RequestParam(required = false, defaultValue = "100") int limit) {
        return traceCollector.getTraces(prefix, limit);
    }

    @GetMapping("/summary")
    public TraceSummaryResponse getSummary(
            @RequestParam(required = false, defaultValue = "") String prefix) {
        return traceCollector.getSummary(prefix);
    }

    @DeleteMapping
    public Map<String, Object> clear() {
        traceCollector.clear();
        return Map.of("cleared", true);
    }
}
