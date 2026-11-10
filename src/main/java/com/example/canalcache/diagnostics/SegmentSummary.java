package com.example.canalcache.diagnostics;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SegmentSummary {
    private int count;
    private double meanMs;
    private double p50Ms;
    private double p95Ms;
    private double p99Ms;
    private double maxMs;

    public static SegmentSummary from(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return new SegmentSummary(0, 0.0, 0.0, 0.0, 0.0, 0.0);
        }
        Collections.sort(values);
        int n = values.size();
        double sum = 0.0;
        for (double v : values) {
            sum += v;
        }
        double mean = round3(sum / n);
        double p50 = round3(values.get((int) Math.floor(n * 0.50)));
        double p95 = round3(values.get(Math.min((int) Math.ceil(n * 0.95) - 1, n - 1)));
        double p99 = round3(values.get(Math.min((int) Math.ceil(n * 0.99) - 1, n - 1)));
        double max = round3(values.get(n - 1));

        return new SegmentSummary(n, mean, p50, p95, p99, max);
    }

    private static double round3(double v) {
        return Math.round(v * 1000.0) / 1000.0;
    }
}
