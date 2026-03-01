package com.di.streamnova.agent.metrics;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;

/**
 * Stored throughput profile (from Profiler warm-up or post-run). Used for
 * learning and to improve future Estimator source cap.
 */
@Value
@Builder
public class ThroughputProfile {
    String runId;
    String sourceType;
    String schemaName;
    String tableName;
    long bytesRead;
    long durationMs;
    int rowsRead;
    double throughputMbPerSec;
    Instant sampledAt;

    public Instant getSampledAt() {
        return sampledAt != null ? sampledAt : Instant.now();
    }

    /** Duration in seconds (for AI anomaly reporting). */
    public double getDurationSec() {
        return durationMs > 0 ? durationMs / 1000.0 : 0.0;
    }

    /** Cost in USD (placeholder; actual cost from EstimateVsActual when available). */
    public double getCostUsd() {
        return 0.0;
    }
}
