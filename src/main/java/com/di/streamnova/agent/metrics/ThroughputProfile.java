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
}
