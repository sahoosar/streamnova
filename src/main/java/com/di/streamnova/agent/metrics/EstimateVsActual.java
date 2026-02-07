package com.di.streamnova.agent.metrics;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;

/**
 * Record of estimated vs actual duration and cost for a run. Used for self-learning
 * and to improve future recommendations.
 */
@Value
@Builder
public class EstimateVsActual {
    String runId;
    Double estimatedDurationSec;
    Double actualDurationSec;
    Double estimatedCostUsd;
    Double actualCostUsd;
    String machineType;
    Integer workerCount;
    Integer shardCount;
    Instant recordedAt;

    public Instant getRecordedAt() {
        return recordedAt != null ? recordedAt : Instant.now();
    }
}
