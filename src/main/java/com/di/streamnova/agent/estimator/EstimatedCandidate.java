package com.di.streamnova.agent.estimator;

import com.di.streamnova.agent.adaptive_execution_planner.ExecutionPlanOption;
import lombok.Builder;
import lombok.Value;

/**
 * A load candidate with estimated duration, cost, and which cap (source/cpu/sink) was the bottleneck.
 */
@Value
@Builder
public class EstimatedCandidate {
    ExecutionPlanOption candidate;
    /** Estimated duration in seconds. */
    double estimatedDurationSec;
    /** Estimated cost in USD (internal; display can convert to GBP). */
    double estimatedCostUsd;
    /** Effective throughput used for duration (MB/s). */
    double effectiveThroughputMbPerSec;
    /** Which cap limited throughput: SOURCE, CPU, SINK, or PARALLELISM. */
    Bottleneck bottleneck;

    public ExecutionPlanOption getCandidate() {
        return candidate;
    }
}
