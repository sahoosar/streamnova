package com.di.streamnova.agent.profiler;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.Optional;

/**
 * Full result of a Profiler run: table profile + optional throughput sample.
 * Persisted for Metrics & Learning Store (estimates vs actuals, throughput profiles).
 */
@Value
@Builder
public class ProfileResult {
    /** Unique run id for this profiling execution (for correlation with later load runs). */
    String runId;
    /** Table-level statistics. */
    TableProfile tableProfile;
    /** Warm-up read throughput sample, if performed. */
    ThroughputSample throughputSample;
    /** When this profile run completed. */
    Instant completedAt;
    /** Error message if profiling failed partially (e.g. stats ok, warm-up failed). */
    String errorMessage;

    public Optional<ThroughputSample> getThroughputSample() {
        return Optional.ofNullable(throughputSample);
    }

    public Optional<String> getErrorMessage() {
        return Optional.ofNullable(errorMessage);
    }
}
