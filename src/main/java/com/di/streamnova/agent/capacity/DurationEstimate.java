package com.di.streamnova.agent.capacity;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Value;

/**
 * Estimate of how long a load will take. First run uses recommender estimate;
 * subsequent runs can use last actual duration from metrics.
 */
@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DurationEstimate {
    /** Estimated duration in minutes (from recommendation or last run). */
    Double estimatedMinutes;
    /** True if no prior successful run for this table (first run). */
    boolean firstRun;
    /** Human-readable message, e.g. "Based on last run: ~5 min" or "First run – estimated ~3 min". */
    String message;
    /** Last run actual duration in minutes (only when history exists). */
    Double lastRunMinutes;
}
