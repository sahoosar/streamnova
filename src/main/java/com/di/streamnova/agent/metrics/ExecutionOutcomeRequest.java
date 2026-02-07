package com.di.streamnova.agent.metrics;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request body to record execution outcome (when a run finishes).
 * Accepts both camelCase and snake_case (e.g. runId or run_id).
 */
@Data
@NoArgsConstructor
public class ExecutionOutcomeRequest {
    @JsonAlias("run_id")
    private String runId;
    private boolean success;
    @JsonAlias("actual_duration_sec")
    private Double actualDurationSec;
    @JsonAlias("actual_cost_usd")
    private Double actualCostUsd;
    @JsonAlias("job_id")
    private String jobId;
    private String message;
    @JsonAlias("estimated_duration_sec")
    private Double estimatedDurationSec;
    @JsonAlias("estimated_cost_usd")
    private Double estimatedCostUsd;
    @JsonAlias("machine_type")
    private String machineType;
    @JsonAlias("worker_count")
    private Integer workerCount;
    @JsonAlias("shard_count")
    private Integer shardCount;
}
