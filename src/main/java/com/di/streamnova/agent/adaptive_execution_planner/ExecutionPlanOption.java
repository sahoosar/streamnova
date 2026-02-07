package com.di.streamnova.agent.adaptive_execution_planner;

import lombok.Builder;
import lombok.Value;

/**
 * A single candidate configuration for the Estimator and Recommender:
 * machine type, worker count, and shard count. Represents one possible execution plan.
 */
@Value
@Builder
public class ExecutionPlanOption {
    /** GCP machine type (e.g. n2-standard-8, n2d-standard-4, c3-standard-4). */
    String machineType;
    /** Number of workers (Dataflow/parallel workers). */
    int workerCount;
    /** Number of shards (parallel read partitions). */
    int shardCount;
    /** vCPUs per machine (derived from machineType). */
    int virtualCpus;
    /** Suggested connection pool size for this candidate (derived from machine type + shards). */
    int suggestedPoolSize;
    /** Optional label for logging (e.g. "n2-std-8w"). */
    String label;

    public String getLabelOrDefault() {
        return label != null && !label.isBlank()
                ? label
                : machineType + "-" + workerCount + "w-" + shardCount + "s";
    }
}
