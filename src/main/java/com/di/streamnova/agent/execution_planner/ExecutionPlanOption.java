package com.di.streamnova.agent.execution_planner;

import lombok.Builder;
import lombok.Value;

/**
 * A single candidate configuration for the Estimator and Recommender:
 * machine type, worker count, and shard plan. Supports shift-based (phased) load when
 * partition count exceeds pool capacity (e.g. 20 partitions, 8 concurrent).
 */
@Value
@Builder
public class ExecutionPlanOption {
    /** GCP machine type (e.g. n2-standard-8, n2d-standard-4, c3-standard-4). */
    String machineType;
    /** Number of workers (Dataflow/parallel workers). */
    int workerCount;
    /** Logical partition count (table split and number of tasks). When null/0, use maxConcurrentShards. */
    Integer partitionCount;
    /** Max shards running at once (reservation and connection limit). When null/0, use partitionCount or shardCount. */
    Integer maxConcurrentShards;
    /** Legacy: total shard count for display/API. When set, used when partitionCount/maxConcurrentShards not set. */
    int shardCount;
    /** vCPUs per machine (derived from machineType). */
    int virtualCpus;
    /** Suggested connection pool size for this candidate (derived from machine type + shards). */
    int suggestedPoolSize;
    /** Optional label for logging (e.g. "n2-std-8w"). */
    String label;

    /** Effective partition count: partitionCount if set, else shardCount. */
    public int getEffectivePartitionCount() {
        if (partitionCount != null && partitionCount > 0) return partitionCount;
        return Math.max(1, shardCount);
    }

    /** Effective max concurrent shards: maxConcurrentShards if set, else partition count (no shift). */
    public int getEffectiveMaxConcurrentShards() {
        if (maxConcurrentShards != null && maxConcurrentShards > 0) return maxConcurrentShards;
        return getEffectivePartitionCount();
    }

    /** Legacy: same as getEffectivePartitionCount() for display and estimation. */
    public int getShardCount() {
        return getEffectivePartitionCount();
    }

    public String getLabelOrDefault() {
        return label != null && !label.isBlank()
                ? label
                : machineType + "-" + workerCount + "w-" + getEffectivePartitionCount() + "s";
    }
}
