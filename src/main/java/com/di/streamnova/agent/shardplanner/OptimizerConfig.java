package com.di.streamnova.agent.shardplanner;

/**
 * Immutable config for MachineTypeBasedOptimizer (record-count and size thresholds).
 * Built from ShardPlannerProperties.
 */
public record OptimizerConfig(
        long highCpuTargetRecordsPerShard,
        long highMemTargetRecordsPerShard,
        long standardTargetRecordsPerShard,
        long fallbackMaxRecordsPerShard,
        double maxMbPerShard,
        double poolHeadroomRatio) {
}
