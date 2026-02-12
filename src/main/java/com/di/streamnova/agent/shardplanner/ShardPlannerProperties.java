package com.di.streamnova.agent.shardplanner;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configurable shard planner limits and machine-type optimizer parameters.
 */
@ConfigurationProperties(prefix = "streamnova.shardplanner")
public class ShardPlannerProperties {

    private int maxShardsCap;
    private int fallbackShardsWhenNoVcpus;

    /** High-CPU: target records per shard when using record-count fallback (from application.properties). */
    private long highCpuTargetRecordsPerShard;
    /** High-Memory: target records per shard when using record-count fallback (from application.properties). */
    private long highMemTargetRecordsPerShard;
    /** Standard: target records per shard when using record-count fallback (from application.properties). */
    private long standardTargetRecordsPerShard;
    /** Min-shards: max records per shard when size not available (from application.properties). */
    private long fallbackMaxRecordsPerShard;
    /** Min-shards: max MB per shard when size available (from application.properties). */
    private double maxMbPerShard;
    /** Pool headroom ratio for safe shard cap (from application.properties). */
    private double poolHeadroomRatio;

    public int getMaxShardsCap() {
        return maxShardsCap;
    }

    public void setMaxShardsCap(int maxShardsCap) {
        this.maxShardsCap = maxShardsCap;
    }

    public int getFallbackShardsWhenNoVcpus() {
        return fallbackShardsWhenNoVcpus;
    }

    public void setFallbackShardsWhenNoVcpus(int fallbackShardsWhenNoVcpus) {
        this.fallbackShardsWhenNoVcpus = fallbackShardsWhenNoVcpus;
    }

    public long getHighCpuTargetRecordsPerShard() {
        return highCpuTargetRecordsPerShard;
    }

    public void setHighCpuTargetRecordsPerShard(long highCpuTargetRecordsPerShard) {
        this.highCpuTargetRecordsPerShard = highCpuTargetRecordsPerShard;
    }

    public long getHighMemTargetRecordsPerShard() {
        return highMemTargetRecordsPerShard;
    }

    public void setHighMemTargetRecordsPerShard(long highMemTargetRecordsPerShard) {
        this.highMemTargetRecordsPerShard = highMemTargetRecordsPerShard;
    }

    public long getStandardTargetRecordsPerShard() {
        return standardTargetRecordsPerShard;
    }

    public void setStandardTargetRecordsPerShard(long standardTargetRecordsPerShard) {
        this.standardTargetRecordsPerShard = standardTargetRecordsPerShard;
    }

    public long getFallbackMaxRecordsPerShard() {
        return fallbackMaxRecordsPerShard;
    }

    public void setFallbackMaxRecordsPerShard(long fallbackMaxRecordsPerShard) {
        this.fallbackMaxRecordsPerShard = fallbackMaxRecordsPerShard;
    }

    public double getMaxMbPerShard() {
        return maxMbPerShard;
    }

    public void setMaxMbPerShard(double maxMbPerShard) {
        this.maxMbPerShard = maxMbPerShard;
    }

    public double getPoolHeadroomRatio() {
        return poolHeadroomRatio;
    }

    public void setPoolHeadroomRatio(double poolHeadroomRatio) {
        this.poolHeadroomRatio = poolHeadroomRatio;
    }

    /** Builds an immutable config for MachineTypeBasedOptimizer (avoids passing full properties). */
    public OptimizerConfig toOptimizerConfig() {
        return new OptimizerConfig(
                highCpuTargetRecordsPerShard,
                highMemTargetRecordsPerShard,
                standardTargetRecordsPerShard,
                fallbackMaxRecordsPerShard,
                maxMbPerShard,
                poolHeadroomRatio);
    }
}
