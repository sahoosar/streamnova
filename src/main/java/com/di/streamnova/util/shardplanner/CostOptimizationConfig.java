package com.di.streamnova.util.shardplanner;

/**
 * Configuration for cost optimization.
 */
public final class CostOptimizationConfig {
    private CostOptimizationConfig() {}
    
    public static final int OPTIMAL_SHARDS_PER_WORKER = 8;  // 8 shards per worker balances cost/performance
    public static final double WORKER_SCALING_THRESHOLD = 1.5;  // 1.5x more workers triggers cost optimization
}
