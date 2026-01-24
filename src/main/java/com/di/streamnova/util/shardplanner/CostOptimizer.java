package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Optimizes shard count for cost efficiency.
 */
@Slf4j
public final class CostOptimizer {
    private CostOptimizer() {}
    
    public static int optimizeForCost(int baseShardCount, ExecutionEnvironment environment, 
                                   Long estimatedRowCount) {
        // For local execution, skip worker-based cost optimization
        if (environment.isLocalExecution) {
            log.info("Local execution: skipping worker-based cost optimization, using vCPU-based allocation");
            return baseShardCount;
        }
        
        if (environment.workerCount <= 0) {
            return baseShardCount;
        }
        
        int shardsPerWorker = calculateOptimalShardsPerWorker(environment);
        int workersNeeded = (int) Math.ceil((double) baseShardCount / shardsPerWorker);
        
        // If we need significantly more workers, reduce shards for cost savings
        if (workersNeeded > environment.workerCount * CostOptimizationConfig.WORKER_SCALING_THRESHOLD) {
            int costOptimalShardCount = environment.workerCount * shardsPerWorker;
            
            // Ensure we don't go below minimum required shards based on dataset size
            int minShardsForData = calculateMinimumShardsForData(estimatedRowCount);
            
            int optimizedShardCount = Math.max(costOptimalShardCount, minShardsForData);
            optimizedShardCount = Math.min(optimizedShardCount, baseShardCount);
            
            if (optimizedShardCount < baseShardCount) {
                log.info("Final cost optimization: {} shards → {} shards (saves ~{} workers, reduces cost by ~{}%)",
                        baseShardCount, optimizedShardCount,
                        workersNeeded - environment.workerCount,
                        (int) ((1.0 - (double) optimizedShardCount / baseShardCount) * 100));
            }
            
            return optimizedShardCount;
        }
        
        return baseShardCount;
    }
    
    public static int calculateOptimalShardsPerWorker(ExecutionEnvironment environment) {
        if (environment.machineType == null || environment.machineType.isBlank()) {
            return CostOptimizationConfig.OPTIMAL_SHARDS_PER_WORKER;
        }
        
        String machineTypeLower = environment.machineType.toLowerCase();
        
        if (machineTypeLower.contains("highcpu")) {
            return Math.max(CostOptimizationConfig.OPTIMAL_SHARDS_PER_WORKER, 
                    environment.virtualCpus * 2);
        }
        
        if (machineTypeLower.contains("highmem") || machineTypeLower.contains("memory")) {
            return Math.max(4, environment.virtualCpus);
        }
        
        return Math.max(CostOptimizationConfig.OPTIMAL_SHARDS_PER_WORKER, environment.virtualCpus);
    }
    
    private static int calculateMinimumShardsForData(Long estimatedRowCount) {
        if (estimatedRowCount == null || estimatedRowCount <= 0) {
            return 1;
        }
        
        if (estimatedRowCount < DatasetScenarioConfig.VERY_SMALL_THRESHOLD) {
            return DatasetScenarioConfig.MIN_SHARDS_VERY_SMALL;
        } else if (estimatedRowCount < DatasetScenarioConfig.SMALL_MEDIUM_THRESHOLD) {
            return DatasetScenarioConfig.MIN_SHARDS_SMALL_MEDIUM;
        } else if (estimatedRowCount < DatasetScenarioConfig.MEDIUM_SMALL_THRESHOLD) {
            return DatasetScenarioConfig.MIN_SHARDS_MEDIUM_SMALL;
        } else if (estimatedRowCount < DatasetScenarioConfig.MEDIUM_THRESHOLD) {
            return (int) Math.max(1, estimatedRowCount / DatasetScenarioConfig.MAX_RECORDS_PER_SHARD_MEDIUM);
        } else {
            return (int) Math.max(1, estimatedRowCount / DatasetScenarioConfig.MAX_RECORDS_PER_SHARD_LARGE);
        }
    }
}
