package com.di.streamnova.agent.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Optimizes shard count for cost efficiency.
 */
@Slf4j
public final class CostOptimizer {
    private CostOptimizer() {}
    
    /**
     * Optimizes shard count for cost; never goes below minShardsFromSize (size-based floor).
     */
    public static int optimizeForCost(int baseShardCount, ExecutionEnvironment environment,
                                      int minShardsFromSize) {
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
            int floor = Math.max(1, minShardsFromSize);

            int optimizedShardCount = Math.max(costOptimalShardCount, floor);
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
}
