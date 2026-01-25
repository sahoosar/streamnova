package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Optimizes shard count based on dataset size scenarios.
 */
@Slf4j
public final class ScenarioOptimizer {
    private ScenarioOptimizer() {}
    
    public static int optimizeForDatasetSize(int sizeBasedShardCount, Long estimatedRowCount,
                                          ExecutionEnvironment environment, Integer databasePoolMaxSize) {
        if (estimatedRowCount == null || estimatedRowCount <= 0) {
            return sizeBasedShardCount;
        }
        
        String envName = environment.cloudProvider.name();
        String scenarioName;
        int optimizedShardCount;
        
        if (estimatedRowCount < DatasetScenarioConfig.VERY_SMALL_THRESHOLD) {
            // Scenario 1: Very Small (< 100K records) - Maximum parallelism
            scenarioName = "VERY SMALL (< 100K)";
            optimizedShardCount = optimizeForVerySmallDataset(
                    sizeBasedShardCount, estimatedRowCount, environment, databasePoolMaxSize);
        } else if (estimatedRowCount < DatasetScenarioConfig.SMALL_MEDIUM_THRESHOLD) {
            // Scenario 2: Small-Medium (100K-500K) - Fast parallel processing
            scenarioName = "SMALL-MEDIUM (100K-500K)";
            optimizedShardCount = optimizeForSmallMediumDataset(
                    sizeBasedShardCount, estimatedRowCount, environment, databasePoolMaxSize);
        } else if (estimatedRowCount < DatasetScenarioConfig.MEDIUM_SMALL_THRESHOLD) {
            // Scenario 3: Medium-Small (500K-1M) - Balanced fast processing
            scenarioName = "MEDIUM-SMALL (500K-1M)";
            optimizedShardCount = optimizeForMediumSmallDataset(
                    sizeBasedShardCount, estimatedRowCount, environment);
        } else if (estimatedRowCount < DatasetScenarioConfig.MEDIUM_THRESHOLD) {
            // Scenario 4: Medium (1M-5M) - Balanced cost/performance
            scenarioName = "MEDIUM (1M-5M)";
            optimizedShardCount = optimizeForMediumDataset(
                    sizeBasedShardCount, estimatedRowCount, environment);
        } else if (estimatedRowCount <= DatasetScenarioConfig.LARGE_THRESHOLD) {
            // Scenario 5: Large (5M-10M) - Cost-effective with good parallelism
            scenarioName = "LARGE (5M-10M)";
            optimizedShardCount = optimizeForLargeDataset(
                    sizeBasedShardCount, estimatedRowCount, environment);
        } else {
            // Beyond 10M - use large dataset logic
            scenarioName = "VERY LARGE (> 10M)";
            optimizedShardCount = optimizeForLargeDataset(
                    sizeBasedShardCount, estimatedRowCount, environment);
        }
        
        log.info("[ENV: {}] Scenario-based optimization [{}]: sizeBased={} → optimized={} shards", 
                envName, scenarioName, sizeBasedShardCount, optimizedShardCount);
        
        return optimizedShardCount;
    }
    
    private static int optimizeForVerySmallDataset(int sizeBasedShardCount, long estimatedRowCount,
                                                   ExecutionEnvironment environment, Integer databasePoolMaxSize) {
        String envName = environment.cloudProvider.name();
        
        // Calculate based on records per shard (2K records/shard for maximum parallelism)
        int recordsBasedShardCount = (int) Math.ceil(
                (double) estimatedRowCount / DatasetScenarioConfig.OPTIMAL_RECORDS_PER_SHARD_VERY_SMALL);
        
        // Ensure we don't exceed max records per shard (5K)
        int minShardsForMaxRecords = (int) Math.ceil(
                (double) estimatedRowCount / DatasetScenarioConfig.MAX_RECORDS_PER_SHARD_VERY_SMALL);
        
        // Use CPU-based calculation for very small datasets (< 10K records)
        int cpuBasedShardCount = 1;
        if (estimatedRowCount < 10_000) {
            cpuBasedShardCount = SmallDatasetOptimizer.calculateForSmallDataset(
                    environment, estimatedRowCount, databasePoolMaxSize);
        }
        
        // Take maximum of all approaches for maximum parallel processing
        int optimalShardCount = Math.max(sizeBasedShardCount, 
                Math.max(recordsBasedShardCount, cpuBasedShardCount));
        optimalShardCount = Math.max(optimalShardCount, DatasetScenarioConfig.MIN_SHARDS_VERY_SMALL);
        optimalShardCount = Math.max(optimalShardCount, minShardsForMaxRecords);
        
        long recordsPerShard = estimatedRowCount / optimalShardCount;
        log.info("[ENV: {}] Very small dataset optimization (< 100K): {} records → {} shards, ~{} records/shard (target: 2K-5K) - maximum parallel processing",
                envName, estimatedRowCount, optimalShardCount, recordsPerShard);
        
        return optimalShardCount;
    }
    
    private static int optimizeForSmallMediumDataset(int sizeBasedShardCount, long estimatedRowCount,
                                                     ExecutionEnvironment environment, Integer databasePoolMaxSize) {
        String envName = environment.cloudProvider.name();
        
        // Calculate based on records per shard (5K records/shard for fast processing)
        int recordsBasedShardCount = (int) Math.ceil(
                (double) estimatedRowCount / DatasetScenarioConfig.OPTIMAL_RECORDS_PER_SHARD_SMALL_MEDIUM);
        
        // Ensure we don't exceed max records per shard (10K)
        int minShardsForMaxRecords = (int) Math.ceil(
                (double) estimatedRowCount / DatasetScenarioConfig.MAX_RECORDS_PER_SHARD_SMALL_MEDIUM);
        
        // Use CPU-based calculation for smaller datasets (< 50K records)
        int cpuBasedShardCount = 1;
        if (estimatedRowCount < 50_000) {
            cpuBasedShardCount = SmallDatasetOptimizer.calculateForSmallDataset(
                    environment, estimatedRowCount, databasePoolMaxSize);
        }
        
        // Take maximum of all approaches for fast parallel processing
        int optimalShardCount = Math.max(sizeBasedShardCount, 
                Math.max(recordsBasedShardCount, cpuBasedShardCount));
        optimalShardCount = Math.max(optimalShardCount, DatasetScenarioConfig.MIN_SHARDS_SMALL_MEDIUM);
        optimalShardCount = Math.max(optimalShardCount, minShardsForMaxRecords);
        
        long recordsPerShard = estimatedRowCount / optimalShardCount;
        log.info("[ENV: {}] Small-medium dataset optimization (100K-500K): {} records → {} shards, ~{} records/shard (target: 5K-10K) - fast parallel processing",
                envName, estimatedRowCount, optimalShardCount, recordsPerShard);
        
        return optimalShardCount;
    }
    
    private static int optimizeForMediumSmallDataset(int sizeBasedShardCount, long estimatedRowCount,
                                                     ExecutionEnvironment environment) {
        String envName = environment.cloudProvider.name();
        
        // Calculate based on records per shard (10K records/shard)
        int recordsBasedShardCount = (int) Math.ceil(
                (double) estimatedRowCount / DatasetScenarioConfig.OPTIMAL_RECORDS_PER_SHARD_MEDIUM_SMALL);
        
        // Ensure we don't exceed max records per shard (20K)
        int minShardsForMaxRecords = (int) Math.ceil(
                (double) estimatedRowCount / DatasetScenarioConfig.MAX_RECORDS_PER_SHARD_MEDIUM_SMALL);
        
        // Use the larger of size-based or records-based, but cap for cost
        int optimalShardCount = Math.max(sizeBasedShardCount, recordsBasedShardCount);
        optimalShardCount = Math.max(optimalShardCount, DatasetScenarioConfig.MIN_SHARDS_MEDIUM_SMALL);
        optimalShardCount = Math.max(optimalShardCount, minShardsForMaxRecords);
        optimalShardCount = Math.min(optimalShardCount, DatasetScenarioConfig.MAX_SHARDS_MEDIUM_SMALL);
        
        // Consider worker limits
        int shardsPerWorker = CostOptimizer.calculateOptimalShardsPerWorker(environment);
        int workerBasedShardCount = environment.workerCount * shardsPerWorker;
        
        // Balance: use records-based but respect worker limits
        if (optimalShardCount > workerBasedShardCount * CostOptimizationConfig.WORKER_SCALING_THRESHOLD) {
            optimalShardCount = Math.min(optimalShardCount, workerBasedShardCount);
            log.info("[ENV: {}] Medium-small dataset: worker limit applied, reducing to {} shards", envName, optimalShardCount);
        }
        
        long recordsPerShard = estimatedRowCount / optimalShardCount;
        log.info("[ENV: {}] Medium-small dataset optimization (500K-1M): {} records → {} shards, ~{} records/shard (target: 10K-20K) - balanced fast processing",
                envName, estimatedRowCount, optimalShardCount, recordsPerShard);
        
        return optimalShardCount;
    }
    
    private static int optimizeForMediumDataset(int sizeBasedShardCount, long estimatedRowCount,
                                                ExecutionEnvironment environment) {
        String envName = environment.cloudProvider.name();
        
        // Calculate based on records per shard (25K records/shard)
        int recordsBasedShardCount = (int) Math.ceil(
                (double) estimatedRowCount / DatasetScenarioConfig.OPTIMAL_RECORDS_PER_SHARD_MEDIUM);
        
        // Ensure we don't exceed max records per shard (50K)
        int minShardsForMaxRecords = (int) Math.ceil(
                (double) estimatedRowCount / DatasetScenarioConfig.MAX_RECORDS_PER_SHARD_MEDIUM);
        
        // Use the larger of size-based or records-based, but cap for cost
        int optimalShardCount = Math.max(sizeBasedShardCount, recordsBasedShardCount);
        optimalShardCount = Math.max(optimalShardCount, minShardsForMaxRecords);
        optimalShardCount = Math.min(optimalShardCount, DatasetScenarioConfig.MAX_SHARDS_MEDIUM);
        
        // Consider worker limits
        int shardsPerWorker = CostOptimizer.calculateOptimalShardsPerWorker(environment);
        int workerBasedShardCount = environment.workerCount * shardsPerWorker;
        
        // Balance: use records-based but respect worker limits
        if (optimalShardCount > workerBasedShardCount * CostOptimizationConfig.WORKER_SCALING_THRESHOLD) {
            optimalShardCount = Math.min(optimalShardCount, workerBasedShardCount);
            log.info("[ENV: {}] Medium dataset: worker limit applied, reducing to {} shards", envName, optimalShardCount);
        }
        
        long recordsPerShard = estimatedRowCount / optimalShardCount;
        log.info("[ENV: {}] Medium dataset optimization (1M-5M): {} records → {} shards, ~{} records/shard (target: 25K-50K) - balanced cost/performance",
                envName, estimatedRowCount, optimalShardCount, recordsPerShard);
        
        return optimalShardCount;
    }
    
    private static int optimizeForLargeDataset(int sizeBasedShardCount, long estimatedRowCount,
                                               ExecutionEnvironment environment) {
        String envName = environment.cloudProvider.name();
        
        // Calculate optimal shards based on records-per-shard target (50K records/shard)
        int recordsBasedShardCount = (int) Math.ceil(
                (double) estimatedRowCount / DatasetScenarioConfig.OPTIMAL_RECORDS_PER_SHARD_LARGE);
        
        // Ensure we don't exceed max records per shard (100K)
        int minShardsForMaxRecords = (int) Math.ceil(
                (double) estimatedRowCount / DatasetScenarioConfig.MAX_RECORDS_PER_SHARD_LARGE);
        
        // Use the larger of: records-based optimal, or minimum for max records constraint
        int optimalShardCount = Math.max(recordsBasedShardCount, minShardsForMaxRecords);
        
        // Cap at MAX_SHARDS_LARGE to control costs (500 shards max)
        optimalShardCount = Math.min(optimalShardCount, DatasetScenarioConfig.MAX_SHARDS_LARGE);
        
        // Also consider worker-based optimization
        int shardsPerWorker = CostOptimizer.calculateOptimalShardsPerWorker(environment);
        int workerBasedShardCount = environment.workerCount * shardsPerWorker;
        
        // For large datasets, prefer records-based calculation but respect worker limits
        // If records-based would require too many workers, use worker-based
        int workersNeededForRecords = (int) Math.ceil((double) optimalShardCount / shardsPerWorker);
        if (workersNeededForRecords > environment.workerCount * CostOptimizationConfig.WORKER_SCALING_THRESHOLD) {
            // Too many workers needed - use worker-based optimization
            optimalShardCount = Math.min(optimalShardCount, workerBasedShardCount);
            log.info("[ENV: {}] Large dataset: worker limit applied, reducing to {} shards (fits in {} workers)",
                    envName, optimalShardCount, environment.workerCount);
        }
        
        // Ensure we don't go below minimum for parallelism
        optimalShardCount = Math.max(optimalShardCount, minShardsForMaxRecords);
        
        // Don't exceed size-based if it's more conservative
        optimalShardCount = Math.min(optimalShardCount, sizeBasedShardCount * 2); // Allow some flexibility
        
        long recordsPerShard = estimatedRowCount / optimalShardCount;
        log.info("[ENV: {}] Large dataset optimization (5M-10M): {} records → {} shards, ~{} records/shard (target: 50K-100K) - cost-effective with good parallelism",
                envName, estimatedRowCount, optimalShardCount, recordsPerShard);
        
        return optimalShardCount;
    }
}
