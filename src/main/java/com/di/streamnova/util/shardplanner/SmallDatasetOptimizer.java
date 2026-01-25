package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Optimizes shard count for very small datasets (< 10K records).
 */
@Slf4j
public final class SmallDatasetOptimizer {
    private SmallDatasetOptimizer() {}
    
    public static int calculateForSmallDataset(ExecutionEnvironment environment, 
                                               long estimatedRowCount, 
                                               Integer databasePoolMaxSize) {
        String envName = environment.cloudProvider.name();
        log.info("[ENV: {}] Optimizing for small dataset: {} records, {} vCPUs, {} workers", 
                envName, estimatedRowCount, environment.virtualCpus, environment.workerCount);
        
        int totalCores = environment.virtualCpus * environment.workerCount;
        int minRecordsPerShard = 50;
        int maxShardsFromData = (int) Math.max(1, estimatedRowCount / minRecordsPerShard);
        
        // Cap by database pool (80% headroom)
        int poolCapacity = (databasePoolMaxSize != null && databasePoolMaxSize > 0) 
                ? databasePoolMaxSize : Integer.MAX_VALUE;
        int safePoolCapacity = (poolCapacity == Integer.MAX_VALUE) 
                ? poolCapacity : Math.max(1, (int) Math.floor(poolCapacity * 0.8));
        
        int optimalShardCount;
        if (estimatedRowCount >= totalCores * minRecordsPerShard) {
            optimalShardCount = totalCores;
        } else if (estimatedRowCount >= minRecordsPerShard) {
            optimalShardCount = Math.min(maxShardsFromData, totalCores);
        } else {
            optimalShardCount = 1;
        }
        
        optimalShardCount = Math.min(optimalShardCount, safePoolCapacity);
        optimalShardCount = Math.min(optimalShardCount, (int) Math.max(1, estimatedRowCount));
        optimalShardCount = Math.max(1, optimalShardCount);
        
        int recordsPerShard = (int) (estimatedRowCount / optimalShardCount);
        int remainder = (int) (estimatedRowCount % optimalShardCount);
        
        log.info("[ENV: {}] Small dataset optimization: {} shards (matching {} cores), ~{} records per shard ({} shards will have +1 record), {} total cores available", 
                envName, optimalShardCount, totalCores, recordsPerShard, remainder, totalCores);
        
        return optimalShardCount;
    }
}
