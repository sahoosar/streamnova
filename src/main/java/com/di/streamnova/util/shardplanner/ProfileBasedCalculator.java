package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Calculates shard count using machine profile when size info is unavailable.
 */
@Slf4j
public final class ProfileBasedCalculator {
    private ProfileBasedCalculator() {}
    
    public static int calculateUsingProfile(ExecutionEnvironment environment, 
                                            MachineProfile profile, 
                                            Integer databasePoolMaxSize) {
        // Calculate queries per worker from CPU and profile
        int queriesPerWorker = Math.max(1, (int) Math.ceil(
                environment.virtualCpus * profile.jdbcConnectionsPerVcpu()));
        
        // Calculate planned total concurrency
        int plannedConcurrency = Math.max(1, environment.workerCount * queriesPerWorker);
        
        // Cap by database pool (80% headroom)
        int poolCapacity = (databasePoolMaxSize != null && databasePoolMaxSize > 0) 
                ? databasePoolMaxSize : Integer.MAX_VALUE;
        int safePoolCapacity = (poolCapacity == Integer.MAX_VALUE) 
                ? poolCapacity : Math.max(1, (int) Math.floor(poolCapacity * 0.8));
        int totalConcurrency = Math.min(plannedConcurrency, safePoolCapacity);
        
        // Calculate shards: concurrency × shardsPerQuery
        long rawShardCount = Math.round(totalConcurrency * profile.shardsPerQuery());
        
        log.info("Profile-based calculation: queriesPerWorker={}, plannedConcurrency={}, safeConcurrency={}, rawShardCount={}",
                queriesPerWorker, plannedConcurrency, totalConcurrency, rawShardCount);
        
        return (int) rawShardCount;
    }
}
