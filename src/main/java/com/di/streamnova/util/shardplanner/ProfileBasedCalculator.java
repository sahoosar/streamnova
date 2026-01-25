package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Calculates shard count using machine profile when size info is unavailable.
 * 
 * <p>This calculator has different execution strategies based on the cloud provider:</p>
 * <ul>
 *   <li><b>Local:</b> Optimized for local development with limited resources</li>
 *   <li><b>GCP:</b> Optimized for Google Cloud Dataflow with machine type awareness</li>
 *   <li><b>AWS:</b> Optimized for AWS (future support)</li>
 * </ul>
 */
@Slf4j
public final class ProfileBasedCalculator {
    private ProfileBasedCalculator() {}
    
    /**
     * Calculates shard count using profile-based strategy.
     * Routes to environment-specific calculation based on cloud provider.
     * 
     * @param environment Execution environment
     * @param profile Machine profile
     * @param databasePoolMaxSize Database pool size limit
     * @return Calculated shard count
     */
    public static int calculateUsingProfile(ExecutionEnvironment environment, 
                                            MachineProfile profile, 
                                            Integer databasePoolMaxSize) {
        String envName = environment.cloudProvider.name();
        log.info("[ENV: {}] Profile-based calculation starting: cloudProvider={}, machineType={}, vCPUs={}, workers={}",
                envName, environment.cloudProvider, environment.machineType, environment.virtualCpus, environment.workerCount);
        
        switch (environment.cloudProvider) {
            case LOCAL:
                return calculateForLocal(environment, profile, databasePoolMaxSize);
            case GCP:
                return calculateForGcp(environment, profile, databasePoolMaxSize);
            case AWS:
                return calculateForAws(environment, profile, databasePoolMaxSize);
            default:
                log.warn("Unknown cloud provider: {}, falling back to local calculation", environment.cloudProvider);
                return calculateForLocal(environment, profile, databasePoolMaxSize);
        }
    }
    
    /**
     * Calculates shards for LOCAL execution.
     * 
     * <p><b>Strategy:</b> Conservative approach for local development</p>
     * <ul>
     *   <li>Uses available CPU cores (vCPUs)</li>
     *   <li>Single worker (local execution)</li>
     *   <li>Conservative concurrency to avoid overwhelming local machine</li>
     *   <li>Respects database pool limits</li>
     * </ul>
     * 
     * @param environment Execution environment
     * @param profile Machine profile
     * @param databasePoolMaxSize Database pool size limit
     * @return Calculated shard count for local execution
     */
    private static int calculateForLocal(ExecutionEnvironment environment, 
                                        MachineProfile profile, 
                                        Integer databasePoolMaxSize) {
        String envName = "LOCAL";
        log.info("[ENV: {}] Local execution profile calculation: vCPUs={}, workers={}", 
                envName, environment.virtualCpus, environment.workerCount);
        
        // Local: Use CPU cores with conservative multiplier
        // Formula: shards = vCPUs × conservative_multiplier
        // Conservative multiplier ensures we don't overwhelm local machine
        double conservativeMultiplier = 1.5;  // Local machines: 1.5x vCPUs for parallelism
        int baseShards = (int) Math.ceil(environment.virtualCpus * conservativeMultiplier);
        baseShards = Math.max(1, baseShards);
        
        // Cap by database pool (80% headroom)
        int poolCapacity = (databasePoolMaxSize != null && databasePoolMaxSize > 0) 
                ? databasePoolMaxSize : Integer.MAX_VALUE;
        int safePoolCapacity = (poolCapacity == Integer.MAX_VALUE) 
                ? poolCapacity : Math.max(1, (int) Math.floor(poolCapacity * 0.8));
        
        int shardCount = Math.min(baseShards, safePoolCapacity);
        shardCount = Math.max(1, shardCount);  // Ensure at least 1 shard
        
        log.info("[ENV: {}] Local profile calculation: baseShards={} (vCPUs={} × {}), poolCap={}, finalShards={}",
                envName, baseShards, environment.virtualCpus, conservativeMultiplier, safePoolCapacity, shardCount);
        
        return shardCount;
    }
    
    /**
     * Calculates shards for GCP Dataflow execution.
     * 
     * <p><b>Strategy:</b> Optimized for GCP Dataflow with machine type awareness</p>
     * <ul>
     *   <li>Uses machine type profile (vCPUs, workers)</li>
     *   <li>Calculates queries per worker based on vCPUs and profile</li>
     *   <li>Considers total concurrency across all workers</li>
     *   <li>Respects database pool limits</li>
     * </ul>
     * 
     * @param environment Execution environment
     * @param profile Machine profile
     * @param databasePoolMaxSize Database pool size limit
     * @return Calculated shard count for GCP execution
     */
    private static int calculateForGcp(ExecutionEnvironment environment, 
                                     MachineProfile profile, 
                                     Integer databasePoolMaxSize) {
        String envName = "GCP";
        log.info("[ENV: {}] GCP Dataflow profile calculation: machineType={}, vCPUs={}, workers={}", 
                envName, environment.machineType, environment.virtualCpus, environment.workerCount);
        
        // GCP: Calculate queries per worker from CPU and profile
        // Formula: queriesPerWorker = ceil(vCPUs × jdbcConnectionsPerVcpu)
        int queriesPerWorker = Math.max(1, (int) Math.ceil(
                environment.virtualCpus * profile.jdbcConnectionsPerVcpu()));
        
        // Calculate planned total concurrency across all workers
        // Formula: plannedConcurrency = workers × queriesPerWorker
        int plannedConcurrency = Math.max(1, environment.workerCount * queriesPerWorker);
        
        // Cap by database pool (80% headroom)
        int poolCapacity = (databasePoolMaxSize != null && databasePoolMaxSize > 0) 
                ? databasePoolMaxSize : Integer.MAX_VALUE;
        int safePoolCapacity = (poolCapacity == Integer.MAX_VALUE) 
                ? poolCapacity : Math.max(1, (int) Math.floor(poolCapacity * 0.8));
        int totalConcurrency = Math.min(plannedConcurrency, safePoolCapacity);
        
        // Calculate shards: concurrency × shardsPerQuery
        // Formula: shards = totalConcurrency × shardsPerQuery
        long rawShardCount = Math.round(totalConcurrency * profile.shardsPerQuery());
        int shardCount = (int) Math.max(1, rawShardCount);
        
        log.info("[ENV: {}] GCP profile calculation: queriesPerWorker={}, plannedConcurrency={}, safeConcurrency={}, " +
                "shardsPerQuery={}, rawShardCount={}, finalShards={}",
                envName, queriesPerWorker, plannedConcurrency, totalConcurrency, 
                profile.shardsPerQuery(), rawShardCount, shardCount);
        
        return shardCount;
    }
    
    /**
     * Calculates shards for AWS execution (future support).
     * 
     * <p><b>Strategy:</b> Optimized for AWS (to be implemented when AWS support is added)</p>
     * <ul>
     *   <li>Similar to GCP but with AWS-specific optimizations</li>
     *   <li>Considers AWS instance types and characteristics</li>
     *   <li>Respects database pool limits</li>
     * </ul>
     * 
     * <p><b>Note:</b> Currently falls back to GCP calculation. Will be enhanced when AWS support is added.</p>
     * 
     * @param environment Execution environment
     * @param profile Machine profile
     * @param databasePoolMaxSize Database pool size limit
     * @return Calculated shard count for AWS execution
     */
    private static int calculateForAws(ExecutionEnvironment environment, 
                                      MachineProfile profile, 
                                      Integer databasePoolMaxSize) {
        String envName = "AWS";
        log.info("[ENV: {}] AWS execution profile calculation: machineType={}, vCPUs={}, workers={} " +
                "(Note: AWS support is in development, using GCP-like calculation)",
                envName, environment.machineType, environment.virtualCpus, environment.workerCount);
        
        // TODO: Implement AWS-specific calculation when AWS support is added
        // For now, use GCP-like calculation as baseline
        // Future enhancements:
        // - AWS instance type detection (e.g., m5.xlarge, c5.2xlarge)
        // - AWS-specific profile factors
        // - AWS-specific concurrency limits
        
        // Temporary: Use GCP calculation as baseline
        int queriesPerWorker = Math.max(1, (int) Math.ceil(
                environment.virtualCpus * profile.jdbcConnectionsPerVcpu()));
        
        int plannedConcurrency = Math.max(1, environment.workerCount * queriesPerWorker);
        
        int poolCapacity = (databasePoolMaxSize != null && databasePoolMaxSize > 0) 
                ? databasePoolMaxSize : Integer.MAX_VALUE;
        int safePoolCapacity = (poolCapacity == Integer.MAX_VALUE) 
                ? poolCapacity : Math.max(1, (int) Math.floor(poolCapacity * 0.8));
        int totalConcurrency = Math.min(plannedConcurrency, safePoolCapacity);
        
        long rawShardCount = Math.round(totalConcurrency * profile.shardsPerQuery());
        int shardCount = (int) Math.max(1, rawShardCount);
        
        log.info("[ENV: {}] AWS profile calculation (temporary): queriesPerWorker={}, plannedConcurrency={}, " +
                "safeConcurrency={}, rawShardCount={}, finalShards={}",
                envName, queriesPerWorker, plannedConcurrency, totalConcurrency, rawShardCount, shardCount);
        
        return shardCount;
    }
}
