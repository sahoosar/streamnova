package com.di.streamnova.util.shardplanner;

/**
 * Applies constraints (pool size, profile bounds) to shard count.
 */
public final class ConstraintApplier {
    private ConstraintApplier() {}
    
    public static int applyConstraints(int shardCount, ExecutionEnvironment environment, 
                                        MachineProfile profile, Integer databasePoolMaxSize) {
        // Cap by database pool (~80% headroom)
        int poolCapacity = (databasePoolMaxSize != null && databasePoolMaxSize > 0) 
                ? databasePoolMaxSize : Integer.MAX_VALUE;
        int safePoolCapacity = (poolCapacity == Integer.MAX_VALUE) 
                ? poolCapacity : Math.max(1, (int) Math.floor(poolCapacity * 0.8));
        
        // Apply profile constraints
        // Machine type max is the hard limit (workers × vCPUs × maxShardsPerVcpu)
        int machineTypeMax = environment.isLocalExecution
                ? environment.virtualCpus * profile.maxShardsPerVcpu()
                : environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu();
        int effectiveMinShards = Math.min(profile.minimumShards(), machineTypeMax);
        
        int constrainedShardCount = Math.min(shardCount, machineTypeMax);
        constrainedShardCount = Math.max(constrainedShardCount, effectiveMinShards);
        constrainedShardCount = Math.min(constrainedShardCount, safePoolCapacity);
        
        return constrainedShardCount;
    }
}
