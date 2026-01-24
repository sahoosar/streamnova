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
        int maxShardsFromProfile = environment.isLocalExecution
                ? Math.max(profile.minimumShards(), environment.virtualCpus * profile.maxShardsPerVcpu())
                : Math.max(profile.minimumShards(), 
                        environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu());
        
        int constrainedShardCount = Math.min(shardCount, maxShardsFromProfile);
        constrainedShardCount = Math.max(constrainedShardCount, profile.minimumShards());
        constrainedShardCount = Math.min(constrainedShardCount, safePoolCapacity);
        
        return constrainedShardCount;
    }
}
