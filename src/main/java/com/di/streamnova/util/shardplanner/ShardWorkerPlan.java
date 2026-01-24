package com.di.streamnova.util.shardplanner;

/**
 * Represents a complete shard and worker plan calculated based on machine type and data characteristics.
 * 
 * This class encapsulates both shard count and worker count together, ensuring they are
 * calculated and validated as a cohesive unit based on machine type resources.
 */
public record ShardWorkerPlan(
        int shardCount,           // Calculated or user-provided shard count
        int workerCount,          // Calculated or user-provided worker count
        String machineType,        // Machine type used for calculation
        int virtualCpus,           // Virtual CPUs extracted from machine type
        String calculationStrategy // Strategy used: "MACHINE_TYPE", "USER_PROVIDED", "RECORD_COUNT", etc.
) {
    /**
     * Creates a plan with user-provided values.
     */
    public static ShardWorkerPlan userProvided(int shardCount, int workerCount, String machineType, int virtualCpus) {
        return new ShardWorkerPlan(shardCount, workerCount, machineType, virtualCpus, "USER_PROVIDED");
    }
    
    /**
     * Creates a plan with machine-type-based calculation.
     */
    public static ShardWorkerPlan machineTypeBased(int shardCount, int workerCount, String machineType, int virtualCpus) {
        return new ShardWorkerPlan(shardCount, workerCount, machineType, virtualCpus, "MACHINE_TYPE");
    }
    
    /**
     * Creates a plan with record-count-based calculation.
     */
    public static ShardWorkerPlan recordCountBased(int shardCount, int workerCount, String machineType, int virtualCpus) {
        return new ShardWorkerPlan(shardCount, workerCount, machineType, virtualCpus, "RECORD_COUNT");
    }
}
