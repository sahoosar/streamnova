package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Validates user-provided shard and worker counts against machine type resource limits.
 * Ensures user values don't exceed machine type capabilities.
 */
@Slf4j
public final class MachineTypeResourceValidator {
    private MachineTypeResourceValidator() {}
    
    /**
     * Validates and caps user-provided worker count based on machine type.
     * 
     * @param userProvidedWorkerCount User-provided worker count
     * @param environment Execution environment
     * @return Validated worker count (capped at machine type maximum)
     */
    public static int validateWorkerCount(int userProvidedWorkerCount, ExecutionEnvironment environment) {
        // For local execution, worker concept doesn't apply the same way
        if (environment.isLocalExecution) {
            log.info("Local execution: worker count validation not applicable, using provided value: {}", userProvidedWorkerCount);
            return Math.max(1, userProvidedWorkerCount);
        }
        
        // If machine type not provided, can't validate - use provided value
        if (environment.machineType == null || environment.machineType.isBlank()) {
            log.warn("Machine type not provided: cannot validate worker count against machine type. Using provided value: {}", 
                    userProvidedWorkerCount);
            return userProvidedWorkerCount;
        }
        
        MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
        int maxWorkers = calculateMaxWorkersForMachineType(environment, profile);
        
        if (userProvidedWorkerCount > maxWorkers) {
            log.warn("User-provided worker count {} exceeds machine type maximum {} (machine: {}, vCPUs: {}). " +
                    "Capping at machine type maximum.", 
                    userProvidedWorkerCount, maxWorkers, environment.machineType, environment.virtualCpus);
            return maxWorkers;
        }
        
        return userProvidedWorkerCount;
    }
    
    /**
     * Calculates maximum shards based on machine type resources.
     * Formula: max_shards = workers × vCPUs × maxShardsPerVcpu
     * 
     * @param environment Execution environment
     * @param profile Machine profile
     * @return Maximum shards supported by machine type
     */
    public static int calculateMaxShardsForMachineType(ExecutionEnvironment environment, MachineProfile profile) {
        if (environment.isLocalExecution) {
            // Local: max shards = vCPUs × maxShardsPerVcpu
            return environment.virtualCpus * profile.maxShardsPerVcpu();
        }
        
        if (environment.machineType == null || environment.machineType.isBlank()) {
            // No machine type: return a reasonable default (can't calculate)
            log.warn("Machine type not provided: cannot calculate max shards. Using default: 1000");
            return 1000;
        }
        
        // GCP: max shards = workers × vCPUs × maxShardsPerVcpu
        int maxShards = environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu();
        return Math.max(maxShards, profile.minimumShards());
    }
    
    /**
     * Calculates maximum workers based on machine type and typical GCP limits.
     * 
     * @param environment Execution environment
     * @param profile Machine profile
     * @return Maximum workers recommended for machine type
     */
    public static int calculateMaxWorkersForMachineType(ExecutionEnvironment environment, MachineProfile profile) {
        if (environment.isLocalExecution) {
            return 1; // Local execution uses 1 worker
        }
        
        if (environment.machineType == null || environment.machineType.isBlank()) {
            // No machine type: return a reasonable default
            log.warn("Machine type not provided: cannot calculate max workers. Using default: 100");
            return 100;
        }
        
        // GCP Dataflow typical limits based on machine type
        // Higher vCPU machines can typically support more workers
        // Conservative estimate: max workers = vCPUs × 4 (for standard machines)
        String machineTypeLower = environment.machineType.toLowerCase();
        
        if (machineTypeLower.contains("highcpu")) {
            // High-CPU: can support more workers (vCPUs × 6)
            return environment.virtualCpus * 6;
        } else if (machineTypeLower.contains("highmem") || machineTypeLower.contains("memory")) {
            // High-Memory: moderate workers (vCPUs × 3)
            return environment.virtualCpus * 3;
        } else {
            // Standard: balanced (vCPUs × 4)
            return environment.virtualCpus * 4;
        }
    }
    
    /**
     * Validates user-provided shard count against machine type maximum.
     * 
     * @param userProvidedShardCount User-provided shard count
     * @param environment Execution environment
     * @param profile Machine profile
     * @return Validated shard count (capped at machine type maximum)
     */
    public static int validateShardCount(int userProvidedShardCount, ExecutionEnvironment environment, MachineProfile profile) {
        int maxShards = calculateMaxShardsForMachineType(environment, profile);
        
        if (userProvidedShardCount > maxShards) {
            log.warn("User-provided shard count {} exceeds machine type maximum {} ({} workers × {} vCPUs × {} maxShards/vCPU). " +
                    "Capping at machine type maximum.", 
                    userProvidedShardCount, maxShards, 
                    environment.workerCount, environment.virtualCpus, profile.maxShardsPerVcpu());
            return maxShards;
        }
        
        return userProvidedShardCount;
    }
}
