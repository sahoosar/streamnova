package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Optimizes shard count primarily based on machine type characteristics.
 * This is the PRIMARY strategy when machine type is provided.
 * Falls back to record-count scenarios only when machine type is not available.
 */
@Slf4j
public final class MachineTypeBasedOptimizer {
    private MachineTypeBasedOptimizer() {}
    
    /**
     * Optimizes shard count based on machine type as the primary factor.
     * 
     * @param sizeBasedShardCount Base shard count from size calculation
     * @param estimatedRowCount Estimated row count
     * @param environment Execution environment
     * @param profile Machine profile
     * @param databasePoolMaxSize Database pool size limit
     * @param dataSizeInfo Data size information
     * @return Optimized shard count based on machine type
     */
    public static int optimizeBasedOnMachineType(int sizeBasedShardCount, Long estimatedRowCount,
                                               ExecutionEnvironment environment, MachineProfile profile,
                                               Integer databasePoolMaxSize, DataSizeInfo dataSizeInfo) {
        
        String machineTypeLower = environment.machineType.toLowerCase();
        int machineTypeBasedShardCount;
        
        // Calculate based on machine type characteristics
        if (machineTypeLower.contains("highcpu")) {
            // High-CPU machines: maximize parallelism, more shards per vCPU
            machineTypeBasedShardCount = calculateForHighCpuMachine(
                    sizeBasedShardCount, estimatedRowCount, environment, profile, dataSizeInfo);
        } else if (machineTypeLower.contains("highmem") || machineTypeLower.contains("memory")) {
            // High-Memory machines: balanced, fewer shards but larger per shard
            machineTypeBasedShardCount = calculateForHighMemMachine(
                    sizeBasedShardCount, estimatedRowCount, environment, profile, dataSizeInfo);
        } else {
            // Standard machines: balanced approach based on vCPUs and workers
            machineTypeBasedShardCount = calculateForStandardMachine(
                    sizeBasedShardCount, estimatedRowCount, environment, profile, dataSizeInfo);
        }
        
        // Apply data size constraints (ensure we have enough shards for the data)
        if (estimatedRowCount != null && estimatedRowCount > 0) {
            // Ensure minimum shards based on data volume
            int minShardsForData = calculateMinimumShardsForDataSize(
                    estimatedRowCount, dataSizeInfo, environment);
            machineTypeBasedShardCount = Math.max(machineTypeBasedShardCount, minShardsForData);
        }
        
        // Cap by machine profile limits
        int maxShardsFromProfile = environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu();
        machineTypeBasedShardCount = Math.min(machineTypeBasedShardCount, maxShardsFromProfile);
        machineTypeBasedShardCount = Math.max(machineTypeBasedShardCount, profile.minimumShards());
        
        // Cap by database pool (80% headroom)
        if (databasePoolMaxSize != null && databasePoolMaxSize > 0) {
            int safePoolCapacity = (int) Math.floor(databasePoolMaxSize * 0.8);
            machineTypeBasedShardCount = Math.min(machineTypeBasedShardCount, safePoolCapacity);
        }
        
        log.info("Machine-type-based optimization [{}]: {} vCPUs, {} workers → {} shards (profile: min={}, max={})",
                environment.machineType, environment.virtualCpus, environment.workerCount,
                machineTypeBasedShardCount, profile.minimumShards(), maxShardsFromProfile);
        
        return machineTypeBasedShardCount;
    }
    
    /**
     * Calculates shards for High-CPU machines.
     * Strategy: Maximum parallelism, more shards per vCPU.
     */
    private static int calculateForHighCpuMachine(int sizeBasedShardCount, Long estimatedRowCount,
                                                   ExecutionEnvironment environment, MachineProfile profile,
                                                   DataSizeInfo dataSizeInfo) {
        // High-CPU: Use vCPUs × maxShardsPerVcpu × workers (more aggressive)
        int cpuBasedShards = environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu();
        
        // Also consider size-based
        int machineTypeShards = Math.max(sizeBasedShardCount, cpuBasedShards);
        
        // For High-CPU, prefer more shards for better CPU utilization
        if (estimatedRowCount != null && estimatedRowCount > 0) {
            // Target smaller records per shard for High-CPU (better parallelism)
            long targetRecordsPerShard = 20_000L; // Smaller for High-CPU
            int recordsBasedShards = (int) Math.ceil((double) estimatedRowCount / targetRecordsPerShard);
            machineTypeShards = Math.max(machineTypeShards, recordsBasedShards);
        }
        
        log.info("High-CPU machine optimization: {} vCPUs × {} workers × {} maxShards/vCPU = {} base shards, final = {}",
                environment.virtualCpus, environment.workerCount, profile.maxShardsPerVcpu(),
                cpuBasedShards, machineTypeShards);
        
        return machineTypeShards;
    }
    
    /**
     * Calculates shards for High-Memory machines.
     * Strategy: Balanced, fewer shards but larger per shard (memory-optimized).
     */
    private static int calculateForHighMemMachine(int sizeBasedShardCount, Long estimatedRowCount,
                                                  ExecutionEnvironment environment, MachineProfile profile,
                                                  DataSizeInfo dataSizeInfo) {
        // High-Memory: Use vCPUs × shardsPerVcpu × workers (conservative)
        int cpuBasedShards = environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu();
        
        // For High-Memory, prefer fewer shards with larger records per shard
        int machineTypeShards = Math.max(sizeBasedShardCount, cpuBasedShards);
        
        if (estimatedRowCount != null && estimatedRowCount > 0) {
            // Target larger records per shard for High-Memory (memory-efficient)
            long targetRecordsPerShard = 100_000L; // Larger for High-Memory
            int recordsBasedShards = (int) Math.ceil((double) estimatedRowCount / targetRecordsPerShard);
            // Use the smaller of the two (fewer shards for memory efficiency)
            machineTypeShards = Math.min(machineTypeShards, Math.max(sizeBasedShardCount, recordsBasedShards));
        }
        
        log.info("High-Memory machine optimization: {} vCPUs × {} workers × {} maxShards/vCPU = {} base shards, final = {}",
                environment.virtualCpus, environment.workerCount, profile.maxShardsPerVcpu(),
                cpuBasedShards, machineTypeShards);
        
        return machineTypeShards;
    }
    
    /**
     * Calculates shards for Standard machines.
     * Strategy: Balanced approach based on vCPUs, workers, and data size.
     */
    private static int calculateForStandardMachine(int sizeBasedShardCount, Long estimatedRowCount,
                                                    ExecutionEnvironment environment, MachineProfile profile,
                                                    DataSizeInfo dataSizeInfo) {
        // Standard: Use vCPUs × shardsPerVcpu × workers
        int cpuBasedShards = environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu();
        
        // Balance between size-based and CPU-based
        int machineTypeShards = Math.max(sizeBasedShardCount, cpuBasedShards);
        
        if (estimatedRowCount != null && estimatedRowCount > 0) {
            // Standard machines: balanced records per shard
            long targetRecordsPerShard = 50_000L; // Balanced for standard
            int recordsBasedShards = (int) Math.ceil((double) estimatedRowCount / targetRecordsPerShard);
            machineTypeShards = Math.max(machineTypeShards, recordsBasedShards);
        }
        
        log.info("Standard machine optimization: {} vCPUs × {} workers × {} maxShards/vCPU = {} base shards, final = {}",
                environment.virtualCpus, environment.workerCount, profile.maxShardsPerVcpu(),
                cpuBasedShards, machineTypeShards);
        
        return machineTypeShards;
    }
    
    /**
     * Calculates minimum shards needed based on data size to ensure proper distribution.
     */
    private static int calculateMinimumShardsForDataSize(Long estimatedRowCount, DataSizeInfo dataSizeInfo,
                                                       ExecutionEnvironment environment) {
        if (estimatedRowCount == null || estimatedRowCount <= 0) {
            return 1;
        }
        
        // Ensure we have enough shards to distribute the data reasonably
        // Target: maximum 200K records per shard (safety limit)
        long maxRecordsPerShard = 200_000L;
        int minShards = (int) Math.ceil((double) estimatedRowCount / maxRecordsPerShard);
        
        // Also consider data size (if available)
        if (dataSizeInfo.hasSizeInformation && dataSizeInfo.totalSizeMb != null) {
            // Ensure we don't exceed 500 MB per shard
            double maxMbPerShard = 500.0;
            int minShardsFromSize = (int) Math.ceil(dataSizeInfo.totalSizeMb / maxMbPerShard);
            minShards = Math.max(minShards, minShardsFromSize);
        }
        
        // Ensure minimum for parallelism
        minShards = Math.max(minShards, environment.workerCount);
        
        return minShards;
    }
}
