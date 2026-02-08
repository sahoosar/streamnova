package com.di.streamnova.agent.shardplanner;

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
        
        String envName = environment.cloudProvider.name();
        log.info("[ENV: {}] Machine-type-based optimization starting [{}]: {} vCPUs, {} workers, data-size-based={}, estimated-rows={}",
                envName, environment.machineType, environment.virtualCpus, environment.workerCount, 
                sizeBasedShardCount, estimatedRowCount);
        
        String machineTypeLower = environment.machineType.toLowerCase();
        int machineTypeBasedShardCount;
        
        // Calculate based on machine type characteristics
        // NOTE: Data size is considered first, then machine type constraints are applied
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
        
        log.debug("After machine-type-specific calculation: {} shards", machineTypeBasedShardCount);
        
        // Apply data size constraints (ensure we have enough shards for the data)
        if (estimatedRowCount != null && estimatedRowCount > 0) {
            // Ensure minimum shards based on data volume
            int minShardsForData = calculateMinimumShardsForDataSize(
                    estimatedRowCount, dataSizeInfo, environment);
            if (minShardsForData > machineTypeBasedShardCount) {
                log.info("[ENV: {}] Data size requires minimum {} shards (current: {}), adjusting upward", 
                        envName, minShardsForData, machineTypeBasedShardCount);
            }
            machineTypeBasedShardCount = Math.max(machineTypeBasedShardCount, minShardsForData);
        }
        
        // CRITICAL: Cap by machine profile limits (machine type constraint)
        // Formula: max_shards = workers × vCPUs × maxShardsPerVcpu
        int maxShardsFromProfile = environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu();
        int beforeCapping = machineTypeBasedShardCount;
        machineTypeBasedShardCount = Math.min(machineTypeBasedShardCount, maxShardsFromProfile);
        
        if (beforeCapping > maxShardsFromProfile) {
            log.warn("[ENV: {}] Data-size-based calculation ({}) exceeds machine type maximum ({} workers × {} vCPUs × {} maxShards/vCPU = {}). " +
                    "Capping at machine type limit to respect resource constraints.",
                    envName, beforeCapping, environment.workerCount, environment.virtualCpus, 
                    profile.maxShardsPerVcpu(), maxShardsFromProfile);
        }
        
        // Ensure minimum shards from profile, but never exceed machine type cap
        int effectiveMinShards = Math.min(profile.minimumShards(), maxShardsFromProfile);
        machineTypeBasedShardCount = Math.max(machineTypeBasedShardCount, effectiveMinShards);
        
        // Cap by database pool (80% headroom)
        if (databasePoolMaxSize != null && databasePoolMaxSize > 0) {
            int safePoolCapacity = (int) Math.floor(databasePoolMaxSize * 0.8);
            if (machineTypeBasedShardCount > safePoolCapacity) {
                log.warn("[ENV: {}] Shard count ({}) exceeds database pool capacity ({}). Capping at pool limit.",
                        envName, machineTypeBasedShardCount, safePoolCapacity);
            }
            machineTypeBasedShardCount = Math.min(machineTypeBasedShardCount, safePoolCapacity);
        }
        
        log.info("[ENV: {}] Machine-type-based optimization [{}]: {} vCPUs, {} workers → {} shards " +
                "(data-driven calculation, capped by machine type: max={}, profile-min={})",
                envName, environment.machineType, environment.virtualCpus, environment.workerCount,
                machineTypeBasedShardCount, maxShardsFromProfile, profile.minimumShards());
        
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
        
        String envName = environment.cloudProvider.name();
        log.info("[ENV: {}] High-CPU machine optimization: {} vCPUs × {} workers × {} maxShards/vCPU = {} base shards, final = {}",
                envName, environment.virtualCpus, environment.workerCount, profile.maxShardsPerVcpu(),
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
        // Start with the minimum of size-based and CPU-based to prefer fewer shards
        int machineTypeShards = Math.min(sizeBasedShardCount, cpuBasedShards);
        
        if (estimatedRowCount != null && estimatedRowCount > 0) {
            // Target larger records per shard for High-Memory (memory-efficient)
            long targetRecordsPerShard = 100_000L; // Larger for High-Memory
            int recordsBasedShards = (int) Math.ceil((double) estimatedRowCount / targetRecordsPerShard);
            // For High-Memory, prefer fewer shards: take the minimum of all options
            // This ensures we don't create more shards than necessary, optimizing for memory
            machineTypeShards = Math.min(machineTypeShards, Math.min(sizeBasedShardCount, recordsBasedShards));
        }
        
        String envName = environment.cloudProvider.name();
        log.info("[ENV: {}] High-Memory machine optimization: {} vCPUs × {} workers × {} maxShards/vCPU = {} base shards, final = {} (preferring fewer shards for memory efficiency)",
                envName, environment.virtualCpus, environment.workerCount, profile.maxShardsPerVcpu(),
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
        // Standard: Calculate based on data size first, vCPUs is a constraint (max), not a target
        int machineTypeShards = sizeBasedShardCount;
        
        // Calculate CPU-based maximum (this is a constraint, not a target)
        int maxShardsFromCpu = environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu();
        
        if (estimatedRowCount != null && estimatedRowCount > 0) {
            // Standard machines: balanced records per shard
            long targetRecordsPerShard = 50_000L; // Balanced for standard
            int recordsBasedShards = (int) Math.ceil((double) estimatedRowCount / targetRecordsPerShard);
            // Use data-based calculation (size or records), not CPU-based
            machineTypeShards = Math.max(sizeBasedShardCount, recordsBasedShards);
        }
        
        // Recommended safe shards for standard: total vCPUs (workers × vCPUs) so work distributes across all cores.
        // E.g. n2-standard-4: 1 worker→4, 2 workers→8, 4 workers→16. Capped by profile max.
        int totalVcpus = environment.workerCount * environment.virtualCpus;
        int minShardsForParallelism = Math.min(Math.max(1, totalVcpus), maxShardsFromCpu);
        machineTypeShards = Math.max(machineTypeShards, minShardsForParallelism);
        
        // Cap at CPU-based maximum (constraint)
        machineTypeShards = Math.min(machineTypeShards, maxShardsFromCpu);
        
        String envName = environment.cloudProvider.name();
        log.info("[ENV: {}] Standard machine optimization: data-based={}, records-based={}, min-parallelism={}, max-from-cpu={}, final={}",
                envName, sizeBasedShardCount, 
                (estimatedRowCount != null && estimatedRowCount > 0) ? (int) Math.ceil((double) estimatedRowCount / 50_000L) : 0,
                minShardsForParallelism, maxShardsFromCpu, machineTypeShards);
        
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
