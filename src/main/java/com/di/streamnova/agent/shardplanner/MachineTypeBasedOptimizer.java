package com.di.streamnova.agent.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Optimizes shard count primarily based on machine type and data size (MB).
 * Data size is primary; record count is used only when size information is not available.
 */
@Slf4j
public final class MachineTypeBasedOptimizer {
    private MachineTypeBasedOptimizer() {}
    
    /**
     * Optimizes shard count based on machine type as the primary factor.
     *
     * @param optimizerConfig Configurable thresholds (from ShardPlannerProperties)
     */
    public static int optimizeBasedOnMachineType(int sizeBasedShardCount, Long estimatedRowCount,
                                               ExecutionEnvironment environment, MachineProfile profile,
                                               Integer databasePoolMaxSize, DataSizeInfo dataSizeInfo,
                                               OptimizerConfig optimizerConfig) {
        
        String envName = environment.cloudProvider.name();
        log.info("[ENV: {}] Machine-type-based optimization starting [{}]: {} vCPUs, {} workers, data-size-based={}, hasSizeInfo={}",
                envName, environment.machineType, environment.virtualCpus, environment.workerCount, 
                sizeBasedShardCount, dataSizeInfo.hasSizeInformation);
        
        String machineTypeLower = environment.machineType.toLowerCase();
        int machineTypeBasedShardCount;
        
        // Calculate based on machine type characteristics
        // NOTE: Data size is considered first, then machine type constraints are applied
        if (machineTypeLower.contains("highcpu")) {
            machineTypeBasedShardCount = calculateForHighCpuMachine(
                    sizeBasedShardCount, estimatedRowCount, environment, profile, dataSizeInfo, optimizerConfig);
        } else if (machineTypeLower.contains("highmem") || machineTypeLower.contains("memory")) {
            machineTypeBasedShardCount = calculateForHighMemMachine(
                    sizeBasedShardCount, estimatedRowCount, environment, profile, dataSizeInfo, optimizerConfig);
        } else {
            machineTypeBasedShardCount = calculateForStandardMachine(
                    sizeBasedShardCount, estimatedRowCount, environment, profile, dataSizeInfo, optimizerConfig);
        }
        
        log.debug("After machine-type-specific calculation: {} shards", machineTypeBasedShardCount);
        
        int minShardsForData = calculateMinimumShardsForDataSize(estimatedRowCount, dataSizeInfo, environment, optimizerConfig);
        if (minShardsForData > machineTypeBasedShardCount) {
            log.info("[ENV: {}] Data requires minimum {} shards (current: {}), adjusting upward",
                    envName, minShardsForData, machineTypeBasedShardCount);
        }
        machineTypeBasedShardCount = Math.max(machineTypeBasedShardCount, minShardsForData);
        
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
        
        double headroom = optimizerConfig.poolHeadroomRatio() > 0 && optimizerConfig.poolHeadroomRatio() <= 1
                ? optimizerConfig.poolHeadroomRatio() : 0.8;
        if (databasePoolMaxSize != null && databasePoolMaxSize > 0) {
            int safePoolCapacity = (int) Math.floor(databasePoolMaxSize * headroom);
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
                                                   DataSizeInfo dataSizeInfo, OptimizerConfig config) {
        int cpuBasedShards = environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu();
        int machineTypeShards = Math.max(sizeBasedShardCount, cpuBasedShards);
        long targetRecords = config.highCpuTargetRecordsPerShard() > 0 ? config.highCpuTargetRecordsPerShard() : 20_000L;
        if (!dataSizeInfo.hasSizeInformation && estimatedRowCount != null && estimatedRowCount > 0) {
            int recordsBasedShards = (int) Math.ceil((double) estimatedRowCount / targetRecords);
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
                                                  DataSizeInfo dataSizeInfo, OptimizerConfig config) {
        int cpuBasedShards = environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu();
        int machineTypeShards = Math.min(sizeBasedShardCount, cpuBasedShards);
        long targetRecords = config.highMemTargetRecordsPerShard() > 0 ? config.highMemTargetRecordsPerShard() : 100_000L;
        if (!dataSizeInfo.hasSizeInformation && estimatedRowCount != null && estimatedRowCount > 0) {
            int recordsBasedShards = (int) Math.ceil((double) estimatedRowCount / targetRecords);
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
                                                    DataSizeInfo dataSizeInfo, OptimizerConfig config) {
        int machineTypeShards = sizeBasedShardCount;
        int maxShardsFromCpu = environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu();
        long targetRecords = config.standardTargetRecordsPerShard() > 0 ? config.standardTargetRecordsPerShard() : 50_000L;
        if (!dataSizeInfo.hasSizeInformation && estimatedRowCount != null && estimatedRowCount > 0) {
            int recordsBasedShards = (int) Math.ceil((double) estimatedRowCount / targetRecords);
            machineTypeShards = Math.max(sizeBasedShardCount, recordsBasedShards);
        }
        
        // Minimum for parallelism: total vCPUs so work distributes across cores.
        // E.g. n2-standard-4: 1 worker→4, 2 workers→8, 4 workers→16. Capped by profile max.
        int totalVcpus = environment.workerCount * environment.virtualCpus;
        int minShardsForParallelism = Math.min(Math.max(1, totalVcpus), maxShardsFromCpu);
        machineTypeShards = Math.max(machineTypeShards, minShardsForParallelism);
        
        // Cap at CPU-based maximum (constraint)
        machineTypeShards = Math.min(machineTypeShards, maxShardsFromCpu);
        
        String envName = environment.cloudProvider.name();
        log.info("[ENV: {}] Standard machine optimization: data-based={}, min-parallelism={}, max-from-cpu={}, final={}",
                envName, sizeBasedShardCount, minShardsForParallelism, maxShardsFromCpu, machineTypeShards);
        
        return machineTypeShards;
    }
    
    /**
     * Minimum shards for data: prefer data size (MB); use record count only when size is not available.
     */
    private static int calculateMinimumShardsForDataSize(Long estimatedRowCount, DataSizeInfo dataSizeInfo,
                                                       ExecutionEnvironment environment, OptimizerConfig config) {
        int minShards = 1;
        if (dataSizeInfo.hasSizeInformation && dataSizeInfo.totalSizeMb != null && dataSizeInfo.totalSizeMb > 0) {
            double maxMb = config.maxMbPerShard() > 0 ? config.maxMbPerShard() : 500.0;
            minShards = (int) Math.ceil(dataSizeInfo.totalSizeMb / maxMb);
        } else if (estimatedRowCount != null && estimatedRowCount > 0) {
            long maxRecords = config.fallbackMaxRecordsPerShard() > 0 ? config.fallbackMaxRecordsPerShard() : 200_000L;
            minShards = (int) Math.ceil((double) estimatedRowCount / maxRecords);
        }
        minShards = Math.max(minShards, environment.workerCount);
        return minShards;
    }
}
