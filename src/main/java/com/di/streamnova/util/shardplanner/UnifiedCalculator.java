package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Unified calculator that calculates both shards and workers together based on machine type.
 * This ensures shards and workers are calculated as a cohesive unit.
 */
@Slf4j
public final class UnifiedCalculator {
    private UnifiedCalculator() {}
    
    /**
     * Calculates optimal workers based on machine type and data characteristics.
     */
    public static int calculateOptimalWorkersForMachineType(
            ExecutionEnvironment environment,
            Long estimatedRowCount,
            Integer averageRowSizeBytes,
            Integer databasePoolMaxSize) {
        
        MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
        
        // Calculate based on data size
        int workersFromData = WorkerCountCalculator.calculateMinimumWorkersForData(
                estimatedRowCount, averageRowSizeBytes, environment, profile);
        
        // Calculate based on machine type maximum
        int maxWorkersFromMachine = MachineTypeResourceValidator.calculateMaxWorkersForMachineType(
                environment, profile);
        
        // Start with data requirements
        int optimalWorkers = workersFromData;
        
        // Cap at machine type maximum
        optimalWorkers = Math.min(optimalWorkers, maxWorkersFromMachine);
        
        // Ensure minimum of 1
        optimalWorkers = Math.max(1, optimalWorkers);
        
        // Round to power of 2 for GCP efficiency
        optimalWorkers = WorkerCountCalculator.roundToOptimalWorkerCount(optimalWorkers);
        
        log.info("Machine-type-based worker calculation: data={}, max={}, final={}", 
                workersFromData, maxWorkersFromMachine, optimalWorkers);
        
        return optimalWorkers;
    }
    
    /**
     * Calculates optimal shards based on machine type, workers, and data characteristics.
     */
    public static int calculateOptimalShardsForMachineType(
            ExecutionEnvironment environment,
            Long estimatedRowCount,
            Integer averageRowSizeBytes,
            Double targetMbPerShard,
            Integer databasePoolMaxSize) {
        
        MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
        
        // Calculate data size
        DataSizeInfo dataSizeInfo = DataSizeCalculator.calculateDataSize(estimatedRowCount, averageRowSizeBytes);
        
        // Use machine-type-based optimizer
        int sizeBasedShardCount = 1;
        if (dataSizeInfo.hasSizeInformation && dataSizeInfo.totalSizeMb != null) {
            double targetMb = (targetMbPerShard != null && targetMbPerShard > 0) 
                    ? targetMbPerShard 
                    : SizeBasedConfig.DEFAULT_TARGET_MB_PER_SHARD;
            sizeBasedShardCount = (int) Math.ceil(dataSizeInfo.totalSizeMb / targetMb);
            sizeBasedShardCount = Math.max(1, sizeBasedShardCount);
        }
        
        // Use machine-type-based optimization
        int optimizedShardCount = MachineTypeBasedOptimizer.optimizeBasedOnMachineType(
                sizeBasedShardCount, estimatedRowCount, environment, profile, 
                databasePoolMaxSize, dataSizeInfo);
        
        // Apply cost optimization
        optimizedShardCount = CostOptimizer.optimizeForCost(
                optimizedShardCount, environment, estimatedRowCount);
        
        // Apply constraints
        optimizedShardCount = ConstraintApplier.applyConstraints(
                optimizedShardCount, environment, profile, databasePoolMaxSize);
        
        // Round to optimal value
        optimizedShardCount = ShardCountRounder.roundToOptimalValue(optimizedShardCount, environment);
        
        return optimizedShardCount;
    }
    
    /**
     * Calculates workers from shard count (for record-count-based scenarios).
     */
    public static int calculateWorkersFromShards(int shardCount, ExecutionEnvironment environment) {
        MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
        int shardsPerWorker = CostOptimizer.calculateOptimalShardsPerWorker(environment);
        int workersNeeded = (int) Math.ceil((double) shardCount / shardsPerWorker);
        workersNeeded = WorkerCountCalculator.roundToOptimalWorkerCount(workersNeeded);
        return Math.max(1, workersNeeded);
    }
}
