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
        
        String envName = environment.cloudProvider.name();
        log.info("[ENV: {}] Machine-type-based worker calculation: data={}, max={}, final={}", 
                envName, workersFromData, maxWorkersFromMachine, optimalWorkers);
        
        return optimalWorkers;
    }
    
    /**
     * Calculates optimal shards based on machine type, workers, and data characteristics.
     * 
     * <p><b>Calculation Strategy (when machine type is provided from config):</b></p>
     * <ol>
     *   <li><b>Data-Driven Calculation:</b> Calculate shards based on data size (MB) and record count</li>
     *   <li><b>Machine Type Optimization:</b> Apply machine-type-specific strategies (High-CPU, High-Memory, Standard)</li>
     *   <li><b>Machine Type Constraints:</b> Cap shards at machine type maximum (workers × vCPUs × maxShardsPerVcpu)</li>
     *   <li><b>Cost Optimization:</b> Optimize for cost efficiency</li>
     *   <li><b>Final Constraints:</b> Apply pool size and profile constraints again (defensive)</li>
     *   <li><b>Rounding:</b> Round to optimal value (power-of-2 for GCP, exact for local)</li>
     * </ol>
     * 
     * <p><b>Key Principle:</b> Data size drives the calculation, but machine type provides the UPPER BOUND constraint.
     * If data size suggests more shards than the machine can handle, it's capped at the machine type limit.</p>
     * 
     * <p><b>Example:</b> If data size suggests 100 shards but machine type (4 workers × 8 vCPUs × 1 maxShards/vCPU = 32 max)
     * can only handle 32 shards, the result will be capped at 32 shards.</p>
     */
    public static int calculateOptimalShardsForMachineType(
            ExecutionEnvironment environment,
            Long estimatedRowCount,
            Integer averageRowSizeBytes,
            Double targetMbPerShard,
            Integer databasePoolMaxSize) {
        
        MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
        String envName = environment.cloudProvider.name();
        
        log.info("[ENV: {}] Calculating optimal shards for machine type: {} ({} vCPUs, {} workers)", 
                envName, environment.machineType, environment.virtualCpus, environment.workerCount);
        
        // STEP 1: Calculate data size
        DataSizeInfo dataSizeInfo = DataSizeCalculator.calculateDataSize(estimatedRowCount, averageRowSizeBytes);
        
        // STEP 2: Calculate size-based shard count from data
        int sizeBasedShardCount = 1;
        if (dataSizeInfo.hasSizeInformation && dataSizeInfo.totalSizeMb != null) {
            double targetMb = (targetMbPerShard != null && targetMbPerShard > 0) 
                    ? targetMbPerShard 
                    : SizeBasedConfig.DEFAULT_TARGET_MB_PER_SHARD;
            sizeBasedShardCount = (int) Math.ceil(dataSizeInfo.totalSizeMb / targetMb);
            sizeBasedShardCount = Math.max(1, sizeBasedShardCount);
            log.info("[ENV: {}] Data-size-based calculation: {} MB total → {} shards (target: {} MB per shard)",
                    envName, String.format("%.2f", dataSizeInfo.totalSizeMb), sizeBasedShardCount, targetMb);
        } else {
            log.info("[ENV: {}] Data size information not available, using default size-based shard count: {}", envName, sizeBasedShardCount);
        }
        
        // STEP 3: Apply machine-type-based optimization (considers data size + machine type characteristics)
        // This method will cap at machine type maximum internally
        int optimizedShardCount = MachineTypeBasedOptimizer.optimizeBasedOnMachineType(
                sizeBasedShardCount, estimatedRowCount, environment, profile, 
                databasePoolMaxSize, dataSizeInfo);
        
        // STEP 4: Apply cost optimization
        optimizedShardCount = CostOptimizer.optimizeForCost(
                optimizedShardCount, environment, estimatedRowCount);
        
        // STEP 5: Apply constraints again (defensive - ensures machine type limits are respected)
        int beforeFinalConstraints = optimizedShardCount;
        optimizedShardCount = ConstraintApplier.applyConstraints(
                optimizedShardCount, environment, profile, databasePoolMaxSize);
        
        if (beforeFinalConstraints != optimizedShardCount) {
            log.info("[ENV: {}] Final constraint application adjusted shard count: {} → {} (machine type limits enforced)",
                    envName, beforeFinalConstraints, optimizedShardCount);
        }
        
        // STEP 6: Round to optimal value
        optimizedShardCount = ShardCountRounder.roundToOptimalValue(optimizedShardCount, environment);
        
        log.info("[ENV: {}] Final shard count for machine type {}: {} shards (data-driven, constrained by machine type: max={})",
                envName, environment.machineType, optimizedShardCount,
                environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu());
        
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
