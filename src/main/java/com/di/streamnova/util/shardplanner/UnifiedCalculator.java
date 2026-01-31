package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Unified calculator that calculates both shards and workers together based on machine type.
 */
@Slf4j
public final class UnifiedCalculator {
    private UnifiedCalculator() {}

    /**
     * Calculates optimal workers based on machine type and data characteristics.
     *
     * @param minimumWorkers When > 0, ensures at least this many workers (null = no floor)
     */
    public static int calculateOptimalWorkersForMachineType(
            ExecutionEnvironment environment,
            Long estimatedRowCount,
            Integer averageRowSizeBytes,
            Integer databasePoolMaxSize,
            Integer minimumWorkers) {

        MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
        int workersFromData = WorkerCountCalculator.calculateMinimumWorkersForData(
                estimatedRowCount, averageRowSizeBytes, environment, profile);
        int maxWorkersFromMachine = MachineTypeResourceValidator.calculateMaxWorkersForMachineType(
                environment, profile);

        int optimalWorkers = workersFromData;
        if (minimumWorkers != null && minimumWorkers > 0) {
            optimalWorkers = Math.max(optimalWorkers, minimumWorkers);
        }
        optimalWorkers = Math.min(optimalWorkers, maxWorkersFromMachine);
        optimalWorkers = Math.max(1, optimalWorkers);
        optimalWorkers = WorkerCountCalculator.roundToOptimalWorkerCount(optimalWorkers);

        String envName = environment.cloudProvider.name();
        log.info("[ENV: {}] Machine-type-based worker calculation: data={}, minWorkers={}, max={}, final={}",
                envName, workersFromData, minimumWorkers != null && minimumWorkers > 0 ? minimumWorkers : "none",
                maxWorkersFromMachine, optimalWorkers);
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
        String envName = environment.cloudProvider.name();

        log.info("[ENV: {}] Calculating optimal shards for machine type: {} ({} vCPUs, {} workers)",
                envName, environment.machineType, environment.virtualCpus, environment.workerCount);

        DataSizeInfo dataSizeInfo = DataSizeCalculator.calculateDataSize(estimatedRowCount, averageRowSizeBytes);

        int sizeBasedShardCount = 1;
        if (dataSizeInfo.hasSizeInformation && dataSizeInfo.totalSizeMb != null) {
            double targetMb = (targetMbPerShard != null && targetMbPerShard > 0)
                    ? targetMbPerShard
                    : SizeBasedConfig.DEFAULT_TARGET_MB_PER_SHARD;
            sizeBasedShardCount = (int) Math.ceil(dataSizeInfo.totalSizeMb / targetMb);
            sizeBasedShardCount = Math.max(1, sizeBasedShardCount);
            log.info("[ENV: {}] Data-size-based calculation: {} MB total → {} shards (target: {} MB per shard)",
                    envName, String.format("%.2f", dataSizeInfo.totalSizeMb), sizeBasedShardCount, targetMb);
        }

        int optimizedShardCount = MachineTypeBasedOptimizer.optimizeBasedOnMachineType(
                sizeBasedShardCount, estimatedRowCount, environment, profile,
                databasePoolMaxSize, dataSizeInfo);

        optimizedShardCount = CostOptimizer.optimizeForCost(
                optimizedShardCount, environment, estimatedRowCount);

        optimizedShardCount = ConstraintApplier.applyConstraints(
                optimizedShardCount, environment, profile, databasePoolMaxSize);

        optimizedShardCount = ShardCountRounder.roundToOptimalValue(optimizedShardCount, environment);

        log.info("[ENV: {}] Final shard count for machine type {}: {} shards (max={})",
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
