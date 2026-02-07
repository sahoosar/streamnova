package com.di.streamnova.agent.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Unified calculator for optimal shards based on machine type, workers, and data characteristics.
 * Agent-only: no worker calculation (removed with WorkerCountCalculator).
 */
@Slf4j
public final class UnifiedCalculator {
    private UnifiedCalculator() {}

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
                optimizedShardCount, environment, sizeBasedShardCount);

        optimizedShardCount = ConstraintApplier.applyConstraints(
                optimizedShardCount, environment, profile, databasePoolMaxSize);

        optimizedShardCount = ShardCountRounder.roundToOptimalValue(optimizedShardCount, environment);

        log.info("[ENV: {}] Final shard count for machine type {}: {} shards (max={})",
                envName, environment.machineType, optimizedShardCount,
                environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu());
        return optimizedShardCount;
    }
}
