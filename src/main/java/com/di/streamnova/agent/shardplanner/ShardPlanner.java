package com.di.streamnova.agent.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Agent-only shard planning: suggests shard count for a candidate (machine type + workers)
 * using table size and optional pool size. Used by AdaptiveExecutionPlannerService and PostgresHandler.
 */
@Slf4j
public final class ShardPlanner {

    private static final int MAX_SHARDS_CAP = 256;
    private static final int FALLBACK_SHARDS_WHEN_NO_VCPUS = 8;

    private ShardPlanner() {}

    /**
     * Suggests shard count for a candidate (machine type + worker count).
     * Uses size-based (SizeBasedConfig), machine-type optimizer, constraints, and rounding.
     *
     * @param machineType           e.g. n2-standard-4
     * @param workerCount           number of workers
     * @param estimatedRowCount     table row count (from profile)
     * @param averageRowSizeBytes   average row size in bytes (from profile)
     * @param databasePoolMaxSize optional max connection pool size (null = no pool cap)
     * @return suggested shard count (1–256)
     */
    public static int suggestShardCountForCandidate(String machineType, int workerCount,
                                                    long estimatedRowCount, int averageRowSizeBytes,
                                                    Integer databasePoolMaxSize) {
        int vCpus = PoolSizeCalculator.extractVcpus(machineType);
        if (vCpus <= 0) {
            log.debug("[CANDIDATE] Could not parse vCPUs from machineType '{}', returning fallback shards", machineType);
            return FALLBACK_SHARDS_WHEN_NO_VCPUS;
        }
        ExecutionEnvironment environment = new ExecutionEnvironment(
                machineType, vCpus, workerCount, false, ExecutionEnvironment.CloudProvider.GCP);

        Long rowCount = estimatedRowCount > 0 ? estimatedRowCount : null;
        Integer rowSize = averageRowSizeBytes > 0 ? averageRowSizeBytes : null;

        int shards = UnifiedCalculator.calculateOptimalShardsForMachineType(
                environment,
                rowCount,
                rowSize,
                SizeBasedConfig.DEFAULT_TARGET_MB_PER_SHARD,
                databasePoolMaxSize);

        if (shards == 1 && workerCount > 1) {
            MachineProfile profile = MachineProfileProvider.getProfile(machineType);
            int maxShards = MachineTypeResourceValidator.calculateMaxShardsForMachineType(environment, profile);
            shards = Math.min(Math.max(1, workerCount), maxShards);
            shards = ShardCountRounder.roundToOptimalValue(shards, environment);
            log.debug("[CANDIDATE] Min-for-parallelism applied (workers={}) → shards={}", workerCount, shards);
        }

        return Math.max(1, Math.min(shards, MAX_SHARDS_CAP));
    }
}
