package com.di.streamnova.agent.shardplanner;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Agent-only shard planning: suggests shard count for a candidate (machine type + workers)
 * using table size and optional pool size. Used by AdaptiveExecutionPlannerService and PostgresHandler.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ShardPlanner {

    private final ShardPlannerProperties properties;

    /**
     * Suggests shard count for a candidate (machine type + worker count).
     * Uses size-based (SizeBasedConfig), machine-type optimizer, constraints, and rounding.
     *
     * @param machineType           e.g. n2-standard-4
     * @param workerCount           number of workers
     * @param estimatedRowCount     table row count (from profile)
     * @param averageRowSizeBytes   average row size in bytes (from profile)
     * @param databasePoolMaxSize   optional max connection pool size (null = no pool cap)
     * @return suggested shard count (1 to maxShardsCap)
     */
    public int suggestShardCountForCandidate(String machineType, int workerCount,
                                            long estimatedRowCount, int averageRowSizeBytes,
                                            Integer databasePoolMaxSize) {
        int vCpus = PoolSizeCalculator.extractVcpus(machineType);
        if (vCpus <= 0) {
            log.debug("[CANDIDATE] Could not parse vCPUs from machineType '{}', returning fallback shards", machineType);
            return properties.getFallbackShardsWhenNoVcpus();
        }
        ExecutionEnvironment environment = new ExecutionEnvironment(
                machineType, vCpus, workerCount, false, ExecutionEnvironment.CloudProvider.GCP);

        Long rowCount = estimatedRowCount > 0 ? estimatedRowCount : null;
        Integer rowSize = averageRowSizeBytes > 0 ? averageRowSizeBytes : null;

        OptimizerConfig optimizerConfig = properties.toOptimizerConfig();
        int shards = UnifiedCalculator.calculateOptimalShardsForMachineType(
                environment,
                rowCount,
                rowSize,
                SizeBasedConfig.DEFAULT_TARGET_MB_PER_SHARD,
                databasePoolMaxSize,
                optimizerConfig);

        if (shards == 1 && workerCount > 1) {
            MachineProfile profile = MachineProfileProvider.getProfile(machineType);
            int maxShards = MachineTypeResourceValidator.calculateMaxShardsForMachineType(environment, profile);
            shards = Math.min(Math.max(1, workerCount), maxShards);
            shards = ShardCountRounder.roundToOptimalValue(shards, environment);
            log.debug("[CANDIDATE] Min-for-parallelism applied (workers={}) → shards={}", workerCount, shards);
        }

        int cap = (databasePoolMaxSize != null && databasePoolMaxSize > 0)
                ? databasePoolMaxSize
                : properties.getMaxShardsCap();
        return Math.max(1, Math.min(shards, cap));
    }

    /**
     * Suggests a shard plan with optional shift-based (phased) execution.
     * When the size-based partition count exceeds pool capacity, returns partitionCount (logical)
     * and maxConcurrentShards (cap at ~80% of pool) so the load can run in waves.
     *
     * @param machineType           e.g. n2-standard-4
     * @param workerCount           number of workers
     * @param estimatedRowCount     table row count (from profile)
     * @param averageRowSizeBytes   average row size in bytes (from profile)
     * @param databasePoolMaxSize   optional max connection pool size (null = no pool cap, no shift)
     * @return plan with partitionCount (logical) and maxConcurrentShards (for reservation/concurrency)
     */
    public ShardPlan suggestShardPlanWithConcurrency(String machineType, int workerCount,
                                                      long estimatedRowCount, int averageRowSizeBytes,
                                                      Integer databasePoolMaxSize) {
        // Partition count: ideal from size/machine, without pool cap (so we can have 20 logical partitions)
        int partitionCount = suggestShardCountForCandidate(
                machineType, workerCount, estimatedRowCount, averageRowSizeBytes, null);
        int maxConcurrentShards = partitionCount;
        if (databasePoolMaxSize != null && databasePoolMaxSize > 0) {
            int poolBased = Math.max(1, (int) Math.floor(databasePoolMaxSize * 0.8));
            if (partitionCount > poolBased) {
                maxConcurrentShards = poolBased;
                log.info("[SHARD_PLAN] Shift-based: partitionCount={}, maxConcurrentShards={} (80% of pool {})",
                        partitionCount, maxConcurrentShards, databasePoolMaxSize);
            }
        }
        return ShardPlan.builder()
                .partitionCount(partitionCount)
                .maxConcurrentShards(maxConcurrentShards)
                .build();
    }

    /**
     * Calculates optimal shard count for the given table size and machine type (used by AI RecommendationTool).
     */
    public int calculateShards(long rowCount, double tableSizeMb, String machineType, int workerCount) {
        int avgRowBytes = (rowCount > 0 && tableSizeMb > 0)
                ? (int) ((tableSizeMb * 1024 * 1024) / rowCount)
                : 1024;
        return suggestShardCountForCandidate(machineType, workerCount, rowCount, avgRowBytes, null);
    }
}
