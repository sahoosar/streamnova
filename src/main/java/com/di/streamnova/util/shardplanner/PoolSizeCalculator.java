package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Calculates maximumPoolSize from pipeline_config.yml.
 * <p>Logic:
 * <ul>
 *   <li>If machine_type provided: calculate from vCPUs + profile</li>
 *   <li>Else: use maximumPoolSize from config (default 8), or fallbackPoolSize when maximumPoolSize=0</li>
 * </ul>
 */
@Slf4j
public final class PoolSizeCalculator {

    private PoolSizeCalculator() {}

    /**
     * Resolves maximumPoolSize from pipeline_config.yml.
     *
     * @param maximumPoolSizeFromConfig from pipeline_config (default 8 when machineType missing)
     * @param fallbackPoolSize          fallback when maximumPoolSize=0
     * @param machineType               machine type (e.g. n2-standard-4), or null/blank
     * @return maximumPoolSize to use
     */
    public static int calculate(int maximumPoolSizeFromConfig, int fallbackPoolSize, String machineType) {
        int poolSize;
        if (machineType != null && !machineType.isBlank()) {
            poolSize = calculateFromMachineType(machineType, fallbackPoolSize);
            log.debug("machineType provided: calculated maximumPoolSize={}", poolSize);
        } else {
            poolSize = maximumPoolSizeFromConfig > 0 ? maximumPoolSizeFromConfig : fallbackPoolSize;
            log.debug("machineType missing: maximumPoolSize from config={}", poolSize);
        }
        poolSize = Math.max(4, Math.min(poolSize, 100)); // Bounds 4–100
        return poolSize;
    }

    /**
     * Derives maximumPoolSize from shard count for local execution.
     * Pool = ceil(shardCount / 0.8) to maintain ~20% headroom.
     *
     * @param shardCount Calculated shard count
     * @return maximumPoolSize (bounded 4–100)
     */
    public static int deriveFromShardCount(int shardCount) {
        int pool = Math.max(1, (int) Math.ceil(shardCount / 0.8));
        return Math.max(4, Math.min(pool, 100));
    }

    /**
     * Calculates maximumPoolSize from machine type (vCPUs + profile).
     *
     * @param machineType       Machine type (e.g. n2-standard-4)
     * @param fallbackPoolSize  When vCPUs cannot be parsed, use this (from pipeline_config)
     * @return Calculated maximumPoolSize
     */
    public static int calculateFromMachineType(String machineType, int fallbackPoolSize) {
        int vCpus = extractVcpus(machineType);
        if (vCpus <= 0) {
            log.debug("Could not extract vCPUs from '{}', using fallback pool size", machineType);
            return fallbackPoolSize;
        }
        MachineProfile profile = MachineProfileProvider.getProfile(machineType);
        int queriesPerWorker = Math.max(1, (int) Math.ceil(vCpus * profile.jdbcConnectionsPerVcpu()));
        int shardsPerWorker = calculateShardsPerWorker(machineType, vCpus);
        return Math.max(queriesPerWorker, shardsPerWorker);
    }

    private static int calculateShardsPerWorker(String machineType, int vCpus) {
        String lower = machineType.toLowerCase();
        if (lower.contains("highcpu")) {
            return Math.max(CostOptimizationConfig.OPTIMAL_SHARDS_PER_WORKER, vCpus * 2);
        }
        if (lower.contains("highmem") || lower.contains("memory")) {
            return Math.max(4, vCpus);
        }
        return Math.max(CostOptimizationConfig.OPTIMAL_SHARDS_PER_WORKER, vCpus);
    }

    private static int extractVcpus(String machineType) {
        if (machineType == null || machineType.isBlank()) {
            return 0;
        }
        String[] parts = machineType.split("-");
        if (parts.length > 0) {
            String lastPart = parts[parts.length - 1];
            try {
                int vcpus = Integer.parseInt(lastPart);
                if (vcpus > 0) {
                    return vcpus;
                }
            } catch (NumberFormatException ignored) {
                // Not a number
            }
        }
        return 0;
    }
}
