package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Adjusts shard count based on machine type characteristics.
 */
@Slf4j
public final class MachineTypeAdjuster {
    private MachineTypeAdjuster() {}
    
    public static int adjustForMachineType(int baseShardCount, ExecutionEnvironment environment, 
                                           MachineProfile profile) {
        if (environment.isLocalExecution) {
            return baseShardCount; // No adjustment for local execution
        }
        
        String machineTypeLower = environment.machineType.toLowerCase();
        int adjustedShardCount = baseShardCount;
        
        if (machineTypeLower.contains("highcpu")) {
            adjustedShardCount = (int) Math.ceil(baseShardCount * 1.5);
            log.info("HighCPU machine detected: increasing shards from {} to {} for better CPU utilization",
                    baseShardCount, adjustedShardCount);
        } else if (machineTypeLower.contains("highmem") || machineTypeLower.contains("memory")) {
            adjustedShardCount = (int) Math.floor(baseShardCount * 0.8);
            adjustedShardCount = Math.max(adjustedShardCount, profile.minimumShards());
            log.info("HighMem machine detected: reducing shards from {} to {} for memory-optimized processing",
                    baseShardCount, adjustedShardCount);
        } else if (machineTypeLower.contains("standard")) {
            if (environment.virtualCpus >= 8) {
                adjustedShardCount = (int) Math.ceil(baseShardCount * 1.1);
                log.info("Large standard machine detected ({} vCPUs): slight increase from {} to {}",
                        environment.virtualCpus, baseShardCount, adjustedShardCount);
            } else {
                log.info("Standard machine detected: using base shard count {}", baseShardCount);
            }
        }
        
        // Ensure adjusted shards respect profile constraints
        int maxShardsFromProfile = environment.isLocalExecution
                ? environment.virtualCpus * profile.maxShardsPerVcpu()
                : environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu();
        adjustedShardCount = Math.min(adjustedShardCount, maxShardsFromProfile);
        adjustedShardCount = Math.max(adjustedShardCount, profile.minimumShards());
        
        return adjustedShardCount;
    }
}
