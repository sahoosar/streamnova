package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Rounds shard count to optimal value (power-of-2 for GCP, exact for local).
 */
@Slf4j
public final class ShardCountRounder {
    private ShardCountRounder() {}
    
    public static int roundToOptimalValue(int shardCount, ExecutionEnvironment environment) {
        if (environment.isLocalExecution) {
            log.info("Local execution: using exact shard count {} (no power-of-2 rounding)", shardCount);
            return shardCount;
        }
        
        // GCP with larger shard count: use power-of-2 for load balancing
        // Note: Shards are calculated based on data size, not forced to match vCPU count
        int rounded = roundToNextPowerOfTwo(shardCount);
        
        // Log if result happens to match vCPU count (coincidence, not forced)
        if (rounded == environment.virtualCpus) {
            log.info("Rounded shard count {} matches vCPU count {} (coincidental, based on data size calculation)", 
                    rounded, environment.virtualCpus);
        }
        
        return rounded;
    }
    
    private static int roundToNextPowerOfTwo(int n) {
        if (n <= 1) return 1;
        if (n > (1 << 30)) return 1 << 30;
        n--;
        n |= n >> 1; n |= n >> 2; n |= n >> 4; n |= n >> 8; n |= n >> 16;
        return n + 1;
    }
}
