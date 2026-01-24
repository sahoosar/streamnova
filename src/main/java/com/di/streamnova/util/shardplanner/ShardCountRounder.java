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
        
        // If close to vCPU count, use exact vCPU count
        if (Math.abs(shardCount - environment.virtualCpus) <= 2 
                && shardCount >= environment.virtualCpus * 0.8) {
            log.info("Using exact vCPU count {} instead of power-of-2 rounding (target was {})", 
                    environment.virtualCpus, shardCount);
            return environment.virtualCpus;
        }
        
        // GCP with larger shard count: use power-of-2 for load balancing
        return roundToNextPowerOfTwo(shardCount);
    }
    
    private static int roundToNextPowerOfTwo(int n) {
        if (n <= 1) return 1;
        if (n > (1 << 30)) return 1 << 30;
        n--;
        n |= n >> 1; n |= n >> 2; n |= n >> 4; n |= n >> 8; n |= n >> 16;
        return n + 1;
    }
}
