package com.di.streamnova.util.shardplanner;

/**
 * Configuration for size-based shard calculation.
 */
public final class SizeBasedConfig {
    private SizeBasedConfig() {}
    
    public static final double DEFAULT_TARGET_MB_PER_SHARD = 200.0;
}
