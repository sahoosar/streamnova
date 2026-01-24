package com.di.streamnova.util.shardplanner;

/**
 * Configuration for dataset size scenarios and their optimization strategies.
 */
public final class DatasetScenarioConfig {
    private DatasetScenarioConfig() {}
    
    // Dataset size thresholds (1L = 100,000, 1M = 1,000,000)
    public static final long VERY_SMALL_THRESHOLD = 100_000L;           // < 100K records
    public static final long SMALL_MEDIUM_THRESHOLD = 500_000L;         // 100K-500K records
    public static final long MEDIUM_SMALL_THRESHOLD = 1_000_000L;       // 500K-1M records
    public static final long MEDIUM_THRESHOLD = 5_000_000L;              // 1M-5M records
    public static final long LARGE_THRESHOLD = 10_000_000L;             // 5M-10M records
    
    // Scenario 1: Very Small (< 100K records) - Maximum parallelism
    public static final long OPTIMAL_RECORDS_PER_SHARD_VERY_SMALL = 2_000L;
    public static final long MAX_RECORDS_PER_SHARD_VERY_SMALL = 5_000L;
    public static final int MIN_SHARDS_VERY_SMALL = 16;
    
    // Scenario 2: Small-Medium (100K-500K) - Fast parallel processing
    public static final long OPTIMAL_RECORDS_PER_SHARD_SMALL_MEDIUM = 5_000L;
    public static final long MAX_RECORDS_PER_SHARD_SMALL_MEDIUM = 10_000L;
    public static final int MIN_SHARDS_SMALL_MEDIUM = 12;
    
    // Scenario 3: Medium-Small (500K-1M) - Balanced fast processing
    public static final long OPTIMAL_RECORDS_PER_SHARD_MEDIUM_SMALL = 10_000L;
    public static final long MAX_RECORDS_PER_SHARD_MEDIUM_SMALL = 20_000L;
    public static final int MIN_SHARDS_MEDIUM_SMALL = 10;
    public static final int MAX_SHARDS_MEDIUM_SMALL = 100;
    
    // Scenario 4: Medium (1M-5M) - Balanced cost/performance
    public static final long OPTIMAL_RECORDS_PER_SHARD_MEDIUM = 25_000L;
    public static final long MAX_RECORDS_PER_SHARD_MEDIUM = 50_000L;
    public static final int MAX_SHARDS_MEDIUM = 200;
    
    // Scenario 5: Large (5M-10M) - Cost-effective with good parallelism
    public static final long OPTIMAL_RECORDS_PER_SHARD_LARGE = 50_000L;
    public static final long MAX_RECORDS_PER_SHARD_LARGE = 100_000L;
    public static final int MAX_SHARDS_LARGE = 500;
}
