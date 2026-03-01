package com.di.streamnova.agent.shardplanner;

import lombok.Builder;
import lombok.Value;

/**
 * Result of shard planning with optional shift-based (phased) execution.
 * When partitionCount &gt; maxConcurrentShards, the load runs in waves: up to maxConcurrentShards
 * shards at a time until all partitionCount partitions are done.
 */
@Value
@Builder
public class ShardPlan {
    /** Logical number of partitions (table split and task count). Used for query and pipeline. */
    int partitionCount;
    /** Max shards running concurrently (reservation and connection limit). When &lt; partitionCount, load runs in phases. */
    int maxConcurrentShards;

    /** Effective shard count for display/API: same as partitionCount. */
    public int getShardCount() {
        return partitionCount;
    }
}
