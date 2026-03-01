package com.di.streamnova.load.plan;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Immutable description of a single shard partition range.
 *
 * <p>The range is <em>inclusive</em> on both ends:
 * {@code WHERE partitionCol BETWEEN minKey AND maxKey}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShardRange {

    /** Zero-based shard position in the overall ordered list. */
    private int shardIndex;

    /** Inclusive lower bound on the partition column. */
    private long minKey;

    /** Inclusive upper bound on the partition column. */
    private long maxKey;

    /**
     * Estimated row count for this shard.
     * Approximated as {@code (maxKey - minKey + 1)} for uniform distributions;
     * adjusted after the pilot shard in iteration 3.
     */
    private long estimatedRows;

    /**
     * {@code true} for shard 0 when iteration 3 is enabled.
     * The pilot shard is executed alone so its real duration can calibrate
     * the remaining shards before they are dispatched.
     */
    private boolean pilot;
}
