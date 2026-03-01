package com.di.streamnova.load.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * REST response for a triggered big-table load.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BigTableLoadResponse {

    /** Unique load run identifier (UUID). */
    private String runId;

    /** Run lifecycle status at response time. */
    private String status;

    /** Machine type chosen by the shard-size ladder. */
    private String machineType;

    /** Number of shards the table was split into (final plan). */
    private int shardCount;

    /** Max concurrent shard reads (≤ poolSize). */
    private int maxConcurrent;

    /** Number of adaptive-planning iterations that ran (1-3). */
    private int iterationsUsed;

    /** Source row count from the profiler. */
    private long sourceRowCount;

    /** Warm-up measured throughput in MB/s (0 if not measured). */
    private double warmUpThroughputMbPerSec;

    /** GCS path prefix where shard files are written. */
    private String gcsStagingPath;

    /** Human-readable summary or error detail. */
    private String message;
}
