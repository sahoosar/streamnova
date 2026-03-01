package com.di.streamnova.load.plan;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

/**
 * Snapshot produced by one planning iteration (1, 2, or 3).
 *
 * <p>Shard sizing is <em>table-size based</em> throughout:
 * <pre>
 *   shardCount = ⌈ totalDataBytes / targetBytesPerShard ⌉
 * </pre>
 *
 * <h3>Three iterations</h3>
 * <ol>
 *   <li><strong>Ladder estimate</strong> – {@code targetBytesPerShard} from
 *       {@link MachineShardLadder}; {@code totalDataBytes = sourceRowCount × avgRowBytes}.</li>
 *   <li><strong>Warm-up calibration</strong> – 10 K-row probe → measured MB/s →
 *       {@code targetBytesPerShard = measuredMbPerSec × targetShardDurationSec × 1_048_576}.</li>
 *   <li><strong>Pilot-shard feedback</strong> (optional) – shard 0 runs alone;
 *       its actual GCS file size ÷ duration gives a precise throughput estimate,
 *       which re-slices shards 1..n.</li>
 * </ol>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IterativePlanResult {

    /** 1, 2, or 3. */
    private int iteration;

    /** Machine type driving the shard-size ladder (e.g. {@code n2d-standard-8}). */
    private String machineType;

    // ---- sizing -------------------------------------------------------

    /** Target bytes per shard chosen in this iteration (ladder / calibrated). */
    private long targetBytesPerShard;

    /** Convenience: {@code targetBytesPerShard / 1_048_576.0} — for log messages. */
    public double getTargetMbPerShard() {
        return targetBytesPerShard / (1024.0 * 1024.0);
    }

    /** Target shard wall-clock duration in seconds (from request; used for calibration). */
    private int targetShardDurationSec;

    // ---- table dimensions --------------------------------------------

    /** Source table row count (from profiler). */
    private long totalRows;

    /** Source table size estimate: {@code totalRows × avgRowBytes} (bytes). */
    private long totalDataBytes;

    /** Average row size in bytes (from profiler). */
    private long avgRowBytes;

    // ---- plan output -------------------------------------------------

    /** Final shard count for this iteration. */
    private int shardCount;

    /** {@code min(shardCount, poolSize)} — caps JDBC concurrency. */
    private int maxConcurrent;

    // ---- iteration-specific telemetry --------------------------------

    /**
     * Warm-up measured throughput in MB/s (iteration 2 only; 0 otherwise).
     */
    private double warmUpThroughputMbPerSec;

    /**
     * Measured pilot-shard duration in ms (iteration 3 only; 0 otherwise).
     */
    private long pilotShardDurationMs;

    /**
     * Actual GCS file size of the pilot shard in bytes (iteration 3 only; 0 otherwise).
     * Used together with {@code pilotShardDurationMs} to compute exact throughput.
     */
    private long pilotShardActualBytes;

    // ---- audit -------------------------------------------------------

    /** Human-readable explanation for any adjustment made in this iteration. */
    private String adjustmentReason;

    /** Final shard ranges produced by this iteration. */
    private List<ShardRange> shards;

    /** Wall-clock time when this plan was computed. */
    private Instant computedAt;
}
