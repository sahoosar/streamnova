package com.di.streamnova.load.plan;

import com.di.streamnova.load.dto.BigTableLoadRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs up to 3 adaptive planning iterations to produce the final shard plan.
 *
 * <p>All shard sizing is <strong>table-size based</strong>:
 * <pre>
 *   shardCount = ⌈ totalDataBytes / targetBytesPerShard ⌉
 * </pre>
 * where {@code totalDataBytes = profiled_row_count × profiled_avg_row_bytes}.
 *
 * <h3>Iteration 1 — Ladder estimate</h3>
 * {@link MachineShardLadder} maps machine type → bytes/shard.
 * <pre>
 *   Example (n2d-standard-8, 10 M rows × 500 B):
 *     targetBytesPerShard = 512 MB
 *     totalDataBytes      = 10 000 000 × 500 = ~4.77 GB
 *     shardCount          = ⌈4 880 MB / 512 MB⌉ = 10
 *     maxConcurrent       = min(10, pool=10) = 10   ← one wave fills the pool
 * </pre>
 *
 * <h3>Iteration 2 — Warm-up calibration</h3>
 * Reads {@value #WARMUP_ROWS} rows and measures actual throughput in MB/s:
 * <pre>
 *   targetBytesPerShard = measuredMbPerSec × targetShardDurationSec × 1_048_576
 *   shardCount          = ⌈ totalDataBytes / targetBytesPerShard ⌉
 * </pre>
 * If the warm-up fails the iteration-1 value is kept.
 *
 * <h3>Iteration 3 — Pilot-shard feedback (optional)</h3>
 * Shard 0 is executed alone.  Its <em>actual GCS file size</em> and wall-clock
 * duration give a direct throughput measurement:
 * <pre>
 *   pilotThroughputBytesPerSec = pilotActualSizeBytes / (pilotDurationMs / 1000)
 *   calibratedBytesPerShard    = pilotThroughputBytesPerSec × targetShardDurationSec
 *   remainingBytes             = totalDataBytes − pilotActualSizeBytes
 *   remainingShards            = ⌈ remainingBytes / calibratedBytesPerShard ⌉
 * </pre>
 * Shards 1..n are re-sliced only when the pilot falls outside the
 * [{@value #MIN_SHARD_DURATION_SEC}, {@value #MAX_SHARD_DURATION_SEC}] s window.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class AdaptiveShardPlanIterator {

    private static final int    WARMUP_ROWS            = 10_000;
    private static final double MIN_SHARD_DURATION_SEC = 90.0;
    private static final double MAX_SHARD_DURATION_SEC = 240.0;
    private static final long   MIN_BYTES_PER_SHARD    = 32L * 1024 * 1024;  // 32 MB floor

    private final MachineShardLadder      ladder;
    private final ShardBoundaryCalculator boundaryCalc;
    private final JdbcTemplate            jdbcTemplate;

    /* ==================================================================== */
    /* Phase A — iterations 1 and 2                                          */
    /* ==================================================================== */

    /**
     * Computes iterations 1 (ladder) and 2 (warm-up) and returns both snapshots.
     * The last element is the plan to hand to Stage 1 (or to the pilot shard).
     *
     * @param totalRows      from the profiler
     * @param avgRowBytes    from the profiler
     * @param totalDataBytes {@code totalRows × avgRowBytes}
     */
    public List<IterativePlanResult> planIterations1And2(
            BigTableLoadRequest req,
            long totalRows,
            long avgRowBytes,
            long totalDataBytes) {

        String machineType = resolve(req.getMachineType());
        int    poolSize    = req.getPoolSize();
        int    targetSec   = req.getTargetShardDurationSec();

        // ── Iteration 1 : ladder ────────────────────────────────────────
        long bytesPerShard1 = ladder.getTargetBytesPerShard(machineType);
        int  shards1        = shardsFor(totalDataBytes, bytesPerShard1);
        int  conc1          = Math.min(shards1, poolSize);

        long[] minMax  = boundaryCalc.fetchMinMax(
                req.getTableSchema(), req.getTableName(), req.getPartitionColumn());

        List<ShardRange> ranges1 = boundaryCalc.computeRanges(minMax[0], minMax[1], shards1);

        IterativePlanResult iter1 = IterativePlanResult.builder()
                .iteration(1)
                .machineType(machineType)
                .targetBytesPerShard(bytesPerShard1)
                .targetShardDurationSec(targetSec)
                .totalRows(totalRows)
                .avgRowBytes(avgRowBytes)
                .totalDataBytes(totalDataBytes)
                .shardCount(shards1)
                .maxConcurrent(conc1)
                .adjustmentReason(String.format(
                    "Ladder: %s → %.0f MB/shard | totalData=%.0f MB → %d shards",
                    machineType, bytesPerShard1 / 1048576.0,
                    totalDataBytes / 1048576.0, shards1))
                .shards(ranges1)
                .computedAt(Instant.now())
                .build();

        log.info("[PLAN-1] machine={} targetMB/shard={} totalDataMB={} shards={} maxConcurrent={}",
                 machineType,
                 String.format("%.0f", bytesPerShard1 / 1048576.0),
                 String.format("%.0f", totalDataBytes / 1048576.0),
                 shards1, conc1);

        // ── Iteration 2 : warm-up calibration ───────────────────────────
        double warmUpMbPerSec = runWarmUp(
                req.getTableSchema(), req.getTableName(), req.getPartitionColumn(),
                minMax[0], avgRowBytes);

        long   bytesPerShard2;
        String reason2;

        if (warmUpMbPerSec > 0.0) {
            bytesPerShard2 = Math.max(MIN_BYTES_PER_SHARD,
                (long) (warmUpMbPerSec * targetSec * 1024.0 * 1024.0));
            reason2 = String.format(
                "Warm-up %.3f MB/s × %ds = %.0f MB/shard | totalData=%.0f MB → shards",
                warmUpMbPerSec, targetSec,
                bytesPerShard2 / 1048576.0, totalDataBytes / 1048576.0);
        } else {
            bytesPerShard2 = bytesPerShard1;
            reason2 = "Warm-up unavailable — retaining ladder estimate";
        }

        int  shards2 = shardsFor(totalDataBytes, bytesPerShard2);
        int  conc2   = Math.min(shards2, poolSize);

        List<ShardRange> ranges2 = boundaryCalc.computeRanges(minMax[0], minMax[1], shards2);
        if (!req.isSkipPilotShard() && shards2 > 1) {
            ranges2.get(0).setPilot(true);
        }

        IterativePlanResult iter2 = IterativePlanResult.builder()
                .iteration(2)
                .machineType(machineType)
                .targetBytesPerShard(bytesPerShard2)
                .targetShardDurationSec(targetSec)
                .totalRows(totalRows)
                .avgRowBytes(avgRowBytes)
                .totalDataBytes(totalDataBytes)
                .shardCount(shards2)
                .maxConcurrent(conc2)
                .warmUpThroughputMbPerSec(warmUpMbPerSec)
                .adjustmentReason(reason2 + " → " + shards2 + " shards")
                .shards(ranges2)
                .computedAt(Instant.now())
                .build();

        log.info("[PLAN-2] warmUp={}MB/s targetMB/shard={} shards={} maxConcurrent={}",
                 String.format("%.3f", warmUpMbPerSec),
                 String.format("%.0f", bytesPerShard2 / 1048576.0),
                 shards2, conc2);

        return List.of(iter1, iter2);
    }

    /* ==================================================================== */
    /* Phase B — iteration 3 : apply pilot-shard feedback                   */
    /* ==================================================================== */

    /**
     * Called after shard 0 (the pilot) has completed.
     *
     * <p>Uses the pilot's <em>actual GCS file size</em> and wall-clock duration
     * to compute precise throughput, then calibrates the target bytes/shard for
     * shards 1..n.
     *
     * @param iter2Plan            plan from iteration 2
     * @param pilotDurationMs      actual wall-clock time for shard 0
     * @param pilotActualSizeBytes actual GCS file size of shard 0 (bytes)
     * @param globalMaxKey         MAX(partitionCol) for range re-slicing
     * @param poolSize             JDBC connection-pool cap
     */
    public IterativePlanResult applyPilotResult(
            IterativePlanResult iter2Plan,
            long                pilotDurationMs,
            long                pilotActualSizeBytes,
            long                globalMaxKey,
            int                 poolSize) {

        double durationSec = pilotDurationMs / 1000.0;
        int    targetSec   = iter2Plan.getTargetShardDurationSec();

        // ── derive throughput from actual pilot file size ──────────────
        long   safeSize    = pilotActualSizeBytes > 0
                           ? pilotActualSizeBytes
                           : iter2Plan.getTargetBytesPerShard();  // fallback
        double pilotMbPerSec = (safeSize / 1048576.0) / Math.max(durationSec, 0.001);
        long   calibratedBytesPerShard = Math.max(MIN_BYTES_PER_SHARD,
                (long) (pilotMbPerSec * targetSec * 1024.0 * 1024.0));

        // ── check if shard was within the target window ────────────────
        if (durationSec >= MIN_SHARD_DURATION_SEC && durationSec <= MAX_SHARD_DURATION_SEC) {
            String reason = String.format(
                "Pilot %.1fs within [%.0f,%.0f]s window — %.3f MB/s measured, no re-slice",
                durationSec, MIN_SHARD_DURATION_SEC, MAX_SHARD_DURATION_SEC, pilotMbPerSec);
            log.info("[PLAN-3] {}", reason);
            return IterativePlanResult.builder()
                    .iteration(3)
                    .machineType(iter2Plan.getMachineType())
                    .targetBytesPerShard(iter2Plan.getTargetBytesPerShard())
                    .targetShardDurationSec(targetSec)
                    .totalRows(iter2Plan.getTotalRows())
                    .avgRowBytes(iter2Plan.getAvgRowBytes())
                    .totalDataBytes(iter2Plan.getTotalDataBytes())
                    .shardCount(iter2Plan.getShardCount())
                    .maxConcurrent(iter2Plan.getMaxConcurrent())
                    .pilotShardDurationMs(pilotDurationMs)
                    .pilotShardActualBytes(pilotActualSizeBytes)
                    .adjustmentReason(reason)
                    .shards(iter2Plan.getShards())
                    .computedAt(Instant.now())
                    .build();
        }

        // ── re-slice shards 1..n with calibrated bytes/shard ──────────
        long   remainingBytes  = Math.max(0L,
                iter2Plan.getTotalDataBytes() - safeSize);
        int    remainingShards = shardsFor(remainingBytes, calibratedBytesPerShard);
        int    totalShards     = 1 + remainingShards;
        int    conc            = Math.min(totalShards, poolSize);

        List<ShardRange> newRanges = boundaryCalc.recomputeFromIndex(
                iter2Plan.getShards(), 1, globalMaxKey, remainingShards);

        String adjective = durationSec > MAX_SHARD_DURATION_SEC ? "slow" : "fast";
        String reason = String.format(
            "Pilot %.1fs (%s) — %.3f MB/s measured → %.0f MB/shard (was %.0f) → %d remaining shards",
            durationSec, adjective, pilotMbPerSec,
            calibratedBytesPerShard / 1048576.0,
            iter2Plan.getTargetBytesPerShard() / 1048576.0,
            remainingShards);

        log.info("[PLAN-3] {}", reason);

        return IterativePlanResult.builder()
                .iteration(3)
                .machineType(iter2Plan.getMachineType())
                .targetBytesPerShard(calibratedBytesPerShard)
                .targetShardDurationSec(targetSec)
                .totalRows(iter2Plan.getTotalRows())
                .avgRowBytes(iter2Plan.getAvgRowBytes())
                .totalDataBytes(iter2Plan.getTotalDataBytes())
                .shardCount(totalShards)
                .maxConcurrent(conc)
                .pilotShardDurationMs(pilotDurationMs)
                .pilotShardActualBytes(pilotActualSizeBytes)
                .adjustmentReason(reason)
                .shards(newRanges)
                .computedAt(Instant.now())
                .build();
    }

    /* ==================================================================== */
    /* Private helpers                                                        */
    /* ==================================================================== */

    private String resolve(String requested) {
        return (requested != null && !requested.isBlank())
            ? requested : ladder.getDefaultMachineType();
    }

    /** Ceiling division — shardCount = ⌈totalBytes / bytesPerShard⌉, minimum 1. */
    private static int shardsFor(long totalBytes, long bytesPerShard) {
        if (bytesPerShard <= 0) return 1;
        return (int) Math.max(1L, (totalBytes + bytesPerShard - 1) / bytesPerShard);
    }

    /**
     * Reads {@value #WARMUP_ROWS} rows starting at {@code startKey} and
     * returns the measured throughput in MB/s, using
     * {@code avgRowBytes} to estimate the bytes read.
     *
     * @return MB/s, or 0.0 if the probe fails or is inconclusive
     */
    /**
     * Reads {@value #WARMUP_ROWS} rows using a streaming cursor (fetchSize=1000)
     * and counts them without materialising the full result into the Java heap.
     *
     * <p>The row callback simply increments an atomic counter — each individual
     * row is not retained in memory.
     */
    private double runWarmUp(String schema, String table, String col,
                             long startKey, long avgRowBytes) {
        try {
            String sql = "SELECT * FROM " + qi(schema) + "." + qi(table)
                       + " WHERE "  + qi(col) + " >= ?"
                       + " ORDER BY " + qi(col)
                       + " LIMIT " + WARMUP_ROWS;

            java.util.concurrent.atomic.AtomicInteger counter =
                    new java.util.concurrent.atomic.AtomicInteger(0);

            long t0 = System.nanoTime();
            // Stream rows through a RowCallbackHandler — no List allocation.
            // Each invocation just increments the counter; the row is discarded.
            jdbcTemplate.query(sql, rs -> counter.incrementAndGet(), startKey);
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

            int cnt = counter.get();
            if (cnt == 0 || elapsedMs <= 0) {
                log.warn("[WARMUP] no rows returned or zero elapsed time");
                return 0.0;
            }
            long   bytesRead = (long) cnt * avgRowBytes;
            double mbPerSec  = (bytesRead / 1048576.0) / (elapsedMs / 1000.0);
            log.info("[WARMUP] {} rows × {} B = {} KB in {}ms → {} MB/s",
                     cnt, avgRowBytes, bytesRead / 1024, elapsedMs,
                     String.format("%.3f", mbPerSec));
            return mbPerSec;

        } catch (Exception ex) {
            log.warn("[WARMUP] failed — {}", ex.getMessage());
            return 0.0;
        }
    }

    private static String qi(String n) {
        return "\"" + n.replace("\"", "\"\"") + "\"";
    }
}
