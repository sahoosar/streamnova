package com.di.streamnova.load;

import com.di.streamnova.load.dto.BigTableLoadRequest;
import com.di.streamnova.load.dto.BigTableLoadResponse;
import com.di.streamnova.load.manifest.GcsManifest;
import com.di.streamnova.load.metadata.*;
import com.di.streamnova.load.plan.AdaptiveShardPlanIterator;
import com.di.streamnova.load.plan.IterativePlanResult;
import com.di.streamnova.load.plan.MachineShardLadder;
import com.di.streamnova.load.plan.ShardRange;
import com.di.streamnova.load.stage.Stage1PostgresToGcs;
import com.di.streamnova.load.stage.Stage2ManifestValidation;
import com.di.streamnova.load.stage.Stage3GcsToBigQuery;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Top-level orchestrator for the 3-stage adaptive big-table load.
 *
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │  PLANNING   (up to 3 adaptive iterations — TABLE-SIZE based)        │
 * │   Iter 1 — MachineShardLadder → targetBytesPerShard                │
 * │            shardCount = ⌈totalDataBytes / targetBytesPerShard⌉     │
 * │   Iter 2 — warm-up probe → measured MB/s → calibrated bytes/shard  │
 * │   Iter 3 — pilot shard → actual file size ÷ duration → re-slice    │
 * ├─────────────────────────────────────────────────────────────────────┤
 * │  STAGE 1  Postgres → GCS                                            │
 * │   pool-limited (maxConcurrent ≤ 10) parallel JdbcTemplate reads    │
 * │   each shard → gs://bucket/prefix/stage1/shard_NNNNNN.jsonl        │
 * ├─────────────────────────────────────────────────────────────────────┤
 * │  STAGE 2  Manifest + Validation                                     │
 * │   writes manifest.json to GCS                                       │
 * │   reconciles SUM(shard rows) vs SELECT COUNT(*) from source         │
 * ├─────────────────────────────────────────────────────────────────────┤
 * │  STAGE 3  GCS → BigQuery                                            │
 * │   BigQuery Load Job (batch, not streaming)                          │
 * │   validates BQ COUNT(*) vs manifest total                           │
 * ├─────────────────────────────────────────────────────────────────────┤
 * │  PROMOTE                                                             │
 * │   marks run as PROMOTED in load_runs                                │
 * └─────────────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <p>All run / shard / manifest / validation state is persisted to Postgres
 * at each transition so the run is fully auditable and restartable.
 */
@Service
@Slf4j
public class BigTableLoadOrchestrator {

    // ---- plan ---------------------------------------------------------------
    private final AdaptiveShardPlanIterator planIterator;
    private final MachineShardLadder        ladder;

    // ---- metadata -----------------------------------------------------------
    private final LoadRunRepository         runRepo;
    private final LoadShardRepository       shardRepo;

    // ---- stages -------------------------------------------------------------
    private final Stage1PostgresToGcs       stage1;
    private final Stage2ManifestValidation  stage2;
    private final Stage3GcsToBigQuery       stage3;

    // ---- misc ---------------------------------------------------------------
    private final JdbcTemplate  jdbcTemplate;
    private final ObjectMapper  om;

    public BigTableLoadOrchestrator(
            AdaptiveShardPlanIterator planIterator,
            MachineShardLadder        ladder,
            LoadRunRepository         runRepo,
            LoadShardRepository       shardRepo,
            Stage1PostgresToGcs       stage1,
            Stage2ManifestValidation  stage2,
            Stage3GcsToBigQuery       stage3,
            JdbcTemplate              jdbcTemplate) {

        this.planIterator  = planIterator;
        this.ladder        = ladder;
        this.runRepo       = runRepo;
        this.shardRepo     = shardRepo;
        this.stage1        = stage1;
        this.stage2        = stage2;
        this.stage3        = stage3;
        this.jdbcTemplate  = jdbcTemplate;
        this.om = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /* ==================================================================== */
    /* Entry point                                                           */
    /* ==================================================================== */

    /**
     * Runs the full pipeline synchronously and returns a response after
     * the run has been promoted (or failed).
     *
     * <p>For async execution wrap this in a {@code @Async} method or use
     * a thread-pool in the controller.
     */
    public BigTableLoadResponse execute(BigTableLoadRequest req) {

        String runId  = UUID.randomUUID().toString();
        String prefix = resolvePrefix(req);

        log.info("[ORCHESTRATOR] runId={} table={}.{} partitionCol={}",
                 runId, req.getTableSchema(), req.getTableName(), req.getPartitionColumn());

        // ── seed run record ─────────────────────────────────────────────
        LoadRun run = LoadRun.builder()
                .id(runId)
                .tableSchema(req.getTableSchema())
                .tableName(req.getTableName())
                .partitionCol(req.getPartitionColumn())
                .machineType(resolveMachine(req))
                .poolSize(req.getPoolSize())
                .gcsBucket(req.getGcsBucket())
                .gcsPrefix(prefix)
                .bqProject(req.getBqProject())
                .bqDataset(req.getBqDataset())
                .bqTable(req.getBqTable())
                .writeDisposition(req.getWriteDisposition())
                .status("PLANNING")
                .iterationCount(0)
                .build();
        runRepo.insert(run);

        try {
            return doExecute(runId, req, prefix);
        } catch (Exception ex) {
            log.error("[ORCHESTRATOR] runId={} FAILED: {}", runId, ex.getMessage(), ex);
            runRepo.markFailed(runId, ex.getMessage());
            return BigTableLoadResponse.builder()
                    .runId(runId)
                    .status("FAILED")
                    .message(ex.getMessage())
                    .build();
        }
    }

    /* ==================================================================== */
    /* Internal pipeline                                                     */
    /* ==================================================================== */

    private BigTableLoadResponse doExecute(String             runId,
                                           BigTableLoadRequest req,
                                           String             prefix) throws JsonProcessingException {

        String schema        = req.getTableSchema();
        String table         = req.getTableName();
        String partitionCol  = req.getPartitionColumn();
        String gcsBucket     = req.getGcsBucket();

        /* ─────────────────────────────────────────────────────────────── */
        /* PLANNING                                                         */
        /* ─────────────────────────────────────────────────────────────── */

        // Profile: row count, avg row bytes, total data bytes
        long[] profile        = profileTable(schema, table);
        long   totalRows      = profile[0];
        long   avgRowBytes    = profile[1];
        long   totalDataBytes = profile[2];   // = totalRows × avgRowBytes

        // Persist profile data so the run record is fully populated
        runRepo.updateProfileStats(runId, totalRows, avgRowBytes, totalDataBytes);

        log.info("[PLAN] table={}.{} rows={} avgRowBytes={} totalDataMB={}",
                 schema, table, totalRows, avgRowBytes,
                 String.format("%.1f", totalDataBytes / (1024.0 * 1024.0)));

        // Iterations 1 and 2 — size-based: shardCount = ⌈totalDataBytes / targetBytesPerShard⌉
        List<IterativePlanResult> allIters =
                planIterator.planIterations1And2(req, totalRows, avgRowBytes, totalDataBytes);

        IterativePlanResult activePlan = allIters.get(allIters.size() - 1);  // iter 2

        // Persist initial plan (before pilot shard)
        updatePlanInDb(runId, allIters, activePlan);

        // Persist shard rows (PENDING) so Stage 1 can update them
        commitShardRows(runId, activePlan.getShards(), partitionCol, activePlan.getIteration());

        /* ─────────────────────────────────────────────────────────────── */
        /* STAGE 1 — Postgres → GCS                                        */
        /* ─────────────────────────────────────────────────────────────── */

        runRepo.markStage1Running(runId);
        boolean pilotDone = false;

        // Iteration 3 — optional pilot shard
        if (!req.isSkipPilotShard() && activePlan.getShards().size() > 1) {
            ShardRange pilotShard = activePlan.getShards().get(0);
            log.info("[PLAN-3] executing pilot shard to calibrate remaining shards");

            long pilotMs = stage1.executePilotShard(
                    runId, pilotShard, schema, table, partitionCol, gcsBucket, prefix);

            // Retrieve the pilot shard's actual GCS file size for precise throughput measurement
            long pilotActualBytes = shardRepo.findByRunId(runId).stream()
                    .filter(s -> s.getShardIndex() == 0)
                    .mapToLong(s -> s.getGcsFileSizeBytes() != null ? s.getGcsFileSizeBytes() : 0L)
                    .findFirst().orElse(0L);

            // Fetch global max key for range re-slicing
            long[] minMax      = fetchMinMaxFromDb(schema, table, partitionCol);
            IterativePlanResult iter3 = planIterator.applyPilotResult(
                    activePlan, pilotMs, pilotActualBytes, minMax[1], req.getPoolSize());

            // Capture iter2 plan BEFORE reassigning activePlan to iter3
            IterativePlanResult iter2Plan = activePlan;

            allIters = new ArrayList<>(allIters);
            allIters.add(iter3);
            activePlan = iter3;

            // Re-slice only when the pilot changed the shard count vs iter2
            if (iter3.getShardCount() != iter2Plan.getShardCount()) {
                skipAndReplaceShards(runId, iter3.getShards(), partitionCol, 3);
            }

            updatePlanInDb(runId, allIters, activePlan);
            pilotDone = true;
        }

        // Execute the batch (all shards minus the already-done pilot)
        GcsManifest manifest = stage1.executeBatch(
                runId,
                activePlan.getShards(),
                schema, table, partitionCol,
                gcsBucket, prefix,
                activePlan.getMaxConcurrent(),
                pilotDone);

        runRepo.markStage1Done(runId);
        log.info("[STAGE1] complete — {} rows in {} files",
                 manifest.getTotalRows(), manifest.getTotalFiles());

        /* ─────────────────────────────────────────────────────────────── */
        /* STAGE 2 — Manifest + Validation                                 */
        /* ─────────────────────────────────────────────────────────────── */

        runRepo.markStage2Running(runId);

        stage2.execute(runId, manifest, gcsBucket, prefix, schema, table,
                       req.getValidationThresholdPct());

        runRepo.markStage2Done(runId);
        log.info("[STAGE2] complete — manifest + validation passed");

        /* ─────────────────────────────────────────────────────────────── */
        /* STAGE 3 — GCS → BigQuery                                        */
        /* ─────────────────────────────────────────────────────────────── */

        runRepo.markStage3Running(runId);

        stage3.execute(runId, manifest,
                       req.getBqProject(), req.getBqDataset(), req.getBqTable(),
                       req.getWriteDisposition(),
                       req.getValidationThresholdPct());

        runRepo.markStage3Done(runId);
        log.info("[STAGE3] complete — BigQuery load + validation passed");

        /* ─────────────────────────────────────────────────────────────── */
        /* PROMOTE                                                          */
        /* ─────────────────────────────────────────────────────────────── */

        runRepo.markPromoting(runId);
        runRepo.markPromoted(runId);
        log.info("[ORCHESTRATOR] runId={} PROMOTED ✓", runId);

        return BigTableLoadResponse.builder()
                .runId(runId)
                .status("PROMOTED")
                .machineType(activePlan.getMachineType())
                .shardCount(activePlan.getShardCount())
                .maxConcurrent(activePlan.getMaxConcurrent())
                .iterationsUsed(allIters.size())
                .sourceRowCount(totalRows)
                .warmUpThroughputMbPerSec(allIters.size() > 1
                        ? allIters.get(1).getWarmUpThroughputMbPerSec() : 0.0)
                .gcsStagingPath("gs://" + gcsBucket + "/" + prefix + "/stage1/")
                .message("Load completed and promoted to BigQuery successfully")
                .build();
    }

    /* ==================================================================== */
    /* Helpers                                                               */
    /* ==================================================================== */

    /**
     * Profiles the source table and returns {@code [rowCount, avgRowBytes, totalDataBytes]}.
     *
     * <ul>
     *   <li><strong>rowCount</strong> – exact {@code COUNT(*)}.</li>
     *   <li><strong>avgRowBytes</strong> – average serialised row size, estimated via
     *       {@code pg_column_size} on a 1 % random sample (capped at 10 K rows).
     *       Falls back to 500 B when the function is unavailable.</li>
     *   <li><strong>totalDataBytes</strong> – {@code rowCount × avgRowBytes}; the
     *       primary input to the shard-count calculation.</li>
     * </ul>
     */
    private long[] profileTable(String schema, String table) {
        // 1. Exact row count
        Long cnt = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM \"" + schema + "\".\"" + table + "\"", Long.class);
        long rowCount = cnt == null ? 0L : cnt;

        // 2. Average row size — sampled to keep the query fast on large tables
        String sizeSql = "SELECT ROUND(AVG(pg_column_size(t.*)))"
                + " FROM \"" + schema + "\".\"" + table + "\" AS t"
                + " TABLESAMPLE SYSTEM(1) LIMIT 10000";
        Long avgBytes;
        try {
            avgBytes = jdbcTemplate.queryForObject(sizeSql, Long.class);
        } catch (Exception ex) {
            log.warn("[PROFILE] pg_column_size sampling failed ({}); defaulting to 500 B",
                     ex.getMessage());
            avgBytes = null;
        }
        long avgRowBytes    = (avgBytes == null || avgBytes <= 0) ? 500L : avgBytes;
        long totalDataBytes = rowCount * avgRowBytes;

        log.info("[PROFILE] {}.{}: rows={} avgRowBytes={} totalDataMB={}",
                 schema, table, rowCount, avgRowBytes,
                 String.format("%.1f", totalDataBytes / (1024.0 * 1024.0)));

        return new long[]{ rowCount, avgRowBytes, totalDataBytes };
    }

    private long[] fetchMinMaxFromDb(String schema, String table, String col) {
        String sql = "SELECT MIN(\"" + col + "\"), MAX(\"" + col + "\") FROM \""
                + schema + "\".\"" + table + "\"";
        long[] result = new long[2];
        jdbcTemplate.query(sql, rs -> {
            if (rs.next()) { result[0] = rs.getLong(1); result[1] = rs.getLong(2); }
        });
        return result;
    }

    private void updatePlanInDb(String runId,
                                List<IterativePlanResult> iters,
                                IterativePlanResult active) throws JsonProcessingException {
        String planJson = om.writeValueAsString(iters);
        runRepo.updatePlan(runId, iters.size(),
                           active.getShardCount(), active.getMaxConcurrent(), planJson);
    }

    private void commitShardRows(String runId, List<ShardRange> shards,
                                 String partitionCol, int iteration) {
        List<LoadShard> rows = new ArrayList<>(shards.size());
        for (ShardRange s : shards) {
            rows.add(LoadShard.builder()
                    .id(UUID.randomUUID().toString())
                    .runId(runId)
                    .shardIndex(s.getShardIndex())
                    .iterationBorn(iteration)
                    .partitionCol(partitionCol)
                    .minKey(s.getMinKey())
                    .maxKey(s.getMaxKey())
                    .estimatedRows(s.getEstimatedRows())
                    .status("PENDING")
                    .attempt(1)
                    .build());
        }
        shardRepo.insertAll(rows);
    }

    /** After iteration 3 re-slices: mark old shards SKIPPED, insert new ones. */
    private void skipAndReplaceShards(String runId, List<ShardRange> newShards,
                                      String partitionCol, int iteration) {
        // Skip all shards from index 1 onwards (shard 0 is already DONE)
        List<LoadShard> existing = shardRepo.findByRunId(runId);
        for (LoadShard s : existing) {
            if (s.getShardIndex() > 0 && "PENDING".equals(s.getStatus())) {
                shardRepo.markSkipped(runId, s.getShardIndex());
            }
        }
        // Insert newly computed shards (index 1 onwards)
        List<ShardRange> newPending = newShards.subList(1, newShards.size());
        commitShardRows(runId, newPending, partitionCol, iteration);
    }

    private String resolvePrefix(BigTableLoadRequest req) {
        String base = (req.getGcsPrefix() != null && !req.getGcsPrefix().isBlank())
                    ? req.getGcsPrefix().replaceAll("/$", "")
                    : "streamnova/loads";
        return base + "/" + req.getTableSchema() + "_" + req.getTableName();
    }

    private String resolveMachine(BigTableLoadRequest req) {
        return (req.getMachineType() != null && !req.getMachineType().isBlank())
             ? req.getMachineType()
             : ladder.getDefaultMachineType();
    }
}
