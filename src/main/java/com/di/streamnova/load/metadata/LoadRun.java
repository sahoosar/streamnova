package com.di.streamnova.load.metadata;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Domain model for the {@code load_runs} table.
 *
 * <p>One row is created at the start of every big-table load operation and
 * updated as the run progresses through its lifecycle stages.
 *
 * <pre>Status flow:
 *   PLANNING → STAGE1_RUNNING → STAGE1_DONE
 *            → STAGE2_RUNNING → STAGE2_DONE
 *            → STAGE3_RUNNING → STAGE3_DONE
 *            → PROMOTING      → PROMOTED
 *   (any stage) → FAILED
 * </pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LoadRun {

    private String  id;

    // ---- source description -----------------------------------------------
    private String  tableSchema;
    private String  tableName;
    private String  partitionCol;

    // ---- profiler outputs --------------------------------------------------
    private Long    sourceRowCount;
    private Integer avgRowBytes;
    private Long    totalDataBytes;

    // ---- chosen plan -------------------------------------------------------
    private String  machineType;
    private Integer workerCount;
    private Integer shardCount;
    private Integer maxConcurrent;
    private Integer poolSize;
    private Integer iterationCount;

    // ---- lifecycle ---------------------------------------------------------
    private String  status;
    private String  errorMessage;

    // ---- destinations ------------------------------------------------------
    private String  gcsBucket;
    private String  gcsPrefix;
    private String  bqProject;
    private String  bqDataset;
    private String  bqTable;
    private String  writeDisposition;

    // ---- timestamps --------------------------------------------------------
    private Instant startedAt;
    private Instant stage1StartedAt;
    private Instant stage1CompletedAt;
    private Instant stage2CompletedAt;
    private Instant stage3StartedAt;
    private Instant stage3CompletedAt;
    private Instant promotedAt;

    // ---- plan audit --------------------------------------------------------
    /** JSON-serialised list of {@code IterativePlanResult} snapshots. */
    private String  planIterationsJson;
}
