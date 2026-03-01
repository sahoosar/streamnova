package com.di.streamnova.load.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * REST request body for the POST /api/load/big-table endpoint.
 *
 * <p>Drives a 3-stage Postgres → GCS → BigQuery adaptive load:
 * <ol>
 *   <li>Stage 1 – sharded JDBC reads (pool-limited) → JSONL files on GCS</li>
 *   <li>Stage 2 – manifest validation (row-count reconciliation)</li>
 *   <li>Stage 3 – BigQuery Load Job (GCS → BQ)</li>
 * </ol>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BigTableLoadRequest {

    // ---- source -----------------------------------------------------------

    /** PostgreSQL schema name (e.g. "public"). */
    @NotBlank
    private String tableSchema;

    /** Table name (e.g. "orders"). */
    @NotBlank
    private String tableName;

    /**
     * Numeric (BIGINT/INTEGER/SERIAL) column used to split the table into
     * shard ranges.  Must be indexed; typically the primary key.
     */
    @NotBlank
    private String partitionColumn;

    // ---- staging ----------------------------------------------------------

    /** GCS bucket for shard JSONL files and the run manifest. */
    @NotBlank
    private String gcsBucket;

    /**
     * Optional key prefix inside the bucket
     * (defaults to {@code streamnova/loads}).
     */
    private String gcsPrefix;

    // ---- target -----------------------------------------------------------

    /** BigQuery billing project (may differ from the data project). */
    @NotBlank
    private String bqProject;

    @NotBlank
    private String bqDataset;

    @NotBlank
    private String bqTable;

    /**
     * BigQuery write disposition.
     * Allowed values: {@code WRITE_TRUNCATE} (default), {@code WRITE_APPEND}.
     */
    @Builder.Default
    private String writeDisposition = "WRITE_TRUNCATE";

    // ---- tuning -----------------------------------------------------------

    /**
     * Machine type used for the shard-size ladder look-up (e.g.
     * {@code n2d-standard-8}).  Defaults to {@code n2d-standard-8}.
     */
    private String machineType;

    /**
     * Maximum JDBC connections used concurrently during Stage 1.
     * Must not exceed the configured HikariCP pool size.
     * Defaults to 10.
     */
    @Builder.Default
    @Positive
    private int poolSize = 10;

    /**
     * Target duration per shard in seconds (used in warm-up calibration).
     * Defaults to 180 s.
     */
    @Builder.Default
    @Positive
    private int targetShardDurationSec = 180;

    /**
     * When {@code true} the pilot-shard (iteration 3) is skipped and the
     * plan from iteration 2 is used directly.
     */
    @Builder.Default
    private boolean skipPilotShard = false;

    /**
     * Row-count delta percentage threshold for Stage-2 validation.
     * Defaults to 0.01 % (0.0001).
     */
    @Builder.Default
    private double validationThresholdPct = 0.0001;
}
