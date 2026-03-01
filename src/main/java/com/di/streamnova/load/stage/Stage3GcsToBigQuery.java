package com.di.streamnova.load.stage;

import com.di.streamnova.load.manifest.GcsManifest;
import com.di.streamnova.load.metadata.*;
import com.di.streamnova.load.validation.RowCountValidator;
import com.di.streamnova.load.validation.ValidationResult;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.threeten.bp.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Stage 3: loads all JSONL shard files from GCS into BigQuery using the
 * <em>BigQuery Storage Load Job API</em> (not streaming inserts).
 *
 * <h3>Steps</h3>
 * <ol>
 *   <li>Build the URI list from the Stage-1 manifest entries.</li>
 *   <li>Submit a {@link LoadJobConfiguration} with auto-schema-detect.</li>
 *   <li>Poll until the job completes (blocking).</li>
 *   <li>Write a Stage-3 manifest + validation record to Postgres.</li>
 *   <li>Validate BQ row count vs manifest total; throw if outside threshold.</li>
 * </ol>
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class Stage3GcsToBigQuery {

    private final BigQuery                 bigQuery;
    private final LoadManifestRepository   manifestRepo;
    private final LoadValidationRepository validationRepo;
    private final RowCountValidator        validator;

    /**
     * Executes the BigQuery load and validates the result.
     *
     * @param stage1Manifest manifest from Stage 1 (provides the source URIs)
     * @param writeDispositionStr {@code "WRITE_TRUNCATE"} or {@code "WRITE_APPEND"}
     * @return post-load validation result
     */
    public ValidationResult execute(
            String      runId,
            GcsManifest stage1Manifest,
            String      bqProject,
            String      bqDataset,
            String      bqTable,
            String      writeDispositionStr,
            double      thresholdPct) {

        log.info("[STAGE3] runId={} loading {} files → {}.{}.{}",
                 runId, stage1Manifest.getTotalFiles(),
                 bqProject, bqDataset, bqTable);

        // 1. Collect source URIs from the manifest
        List<String> sourceUris = new ArrayList<>(stage1Manifest.getFiles().size());
        for (GcsManifest.Entry e : stage1Manifest.getFiles()) {
            sourceUris.add(e.getGcsPath());
        }

        // 2. Configure and submit the load job
        TableId tableId = TableId.of(bqProject, bqDataset, bqTable);

        WriteDisposition writeDisposition = "WRITE_APPEND".equalsIgnoreCase(writeDispositionStr)
                ? WriteDisposition.WRITE_APPEND
                : WriteDisposition.WRITE_TRUNCATE;

        LoadJobConfiguration jobConfig = LoadJobConfiguration
                .newBuilder(tableId, sourceUris, FormatOptions.json())
                .setAutodetect(true)
                .setWriteDisposition(writeDisposition)
                .setIgnoreUnknownValues(false)
                .setMaxBadRecords(0)
                .build();

        String jobId = "streamnova-stage3-" + runId.substring(0, 8) + "-" + System.currentTimeMillis();
        Job job = bigQuery.create(JobInfo.newBuilder(jobConfig)
                .setJobId(JobId.newBuilder().setJob(jobId).setProject(bqProject).build())
                .build());

        log.info("[STAGE3] BigQuery load job submitted: {}", jobId);

        // 3. Wait for completion — cap at 4 hours so a stalled BQ job
        //    does not block the JVM indefinitely.
        //    BQ load jobs are managed server-side and survive JVM restarts;
        //    the job can be inspected manually if the timeout fires.
        try {
            job = job.waitFor(RetryOption.totalTimeout(Duration.ofHours(4)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Stage 3 interrupted waiting for BQ job " + jobId, e);
        }

        if (job == null) {
            // waitFor returns null on timeout OR if the job no longer exists
            throw new RuntimeException(
                    "Stage 3: BigQuery job timed out or no longer exists: " + jobId);
        }
        if (job.getStatus().getError() != null) {
            BigQueryError err = job.getStatus().getError();
            throw new RuntimeException(
                    "Stage 3 BQ load failed [" + jobId + "]: " + err.getMessage());
        }

        JobStatistics.LoadStatistics stats = job.getStatistics();
        long outputRows  = stats.getOutputRows()  != null ? stats.getOutputRows()  : 0L;
        long outputBytes = stats.getOutputBytes() != null ? stats.getOutputBytes() : 0L;

        log.info("[STAGE3] BQ load complete: outputRows={} outputBytes={}",
                 outputRows, outputBytes);

        // 4. Persist Stage-3 manifest record
        LoadManifest m3 = LoadManifest.builder()
                .id(UUID.randomUUID().toString())
                .runId(runId)
                .stage("STAGE3")
                .manifestGcsPath(null)          // no separate GCS file for Stage 3
                .totalFiles(stage1Manifest.getTotalFiles())
                .totalRows(outputRows)
                .totalBytes(outputBytes)
                .build();
        manifestRepo.insert(m3);

        // 5. Validate BQ row count vs Stage-1 manifest total
        ValidationResult result = validator.validateStage3(
                bqProject, bqDataset, bqTable,
                stage1Manifest.getTotalRows(),
                thresholdPct);

        LoadValidation vRow = LoadValidation.builder()
                .id(UUID.randomUUID().toString())
                .runId(runId)
                .stage("AFTER_STAGE3")
                .expectedRows(result.getExpectedRows())
                .actualRows(result.getActualRows())
                .deltaRows(result.getDeltaRows())
                .deltaPct(result.getDeltaPct())
                .thresholdPct(result.getThresholdPct())
                .passed(result.isPassed())
                .detail(result.getDetail())
                .build();
        validationRepo.insert(vRow);

        if (!result.isPassed()) {
            throw new Stage2ManifestValidation.ValidationException(
                    "Stage-3 BQ validation FAILED: " + result.getDetail());
        }

        log.info("[STAGE3] BQ validation PASSED");
        return result;
    }
}
