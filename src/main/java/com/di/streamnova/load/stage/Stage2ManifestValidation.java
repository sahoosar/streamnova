package com.di.streamnova.load.stage;

import com.di.streamnova.load.manifest.GcsManifest;
import com.di.streamnova.load.manifest.ManifestWriter;
import com.di.streamnova.load.metadata.*;
import com.di.streamnova.load.validation.RowCountValidator;
import com.di.streamnova.load.validation.ValidationResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Stage 2: write the GCS manifest and reconcile row counts.
 *
 * <h3>Steps</h3>
 * <ol>
 *   <li>Serialize {@link GcsManifest} → {@code gs://bucket/prefix/stage1/manifest.json}</li>
 *   <li>Persist a {@link LoadManifest} row to Postgres.</li>
 *   <li>Run {@link RowCountValidator#validateStage1} (manifest rows vs
 *       {@code SELECT COUNT(*)} from source).</li>
 *   <li>Persist a {@link LoadValidation} row to Postgres.</li>
 *   <li>Throw {@link ValidationException} if the check fails.</li>
 * </ol>
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class Stage2ManifestValidation {

    private final ManifestWriter         manifestWriter;
    private final LoadManifestRepository manifestRepo;
    private final LoadValidationRepository validationRepo;
    private final RowCountValidator      validator;

    /**
     * @throws ValidationException if row-count divergence exceeds the threshold
     */
    public ValidationResult execute(
            String      runId,
            GcsManifest manifest,
            String      gcsBucket,
            String      gcsPrefix,
            String      sourceSchema,
            String      sourceTable,
            double      thresholdPct) {

        log.info("[STAGE2] runId={} writing manifest (files={} rows={})",
                 runId, manifest.getTotalFiles(), manifest.getTotalRows());

        // 1. Write manifest JSON to GCS
        String manifestGcsPath = manifestWriter.write(gcsBucket, gcsPrefix, manifest);

        // 2. Persist manifest record to Postgres
        LoadManifest m = LoadManifest.builder()
                .id(UUID.randomUUID().toString())
                .runId(runId)
                .stage("STAGE1")
                .manifestGcsPath(manifestGcsPath)
                .totalFiles(manifest.getTotalFiles())
                .totalRows(manifest.getTotalRows())
                .totalBytes(manifest.getTotalBytes())
                .build();
        manifestRepo.insert(m);

        // 3. Row-count reconciliation
        ValidationResult result = validator.validateStage1(
                sourceSchema, sourceTable,
                manifest.getTotalRows(),
                thresholdPct);

        // 4. Persist validation record
        LoadValidation vRow = LoadValidation.builder()
                .id(UUID.randomUUID().toString())
                .runId(runId)
                .stage("AFTER_STAGE1")
                .expectedRows(result.getExpectedRows())
                .actualRows(result.getActualRows())
                .deltaRows(result.getDeltaRows())
                .deltaPct(result.getDeltaPct())
                .thresholdPct(result.getThresholdPct())
                .passed(result.isPassed())
                .detail(result.getDetail())
                .build();
        validationRepo.insert(vRow);

        // 5. Guard
        if (!result.isPassed()) {
            throw new ValidationException(
                    "Stage-2 row-count validation FAILED: " + result.getDetail());
        }

        log.info("[STAGE2] validation PASSED — delta={} ({}%)",
                 result.getDeltaRows(), result.getDeltaPct());
        return result;
    }

    /* ------------------------------------------------------------------ */

    /** Thrown when row-count reconciliation exceeds the threshold. */
    public static class ValidationException extends RuntimeException {
        public ValidationException(String message) { super(message); }
    }
}
