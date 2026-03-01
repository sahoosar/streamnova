package com.di.streamnova.load.metadata;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Domain model for the {@code load_validations} table.
 *
 * <p>Row-count reconciliation is performed twice:
 * <ul>
 *   <li><strong>AFTER_STAGE1</strong> – manifest total rows vs
 *       {@code SELECT COUNT(*)} from the source table.</li>
 *   <li><strong>AFTER_STAGE3</strong> – BigQuery row count vs manifest total.</li>
 * </ul>
 *
 * <p>A run is only promoted if both validations pass.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LoadValidation {

    private String  id;
    private String  runId;

    /** {@code AFTER_STAGE1} or {@code AFTER_STAGE3}. */
    private String  stage;

    private Long    expectedRows;
    private Long    actualRows;
    private Long    deltaRows;

    /** Absolute percentage deviation: {@code |delta| / expected * 100}. */
    private Double  deltaPct;

    /** Acceptable threshold in percent (e.g. {@code 0.01} = 0.01 %). */
    private Double  thresholdPct;

    private boolean passed;
    private String  detail;
    private Instant validatedAt;
}
