package com.di.streamnova.load.validation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Outcome of a single row-count reconciliation check.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ValidationResult {

    /** {@code AFTER_STAGE1} or {@code AFTER_STAGE3}. */
    private String  stage;

    private long    expectedRows;
    private long    actualRows;
    private long    deltaRows;

    /** Absolute percentage deviation: {@code |deltaRows| / expectedRows × 100}. */
    private double  deltaPct;

    /** Configured threshold in percent. */
    private double  thresholdPct;

    private boolean passed;
    private String  detail;
}
