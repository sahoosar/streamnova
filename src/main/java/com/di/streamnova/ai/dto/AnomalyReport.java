package com.di.streamnova.ai.dto;

import lombok.Builder;
import lombok.Data;

/** Structured output from {@code AnomalyDetectionAgent}. */
@Data
@Builder
public class AnomalyReport {
    private String  machineType;
    private int     runsAnalysed;
    private String  summary;
    private boolean anomalyDetected;
    private long    durationMs;
    private boolean success;
    private String  errorMessage;
}
