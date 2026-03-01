package com.di.streamnova.ai.dto;

import lombok.Builder;
import lombok.Data;

/** Structured output from {@code DataQualityAgent}. */
@Data
@Builder
public class DataQualityReport {
    private String  schema;
    private String  tableName;
    private String  sourceType;
    private String  aiSummary;
    private long    durationMs;
    private boolean success;
    private String  errorMessage;
}
