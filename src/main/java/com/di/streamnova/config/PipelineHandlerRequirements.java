package com.di.streamnova.config;

import lombok.Builder;
import lombok.Value;

/**
 * Pipeline handler requirements: source (e.g. oracle), intermediate (e.g. gcs), destination (e.g. bigquery).
 * Resolved from {@link LoadConfig} at execution start so the pipeline knows and executes accordingly.
 */
@Value
@Builder
public class PipelineHandlerRequirements {

    /** Source handler type (e.g. oracle, postgres). */
    String sourceHandlerType;
    /** Intermediate/staging handler type (e.g. gcs). May be null if no staging. */
    String intermediateHandlerType;
    /** Target/destination handler type (e.g. bigquery). */
    String targetHandlerType;

    public boolean hasIntermediate() {
        return intermediateHandlerType != null && !intermediateHandlerType.isBlank();
    }
}
