package com.di.streamnova.config;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Single action entry in pipeline listener config. Maps a pipeline action to an optional event type for logging.
 */
@Data
@NoArgsConstructor
public class PipelineActionConfig {
    /** Action name: SOURCE_READ, INTERMEDIATE_WRITE, TARGET_WRITE. */
    private String action;
    /** Optional event type prefix for TransactionEventLogger (e.g. LOAD_SOURCE_READ). */
    private String eventType;
}
