package com.di.streamnova.agent.execution_engine;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Single entry in the actions mapping: pipeline action (e.g. SOURCE_READ) mapped to the handler key and optional event type.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ActionMappingEntry {
    /** Pipeline action: SOURCE_READ, INTERMEDIATE_WRITE, TARGET_WRITE. */
    private String action;
    /** Handler key for this stage (e.g. postgres, gcs, bigquery). */
    private String handler;
    /** Event type for logging (e.g. LOAD_SOURCE_READ). */
    private String eventType;
}
