package com.di.streamnova.agent.execution_engine;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Connection details for one listener stage (action + handler) after datasources are created/resolved.
 * For create-datasource, success and error report per-stage outcome.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StageConnectionDetail {
    /** Pipeline action: SOURCE_READ, INTERMEDIATE_WRITE, TARGET_WRITE. */
    private String action;
    /** Handler key (e.g. postgres, gcs, bigquery). */
    private String handler;
    /** Connection/details for this stage (type, jdbcUrl or gcsPath or dataset/table, etc.). Passwords masked. */
    private Map<String, Object> connectionDetails;
    /** True if this stage was created/resolved successfully; false if pool creation or validation failed. */
    private Boolean success;
    /** Error message for this stage when success is false (e.g. connection refused, missing config). */
    private String error;
}
