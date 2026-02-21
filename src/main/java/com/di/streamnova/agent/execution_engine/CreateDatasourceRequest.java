package com.di.streamnova.agent.execution_engine;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request body for POST /api/agent/pipeline-listener/create-datasource.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateDatasourceRequest {
    /** Source handler key (e.g. postgres). Required for 2-stage and 3-stage. */
    private String source;
    /** Intermediate handler key (e.g. gcs). Required only for 3-stage; omit for 2-stage. */
    private String intermediate;
    /** Target handler key (e.g. gcs, bigquery). Required for 2-stage and 3-stage. */
    private String target;
}
