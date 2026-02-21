package com.di.streamnova.agent.execution_engine;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * JSON response with datasource connection details per listener stage (after creating/ensuring datasources per actions mapping).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatasourceConnectionDetailsResponse {
    /** Resolved stage count: 2 or 3. */
    private int stages;
    /** Mode label: "2-stage" or "3-stage". */
    private String mode;
    /** Connection details per stage (action, handler, connectionDetails). */
    private List<StageConnectionDetail> stageDetails;
    /** Optional message. */
    private String message;
}
