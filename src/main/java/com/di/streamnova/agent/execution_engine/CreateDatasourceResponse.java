package com.di.streamnova.agent.execution_engine;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Response for create-datasource: true only when all connection pools for listener events were created successfully.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateDatasourceResponse {
    /** True only if every event that requires a pool had getOrInit (or equivalent) succeed. */
    private boolean success;
    /** Event types or action names that failed when success is false. */
    private List<String> failedEvents;
    /** Optional stage details for diagnostics. */
    private List<StageConnectionDetail> stageDetails;
    /** Optional message. */
    private String message;
}
