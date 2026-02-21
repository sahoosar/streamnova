package com.di.streamnova.agent.execution_engine;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * JSON response for pipeline listener actions mapping: 2-stage or 3-stage with each action mapped to its handler.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ActionsMappingResponse {
    /** Resolved stage count: 2 or 3. */
    private int stages;
    /** Mode label: "2-stage" or "3-stage". */
    private String mode;
    /** List of actions with handler and eventType (SOURCE_READ→sourceKey, TARGET_WRITE→targetKey, etc.). */
    private List<ActionMappingEntry> actionsMapping;
    /** Entrypoint config file for listener stages (e.g. pipeline_listener_config.yml). */
    private String entrypoint;
    /** Optional workflow/template type (e.g. db_to_gcs) when a template applies for this source/target. */
    private String workflowType;
    /** Optional message. */
    private String message;
}
