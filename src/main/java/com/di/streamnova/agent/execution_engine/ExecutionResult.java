package com.di.streamnova.agent.execution_engine;

import lombok.Builder;
import lombok.Value;

import java.util.Collections;
import java.util.Map;

/**
 * Result of an execution request. Returned as JSON from POST /api/agent/execute.
 * Includes resolved stage count and handler selection after user selects 2-stage or 3-stage.
 */
@Value
@Builder
public class ExecutionResult {
    boolean success;
    String jobId;
    String message;
    /** Resolved stage count: 2 (source→target) or 3 (source→intermediate→target). */
    @Builder.Default
    int stages = 0;
    /** Resolved handler selection: keys "source", "intermediate" (null for 2-stage), "target". Values are the assigned handler types loaded from the respective event config (e.g. source=postgres from database_event_config.yml, target=gcs from gcs_event_config.yml). */
    @Builder.Default
    Map<String, String> selection = Collections.emptyMap();
}
