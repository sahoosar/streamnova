package com.di.streamnova.agent.trigger;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Value;

import java.util.Map;

/**
 * Response for POST /api/agent/webhook/trigger.
 * Contains execution run id, job id if executed, and status.
 */
@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WebhookTriggerResponse {

    /** Execution run id from recommend (use when reporting outcome via POST /api/agent/metrics/execution-outcome). */
    String executionRunId;
    /** Dataflow job id if execute was run and succeeded. */
    String jobId;
    /** Overall success: recommend succeeded and, if executeImmediately, execute succeeded. */
    boolean success;
    /** Human-readable message (e.g. error reason). */
    String message;
    /** Resolved stage count: 2 or 3. */
    Integer stages;
    /** Resolved handler selection: source, intermediate (if 3-stage), target. */
    Map<String, String> selection;
}
