package com.di.streamnova.agent.execution_engine;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Builder;
import lombok.Value;

import java.util.List;
import java.util.Map;

/**
 * Response for GET /api/agent/pipeline-handlers. Returns availableSourceHandlers, message, handlers, configured, and currentPipelineHandlers.
 */
@Value
@Builder
@JsonPropertyOrder({"availableSourceHandlers", "handlers", "configured", "currentPipelineHandlers", "message"})
public class PipelineHandlersResponse {

    /** Registered source handler types. Use these values for source in actions-mapping and execute. */
    List<String> availableSourceHandlers;

    /** Human-readable summary: 2-stage vs 3-stage, how to select handlers, and how to call execute. */
    String message;

    /** Handler type → descriptor (className, roles: source, intermediate, target). */
    Map<String, HandlerDescriptor> handlers;

    /** Configured keys per stage: "source", "intermediate", "target" → list of handler keys. */
    Map<String, List<String>> configured;

    /** Current pipeline selection from config defaults. Override via POST /api/agent/execute. */
    Map<String, String> currentPipelineHandlers;

    @Value
    @Builder
    public static class HandlerDescriptor {
        String className;
        List<String> roles;
    }
}
