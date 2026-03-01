package com.di.streamnova.agent.trigger;

import com.di.streamnova.agent.metrics.ExecutionStatus;
import com.di.streamnova.agent.metrics.MetricsLearningService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.slf4j.MDC;

import java.util.List;
import java.util.stream.Collectors;

/**
 * API to list execution runs (for the 10 agents to track their requests: success/failure per caller).
 * When a datasource is configured and a JDBC store is used, runs are persisted and survive restarts.
 */
@RestController
@RequestMapping("/api/agent/executions")
@RequiredArgsConstructor
public class AgentExecutionsController {

    private final MetricsLearningService metricsLearningService;

    /**
     * List execution runs for the given caller agent only. Each agent gets only their own invocation calls.
     * callerAgentId is required so that agent-1 sees only agent-1 runs, agent-2 only agent-2, etc.
     *
     * @param callerAgentId required; e.g. agent-1, agent-2. Returns only runs triggered by this agent.
     * @param limit max number of runs to return (default 50, max 500).
     */
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> listExecutions(
            @RequestParam String callerAgentId,
            @RequestParam(required = false, defaultValue = "50") int limit) {
        if (callerAgentId == null || callerAgentId.isBlank()) {
            return ResponseEntity.badRequest()
                    .body("callerAgentId is required. Each agent must pass their id to list only their runs (e.g. ?callerAgentId=agent-1).");
        }
        String trimmed = callerAgentId.trim();
        MDC.put("caller_agent_id", trimmed);
        try {
            int effectiveLimit = Math.min(Math.max(1, limit), 500);
            List<ExecutionStatus> statuses = metricsLearningService.getExecutionStatusesByCallerAgentId(trimmed, effectiveLimit);
            List<ExecutionStatusDto> dtos = statuses.stream()
                    .map(ExecutionStatusDto::from)
                    .collect(Collectors.toList());
            return ResponseEntity.ok(dtos);
        } finally {
            MDC.remove("caller_agent_id");
        }
    }

    /**
     * Get status of a single run by run id (executionRunId from the webhook trigger response).
     * Each agent should store the executionRunId from POST /api/agent/webhook/trigger and poll this endpoint until status is SUCCESS or FAILED.
     */
    @GetMapping(path = "/{runId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ExecutionStatusDto> getExecutionByRunId(@PathVariable String runId) {
        if (runId != null && !runId.isBlank()) {
            MDC.put("execution_run_id", runId);
        }
        try {
            return metricsLearningService.getExecutionStatus(runId)
                    .map(ExecutionStatusDto::from)
                    .map(ResponseEntity::ok)
                    .orElse(ResponseEntity.notFound().build());
        } finally {
            MDC.remove("execution_run_id");
        }
    }

    /** DTO for JSON response (flat, stable fields for clients). */
    @lombok.Value
    public static class ExecutionStatusDto {
        String runId;
        String profileRunId;
        String callerAgentId;
        String mode;
        String loadPattern;
        String sourceType;
        String schemaName;
        String tableName;
        String status;
        String startedAt;
        String finishedAt;
        String jobId;
        String message;

        static ExecutionStatusDto from(ExecutionStatus s) {
            return new ExecutionStatusDto(
                    s.getRunId(),
                    s.getProfileRunId(),
                    s.getCallerAgentId(),
                    s.getMode(),
                    s.getLoadPattern(),
                    s.getSourceType(),
                    s.getSchemaName(),
                    s.getTableName(),
                    s.getStatus(),
                    s.getStartedAt() != null ? s.getStartedAt().toString() : null,
                    s.getFinishedAt() != null ? s.getFinishedAt().toString() : null,
                    s.getJobId(),
                    s.getMessage()
            );
        }
    }
}
