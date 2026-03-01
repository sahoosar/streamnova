package com.di.streamnova.agent.trigger;

import com.di.streamnova.agent.execution_engine.ExecutionResult;
import com.di.streamnova.agent.recommender.RecommendationResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.slf4j.MDC;

import com.di.streamnova.util.MdcPropagation;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Webhook entry point: POST triggers the agent flow (recommend, then optionally execute).
 * Use this when an external system (CI/CD, scheduler, workflow) needs to trigger a run via HTTP callback.
 */
@Slf4j
@RestController
@RequestMapping("/api/agent/webhook")
@RequiredArgsConstructor
public class AgentWebhookController {

    private static final String WEBHOOK_TRIGGER_ENDPOINT = "/api/agent/webhook/trigger";
    private static final ObjectMapper JSON = new ObjectMapper();

    private final AgentTriggerService agentTriggerService;
    private final AgentInvocationAuditService auditService;

    /**
     * Trigger the agent: run recommend (profile → candidates → estimate → recommend), then if
     * {@code executeImmediately} is true, run execute with the recommended candidate.
     * <p>
     * Returns 202 Accepted with executionRunId and jobId (if executed). Client should report
     * outcome via POST /api/agent/metrics/execution-outcome with the same executionRunId.
     *
     * @param request source, target, mode, executeImmediately, optional guardrail overrides
     * @return 202 with WebhookTriggerResponse, or 204 if no recommendation, or 400 if invalid
     */
    @PostMapping(
            path = "/trigger",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<WebhookTriggerResponse> trigger(@Valid @RequestBody WebhookTriggerRequest request) {
        String callerAgentId = request.getCallerAgentId() != null && !request.getCallerAgentId().isBlank()
                ? request.getCallerAgentId().trim() : null;
        if (callerAgentId != null) {
            MDC.put("caller_agent_id", callerAgentId);
        }
        try {
            if (request.getMode() == null) {
                request.setMode(com.di.streamnova.agent.recommender.UserMode.BALANCED);
            }
            boolean warmUp = request.getWarmUp() != null && request.getWarmUp();

            log.info("[WEBHOOK] trigger source={} target={} mode={} executeImmediately={} callerAgentId={}",
                    request.getSource(), request.getTarget(), request.getMode(), request.isExecuteImmediately(), request.getCallerAgentId());

            Optional<RecommendationResult> recommendOpt = agentTriggerService.recommend(
                request.getMode(),
                request.getSource(),
                warmUp,
                request.getLoadPattern(),
                request.getMaxCostUsd(),
                request.getMaxDurationSec(),
                request.getMinThroughputMbPerSec(),
                request.getAllowedMachineTypes(),
                request.getDatabasePoolMaxSize(),
                request.getCallerAgentId());

            if (recommendOpt.isEmpty()) {
                auditWebhookInvocationAsync(request, null, 204, "no content");
                return ResponseEntity.noContent().build();
            }

            RecommendationResult recommend = recommendOpt.get();
            String executionRunId = recommend.getExecutionRunId();
            if (executionRunId != null && !executionRunId.isBlank()) {
                MDC.put("execution_run_id", executionRunId);
            }

            if (!request.isExecuteImmediately()) {
                WebhookTriggerResponse resp = WebhookTriggerResponse.builder()
                        .executionRunId(executionRunId)
                        .jobId(null)
                        .success(true)
                        .message("Recommend completed; execute not requested")
                        .build();
                auditWebhookInvocationAsync(request, resp, 202, null);
                return ResponseEntity.accepted().body(resp);
            }

            ExecutionResult execResult = agentTriggerService.execute(
                    recommend,
                    request.getSource(),
                    request.getIntermediate(),
                    request.getTarget(),
                    request.getExclusive());

            WebhookTriggerResponse response = WebhookTriggerResponse.builder()
                    .executionRunId(executionRunId)
                    .jobId(execResult.getJobId())
                    .success(execResult.isSuccess())
                    .message(execResult.getMessage())
                    .stages(execResult.getStages())
                    .selection(execResult.getSelection())
                    .build();

            if (execResult.isSuccess()) {
                auditWebhookInvocationAsync(request, response, 202, null);
                return ResponseEntity.accepted().body(response);
            }
            auditWebhookInvocationAsync(request, response, 422, null);
            return ResponseEntity.unprocessableEntity().body(response);
        } finally {
            MDC.remove("caller_agent_id");
            MDC.remove("execution_run_id");
        }
    }

    /** Fire-and-forget audit so response is not blocked. */
    private void auditWebhookInvocationAsync(WebhookTriggerRequest request, WebhookTriggerResponse response, int status, String responseSummaryOverride) {
        String callerAgentId = request.getCallerAgentId() != null && !request.getCallerAgentId().isBlank() ? request.getCallerAgentId().trim() : null;
        if (callerAgentId == null) return;
        String runId = response != null ? response.getExecutionRunId() : null;
        String requestSummary = toJsonSafe(request);
        String responseSummary = responseSummaryOverride != null ? responseSummaryOverride : (response != null ? toJsonSafe(response) : null);
        CompletableFuture.runAsync(MdcPropagation.wrapRunnable(() ->
                auditService.saveWebhookInvocation(callerAgentId, runId, WEBHOOK_TRIGGER_ENDPOINT, requestSummary, status, responseSummary)));
    }

    private static String toJsonSafe(Object o) {
        if (o == null) return null;
        try {
            return JSON.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            return "{\"serializeError\":\"" + e.getMessage() + "\"}";
        }
    }
}
