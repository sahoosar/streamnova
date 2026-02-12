package com.di.streamnova.agent.execution_engine;

import com.di.streamnova.agent.execution_planner.ExecutionPlanOption;
import com.di.streamnova.agent.capacity.CapacityMessageService;
import com.di.streamnova.agent.capacity.ResourceLimitResponse;
import com.di.streamnova.agent.capacity.ShardAvailabilityService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * REST API to execute a load with a given candidate (e.g. the recommended one from GET /api/agent/recommend).
 * Async: submits the job and returns immediately; shards are reserved until the run completes.
 * Run completion must be reported via POST /api/agent/metrics/execution-outcome (same runId); shards are released there.
 */
@Slf4j
@RestController
@RequestMapping("/api/agent/execute")
@RequiredArgsConstructor
public class ExecutionController {

    private final ExecutionEngine executionEngine;
    private final ShardAvailabilityService shardAvailabilityService;
    private final CapacityMessageService capacityMessageService;

    /**
     * Submit the pipeline with the given candidate (async). Reserves shards for this run; they are released
     * when POST /api/agent/metrics/execution-outcome is called with the same runId.
     * Body: { "candidate": { "machineType", "workerCount", "shardCount", ... }, "executionRunId?" }.
     * executionRunId should be passed from recommend response and reused when reporting outcome.
     */
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> execute(@RequestBody ExecuteRequest request) {
        if (request == null || request.getCandidate() == null) {
            return ResponseEntity.badRequest()
                    .body(ExecutionResult.builder().success(false).jobId(null).message("Missing candidate").build());
        }
        ExecuteRequest.CandidateBody c = request.getCandidate();
        if (c.getMachineType() == null || c.getMachineType().isBlank()) {
            return ResponseEntity.badRequest()
                    .body(ExecutionResult.builder().success(false).jobId(null).message("Missing candidate.machineType").build());
        }
        int workers = c.getWorkerCount() != null && c.getWorkerCount() > 0 ? c.getWorkerCount() : 1;
        int shards = c.getShardCount() != null && c.getShardCount() > 0 ? c.getShardCount() : 1;
        int vcpus = c.getVirtualCpus() != null && c.getVirtualCpus() > 0 ? c.getVirtualCpus() : 4;
        int poolSize = c.getSuggestedPoolSize() != null && c.getSuggestedPoolSize() >= 0 ? c.getSuggestedPoolSize() : 0;

        ExecutionPlanOption candidate = ExecutionPlanOption.builder()
                .machineType(c.getMachineType().trim())
                .workerCount(workers)
                .shardCount(shards)
                .virtualCpus(vcpus)
                .suggestedPoolSize(poolSize)
                .label(c.getLabel())
                .build();

        String runId = request.getExecutionRunId() != null && !request.getExecutionRunId().isBlank()
                ? request.getExecutionRunId()
                : "exec-" + UUID.randomUUID();

        if (!shardAvailabilityService.tryReserve(shards, runId)) {
            int available = shardAvailabilityService.getAvailableShards();
            ResourceLimitResponse body = capacityMessageService.buildShardsNotAvailableResponse(available, shards);
            return ResponseEntity.status(429)
                    .header("Retry-After", body.getRetryAfterSeconds() != null ? String.valueOf(body.getRetryAfterSeconds()) : null)
                    .body(body);
        }

        try {
            ExecutionResult result = executionEngine.execute(candidate);
            if (!result.isSuccess()) {
                shardAvailabilityService.release(runId);
            }
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("[EXECUTION] Execute failed for runId={}: {}", runId, e.getMessage(), e);
            shardAvailabilityService.release(runId);
            String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            return ResponseEntity.status(500)
                    .body(ExecutionResult.builder().success(false).jobId(null).message("Execution failed: " + message).build());
        }
    }
}
