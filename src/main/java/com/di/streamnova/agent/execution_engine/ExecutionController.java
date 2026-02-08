package com.di.streamnova.agent.execution_engine;

import com.di.streamnova.agent.adaptive_execution_planner.ExecutionPlanOption;
import com.di.streamnova.agent.metrics.MetricsLearningService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API to execute a load with a given candidate (e.g. the recommended one from GET /api/agent/recommend).
 * Enables flow: GET recommend → POST execute with recommended candidate.
 * When executionRunId is provided, execution status is updated to SUCCESS/FAILED so status does not stay RUNNING.
 */
@Slf4j
@RestController
@RequestMapping("/api/agent/execute")
@RequiredArgsConstructor
public class ExecutionController {

    private final ExecutionEngine executionEngine;
    private final MetricsLearningService metricsLearningService;

    /**
     * Run the pipeline with the given candidate.
     * Body: { "candidate": { "machineType", "workerCount", "shardCount", "virtualCpus?", "suggestedPoolSize?", "label?" }, "executionRunId?" }.
     * If executionRunId is set, updates execution status to SUCCESS/FAILED after run.
     */
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ExecutionResult> execute(@RequestBody ExecuteRequest request) {
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

        ExecutionResult result = executionEngine.execute(candidate);

        if (request.getExecutionRunId() != null && !request.getExecutionRunId().isBlank()) {
            try {
                metricsLearningService.recordRunFinished(
                        request.getExecutionRunId(),
                        result.isSuccess(),
                        null,
                        null,
                        result.getJobId(),
                        result.getMessage());
            } catch (Exception ex) {
                log.warn("[EXECUTION] Failed to update execution status for runId={}: {}", request.getExecutionRunId(), ex.getMessage());
            }
        }
        return ResponseEntity.ok(result);
    }
}
