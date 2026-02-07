package com.di.streamnova.agent.metrics;

import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * REST API for Metrics & Learning Store: estimates vs actuals, throughput profiles, execution status.
 * Use with context path: e.g. GET /streamnova/api/agent/metrics/estimates-vs-actuals
 */
@RestController
@RequestMapping("/api/agent/metrics")
@RequiredArgsConstructor
public class MetricsLearningController {

    private final MetricsLearningService metricsLearningService;

    // --- Estimates vs actuals ---

    /**
     * Get estimates vs actuals: by runId or recent list.
     */
    @GetMapping(value = "/estimates-vs-actuals", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<EstimateVsActual>> estimatesVsActuals(
            @RequestParam(required = false) String runId,
            @RequestParam(required = false) String sourceType,
            @RequestParam(required = false) String schemaName,
            @RequestParam(required = false) String tableName,
            @RequestParam(required = false, defaultValue = "50") int limit) {
        List<EstimateVsActual> list = runId != null && !runId.isBlank()
                ? metricsLearningService.getEstimatesVsActuals(runId)
                : metricsLearningService.getRecentEstimatesVsActuals(sourceType, schemaName, tableName, limit);
        return ResponseEntity.ok(list);
    }

    // --- Throughput profiles ---

    /**
     * Get recent throughput profiles (optionally filter by table).
     */
    @GetMapping(value = "/throughput-profiles", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<ThroughputProfile>> throughputProfiles(
            @RequestParam(required = false) String runId,
            @RequestParam(required = false) String sourceType,
            @RequestParam(required = false) String schemaName,
            @RequestParam(required = false) String tableName,
            @RequestParam(required = false, defaultValue = "50") int limit) {
        if (runId != null && !runId.isBlank()) {
            return metricsLearningService.getThroughputProfileByRunId(runId)
                    .map(List::of)
                    .map(ResponseEntity::ok)
                    .orElse(ResponseEntity.noContent().build());
        }
        List<ThroughputProfile> list = metricsLearningService.getThroughputProfiles(sourceType, schemaName, tableName, limit);
        return ResponseEntity.ok(list);
    }

    // --- Execution status ---

    /**
     * Get execution status: by runId or recent list or by status filter.
     */
    @GetMapping(value = "/execution-status", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> executionStatus(
            @RequestParam(required = false) String runId,
            @RequestParam(required = false) String status,
            @RequestParam(required = false, defaultValue = "50") int limit) {
        if (runId != null && !runId.isBlank()) {
            return metricsLearningService.getExecutionStatus(runId)
                    .map(ResponseEntity::ok)
                    .orElse(ResponseEntity.notFound().build());
        }
        if (status != null && !status.isBlank()) {
            List<ExecutionStatus> list = metricsLearningService.getExecutionStatusesByStatus(status, limit);
            return ResponseEntity.ok(list);
        }
        List<ExecutionStatus> list = metricsLearningService.getRecentExecutionStatuses(limit);
        return ResponseEntity.ok(list);
    }

    /**
     * Record execution outcome (run finished). Updates execution status and optionally records estimate vs actual.
     */
    @PostMapping(value = "/execution-outcome", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ExecutionStatus> recordExecutionOutcome(@RequestBody ExecutionOutcomeRequest request) {
        if (request == null || request.getRunId() == null || request.getRunId().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        metricsLearningService.recordRunFinished(request.getRunId(), request.isSuccess(),
                request.getActualDurationSec(), request.getActualCostUsd(),
                request.getJobId(), request.getMessage());
        if (request.getEstimatedDurationSec() != null && request.getEstimatedCostUsd() != null
                && (request.getActualDurationSec() != null || request.getActualCostUsd() != null)) {
            metricsLearningService.recordEstimateVsActual(request.getRunId(),
                    request.getEstimatedDurationSec(), request.getEstimatedCostUsd(),
                    request.getActualDurationSec(), request.getActualCostUsd(),
                    request.getMachineType() != null ? request.getMachineType() : "",
                    request.getWorkerCount() != null ? request.getWorkerCount() : 0,
                    request.getShardCount() != null ? request.getShardCount() : 0);
        }
        return metricsLearningService.getExecutionStatus(request.getRunId())
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.ok().build());
    }
}
