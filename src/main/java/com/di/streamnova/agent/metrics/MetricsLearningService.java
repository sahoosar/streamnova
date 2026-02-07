package com.di.streamnova.agent.metrics;

import com.di.streamnova.agent.profiler.ThroughputSample;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

/**
 * Facade for Metrics & Learning Store: record estimates vs actuals, throughput profiles,
 * execution status; query for reporting and learning.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MetricsLearningService {

    private final MetricsLearningStore store;

    // --- Recording ---

    public void recordRunStarted(String runId, String profileRunId, String mode, String loadPattern,
                                 String sourceType, String schemaName, String tableName) {
        ExecutionStatus status = ExecutionStatus.builder()
                .runId(runId)
                .profileRunId(profileRunId)
                .mode(mode != null ? mode : "BALANCED")
                .loadPattern(loadPattern != null ? loadPattern : "DIRECT")
                .sourceType(sourceType != null ? sourceType : "postgres")
                .schemaName(schemaName != null ? schemaName : "public")
                .tableName(tableName)
                .status(ExecutionStatus.RUNNING)
                .startedAt(Instant.now())
                .createdAt(Instant.now())
                .build();
        store.saveExecutionStatus(status);
        log.debug("[METRICS] Run started: runId={}", runId);
    }

    public void recordRunFinished(String runId, boolean success, Double actualDurationSec, Double actualCostUsd,
                                  String jobId, String message) {
        store.updateExecutionStatus(runId,
                success ? ExecutionStatus.SUCCESS : ExecutionStatus.FAILED,
                Instant.now(), jobId, message);
        log.debug("[METRICS] Run finished: runId={} success={}", runId, success);
    }

    public void recordEstimateVsActual(String runId, double estimatedDurationSec, double estimatedCostUsd,
                                       Double actualDurationSec, Double actualCostUsd,
                                       String machineType, int workerCount, int shardCount) {
        EstimateVsActual record = EstimateVsActual.builder()
                .runId(runId)
                .estimatedDurationSec(estimatedDurationSec)
                .actualDurationSec(actualDurationSec)
                .estimatedCostUsd(estimatedCostUsd)
                .actualCostUsd(actualCostUsd != null ? actualCostUsd : 0.0)
                .machineType(machineType)
                .workerCount(workerCount)
                .shardCount(shardCount)
                .recordedAt(Instant.now())
                .build();
        store.saveEstimateVsActual(record);
        log.debug("[METRICS] Recorded estimate vs actual: runId={}", runId);
    }

    public void recordThroughputProfile(String runId, ThroughputSample sample) {
        if (sample == null) return;
        ThroughputProfile profile = ThroughputProfile.builder()
                .runId(runId)
                .sourceType(sample.getSourceType())
                .schemaName(sample.getSchemaName())
                .tableName(sample.getTableName())
                .bytesRead(sample.getBytesRead())
                .durationMs(sample.getDurationMs())
                .rowsRead(sample.getRowsRead())
                .throughputMbPerSec(sample.getThroughputMbPerSec())
                .sampledAt(sample.getSampledAt() != null ? sample.getSampledAt() : Instant.now())
                .build();
        store.saveThroughputProfile(profile);
        log.debug("[METRICS] Recorded throughput profile: runId={} throughput={} MB/s", runId, sample.getThroughputMbPerSec());
    }

    public void saveThroughputProfile(ThroughputProfile profile) {
        if (profile != null) store.saveThroughputProfile(profile);
    }

    public void saveEstimateVsActual(EstimateVsActual record) {
        if (record != null) store.saveEstimateVsActual(record);
    }

    public void saveExecutionStatus(ExecutionStatus status) {
        if (status != null) store.saveExecutionStatus(status);
    }

    // --- Queries ---

    public List<EstimateVsActual> getEstimatesVsActuals(String runId) {
        return runId != null && !runId.isBlank()
                ? store.findEstimatesVsActualsByRunId(runId)
                : store.findRecentEstimatesVsActuals(null, null, null, 50);
    }

    public List<EstimateVsActual> getRecentEstimatesVsActuals(String sourceType, String schemaName, String tableName, int limit) {
        return store.findRecentEstimatesVsActuals(sourceType, schemaName, tableName, limit <= 0 ? 50 : Math.min(limit, 100));
    }

    public List<ThroughputProfile> getThroughputProfiles(String sourceType, String schemaName, String tableName, int limit) {
        return store.findRecentThroughputProfiles(sourceType, schemaName, tableName, limit <= 0 ? 50 : Math.min(limit, 100));
    }

    public java.util.Optional<ThroughputProfile> getThroughputProfileByRunId(String runId) {
        return store.findThroughputProfileByRunId(runId);
    }

    public List<ExecutionStatus> getRecentExecutionStatuses(int limit) {
        return store.findRecentExecutionStatuses(limit <= 0 ? 50 : Math.min(limit, 100));
    }

    public java.util.Optional<ExecutionStatus> getExecutionStatus(String runId) {
        return store.findExecutionStatusByRunId(runId);
    }

    public List<ExecutionStatus> getExecutionStatusesByStatus(String status, int limit) {
        return store.findExecutionStatusesByStatus(status, limit <= 0 ? 50 : Math.min(limit, 100));
    }
}
