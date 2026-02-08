package com.di.streamnova.agent.metrics;

import com.di.streamnova.agent.profiler.ThroughputSample;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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

    /**
     * Returns estimate-vs-actual records for recent runs that completed successfully and match the optional table filter.
     * Used by the learning loop to compute correction factors and success counts.
     */
    public List<EstimateVsActual> getRecentSuccessfulEstimateVsActuals(String sourceType, String schemaName, String tableName, int limit) {
        int fetch = Math.min(limit * 3, 200);
        List<ExecutionStatus> statuses = store.findExecutionStatusesByStatus(ExecutionStatus.SUCCESS, fetch);
        List<EstimateVsActual> out = new ArrayList<>();
        for (ExecutionStatus s : statuses) {
            if (sourceType != null && !sourceType.equals(s.getSourceType())) continue;
            if (schemaName != null && !schemaName.isBlank() && !schemaName.equals(s.getSchemaName())) continue;
            if (tableName != null && !tableName.isBlank() && !tableName.equals(s.getTableName())) continue;
            if (out.size() >= limit) break;
            store.findEstimatesVsActualsByRunId(s.getRunId()).stream().findFirst().ifPresent(out::add);
        }
        return out;
    }

    private static String machineFamily(String machineType) {
        if (machineType == null || machineType.isBlank()) return "n2";
        String lower = machineType.toLowerCase();
        if (lower.startsWith("n2d")) return "n2d";
        if (lower.startsWith("c3")) return "c3";
        return "n2";
    }

    private static double clampCorrection(double ratio) {
        if (ratio <= 0 || Double.isNaN(ratio)) return 1.0;
        return Math.max(0.5, Math.min(2.0, ratio));
    }

    /**
     * Builds learning signals from recent successful runs: duration/cost correction by machine family
     * and success count per machine type. Used by EstimatorService (corrections) and RecommenderService (success counts).
     */
    public LearningSignals getLearningSignals(String sourceType, String schemaName, String tableName, int limit) {
        List<EstimateVsActual> records = getRecentSuccessfulEstimateVsActuals(sourceType, schemaName, tableName, limit);
        if (records.isEmpty()) {
            return LearningSignals.builder()
                    .durationCorrectionByMachineFamily(Map.of())
                    .costCorrectionByMachineFamily(Map.of())
                    .successCountByMachineType(Map.of())
                    .build();
        }
        Map<String, List<Double>> durationRatiosByFamily = new HashMap<>();
        Map<String, List<Double>> costRatiosByFamily = new HashMap<>();
        Map<String, Long> successCountByMachineType = new HashMap<>();
        for (EstimateVsActual r : records) {
            if (r.getEstimatedDurationSec() == null || r.getEstimatedDurationSec() <= 0
                    || r.getActualDurationSec() == null || r.getActualDurationSec() < 0) continue;
            String family = machineFamily(r.getMachineType());
            double durRatio = r.getActualDurationSec() / r.getEstimatedDurationSec();
            durationRatiosByFamily.computeIfAbsent(family, k -> new ArrayList<>()).add(durRatio);
            if (r.getEstimatedCostUsd() != null && r.getEstimatedCostUsd() > 0 && r.getActualCostUsd() != null && r.getActualCostUsd() >= 0) {
                double costRatio = r.getActualCostUsd() / r.getEstimatedCostUsd();
                costRatiosByFamily.computeIfAbsent(family, k -> new ArrayList<>()).add(costRatio);
            }
            String mt = r.getMachineType() != null ? r.getMachineType() : "unknown";
            successCountByMachineType.merge(mt, 1L, Long::sum);
        }
        Map<String, Double> durationCorrection = new HashMap<>();
        for (Map.Entry<String, List<Double>> e : durationRatiosByFamily.entrySet()) {
            double avg = e.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(1.0);
            durationCorrection.put(e.getKey(), clampCorrection(avg));
        }
        Map<String, Double> costCorrection = new HashMap<>();
        for (Map.Entry<String, List<Double>> e : costRatiosByFamily.entrySet()) {
            double avg = e.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(1.0);
            costCorrection.put(e.getKey(), clampCorrection(avg));
        }
        return LearningSignals.builder()
                .durationCorrectionByMachineFamily(durationCorrection)
                .costCorrectionByMachineFamily(costCorrection)
                .successCountByMachineType(successCountByMachineType)
                .build();
    }
}
