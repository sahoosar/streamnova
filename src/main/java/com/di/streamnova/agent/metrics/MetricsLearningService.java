package com.di.streamnova.agent.metrics;

import com.di.streamnova.agent.profiler.ThroughputSample;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service over MetricsLearningStore: records runs and throughput, and derives
 * learning signals (duration/cost correction by machine family, success count by machine type)
 * for the estimator and recommender.
 */
@Service
@RequiredArgsConstructor
public class MetricsLearningService {

    private final MetricsLearningStore store;

    /**
     * Derives learning signals from past runs for the given table: duration/cost correction
     * by machine family and success count by machine type.
     */
    public LearningSignals getLearningSignals(String sourceType, String schemaName, String tableName, int limit) {
        List<EstimateVsActual> recent = store.findRecentEstimatesVsActuals(sourceType, schemaName, tableName, limit);
        if (recent == null || recent.isEmpty()) {
            return LearningSignals.builder()
                    .durationCorrectionByMachineFamily(Map.of())
                    .costCorrectionByMachineFamily(Map.of())
                    .successCountByMachineType(Map.of())
                    .build();
        }
        // Only records that have actuals (successful runs)
        List<EstimateVsActual> successful = recent.stream()
                .filter(r -> r.getActualDurationSec() != null && r.getActualDurationSec() >= 0
                        && r.getEstimatedDurationSec() != null && r.getEstimatedDurationSec() > 0)
                .collect(Collectors.toList());

        Map<String, List<Double>> durationRatiosByFamily = new HashMap<>();
        Map<String, List<Double>> costRatiosByFamily = new HashMap<>();
        Map<String, Long> successByMachineType = new HashMap<>();

        for (EstimateVsActual r : successful) {
            String family = machineFamily(r.getMachineType());
            double estDur = r.getEstimatedDurationSec();
            double actDur = r.getActualDurationSec();
            if (estDur > 0 && actDur >= 0) {
                durationRatiosByFamily.computeIfAbsent(family, k -> new ArrayList<>()).add(actDur / estDur);
            }
            Double estCost = r.getEstimatedCostUsd();
            Double actCost = r.getActualCostUsd();
            if (estCost != null && estCost > 0 && actCost != null && actCost >= 0) {
                costRatiosByFamily.computeIfAbsent(family, k -> new ArrayList<>()).add(actCost / estCost);
            }
            String mt = r.getMachineType() != null ? r.getMachineType() : "unknown";
            successByMachineType.merge(mt, 1L, Long::sum);
        }

        Map<String, Double> durationCorrection = new HashMap<>();
        durationRatiosByFamily.forEach((family, ratios) ->
                durationCorrection.put(family, ratios.stream().mapToDouble(Double::doubleValue).average().orElse(1.0)));
        Map<String, Double> costCorrection = new HashMap<>();
        costRatiosByFamily.forEach((family, ratios) ->
                costCorrection.put(family, ratios.stream().mapToDouble(Double::doubleValue).average().orElse(1.0)));

        return LearningSignals.builder()
                .durationCorrectionByMachineFamily(durationCorrection)
                .costCorrectionByMachineFamily(costCorrection)
                .successCountByMachineType(successByMachineType)
                .build();
    }

    /**
     * Returns recent estimate-vs-actual records that have actual duration (successful runs),
     * for duration estimate messaging.
     */
    public List<EstimateVsActual> getRecentSuccessfulEstimateVsActuals(String sourceType, String schemaName, String tableName, int limit) {
        List<EstimateVsActual> recent = store.findRecentEstimatesVsActuals(sourceType, schemaName, tableName, limit);
        if (recent == null) return List.of();
        return recent.stream()
                .filter(r -> r.getActualDurationSec() != null && r.getActualDurationSec() >= 0)
                .limit(limit)
                .collect(Collectors.toList());
    }

    /**
     * Saves a throughput profile from a profiler sample (e.g. warm-up read).
     */
    public void recordThroughputProfile(String profileRunId, ThroughputSample sample) {
        if (profileRunId == null || sample == null) return;
        ThroughputProfile profile = ThroughputProfile.builder()
                .runId(profileRunId)
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
    }

    /**
     * Records that an execution run has started (PLANNED/RUNNING). Status is updated later via execution-outcome.
     */
    public void recordRunStarted(String executionRunId, String profileRunId, String mode, String loadPattern,
                                 String sourceType, String schemaName, String tableName) {
        if (executionRunId == null) return;
        Instant now = Instant.now();
        ExecutionStatus status = ExecutionStatus.builder()
                .runId(executionRunId)
                .profileRunId(profileRunId)
                .mode(mode)
                .loadPattern(loadPattern)
                .sourceType(sourceType)
                .schemaName(schemaName)
                .tableName(tableName)
                .status(ExecutionStatus.RUNNING)
                .startedAt(now)
                .finishedAt(null)
                .createdAt(now)
                .jobId(null)
                .message(null)
                .build();
        store.saveExecutionStatus(status);
    }

    /**
     * Returns recent throughput profiles for the given table (for fallback throughput in estimator).
     */
    public List<ThroughputProfile> getThroughputProfiles(String sourceType, String schemaName, String tableName, int limit) {
        return store.findRecentThroughputProfiles(sourceType, schemaName, tableName, limit);
    }

    private static String machineFamily(String machineType) {
        if (machineType == null || machineType.isBlank()) return "n2";
        String lower = machineType.toLowerCase();
        if (lower.startsWith("n2d")) return "n2d";
        if (lower.startsWith("c3")) return "c3";
        return "n2";
    }
}
