package com.di.streamnova.agent.capacity;

import com.di.streamnova.agent.metrics.EstimateVsActual;
import com.di.streamnova.agent.metrics.MetricsLearningService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Builds user-facing messages for resource limits (429) and duration estimates.
 * Configurable via streamnova.capacity.* (minimal config).
 */
@Service
@RequiredArgsConstructor
public class CapacityMessageService {

    private final MetricsLearningService metricsLearningService;

    @Value("${streamnova.capacity.resource-limited-message:Due to minimal resources, the request cannot be processed. Please retry later.}")
    private String resourceLimitedMessage;

    @Value("${streamnova.capacity.retry-after-sec:60}")
    private int retryAfterSec;

    @Value("${streamnova.capacity.shards-not-available-message:Not enough shards available; please wait for full availability.}")
    private String shardsNotAvailableMessage;

    /** Builds the response body for 429 (resource limit). */
    public ResourceLimitResponse buildResourceLimitResponse() {
        return ResourceLimitResponse.builder()
                .code("RESOURCE_LIMIT")
                .message(resourceLimitedMessage != null && !resourceLimitedMessage.isBlank() ? resourceLimitedMessage : "Due to minimal resources, the request cannot be processed. Please retry later.")
                .retryAfterSeconds(retryAfterSec > 0 ? retryAfterSec : null)
                .build();
    }

    /** Builds the response body for 429 when shards are not available (execute blocked). */
    public ResourceLimitResponse buildShardsNotAvailableResponse(int availableShards, int requiredShards) {
        String msg = (shardsNotAvailableMessage != null && !shardsNotAvailableMessage.isBlank())
                ? shardsNotAvailableMessage
                : "Not enough shards available; please wait for full availability.";
        return ResourceLimitResponse.builder()
                .code("SHARDS_NOT_AVAILABLE")
                .message(msg)
                .retryAfterSeconds(retryAfterSec > 0 ? retryAfterSec : null)
                .availableShards(availableShards)
                .requiredShards(requiredShards)
                .build();
    }

    /**
     * Builds duration estimate for the user: uses last run actual if available,
     * otherwise first-run estimate from the recommended candidate.
     */
    public DurationEstimate buildDurationEstimate(String sourceType, String schemaName, String tableName,
                                                  double recommendedEstimatedDurationSec) {
        List<EstimateVsActual> lastRuns = metricsLearningService.getRecentSuccessfulEstimateVsActuals(
                sourceType, schemaName, tableName, 1);
        if (lastRuns != null && !lastRuns.isEmpty()) {
            EstimateVsActual last = lastRuns.get(0);
            Double actualSec = last != null ? last.getActualDurationSec() : null;
            if (actualSec != null && Double.isFinite(actualSec) && actualSec >= 0) {
                double lastRunMin = actualSec / 60.0;
                return DurationEstimate.builder()
                        .estimatedMinutes(roundOneDecimal(lastRunMin))
                        .firstRun(false)
                        .message("Based on last run: ~" + formatMinutes(lastRunMin) + " min")
                        .lastRunMinutes(roundOneDecimal(lastRunMin))
                        .build();
            }
        }
        double safeSec = Double.isFinite(recommendedEstimatedDurationSec) && recommendedEstimatedDurationSec >= 0
                ? recommendedEstimatedDurationSec : 0;
        double estMin = safeSec / 60.0;
        return DurationEstimate.builder()
                .estimatedMinutes(roundOneDecimal(estMin))
                .firstRun(true)
                .message("First run – estimated ~" + formatMinutes(estMin) + " min; actual time may vary.")
                .lastRunMinutes(null)
                .build();
    }

    private static double roundOneDecimal(double v) {
        return Double.isFinite(v) ? Math.round(v * 10.0) / 10.0 : 0;
    }

    private static String formatMinutes(double min) {
        if (!Double.isFinite(min) || min < 0) return "?";
        if (min < 1) return String.format("%.1f", min);
        return String.format("%.0f", min);
    }
}
