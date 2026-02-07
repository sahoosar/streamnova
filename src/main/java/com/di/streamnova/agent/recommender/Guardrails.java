package com.di.streamnova.agent.recommender;

import com.di.streamnova.agent.estimator.EstimatedCandidate;
import lombok.Builder;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Guardrail limits for recommendation: max cost, max duration, min throughput.
 * Candidates that violate guardrails are excluded (or reported) so recommendations stay within bounds.
 */
@Value
@Builder
public class Guardrails {
    /** Max allowed cost in USD (null = no limit). */
    Double maxCostUsd;
    /** Max allowed duration in seconds (null = no limit). */
    Double maxDurationSec;
    /** Min required effective throughput in MB/s (null = no limit). */
    Double minThroughputMbPerSec;

    /**
     * Returns true if the candidate passes all set guardrails.
     */
    public boolean passes(EstimatedCandidate e) {
        if (e == null) return false;
        if (maxCostUsd != null && e.getEstimatedCostUsd() > maxCostUsd) return false;
        if (maxDurationSec != null && e.getEstimatedDurationSec() > maxDurationSec) return false;
        if (minThroughputMbPerSec != null && e.getEffectiveThroughputMbPerSec() < minThroughputMbPerSec) return false;
        return true;
    }

    /**
     * Returns a short description of why the candidate failed (empty if it passes).
     */
    public List<String> violations(EstimatedCandidate e) {
        List<String> out = new ArrayList<>();
        if (e == null) return out;
        if (maxCostUsd != null && e.getEstimatedCostUsd() > maxCostUsd) {
            out.add("cost $" + String.format("%.4f", e.getEstimatedCostUsd()) + " > max $" + maxCostUsd);
        }
        if (maxDurationSec != null && e.getEstimatedDurationSec() > maxDurationSec) {
            out.add("duration " + String.format("%.0f", e.getEstimatedDurationSec()) + "s > max " + maxDurationSec + "s");
        }
        if (minThroughputMbPerSec != null && e.getEffectiveThroughputMbPerSec() < minThroughputMbPerSec) {
            out.add("throughput " + String.format("%.1f", e.getEffectiveThroughputMbPerSec()) + " MB/s < min " + minThroughputMbPerSec + " MB/s");
        }
        return out;
    }

    public boolean isEmpty() {
        return maxCostUsd == null && maxDurationSec == null && minThroughputMbPerSec == null;
    }
}
