package com.di.streamnova.agent.recommender;

import com.di.streamnova.agent.estimator.EstimatedCandidate;
import lombok.Builder;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Guardrail limits for recommendation: max cost, max duration, min throughput, allowed machine types.
 * Candidates that violate guardrails are excluded (or reported) so recommendations stay within bounds.
 * Business-defined allowed machine types cannot be overridden.
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
    /** Allowed machine types: exact match or family prefix (e.g. "n2-standard-2" exact; "n2" allows n2-*). Null/empty = no restriction. Includes n2-standard-*, n2d-standard-2/4/8/16/32, c3-standard-4/8. */
    List<String> allowedMachineTypes;

    /**
     * Returns true if the given machine type is allowed by the list (exact or prefix match).
     * Trims and ignores blank entries in the allowed list. Use for both guardrails and execution-time enforcement.
     */
    public static boolean isMachineTypeAllowed(String machineType, List<String> allowed) {
        if (machineType == null || machineType.isBlank() || allowed == null || allowed.isEmpty()) return true;
        for (String a : allowed) {
            if (a == null) continue;
            String trimmed = a.trim();
            if (trimmed.isBlank()) continue;
            if (machineType.equals(trimmed) || machineType.startsWith(trimmed + "-")) return true;
        }
        return false;
    }

    /**
     * Returns true if the candidate passes all set guardrails.
     * Corner case: NaN or non-finite cost/duration/throughput fail when that guard is set.
     */
    public boolean passes(EstimatedCandidate e) {
        if (e == null) return false;
        if (allowedMachineTypes != null && !allowedMachineTypes.isEmpty()) {
            String mt = e.getCandidate() != null ? e.getCandidate().getMachineType() : null;
            if (!isMachineTypeAllowed(mt, allowedMachineTypes)) return false;
        }
        if (maxCostUsd != null) {
            double cost = e.getEstimatedCostUsd();
            if (!Double.isFinite(cost) || cost > maxCostUsd) return false;
        }
        if (maxDurationSec != null) {
            double dur = e.getEstimatedDurationSec();
            if (!Double.isFinite(dur) || dur > maxDurationSec) return false;
        }
        if (minThroughputMbPerSec != null) {
            double tp = e.getEffectiveThroughputMbPerSec();
            if (!Double.isFinite(tp) || tp < minThroughputMbPerSec) return false;
        }
        return true;
    }

    /**
     * Returns a short description of why the candidate failed (empty if it passes).
     */
    public List<String> violations(EstimatedCandidate e) {
        List<String> out = new ArrayList<>();
        if (e == null) return out;
        if (allowedMachineTypes != null && !allowedMachineTypes.isEmpty()) {
            String mt = e.getCandidate() != null ? e.getCandidate().getMachineType() : null;
            if (!isMachineTypeAllowed(mt, allowedMachineTypes)) {
                String listStr = allowedMachineTypes.stream().filter(s -> s != null && !s.isBlank()).collect(Collectors.joining(", "));
                out.add("machineType " + (mt != null ? mt : "null") + " not in allowed list: [" + listStr + "]");
            }
        }
        if (maxCostUsd != null) {
            double cost = e.getEstimatedCostUsd();
            if (!Double.isFinite(cost) || cost > maxCostUsd) {
                out.add(Double.isFinite(cost) ? "cost $" + String.format("%.4f", cost) + " > max $" + maxCostUsd : "cost invalid (NaN/infinite)");
            }
        }
        if (maxDurationSec != null) {
            double dur = e.getEstimatedDurationSec();
            if (!Double.isFinite(dur) || dur > maxDurationSec) {
                out.add(Double.isFinite(dur) ? "duration " + String.format("%.0f", dur) + "s > max " + maxDurationSec + "s" : "duration invalid (NaN/infinite)");
            }
        }
        if (minThroughputMbPerSec != null) {
            double tp = e.getEffectiveThroughputMbPerSec();
            if (!Double.isFinite(tp) || tp < minThroughputMbPerSec) {
                out.add(Double.isFinite(tp) ? "throughput " + String.format("%.1f", tp) + " MB/s < min " + minThroughputMbPerSec + " MB/s" : "throughput invalid (NaN/infinite)");
            }
        }
        return out;
    }

    public boolean isEmpty() {
        return maxCostUsd == null && maxDurationSec == null && minThroughputMbPerSec == null
                && (allowedMachineTypes == null || allowedMachineTypes.isEmpty());
    }
}
