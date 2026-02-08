package com.di.streamnova.agent.metrics;

import lombok.Builder;
import lombok.Value;

import java.util.Map;

/**
 * Signals derived from past runs for learning: correction factors for estimates
 * and success counts per machine type so the agent can tune estimates and prefer
 * candidates that historically met SLA.
 */
@Value
@Builder
public class LearningSignals {
    /** Duration correction by machine family (e.g. "n2" -> 0.95). Apply: adjustedDuration = rawDuration * factor. */
    Map<String, Double> durationCorrectionByMachineFamily;
    /** Cost correction by machine family. Apply: adjustedCost = rawCost * factor. */
    Map<String, Double> costCorrectionByMachineFamily;
    /** Number of successful runs per machine type (e.g. "n2-standard-8" -> 5). Used to prefer proven configs. */
    Map<String, Long> successCountByMachineType;
}
