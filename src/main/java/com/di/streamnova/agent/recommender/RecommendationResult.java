package com.di.streamnova.agent.recommender;

import com.di.streamnova.agent.estimator.EstimatedCandidate;
import lombok.Builder;
import lombok.Value;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Result of recommendation: Cheapest / Fastest / Balanced + recommended for mode; COST vs FAST scores; guardrail enforcement.
 * Cost is in USD internally; display can convert to GBP.
 */
@Value
@Builder
public class RecommendationResult {
    UserMode mode;
    /** Recommended for the requested mode (one of cheapest / fastest / balanced). */
    EstimatedCandidate recommended;
    /** Cheapest candidate (lowest cost). */
    EstimatedCandidate cheapest;
    /** Fastest candidate (lowest duration). */
    EstimatedCandidate fastest;
    /** Balanced candidate (best cost×time). */
    EstimatedCandidate balanced;
    /** All estimated candidates (for comparison). */
    List<EstimatedCandidate> allEstimated;
    /** Scored candidates (costScore, fastScore 0–100). */
    List<ScoredCandidate> scoredCandidates;
    /** Whether guardrails were applied. */
    boolean guardrailsApplied;
    /** Descriptions of candidates that violated guardrails. */
    List<String> guardrailViolations;
    /** Execution run id recorded in Metrics & Learning Store (for correlating estimates vs actuals). */
    String executionRunId;
    /** Optional USD→GBP rate for getEstimatedCostGbp(); if null, 0.79 is used. */
    Double usdToGbpRate;

    public Optional<EstimatedCandidate> getRecommended() {
        return Optional.ofNullable(recommended);
    }

    public List<String> getGuardrailViolations() {
        return guardrailViolations != null ? guardrailViolations : Collections.emptyList();
    }

    /** Estimated cost in GBP; uses usdToGbpRate if set, else 0.79. Configure via streamnova.estimator.usd-to-gbp. */
    public double getEstimatedCostGbp() {
        if (recommended == null) return 0.0;
        double rate = (usdToGbpRate != null && usdToGbpRate > 0) ? usdToGbpRate : 0.79;
        return recommended.getEstimatedCostUsd() * rate;
    }
}
