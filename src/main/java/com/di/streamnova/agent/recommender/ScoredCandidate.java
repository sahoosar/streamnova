package com.di.streamnova.agent.recommender;

import com.di.streamnova.agent.estimator.EstimatedCandidate;
import lombok.Builder;
import lombok.Value;

/**
 * An estimated candidate with COST vs FAST scores (0–100). Higher = better for that dimension.
 * Used for ranking and for displaying Cheapest vs Fastest vs Balanced.
 */
@Value
@Builder
public class ScoredCandidate {
    EstimatedCandidate estimated;
    /** Cost score 0–100: higher = cheaper (best cost in list = 100). */
    double costScore;
    /** Fast score 0–100: higher = faster (best duration in list = 100). */
    double fastScore;
    /** Balanced score: lower raw = better; can derive rank. */
    double balancedScoreRaw;
    /** Whether this candidate passes guardrails (if guardrails were applied). */
    boolean passesGuardrails;

    public EstimatedCandidate getEstimated() {
        return estimated;
    }
}
