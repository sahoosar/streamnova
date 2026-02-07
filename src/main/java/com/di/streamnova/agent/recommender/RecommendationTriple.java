package com.di.streamnova.agent.recommender;

import com.di.streamnova.agent.estimator.EstimatedCandidate;
import lombok.Builder;
import lombok.Value;

import java.util.List;

/**
 * Cheapest, Fastest, and Balanced recommendations plus guardrail violations.
 */
@Value
@Builder
public class RecommendationTriple {
    EstimatedCandidate cheapest;
    EstimatedCandidate fastest;
    EstimatedCandidate balanced;
    /** Candidates (by label) that violated guardrails (for reporting). */
    List<String> guardrailViolations;
}
