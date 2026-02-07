package com.di.streamnova.agent.recommender;

import com.di.streamnova.agent.estimator.EstimatedCandidate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * COST vs FAST scoring; Cheapest / Fastest / Balanced picks; Guardrail enforcement.
 */
@Slf4j
@Service
public class RecommenderService {

    /**
     * Returns the recommended candidate for the given mode (backward compatible).
     */
    public Optional<EstimatedCandidate> recommend(List<EstimatedCandidate> estimated, UserMode mode) {
        return recommend(estimated, mode, null);
    }

    /**
     * Returns the recommended candidate for the given mode, applying guardrails when provided.
     * Only candidates that pass guardrails are considered.
     */
    public Optional<EstimatedCandidate> recommend(List<EstimatedCandidate> estimated, UserMode mode, Guardrails guardrails) {
        List<EstimatedCandidate> eligible = applyGuardrails(estimated, guardrails);
        if (eligible.isEmpty()) {
            log.warn("[RECOMMENDER] No candidates pass guardrails");
            return Optional.empty();
        }
        if (eligible.size() == 1) {
            return Optional.of(eligible.get(0));
        }
        Comparator<EstimatedCandidate> comparator = comparatorFor(mode);
        EstimatedCandidate best = eligible.stream().min(comparator).orElse(null);
        if (best != null) {
            log.info("[RECOMMENDER] Mode={} → {} (est. {}s, ${})",
                    mode, best.getCandidate().getLabelOrDefault(),
                    String.format("%.1f", best.getEstimatedDurationSec()),
                    String.format("%.4f", best.getEstimatedCostUsd()));
        }
        return Optional.ofNullable(best);
    }

    /**
     * COST vs FAST scoring: assigns costScore and fastScore (0–100) to each candidate.
     * Best cost in list = 100 cost score; best duration = 100 fast score.
     */
    public List<ScoredCandidate> scoreCandidates(List<EstimatedCandidate> estimated, Guardrails guardrails) {
        if (estimated == null || estimated.isEmpty()) return List.of();

        double minCost = estimated.stream().mapToDouble(EstimatedCandidate::getEstimatedCostUsd).min().orElse(0.0);
        double maxCost = estimated.stream().mapToDouble(EstimatedCandidate::getEstimatedCostUsd).max().orElse(1.0);
        double costRange = maxCost - minCost;
        if (costRange < 1e-9) costRange = 1.0;

        double minDur = estimated.stream().mapToDouble(EstimatedCandidate::getEstimatedDurationSec).min().orElse(0.0);
        double maxDur = estimated.stream().mapToDouble(EstimatedCandidate::getEstimatedDurationSec).max().orElse(1.0);
        double durRange = maxDur - minDur;
        if (durRange < 1e-9) durRange = 1.0;

        List<ScoredCandidate> out = new ArrayList<>();
        for (EstimatedCandidate e : estimated) {
            double costScore = 100.0 * (1.0 - (e.getEstimatedCostUsd() - minCost) / costRange);
            double fastScore = 100.0 * (1.0 - (e.getEstimatedDurationSec() - minDur) / durRange);
            double balancedRaw = e.getEstimatedCostUsd() * e.getEstimatedDurationSec();
            boolean passes = guardrails == null || guardrails.isEmpty() || guardrails.passes(e);
            out.add(ScoredCandidate.builder()
                    .estimated(e)
                    .costScore(Math.max(0, Math.min(100, costScore)))
                    .fastScore(Math.max(0, Math.min(100, fastScore)))
                    .balancedScoreRaw(balancedRaw)
                    .passesGuardrails(passes)
                    .build());
        }
        return out;
    }

    /**
     * Filter to candidates that pass guardrails. Returns all if guardrails null/empty.
     */
    public List<EstimatedCandidate> applyGuardrails(List<EstimatedCandidate> estimated, Guardrails guardrails) {
        if (estimated == null) return List.of();
        if (guardrails == null || guardrails.isEmpty()) return estimated;
        return estimated.stream().filter(guardrails::passes).collect(Collectors.toList());
    }

    /**
     * Returns candidates that violate guardrails (for reporting).
     */
    public List<String> guardrailViolations(List<EstimatedCandidate> estimated, Guardrails guardrails) {
        if (estimated == null || guardrails == null || guardrails.isEmpty()) return List.of();
        List<String> out = new ArrayList<>();
        for (EstimatedCandidate e : estimated) {
            List<String> v = guardrails.violations(e);
            if (!v.isEmpty()) {
                out.add(e.getCandidate().getLabelOrDefault() + ": " + String.join("; ", v));
            }
        }
        return out;
    }

    /**
     * Returns Cheapest, Fastest, and Balanced picks (from candidates that pass guardrails).
     */
    public RecommendationTriple recommendCheapestFastestBalanced(List<EstimatedCandidate> estimated, Guardrails guardrails) {
        List<EstimatedCandidate> eligible = applyGuardrails(estimated, guardrails);
        if (eligible.isEmpty()) {
            return RecommendationTriple.builder()
                    .cheapest(null).fastest(null).balanced(null)
                    .guardrailViolations(guardrailViolations(estimated, guardrails))
                    .build();
        }
        EstimatedCandidate cheapest = eligible.stream()
                .min(Comparator.comparingDouble(EstimatedCandidate::getEstimatedCostUsd)
                        .thenComparingDouble(EstimatedCandidate::getEstimatedDurationSec))
                .orElse(null);
        EstimatedCandidate fastest = eligible.stream()
                .min(Comparator.comparingDouble(EstimatedCandidate::getEstimatedDurationSec)
                        .thenComparingDouble(EstimatedCandidate::getEstimatedCostUsd))
                .orElse(null);
        EstimatedCandidate balanced = eligible.stream()
                .min(Comparator.comparingDouble(RecommenderService::balancedScore)
                        .thenComparingDouble(EstimatedCandidate::getEstimatedCostUsd))
                .orElse(null);
        return RecommendationTriple.builder()
                .cheapest(cheapest)
                .fastest(fastest)
                .balanced(balanced)
                .guardrailViolations(guardrailViolations(estimated, guardrails))
                .build();
    }

    private static Comparator<EstimatedCandidate> comparatorFor(UserMode mode) {
        switch (mode) {
            case COST_OPTIMAL:
                return Comparator.comparingDouble(EstimatedCandidate::getEstimatedCostUsd)
                        .thenComparingDouble(EstimatedCandidate::getEstimatedDurationSec);
            case FAST_LOAD:
                return Comparator.comparingDouble(EstimatedCandidate::getEstimatedDurationSec)
                        .thenComparingDouble(EstimatedCandidate::getEstimatedCostUsd);
            case BALANCED:
                return Comparator.comparingDouble(RecommenderService::balancedScore)
                        .thenComparingDouble(EstimatedCandidate::getEstimatedCostUsd);
            default:
                return Comparator.comparingDouble(EstimatedCandidate::getEstimatedCostUsd)
                        .thenComparingDouble(EstimatedCandidate::getEstimatedDurationSec);
        }
    }

    private static double balancedScore(EstimatedCandidate e) {
        return e.getEstimatedCostUsd() * e.getEstimatedDurationSec();
    }
}
