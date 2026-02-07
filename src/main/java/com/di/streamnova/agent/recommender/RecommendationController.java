package com.di.streamnova.agent.recommender;

import com.di.streamnova.agent.adaptive_execution_planner.AdaptivePlanResult;
import com.di.streamnova.agent.adaptive_execution_planner.AdaptiveExecutionPlannerService;
import com.di.streamnova.agent.estimator.EstimationContext;
import com.di.streamnova.agent.estimator.EstimatorService;
import com.di.streamnova.agent.estimator.LoadPattern;
import com.di.streamnova.agent.metrics.MetricsLearningService;
import com.di.streamnova.agent.profiler.ProfileResult;
import com.di.streamnova.agent.profiler.ProfilerService;

import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * API for low-cost vs speed: run Profiler → Candidates → Estimator → Recommender by mode.
 * Use: GET /streamnova/api/agent/recommend?mode=COST_OPTIMAL | FAST_LOAD | BALANCED
 */
@RestController
@RequestMapping("/api/agent/recommend")
@RequiredArgsConstructor
public class RecommendationController {

    private final ProfilerService profilerService;
    private final AdaptiveExecutionPlannerService adaptiveExecutionPlannerService;
    private final EstimatorService estimatorService;
    private final RecommenderService recommenderService;
    private final MetricsLearningService metricsLearningService;

    /**
     * Runs full pipeline: profile → generate candidates → estimate time/cost → recommend by mode.
     *
     * @param mode        COST_OPTIMAL (low cost), FAST_LOAD (speed), or BALANCED
     * @param source      optional source key (e.g. postgres)
     * @param warmUp      run warm-up for throughput (default true)
     * @param loadPattern       DIRECT (source→BQ) or GCS_BQ (source→GCS→BQ); applies sink cap when set
     * @param maxCostUsd        guardrail: max cost in USD (optional)
     * @param maxDurationSec    guardrail: max duration in seconds (optional)
     * @param minThroughputMbPerSec guardrail: min throughput MB/s (optional)
     */
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<RecommendationResult> recommend(
            @RequestParam(defaultValue = "BALANCED") UserMode mode,
            @RequestParam(required = false) String source,
            @RequestParam(required = false, defaultValue = "true") boolean warmUp,
            @RequestParam(required = false) LoadPattern loadPattern,
            @RequestParam(required = false) Double maxCostUsd,
            @RequestParam(required = false) Double maxDurationSec,
            @RequestParam(required = false) Double minThroughputMbPerSec) {
        ProfileResult profileResult = profilerService.profile(source, warmUp);
        if (profileResult == null || profileResult.getTableProfile() == null) {
            return ResponseEntity.noContent().build();
        }

        AdaptivePlanResult genResult = adaptiveExecutionPlannerService.generate(
                profileResult.getTableProfile(), profileResult.getRunId(), null, null);
        if (genResult.getCandidates() == null || genResult.getCandidates().isEmpty()) {
            return ResponseEntity.noContent().build();
        }

        List<com.di.streamnova.agent.estimator.EstimatedCandidate> estimated;
        if (loadPattern != null) {
            EstimationContext ctx = EstimationContext.builder()
                    .profile(profileResult.getTableProfile())
                    .loadPattern(loadPattern)
                    .sourceType(profileResult.getTableProfile().getSourceType())
                    .throughputSample(profileResult.getThroughputSample().orElse(null))
                    .build();
            estimated = estimatorService.estimateWithCaps(ctx, genResult.getCandidates());
        } else {
            estimated = estimatorService.estimate(
                    profileResult.getTableProfile(),
                    genResult.getCandidates(),
                    profileResult.getThroughputSample().orElse(null));
        }

        Guardrails guardrails = (maxCostUsd != null || maxDurationSec != null || minThroughputMbPerSec != null)
                ? Guardrails.builder()
                        .maxCostUsd(maxCostUsd)
                        .maxDurationSec(maxDurationSec)
                        .minThroughputMbPerSec(minThroughputMbPerSec)
                        .build()
                : null;

        RecommendationTriple triple = recommenderService.recommendCheapestFastestBalanced(estimated, guardrails);
        List<ScoredCandidate> scored = recommenderService.scoreCandidates(estimated, guardrails);
        Optional<com.di.streamnova.agent.estimator.EstimatedCandidate> recommended =
                recommenderService.recommend(estimated, mode, guardrails);

        return recommended
                .map(rec -> {
                    String profileRunId = profileResult.getRunId();
                    if (profileResult.getThroughputSample().isPresent()) {
                        metricsLearningService.recordThroughputProfile(profileRunId, profileResult.getThroughputSample().get());
                    }
                    String executionRunId = "exec-" + UUID.randomUUID();
                    com.di.streamnova.agent.profiler.TableProfile tp = profileResult.getTableProfile();
                    metricsLearningService.recordRunStarted(executionRunId, profileRunId, mode.name(),
                            loadPattern != null ? loadPattern.name() : "DIRECT",
                            tp.getSourceType(), tp.getSchemaName(), tp.getTableName());
                    return ResponseEntity.ok(RecommendationResult.builder()
                            .mode(mode)
                            .recommended(rec)
                            .cheapest(triple.getCheapest())
                            .fastest(triple.getFastest())
                            .balanced(triple.getBalanced())
                            .allEstimated(estimated)
                            .scoredCandidates(scored)
                            .guardrailsApplied(guardrails != null && !guardrails.isEmpty())
                            .guardrailViolations(triple.getGuardrailViolations() != null ? triple.getGuardrailViolations() : List.of())
                            .executionRunId(executionRunId)
                            .build());
                })
                .orElse(ResponseEntity.noContent().build());
    }
}
