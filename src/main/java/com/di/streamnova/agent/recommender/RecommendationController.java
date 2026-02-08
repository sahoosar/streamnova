package com.di.streamnova.agent.recommender;

import com.di.streamnova.agent.adaptive_execution_planner.AdaptivePlanResult;
import com.di.streamnova.agent.adaptive_execution_planner.AdaptiveExecutionPlannerService;
import com.di.streamnova.agent.estimator.EstimationContext;
import com.di.streamnova.agent.estimator.EstimatorService;
import com.di.streamnova.agent.estimator.LoadPattern;
import com.di.streamnova.agent.metrics.LearningSignals;
import com.di.streamnova.agent.metrics.MetricsLearningService;
import com.di.streamnova.agent.profiler.ProfileResult;
import com.di.streamnova.agent.profiler.ProfilerService;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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

    @Value("${streamnova.guardrails.allowed-machine-types:}")
    private String allowedMachineTypesConfig;

    @Value("${streamnova.guardrails.max-duration-sec:#{null}}")
    private Double defaultMaxDurationSec;
    @Value("${streamnova.guardrails.max-cost-usd:#{null}}")
    private Double defaultMaxCostUsd;

    @Value("${streamnova.estimator.usd-to-gbp:0.79}")
    private double usdToGbp;

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
     * @param allowedMachineTypes   guardrail: allowed machine types, comma-separated. Default from config: n2-standard-32, n2-standard-64, n2-standard-128 (optional)
     */
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<RecommendationResult> recommend(
            @RequestParam(defaultValue = "BALANCED") UserMode mode,
            @RequestParam(required = false) String source,
            @RequestParam(required = false, defaultValue = "true") boolean warmUp,
            @RequestParam(required = false) LoadPattern loadPattern,
            @RequestParam(required = false) Double maxCostUsd,
            @RequestParam(required = false) Double maxDurationSec,
            @RequestParam(required = false) Double minThroughputMbPerSec,
            @RequestParam(required = false) List<String> allowedMachineTypes) {
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

        List<String> effectiveAllowed = (allowedMachineTypes != null && !allowedMachineTypes.isEmpty())
                ? allowedMachineTypes
                : parseAllowedMachineTypes(allowedMachineTypesConfig);
        Double effectiveMaxDurationSec = sanitizeGuardrailDouble(maxDurationSec != null ? maxDurationSec : defaultMaxDurationSec);
        Double effectiveMaxCostUsd = sanitizeGuardrailDouble(maxCostUsd != null ? maxCostUsd : defaultMaxCostUsd);
        boolean hasGuardrails = effectiveMaxCostUsd != null || effectiveMaxDurationSec != null || minThroughputMbPerSec != null
                || !effectiveAllowed.isEmpty();
        Guardrails guardrails = hasGuardrails
                ? Guardrails.builder()
                        .maxCostUsd(effectiveMaxCostUsd)
                        .maxDurationSec(effectiveMaxDurationSec)
                        .minThroughputMbPerSec(minThroughputMbPerSec)
                        .allowedMachineTypes(effectiveAllowed.isEmpty() ? null : effectiveAllowed)
                        .build()
                : null;

        com.di.streamnova.agent.profiler.TableProfile tp = profileResult.getTableProfile();
        LearningSignals learningSignals = metricsLearningService.getLearningSignals(
                tp.getSourceType(), tp.getSchemaName(), tp.getTableName(), 100);
        Map<String, Long> successCountByMachineType = learningSignals.getSuccessCountByMachineType();
        if (successCountByMachineType != null && successCountByMachineType.isEmpty()) successCountByMachineType = null;

        RecommendationTriple triple = recommenderService.recommendCheapestFastestBalanced(estimated, guardrails, successCountByMachineType);
        List<ScoredCandidate> scored = recommenderService.scoreCandidates(estimated, guardrails);
        Optional<com.di.streamnova.agent.estimator.EstimatedCandidate> recommended =
                recommenderService.recommend(estimated, mode, guardrails, successCountByMachineType);

        return recommended
                .map(rec -> {
                    String profileRunId = profileResult.getRunId();
                    if (profileResult.getThroughputSample().isPresent()) {
                        metricsLearningService.recordThroughputProfile(profileRunId, profileResult.getThroughputSample().get());
                    }
                    String executionRunId = "exec-" + UUID.randomUUID();
                    com.di.streamnova.agent.profiler.TableProfile tableProfile = profileResult.getTableProfile();
                    metricsLearningService.recordRunStarted(executionRunId, profileRunId, mode.name(),
                            loadPattern != null ? loadPattern.name() : "DIRECT",
                            tableProfile.getSourceType(), tableProfile.getSchemaName(), tableProfile.getTableName());
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
                            .usdToGbpRate(usdToGbp)
                            .build());
                })
                .orElse(ResponseEntity.noContent().build());
    }

    private static List<String> parseAllowedMachineTypes(String config) {
        if (config == null || config.isBlank()) return List.of();
        return Arrays.stream(config.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    /** Corner case: guardrail value must be finite and positive; otherwise treat as no limit. */
    private static Double sanitizeGuardrailDouble(Double value) {
        if (value == null || !Double.isFinite(value) || value < 0) return null;
        return value;
    }
}
