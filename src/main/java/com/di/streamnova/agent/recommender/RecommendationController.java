package com.di.streamnova.agent.recommender;

import com.di.streamnova.agent.execution_planner.AdaptivePlanResult;
import com.di.streamnova.agent.execution_planner.AdaptiveExecutionPlannerService;
import com.di.streamnova.agent.capacity.CapacityMessageService;
import com.di.streamnova.agent.capacity.ResourceLimitResponse;
import com.di.streamnova.agent.estimator.EstimationContext;
import com.di.streamnova.agent.estimator.EstimatorService;
import com.di.streamnova.agent.estimator.LoadPattern;
import com.di.streamnova.agent.metrics.LearningSignals;
import com.di.streamnova.agent.metrics.MetricsLearningService;
import com.di.streamnova.agent.profiler.ProfileResult;
import com.di.streamnova.agent.profiler.ProfilerService;
import com.di.streamnova.config.PipelineConfigService;
import com.di.streamnova.config.PipelineConfigSource;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import jakarta.annotation.PostConstruct;
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
    private final CapacityMessageService capacityMessageService;
    private final PipelineConfigService pipelineConfigService;

    @Value("${streamnova.guardrails.allowed-machine-types}")
    private String allowedMachineTypesConfig;

    @Value("${streamnova.guardrails.max-duration-sec:#{null}}")
    private Double defaultMaxDurationSec;
    @Value("${streamnova.guardrails.max-cost-usd:#{null}}")
    private Double defaultMaxCostUsd;

    @Value("${streamnova.estimator.usd-to-gbp}")
    private double usdToGbp;

    @Value("${streamnova.recommend.max-concurrent-requests}")
    private int maxConcurrentRequests;
    @Value("${streamnova.recommend.concurrent-request-acquire-timeout-sec}")
    private int concurrentRequestAcquireTimeoutSec;

    private Semaphore recommendSemaphore;

    @PostConstruct
    void initConcurrencyLimit() {
        recommendSemaphore = (maxConcurrentRequests > 0) ? new Semaphore(maxConcurrentRequests) : null;
    }

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
     * @param allowedMachineTypes   guardrail: allowed machine types, comma-separated. Default from config (optional)
     * @param databasePoolMaxSize  max DB connection pool size; shards capped at 80%. If omitted, uses pipeline_config.yml maximumPoolSize (single source of truth)
     */
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> recommend(
            @RequestParam(defaultValue = "BALANCED") UserMode mode,
            @RequestParam(required = false) String source,
            @RequestParam(required = false, defaultValue = "true") boolean warmUp,
            @RequestParam(required = false) LoadPattern loadPattern,
            @RequestParam(required = false) Double maxCostUsd,
            @RequestParam(required = false) Double maxDurationSec,
            @RequestParam(required = false) Double minThroughputMbPerSec,
            @RequestParam(required = false) List<String> allowedMachineTypes,
            @RequestParam(required = false) Integer databasePoolMaxSize) {
        if (recommendSemaphore != null) {
            boolean acquired;
            try {
                acquired = recommendSemaphore.tryAcquire(
                        Math.max(1, concurrentRequestAcquireTimeoutSec), TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return ResponseEntity.status(429).body(capacityMessageService.buildResourceLimitResponse());
            }
            if (!acquired) {
                ResourceLimitResponse body = capacityMessageService.buildResourceLimitResponse();
                return ResponseEntity.status(429)
                        .header("Retry-After", body.getRetryAfterSeconds() != null ? String.valueOf(body.getRetryAfterSeconds()) : null)
                        .body(body);
            }
        }
        try {
            return doRecommend(mode, source, warmUp, loadPattern, maxCostUsd, maxDurationSec,
                    minThroughputMbPerSec, allowedMachineTypes, databasePoolMaxSize);
        } finally {
            if (recommendSemaphore != null) {
                recommendSemaphore.release();
            }
        }
    }

    private ResponseEntity<?> doRecommend(
            UserMode mode, String source, boolean warmUp, LoadPattern loadPattern,
            Double maxCostUsd, Double maxDurationSec, Double minThroughputMbPerSec,
            List<String> allowedMachineTypes, Integer databasePoolMaxSize) {
        ProfileResult profileResult = profilerService.profile(source, warmUp);
        if (profileResult == null || profileResult.getTableProfile() == null) {
            return ResponseEntity.noContent().build();
        }

        Integer effectivePoolMaxSize = resolvePoolMaxSize(databasePoolMaxSize, source);
        AdaptivePlanResult genResult = adaptiveExecutionPlannerService.generate(
                profileResult.getTableProfile(), profileResult.getRunId(), null, null, effectivePoolMaxSize);
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
        if (estimated == null || estimated.isEmpty()) {
            return ResponseEntity.noContent().build();
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
        Map<String, Long> successCountByMachineType = (learningSignals != null) ? learningSignals.getSuccessCountByMachineType() : null;
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
                    com.di.streamnova.agent.capacity.DurationEstimate durationEstimate = capacityMessageService.buildDurationEstimate(
                            tableProfile.getSourceType(), tableProfile.getSchemaName(), tableProfile.getTableName(),
                            rec.getEstimatedDurationSec());
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
                            .durationEstimate(durationEstimate)
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

    /** Pool size: request param overrides; otherwise from pipeline_config.yml (single source of truth). */
    private Integer resolvePoolMaxSize(Integer requestParam, String sourceKey) {
        if (requestParam != null && requestParam > 0) return requestParam;
        PipelineConfigSource src = pipelineConfigService.getEffectiveSourceConfig(sourceKey);
        if (src == null) return null;
        int pool = src.getMaximumPoolSize() > 0 ? src.getMaximumPoolSize() : src.getFallbackPoolSize();
        return pool > 0 ? pool : null;
    }
}
