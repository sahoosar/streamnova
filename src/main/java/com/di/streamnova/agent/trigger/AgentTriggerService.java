package com.di.streamnova.agent.trigger;

import com.di.streamnova.agent.capacity.CapacityMessageService;
import com.di.streamnova.agent.capacity.ShardAvailabilityService;
import com.di.streamnova.agent.estimator.EstimatedCandidate;
import com.di.streamnova.agent.estimator.EstimationContext;
import com.di.streamnova.agent.estimator.EstimatorService;
import com.di.streamnova.agent.estimator.LoadPattern;
import com.di.streamnova.agent.execution_engine.ExecuteRequest;
import com.di.streamnova.agent.execution_engine.ExecutionEngine;
import com.di.streamnova.agent.execution_engine.ExecutionResult;
import com.di.streamnova.agent.execution_planner.AdaptiveExecutionPlannerService;
import com.di.streamnova.agent.execution_planner.AdaptivePlanResult;
import com.di.streamnova.agent.execution_planner.ExecutionPlanOption;
import com.di.streamnova.agent.metrics.LearningSignals;
import com.di.streamnova.agent.metrics.MetricsLearningService;
import com.di.streamnova.agent.profiler.ProfileResult;
import com.di.streamnova.agent.profiler.ProfilerService;
import com.di.streamnova.agent.profiler.TableProfile;
import com.di.streamnova.agent.recommender.Guardrails;
import com.di.streamnova.agent.recommender.RecommendationResult;
import com.di.streamnova.agent.recommender.RecommendationTriple;
import com.di.streamnova.agent.recommender.RecommenderService;
import com.di.streamnova.agent.recommender.ScoredCandidate;
import com.di.streamnova.agent.recommender.UserMode;
import com.di.streamnova.config.HandlerOverrides;
import com.di.streamnova.config.PipelineConfigService;
import com.di.streamnova.config.PipelineConfigSource;
import com.di.streamnova.guardrail.GuardrailService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Service that runs the agent flow: recommend (profile → candidates → estimate → recommend) and optionally execute.
 * Used by the webhook and by {@link com.di.streamnova.agent.recommender.RecommendationController}.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AgentTriggerService {

    private final ProfilerService profilerService;
    private final AdaptiveExecutionPlannerService adaptiveExecutionPlannerService;
    private final EstimatorService estimatorService;
    private final RecommenderService recommenderService;
    private final MetricsLearningService metricsLearningService;
    private final CapacityMessageService capacityMessageService;
    private final PipelineConfigService pipelineConfigService;
    private final GuardrailService guardrailService;
    private final ExecutionEngine executionEngine;
    private final ShardAvailabilityService shardAvailabilityService;

    @Value("${streamnova.estimator.usd-to-gbp:0.79}")
    private double usdToGbp;

    /**
     * Runs recommend: profile → candidates → estimate → recommend by mode.
     * Returns empty if no profile or no passing candidate.
     * @param callerAgentId optional id of the external agent that triggered this run (for tracking when multiple agents call the webhook)
     */
    public Optional<RecommendationResult> recommend(
            UserMode mode,
            String source,
            boolean warmUp,
            LoadPattern loadPattern,
            Double maxCostUsd,
            Double maxDurationSec,
            Double minThroughputMbPerSec,
            List<String> allowedMachineTypes,
            Integer databasePoolMaxSize,
            String callerAgentId) {

        ProfileResult profileResult = profilerService.profile(source, warmUp);
        if (profileResult == null || profileResult.getTableProfile() == null) {
            return Optional.empty();
        }

        Integer effectivePoolMaxSize = resolvePoolMaxSize(databasePoolMaxSize, source);
        AdaptivePlanResult genResult = adaptiveExecutionPlannerService.generate(
                profileResult.getTableProfile(), profileResult.getRunId(), null, null, effectivePoolMaxSize);
        if (genResult.getCandidates() == null || genResult.getCandidates().isEmpty()) {
            return Optional.empty();
        }

        List<EstimatedCandidate> estimated;
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
            return Optional.empty();
        }

        Guardrails guardrails = guardrailService.buildWithOverrides(
                maxCostUsd, maxDurationSec, minThroughputMbPerSec, allowedMachineTypes);

        TableProfile tp = profileResult.getTableProfile();
        LearningSignals learningSignals = metricsLearningService.getLearningSignals(
                tp.getSourceType(), tp.getSchemaName(), tp.getTableName(), 100);
        Map<String, Long> successCountByMachineType =
                (learningSignals != null) ? learningSignals.getSuccessCountByMachineType() : null;
        if (successCountByMachineType != null && successCountByMachineType.isEmpty()) {
            successCountByMachineType = null;
        }

        RecommendationTriple triple = recommenderService.recommendCheapestFastestBalanced(
                estimated, guardrails, successCountByMachineType);
        List<ScoredCandidate> scored = recommenderService.scoreCandidates(estimated, guardrails);
        Optional<EstimatedCandidate> recommended =
                recommenderService.recommend(estimated, mode, guardrails, successCountByMachineType);

        return recommended.map(rec -> {
            String profileRunId = profileResult.getRunId();
            if (profileResult.getThroughputSample().isPresent()) {
                metricsLearningService.recordThroughputProfile(
                        profileRunId, profileResult.getThroughputSample().get());
            }
            String executionRunId = "exec-" + UUID.randomUUID();
            metricsLearningService.recordRunStarted(executionRunId, profileRunId, mode.name(),
                    loadPattern != null ? loadPattern.name() : "DIRECT",
                    tp.getSourceType(), tp.getSchemaName(), tp.getTableName(), callerAgentId);
            var durationEstimate = capacityMessageService.buildDurationEstimate(
                    tp.getSourceType(), tp.getSchemaName(), tp.getTableName(),
                    rec.getEstimatedDurationSec());
            return RecommendationResult.builder()
                    .mode(mode)
                    .recommended(rec)
                    .cheapest(triple.getCheapest())
                    .fastest(triple.getFastest())
                    .balanced(triple.getBalanced())
                    .allEstimated(estimated)
                    .scoredCandidates(scored)
                    .guardrailsApplied(guardrails != null && !guardrails.isEmpty())
                    .guardrailViolations(triple.getGuardrailViolations() != null
                            ? triple.getGuardrailViolations() : List.of())
                    .executionRunId(executionRunId)
                    .usdToGbpRate(usdToGbp)
                    .durationEstimate(durationEstimate)
                    .build();
        });
    }

    /**
     * Executes with the recommended candidate and optional handler overrides.
     * Reserves shards or run slot (exclusive vs shared); releases on failure. Caller should report outcome via POST execution-outcome.
     */
    public ExecutionResult execute(
            RecommendationResult recommendResult,
            String source,
            String intermediate,
            String target) {
        return execute(recommendResult, source, intermediate, target, null);
    }

    /**
     * Executes with the recommended candidate, overrides, and optional exclusive mode.
     * @param exclusive when true, reserve full pool; when false or null, shared (subject to max-concurrent-runs)
     */
    public ExecutionResult execute(
            RecommendationResult recommendResult,
            String source,
            String intermediate,
            String target,
            Boolean exclusive) {

        EstimatedCandidate rec = recommendResult.getRecommended().orElse(null);
        if (rec == null || rec.getCandidate() == null) {
            return ExecutionResult.builder()
                    .success(false)
                    .jobId(null)
                    .message("No recommended candidate to execute")
                    .build();
        }

        ExecutionPlanOption candidate = rec.getCandidate();
        String runId = recommendResult.getExecutionRunId();
        if (runId == null || runId.isBlank()) {
            runId = "exec-" + UUID.randomUUID();
        }

        boolean exclusiveRun = Boolean.TRUE.equals(exclusive);
        if (!shardAvailabilityService.tryReserve(candidate.getEffectiveMaxConcurrentShards(), runId, exclusiveRun)) {
            String reason = com.di.streamnova.agent.capacity.ShardAvailabilityService.getAndClearExclusiveRejectionReason();
            if (reason == null) reason = com.di.streamnova.agent.capacity.ShardAvailabilityService.getAndClearExecuteLimitRejectionReason();
            return ExecutionResult.builder()
                    .success(false)
                    .jobId(null)
                    .message(reason != null ? reason : "Shards not available; try again later")
                    .build();
        }

        ExecuteRequest.CandidateBody body = new ExecuteRequest.CandidateBody();
        body.setMachineType(candidate.getMachineType());
        body.setWorkerCount(candidate.getWorkerCount());
        body.setShardCount(candidate.getShardCount());
        body.setPartitionCount(candidate.getPartitionCount());
        body.setMaxConcurrentShards(candidate.getMaxConcurrentShards());
        body.setVirtualCpus(candidate.getVirtualCpus());
        body.setSuggestedPoolSize(candidate.getSuggestedPoolSize());
        body.setLabel(candidate.getLabel());

        ExecuteRequest request = new ExecuteRequest();
        request.setCandidate(body);
        request.setExecutionRunId(runId);
        request.setSource(trimToNull(source));
        request.setIntermediate(trimToNull(intermediate));
        request.setTarget(trimToNull(target));
        request.setExclusive(exclusive);

        HandlerOverrides overrides = HandlerOverrides.builder()
                .source(trimToNull(source))
                .intermediate(trimToNull(intermediate))
                .target(trimToNull(target))
                .build();

        try {
            ExecutionResult result = executionEngine.execute(candidate, overrides, request);
            if (!result.isSuccess()) {
                shardAvailabilityService.release(runId);
            }
            return result;
        } catch (Exception e) {
            log.error("[WEBHOOK] Execute failed for runId={}: {}", runId, e.getMessage(), e);
            shardAvailabilityService.release(runId);
            return ExecutionResult.builder()
                    .success(false)
                    .jobId(null)
                    .message(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName())
                    .stages((intermediate != null && !intermediate.isBlank()) ? 3 : 2)
                    .selection(Map.of(
                            "source", source != null ? source : "",
                            "intermediate", intermediate != null ? intermediate : "",
                            "target", target != null ? target : ""))
                    .build();
        }
    }

    private Integer resolvePoolMaxSize(Integer requestParam, String sourceKey) {
        if (requestParam != null && requestParam > 0) return requestParam;
        PipelineConfigSource src = pipelineConfigService.getEffectiveSourceConfig(sourceKey);
        if (src == null) return null;
        int pool = src.getMaximumPoolSize() > 0 ? src.getMaximumPoolSize() : src.getFallbackPoolSize();
        return pool > 0 ? pool : null;
    }

    private static String trimToNull(String s) {
        return s != null && !s.isBlank() ? s.trim() : null;
    }
}
