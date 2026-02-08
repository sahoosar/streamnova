package com.di.streamnova.agent.estimator;

import com.di.streamnova.agent.adaptive_execution_planner.ExecutionPlanOption;
import com.di.streamnova.agent.metrics.LearningSignals;
import com.di.streamnova.agent.metrics.MetricsLearningService;
import com.di.streamnova.agent.metrics.ThroughputProfile;
import com.di.streamnova.agent.profiler.TableProfile;
import com.di.streamnova.agent.profiler.ThroughputSample;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Estimates time and cost per candidate. Applies source cap (e.g. Oracle), CPU cap,
 * and sink cap (BQ direct / GCS+BQ), then predicts duration and cost.
 * USD-per-vCPU-hour rates are configurable via streamnova.estimator.usd-per-vcpu-hour.* .
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EstimatorService {

    private static final double DEFAULT_THROUGHPUT_MB_PER_SEC = 50.0;
    private static final int LEARNING_SIGNALS_LIMIT = 100;
    private static final int THROUGHPUT_HISTORY_LIMIT = 10;

    private final MetricsLearningService metricsLearningService;

    @Value("${streamnova.estimator.usd-per-vcpu-hour.n2:0.031}")
    private double usdPerVcpuHourN2;
    @Value("${streamnova.estimator.usd-per-vcpu-hour.n2d:0.027}")
    private double usdPerVcpuHourN2d;
    @Value("${streamnova.estimator.usd-per-vcpu-hour.c3:0.035}")
    private double usdPerVcpuHourC3;

    /**
     * Estimates duration and cost for each candidate (backward compatible).
     * Uses DIRECT load pattern and profile's source type.
     */
    public List<EstimatedCandidate> estimate(TableProfile profile, List<ExecutionPlanOption> candidates,
                                             ThroughputSample throughputSample) {
        EstimationContext ctx = EstimationContext.builder()
                .profile(profile)
                .loadPattern(LoadPattern.DIRECT)
                .sourceType(profile != null ? profile.getSourceType() : null)
                .throughputSample(throughputSample)
                .build();
        return estimateWithCaps(ctx, candidates);
    }

    /**
     * Estimates with explicit context: source type, load pattern (DIRECT vs GCS_BQ).
     * Applies source cap (Oracle), CPU cap, and sink cap (BQ direct / GCS+BQ).
     */
    public List<EstimatedCandidate> estimateWithCaps(EstimationContext context, List<ExecutionPlanOption> candidates) {
        if (context == null || context.getProfile() == null || candidates == null || candidates.isEmpty()) {
            return List.of();
        }
        TableProfile profile = context.getProfile();
        LearningSignals signals = metricsLearningService.getLearningSignals(
                profile.getSourceType(), profile.getSchemaName(), profile.getTableName(), LEARNING_SIGNALS_LIMIT);
        Map<String, Double> durationCorrection = signals.getDurationCorrectionByMachineFamily() != null
                ? signals.getDurationCorrectionByMachineFamily() : Map.of();
        Map<String, Double> costCorrection = signals.getCostCorrectionByMachineFamily() != null
                ? signals.getCostCorrectionByMachineFamily() : Map.of();

        double totalMb = context.getTotalMb();
        double measured = context.getMeasuredThroughputMbPerSec();
        double throughputForCap = measured > 0 ? measured : fallbackThroughputFromHistory(profile);
        double sourceThroughput = SourceCap.effectiveSourceThroughputMbPerSec(throughputForCap, context.getSourceType());
        LoadPattern loadPattern = context.getLoadPattern() != null ? context.getLoadPattern() : LoadPattern.DIRECT;
        double sinkCap = SinkCap.getCapMbPerSec(loadPattern);

        List<EstimatedCandidate> out = new ArrayList<>();
        for (ExecutionPlanOption c : candidates) {
            double cpuCap = CpuCap.getCapMbPerSec(c);
            int shards = Math.max(1, c.getShardCount());
            int slots = c.getWorkerCount() * Math.max(1, c.getVirtualCpus());
            int effectiveParallelism = Math.min(shards, slots);
            if (effectiveParallelism <= 0) {
                out.add(buildEstimated(c, 3600.0, estimateCostUsd(3600.0, c, this), 0.0, Bottleneck.PARALLELISM));
                continue;
            }
            double overheadFactor = 1.0 + (30.0 / slots);
            double parallelThroughput = (sourceThroughput * effectiveParallelism) / overheadFactor;

            double effectiveMbPerSec = parallelThroughput;
            Bottleneck bottleneck = Bottleneck.PARALLELISM;
            if (cpuCap < effectiveMbPerSec) {
                effectiveMbPerSec = cpuCap;
                bottleneck = Bottleneck.CPU;
            }
            if (sinkCap < effectiveMbPerSec) {
                effectiveMbPerSec = sinkCap;
                bottleneck = Bottleneck.SINK;
            }
            if (effectiveMbPerSec >= parallelThroughput - 1e-6) {
                bottleneck = (context.getSourceType() != null && context.getSourceType().toLowerCase().contains("oracle"))
                        ? Bottleneck.SOURCE : Bottleneck.PARALLELISM;
            }

            double durationSec = effectiveMbPerSec > 0 ? totalMb / effectiveMbPerSec : 3600.0;
            double costUsd = estimateCostUsd(durationSec, c, this);
            String family = machineFamily(c.getMachineType());
            double durFactor = durationCorrection.getOrDefault(family, 1.0);
            double costFactor = costCorrection.getOrDefault(family, 1.0);
            durationSec *= durFactor;
            costUsd *= costFactor;
            if (!Double.isFinite(durationSec) || durationSec < 0) durationSec = 3600.0;
            if (!Double.isFinite(costUsd) || costUsd < 0) costUsd = 0.0;
            out.add(buildEstimated(c, durationSec, costUsd, effectiveMbPerSec, bottleneck));
        }
        if (!durationCorrection.isEmpty() || !costCorrection.isEmpty()) {
            log.debug("[ESTIMATOR] Applied learning corrections (duration/cost by family) from {} past runs", LEARNING_SIGNALS_LIMIT);
        }
        log.debug("[ESTIMATOR] Estimated {} candidates (source cap applied, loadPattern={})", out.size(), loadPattern);
        return out;
    }

    private static EstimatedCandidate buildEstimated(ExecutionPlanOption c, double durationSec, double costUsd,
                                                    double effectiveMbPerSec, Bottleneck bottleneck) {
        return EstimatedCandidate.builder()
                .candidate(c)
                .estimatedDurationSec(durationSec)
                .estimatedCostUsd(costUsd)
                .effectiveThroughputMbPerSec(effectiveMbPerSec)
                .bottleneck(bottleneck != null ? bottleneck : Bottleneck.PARALLELISM)
                .build();
    }

    private static double estimateCostUsd(double durationSec, ExecutionPlanOption c, EstimatorService svc) {
        double hours = durationSec / 3600.0;
        double usdPerVcpuHour = svc.usdPerVcpuHourFor(c.getMachineType());
        double vcpuHours = hours * c.getVirtualCpus() * c.getWorkerCount();
        return vcpuHours * usdPerVcpuHour;
    }

    private double usdPerVcpuHourFor(String machineType) {
        if (machineType == null) return usdPerVcpuHourN2;
        String lower = machineType.toLowerCase();
        if (lower.startsWith("n2d")) return usdPerVcpuHourN2d;
        if (lower.startsWith("c3")) return usdPerVcpuHourC3;
        return usdPerVcpuHourN2;
    }

    private static String machineFamily(String machineType) {
        if (machineType == null || machineType.isBlank()) return "n2";
        String lower = machineType.toLowerCase();
        if (lower.startsWith("n2d")) return "n2d";
        if (lower.startsWith("c3")) return "c3";
        return "n2";
    }

    /** When no warm-up throughput is available, use average of recent throughput profiles for this table if any. */
    private double fallbackThroughputFromHistory(TableProfile profile) {
        if (profile == null) return DEFAULT_THROUGHPUT_MB_PER_SEC;
        List<ThroughputProfile> profiles = metricsLearningService.getThroughputProfiles(
                profile.getSourceType(), profile.getSchemaName(), profile.getTableName(), THROUGHPUT_HISTORY_LIMIT);
        if (profiles == null || profiles.isEmpty()) return DEFAULT_THROUGHPUT_MB_PER_SEC;
        double avg = profiles.stream()
                .mapToDouble(ThroughputProfile::getThroughputMbPerSec)
                .filter(t -> t > 0)
                .average()
                .orElse(DEFAULT_THROUGHPUT_MB_PER_SEC);
        if (avg > 0) {
            log.debug("[ESTIMATOR] Using historical throughput fallback: {} MB/s (from {} profiles)", avg, profiles.size());
        }
        return avg > 0 ? avg : DEFAULT_THROUGHPUT_MB_PER_SEC;
    }
}
