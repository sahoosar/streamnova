package com.di.streamnova.agent.estimator;

import com.di.streamnova.agent.adaptive_execution_planner.ExecutionPlanOption;
import com.di.streamnova.agent.profiler.TableProfile;
import com.di.streamnova.agent.profiler.ThroughputSample;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Estimates time and cost per candidate. Applies source cap (e.g. Oracle), CPU cap,
 * and sink cap (BQ direct / GCS+BQ), then predicts duration and cost.
 */
@Slf4j
@Service
public class EstimatorService {

    private static final double DEFAULT_THROUGHPUT_MB_PER_SEC = 50.0;
    private static final double USD_PER_VCPU_HOUR_N2 = 0.031;
    private static final double USD_PER_VCPU_HOUR_N2D = 0.027;
    private static final double USD_PER_VCPU_HOUR_C3 = 0.035;

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
        double totalMb = context.getTotalMb();
        double measured = context.getMeasuredThroughputMbPerSec();
        double sourceThroughput = SourceCap.effectiveSourceThroughputMbPerSec(
                measured > 0 ? measured : DEFAULT_THROUGHPUT_MB_PER_SEC,
                context.getSourceType());
        LoadPattern loadPattern = context.getLoadPattern() != null ? context.getLoadPattern() : LoadPattern.DIRECT;
        double sinkCap = SinkCap.getCapMbPerSec(loadPattern);

        List<EstimatedCandidate> out = new ArrayList<>();
        for (ExecutionPlanOption c : candidates) {
            double cpuCap = CpuCap.getCapMbPerSec(c);
            int shards = Math.max(1, c.getShardCount());
            int slots = c.getWorkerCount() * Math.max(1, c.getVirtualCpus());
            int effectiveParallelism = Math.min(shards, slots);
            if (effectiveParallelism <= 0) {
                out.add(buildEstimated(c, 3600.0, estimateCostUsd(3600.0, c), 0.0, Bottleneck.PARALLELISM));
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
            double costUsd = estimateCostUsd(durationSec, c);
            out.add(buildEstimated(c, durationSec, costUsd, effectiveMbPerSec, bottleneck));
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

    private static double estimateCostUsd(double durationSec, ExecutionPlanOption c) {
        double hours = durationSec / 3600.0;
        double usdPerVcpuHour = usdPerVcpuHourFor(c.getMachineType());
        double vcpuHours = hours * c.getVirtualCpus() * c.getWorkerCount();
        return vcpuHours * usdPerVcpuHour;
    }

    private static double usdPerVcpuHourFor(String machineType) {
        if (machineType == null) return USD_PER_VCPU_HOUR_N2;
        String lower = machineType.toLowerCase();
        if (lower.startsWith("n2d")) return USD_PER_VCPU_HOUR_N2D;
        if (lower.startsWith("c3")) return USD_PER_VCPU_HOUR_C3;
        return USD_PER_VCPU_HOUR_N2;
    }
}
