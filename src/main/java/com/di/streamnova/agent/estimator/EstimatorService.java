package com.di.streamnova.agent.estimator;

import com.di.streamnova.agent.execution_planner.ExecutionPlanOption;
import com.di.streamnova.agent.metrics.LearningSignals;
import com.di.streamnova.agent.metrics.MetricsLearningService;
import com.di.streamnova.agent.metrics.ThroughputProfile;
import com.di.streamnova.agent.profiler.TableProfile;
import com.di.streamnova.agent.profiler.ThroughputSample;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Estimates time and cost per candidate. Applies source cap (e.g. Oracle), CPU cap,
 * and sink cap (BQ direct / GCS+BQ), then predicts duration and cost.
 * USD-per-vCPU-hour rates are configurable via streamnova.estimator.usd-per-vcpu-hour.* .
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EstimatorService {

    private final MetricsLearningService metricsLearningService;

    @Value("${streamnova.estimator.usd-per-vcpu-hour.n2}")
    private double usdPerVcpuHourN2;
    @Value("${streamnova.estimator.usd-per-vcpu-hour.n2d}")
    private double usdPerVcpuHourN2d;
    @Value("${streamnova.estimator.usd-per-vcpu-hour.c3}")
    private double usdPerVcpuHourC3;

    @Value("${streamnova.estimator.default-throughput-mb-per-sec}")
    private double defaultThroughputMbPerSec;
    @Value("${streamnova.estimator.fallback-duration-sec}")
    private double fallbackDurationSec;
    @Value("${streamnova.estimator.learning-signals-limit}")
    private int learningSignalsLimit;
    @Value("${streamnova.estimator.throughput-history-limit}")
    private int throughputHistoryLimit;

    @Value("${streamnova.estimator.source-cap.postgres}")
    private double sourceCapPostgres;
    @Value("${streamnova.estimator.source-cap.oracle}")
    private double sourceCapOracle;
    @Value("${streamnova.estimator.sink-cap.bq-direct}")
    private double sinkCapBqDirect;
    @Value("${streamnova.estimator.sink-cap.gcs-bq}")
    private double sinkCapGcsBq;
    @Value("${streamnova.estimator.cpu-cap-mb-per-sec-per-vcpu}")
    private double cpuCapMbPerSecPerVcpu;

    /** Ordered prefixes for machine family (first match wins; e.g. n2d before n2). From application.properties. */
    @Value("${streamnova.estimator.machine-family-prefixes}")
    private String machineFamilyPrefixesConfig;
    @Value("${streamnova.estimator.machine-family-default}")
    private String machineFamilyDefault;

    /** Resolved list of prefixes (order matters: n2d before n2). */
    private List<String> machineFamilyPrefixes;
    /** Family name -> USD per vCPU hour (populated from n2/n2d/c3 rates). */
    private Map<String, Double> usdPerVcpuHourByFamily;

    @PostConstruct
    void initMachineFamilyConfig() {
        machineFamilyPrefixes = parseMachineFamilyPrefixes(machineFamilyPrefixesConfig);
        usdPerVcpuHourByFamily = new LinkedHashMap<>();
        usdPerVcpuHourByFamily.put("n2d", usdPerVcpuHourN2d);
        usdPerVcpuHourByFamily.put("n2", usdPerVcpuHourN2);
        usdPerVcpuHourByFamily.put("c3", usdPerVcpuHourC3);
    }

    private static List<String> parseMachineFamilyPrefixes(String config) {
        if (config == null || config.isBlank()) return List.of("n2d", "n2", "c3");
        return Arrays.stream(config.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

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
                profile.getSourceType(), profile.getSchemaName(), profile.getTableName(), learningSignalsLimit);
        Map<String, Double> durationCorrection = signals.getDurationCorrectionByMachineFamily() != null
                ? signals.getDurationCorrectionByMachineFamily() : Map.of();
        Map<String, Double> costCorrection = signals.getCostCorrectionByMachineFamily() != null
                ? signals.getCostCorrectionByMachineFamily() : Map.of();

        double totalMb = context.getTotalMb();
        double measured = context.getMeasuredThroughputMbPerSec();
        double throughputForCap = measured > 0 ? measured : fallbackThroughputFromHistory(profile);
        double sourceThroughput = SourceCap.effectiveSourceThroughputMbPerSec(throughputForCap, context.getSourceType(), sourceCapPostgres, sourceCapOracle);
        LoadPattern loadPattern = context.getLoadPattern() != null ? context.getLoadPattern() : LoadPattern.DIRECT;
        double sinkCap = SinkCap.getCapMbPerSec(loadPattern, sinkCapBqDirect, sinkCapGcsBq);

        List<EstimatedCandidate> out = new ArrayList<>();
        for (ExecutionPlanOption c : candidates) {
            double cpuCap = CpuCap.getCapMbPerSec(c, cpuCapMbPerSecPerVcpu);
            int shards = Math.max(1, c.getShardCount());
            int slots = c.getWorkerCount() * Math.max(1, c.getVirtualCpus());
            int effectiveParallelism = Math.min(shards, slots);
            if (effectiveParallelism <= 0) {
                out.add(buildEstimated(c, fallbackDurationSec, estimateCostUsd(fallbackDurationSec, c, this), 0.0, Bottleneck.PARALLELISM));
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

            double durationSec = effectiveMbPerSec > 0 ? totalMb / effectiveMbPerSec : fallbackDurationSec;
            double costUsd = estimateCostUsd(durationSec, c, this);
            String family = machineFamily(c.getMachineType());
            double durFactor = durationCorrection.getOrDefault(family, 1.0);
            double costFactor = costCorrection.getOrDefault(family, 1.0);
            durationSec *= durFactor;
            costUsd *= costFactor;
            if (!Double.isFinite(durationSec) || durationSec < 0) durationSec = fallbackDurationSec;
            if (!Double.isFinite(costUsd) || costUsd < 0) costUsd = 0.0;
            out.add(buildEstimated(c, durationSec, costUsd, effectiveMbPerSec, bottleneck));
        }
        if (!durationCorrection.isEmpty() || !costCorrection.isEmpty()) {
            log.debug("[ESTIMATOR] Applied learning corrections (duration/cost by family) from {} past runs", learningSignalsLimit);
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
        String family = machineFamily(machineType);
        return usdPerVcpuHourByFamily.getOrDefault(family, usdPerVcpuHourN2);
    }

    /** Resolves machine family from type using configurable ordered prefixes (e.g. n2d before n2). */
    private String machineFamily(String machineType) {
        if (machineType == null || machineType.isBlank()) return machineFamilyDefault != null ? machineFamilyDefault : "n2";
        String lower = machineType.toLowerCase();
        for (String prefix : machineFamilyPrefixes) {
            if (prefix != null && !prefix.isEmpty() && lower.startsWith(prefix.toLowerCase())) {
                return prefix;
            }
        }
        return machineFamilyDefault != null ? machineFamilyDefault : "n2";
    }

    /** When no warm-up throughput is available, use average of recent throughput profiles for this table if any. */
    private double fallbackThroughputFromHistory(TableProfile profile) {
        if (profile == null) return defaultThroughputMbPerSec;
        List<ThroughputProfile> profiles = metricsLearningService.getThroughputProfiles(
                profile.getSourceType(), profile.getSchemaName(), profile.getTableName(), throughputHistoryLimit);
        if (profiles == null || profiles.isEmpty()) return defaultThroughputMbPerSec;
        double avg = profiles.stream()
                .mapToDouble(ThroughputProfile::getThroughputMbPerSec)
                .filter(t -> t > 0)
                .average()
                .orElse(defaultThroughputMbPerSec);
        if (avg > 0) {
            log.debug("[ESTIMATOR] Using historical throughput fallback: {} MB/s (from {} profiles)", avg, profiles.size());
        }
        return avg > 0 ? avg : defaultThroughputMbPerSec;
    }
}
