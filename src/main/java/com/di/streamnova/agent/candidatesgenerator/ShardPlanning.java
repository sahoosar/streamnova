package com.di.streamnova.agent.candidatesgenerator;

import com.di.streamnova.agent.profiler.TableProfile;
import com.di.streamnova.util.shardplanner.ExecutionEnvironment;
import com.di.streamnova.util.shardplanner.MachineProfileProvider;
import com.di.streamnova.util.shardplanner.MachineTypeResourceValidator;
import com.di.streamnova.util.shardplanner.ShardCountRounder;
import com.di.streamnova.util.shardplanner.SizeBasedConfig;
import com.di.streamnova.util.shardplanner.UnifiedCalculator;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Generates candidate shard counts using the util.shardplanner package so agent and pipeline
 * use the same size-based logic, machine caps, and rounding (ShardCountRounder, SizeBasedConfig).
 */
@Slf4j
public final class ShardPlanning {

    private static final int MIN_SHARDS = 1;
    private static final int MAX_SHARDS_CAP = 256;
    private static final int FALLBACK_SHARDS_WHEN_NO_PROFILE = 8;

    private ShardPlanning() {}

    /**
     * Suggests a single shard count via util.shardplanner: UnifiedCalculator (size-based + machine-type
     * optimizer, constraint applier, ShardCountRounder). When the result would be 1 but workerCount > 1,
     * uses at least workerCount shards so parallelism is utilized.
     */
    public static int suggestShardCount(TableProfile profile, String machineType, int workerCount) {
        if (profile == null) return FALLBACK_SHARDS_WHEN_NO_PROFILE;
        int vCpus = MachineLadder.extractVcpus(machineType);
        if (vCpus <= 0) return FALLBACK_SHARDS_WHEN_NO_PROFILE;

        ExecutionEnvironment environment = new ExecutionEnvironment(
                machineType, vCpus, workerCount, false, ExecutionEnvironment.CloudProvider.GCP);

        int shards = UnifiedCalculator.calculateOptimalShardsForMachineType(
                environment,
                profile.getRowCountEstimate(),
                profile.getAvgRowSizeBytes(),
                SizeBasedConfig.DEFAULT_TARGET_MB_PER_SHARD,
                null);

        if (shards == 1 && workerCount > 1) {
            var profile_mp = com.di.streamnova.util.shardplanner.MachineProfileProvider.getProfile(machineType);
            int maxShards = MachineTypeResourceValidator.calculateMaxShardsForMachineType(environment, profile_mp);
            shards = Math.min(Math.max(MIN_SHARDS, workerCount), Math.min(maxShards, MAX_SHARDS_CAP));
            shards = com.di.streamnova.util.shardplanner.ShardCountRounder.roundToOptimalValue(shards, environment);
            log.debug("[CANDIDATES] ShardPlanning: min-for-parallelism applied (workers={}) → shards={}", workerCount, shards);
        }

        return Math.max(MIN_SHARDS, Math.min(shards, MAX_SHARDS_CAP));
    }

    /**
     * Returns multiple shard count candidates (suggested, half, double) for variety.
     * Suggested value comes from util.shardplanner pipeline.
     */
    public static List<Integer> getShardCandidates(TableProfile profile, String machineType, int workerCount) {
        int suggested = suggestShardCount(profile, machineType, workerCount);
        List<Integer> out = new ArrayList<>();
        out.add(suggested);
        int half = Math.max(MIN_SHARDS, suggested / 2);
        if (half != suggested) out.add(half);
        int double_ = Math.min(MAX_SHARDS_CAP, suggested * 2);
        if (double_ != suggested) out.add(double_);
        Collections.sort(out);
        return Collections.unmodifiableList(out);
    }
}
