package com.di.streamnova.agent.execution_planner;

import com.di.streamnova.agent.profiler.TableProfile;
import com.di.streamnova.agent.shardplanner.PoolSizeCalculator;
import com.di.streamnova.agent.shardplanner.ShardPlanner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Orchestrates the Candidate Generator: machine ladder (n2 → n2d → c3),
 * worker scaling, and shard planning to produce a list of ExecutionPlanOptions for
 * the Estimator and Recommender.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AdaptiveExecutionPlannerService {

    private final MachineLadder machineLadder;
    private final ShardPlanner shardPlanner;

    @Value("${streamnova.recommend.max-candidates}")
    private int defaultMaxCandidates;

    @Value("${streamnova.recommend.max-workers}")
    private int maxWorkers;

    @Value("${streamnova.recommend.max-workers-cap}")
    private int maxWorkersCap;

    @Value("${streamnova.recommend.fallback-pool-size}")
    private int fallbackPoolSize;

    /**
     * Generates load candidates from a table profile. Uses full machine ladder
     * and reduced worker steps to keep candidate count bounded.
     *
     * @param tableProfile  profile from Profiler (row count, row size)
     * @param profileRunId   optional run id from Profiler
     * @return result with list of candidates
     */
    public AdaptivePlanResult generate(TableProfile tableProfile, String profileRunId) {
        return generate(tableProfile, profileRunId, defaultMaxCandidates, null, null);
    }

    /**
     * Generates load candidates with optional max count and machine family filter.
     *
     * @param tableProfile   profile from Profiler
     * @param profileRunId   optional run id
     * @param maxCandidates  cap on number of candidates (default 24)
     * @param machineFamily  optional filter: "n2", "n2d", "c3" (null = all)
     */
    public AdaptivePlanResult generate(TableProfile tableProfile, String profileRunId,
                                             Integer maxCandidates, String machineFamily) {
        return generate(tableProfile, profileRunId, maxCandidates, machineFamily, null);
    }

    /**
     * Generates load candidates with optional max count, machine family filter, and database pool cap.
     * When databasePoolMaxSize is set, shard count per candidate is capped at 80% of pool (connection headroom).
     *
     * @param tableProfile         profile from Profiler
     * @param profileRunId         optional run id
     * @param maxCandidates        cap on number of candidates (default 24)
     * @param machineFamily        optional filter: "n2", "n2d", "c3" (null = all)
     * @param databasePoolMaxSize  optional max connection pool size; when set, shards ≤ floor(pool * 0.8)
     */
    public AdaptivePlanResult generate(TableProfile tableProfile, String profileRunId,
                                             Integer maxCandidates, String machineFamily,
                                             Integer databasePoolMaxSize) {
        if (tableProfile == null) {
            log.warn("[CANDIDATES] No table profile; returning empty result");
            return AdaptivePlanResult.builder()
                    .tableProfile(null)
                    .candidates(List.of())
                    .profileRunId(profileRunId)
                    .build();
        }

        int cap = maxCandidates != null && maxCandidates > 0 ? maxCandidates : defaultMaxCandidates;
        Set<ExecutionPlanOption> seen = new LinkedHashSet<>();
        List<ExecutionPlanOption> candidates = new ArrayList<>();
        if (databasePoolMaxSize != null && databasePoolMaxSize > 0) {
            log.debug("[CANDIDATES] Pool cap applied: shards ≤ 80% of {} = {}", databasePoolMaxSize, (int) Math.floor(databasePoolMaxSize * 0.8));
        }

        // Use n2d first, then n2, then c3 (family order from config)
        List<List<String>> tiers = machineFamily != null && !machineFamily.isBlank()
                ? List.of(machineLadder.getLadder(machineFamily))
                : List.of(
                        machineLadder.getN2dLadder(),
                        machineLadder.getN2Ladder(),
                        machineLadder.getC3Ladder());

        for (List<String> machineTypes : tiers) {
            if (candidates.size() >= cap) break;
            for (String machineType : machineTypes) {
                if (candidates.size() >= cap) break;
                int vCpus = MachineLadder.extractVcpus(machineType);
                int effectiveMaxWorkers = Math.min(maxWorkers, maxWorkersCap);
                List<Integer> workerCounts = WorkerScaling.getReducedWorkerCandidates(effectiveMaxWorkers);
                for (int workers : workerCounts) {
                    if (candidates.size() >= cap) break;
                    int shards = shardPlanner.suggestShardCountForCandidate(
                            machineType, workers,
                            tableProfile.getRowCountEstimate(), tableProfile.getAvgRowSizeBytes(),
                            databasePoolMaxSize);
                    int poolSize = PoolSizeCalculator.calculateFromMachineType(machineType, fallbackPoolSize);
                    poolSize = Math.max(poolSize, Math.min(shards, 100));
                    ExecutionPlanOption c = ExecutionPlanOption.builder()
                            .machineType(machineType)
                            .workerCount(workers)
                            .shardCount(shards)
                            .virtualCpus(vCpus)
                            .suggestedPoolSize(poolSize)
                            .label(machineType + "-" + workers + "w-" + shards + "s")
                            .build();
                    if (seen.add(c)) {
                        candidates.add(c);
                    }
                }
            }
        }

        log.info("[CANDIDATES] Generated {} candidates for table {} (n2d first, then n2, then c3; profileRunId={})",
                candidates.size(), tableProfile.getTableName(), profileRunId);

        return AdaptivePlanResult.builder()
                .tableProfile(tableProfile)
                .candidates(List.copyOf(candidates))
                .profileRunId(profileRunId)
                .build();
    }
}
