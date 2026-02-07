package com.di.streamnova.agent.adaptive_execution_planner;

import com.di.streamnova.agent.profiler.TableProfile;
import lombok.Builder;
import lombok.Value;

import java.util.List;

/**
 * Result of candidate generation: table profile plus a list of load candidates
 * for the Estimator to score (time/cost) and Recommender to rank.
 */
@Value
@Builder
public class AdaptivePlanResult {
    /** Profile used to generate candidates (row count, row size, etc.). */
    TableProfile tableProfile;
    /** Generated candidates (machine type, workers, shards). */
    List<ExecutionPlanOption> candidates;
    /** Optional run id from the profiler. */
    String profileRunId;

    public int getCandidateCount() {
        return candidates != null ? candidates.size() : 0;
    }
}
