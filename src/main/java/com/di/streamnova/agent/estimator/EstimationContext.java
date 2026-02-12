package com.di.streamnova.agent.estimator;

import com.di.streamnova.agent.execution_planner.ExecutionPlanOption;
import com.di.streamnova.agent.profiler.TableProfile;
import com.di.streamnova.agent.profiler.ThroughputSample;
import lombok.Builder;
import lombok.Value;

/**
 * Input context for estimation: profile, load pattern, source type, and optional throughput.
 * Used to compute source/cpu/sink caps and effective duration.
 */
@Value
@Builder
public class EstimationContext {
    TableProfile profile;
    LoadPattern loadPattern;
    /** Source type (e.g. postgres, oracle) for source cap. */
    String sourceType;
    ThroughputSample throughputSample;

    public double getTotalMb() {
        if (profile == null || profile.getEstimatedTotalBytes() <= 0) return 0.0;
        return profile.getEstimatedTotalBytes() / (1024.0 * 1024.0);
    }

    public double getMeasuredThroughputMbPerSec() {
        if (throughputSample != null && throughputSample.getThroughputMbPerSec() > 0) {
            return throughputSample.getThroughputMbPerSec();
        }
        return 0.0;
    }
}
