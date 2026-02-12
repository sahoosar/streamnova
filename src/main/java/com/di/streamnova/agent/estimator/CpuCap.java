package com.di.streamnova.agent.estimator;

import com.di.streamnova.agent.execution_planner.ExecutionPlanOption;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * CPU-bound throughput cap: max MB/s the worker fleet can consume from the source.
 * Used by Estimator so duration is not better than CPU capacity allows.
 * Configured via streamnova.estimator.cpu-cap-mb-per-sec-per-vcpu.
 */
@Slf4j
@Component
public class CpuCap {

    private final double mbPerSecPerVcpu;

    public CpuCap(@Value("${streamnova.estimator.cpu-cap-mb-per-sec-per-vcpu}") double mbPerSecPerVcpu) {
        this.mbPerSecPerVcpu = mbPerSecPerVcpu;
    }

    /**
     * Returns the CPU-side throughput cap in MB/s for this candidate.
     * Total capacity = workers × vCPUs × mbPerSecPerVcpu (from config).
     */
    public double getCapMbPerSec(ExecutionPlanOption c) {
        if (c == null) return 0.0;
        int vCpus = Math.max(1, c.getVirtualCpus());
        int workers = Math.max(1, c.getWorkerCount());
        double cap = workers * vCpus * mbPerSecPerVcpu;
        log.trace("[ESTIMATOR] CPU cap: {} workers × {} vCPUs = {} MB/s", workers, vCpus, cap);
        return cap;
    }
}
