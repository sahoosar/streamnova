package com.di.streamnova.agent.estimator;

import com.di.streamnova.agent.execution_planner.ExecutionPlanOption;
import lombok.extern.slf4j.Slf4j;

/**
 * CPU-bound throughput cap: max MB/s the worker fleet can consume from the source.
 * Used by Estimator so duration is not better than CPU capacity allows.
 */
@Slf4j
public final class CpuCap {

    /** Approximate max MB/s per vCPU (read + transform); conservative. */
    private static final double MB_PER_SEC_PER_VCPU = 60.0;

    private CpuCap() {}

    /**
     * Returns the CPU-side throughput cap in MB/s for this candidate.
     * Total capacity = workers × vCPUs × MB_PER_SEC_PER_VCPU.
     */
    public static double getCapMbPerSec(ExecutionPlanOption c) {
        return getCapMbPerSec(c, MB_PER_SEC_PER_VCPU);
    }

    /**
     * Returns the CPU-side throughput cap in MB/s with configurable MB/s per vCPU.
     */
    public static double getCapMbPerSec(ExecutionPlanOption c, double mbPerSecPerVcpu) {
        if (c == null) return 0.0;
        int vCpus = Math.max(1, c.getVirtualCpus());
        int workers = Math.max(1, c.getWorkerCount());
        double cap = workers * vCpus * mbPerSecPerVcpu;
        log.trace("[ESTIMATOR] CPU cap: {} workers × {} vCPUs = {} MB/s", workers, vCpus, cap);
        return cap;
    }
}
