package com.di.streamnova.agent.execution_planner;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generates candidate worker counts for a given machine type and optional data profile.
 * Used by Candidate Generator to scale workers across the machine ladder.
 */
@Slf4j
public final class WorkerScaling {

    private static final int MIN_WORKERS = 1;
    private static final int MAX_WORKERS_DEFAULT = 32;

    private WorkerScaling() {}

    /**
     * Builds worker list from min to cap, step +1 (linear progression).
     */
    private static List<Integer> workersLinear(int cap) {
        int max = Math.max(MIN_WORKERS, Math.min(cap, MAX_WORKERS_DEFAULT));
        List<Integer> out = new ArrayList<>();
        for (int w = MIN_WORKERS; w <= max; w++) {
            out.add(w);
        }
        return Collections.unmodifiableList(out);
    }

    /**
     * Returns default worker candidates: 1, 2, 3, 4, ... up to cap (capped by maxWorkers).
     */
    public static List<Integer> getDefaultWorkerCandidates(int maxWorkers) {
        return workersLinear(maxWorkers);
    }

    /**
     * Returns worker candidates suitable for the given machine type.
     * Linear steps: 1, 2, 3, 4, ... up to cap.
     */
    public static List<Integer> getWorkerCandidatesForMachine(String machineType, int maxWorkers) {
        int cap = Math.min(maxWorkers, MAX_WORKERS_DEFAULT);
        return workersLinear(cap);
    }

    /**
     * Returns worker candidates scaled by estimated data size: more workers for larger tables.
     * Still capped by maxWorkers; uses linear steps 1, 2, 3, ...
     */
    public static List<Integer> getWorkerCandidatesForDataSize(long estimatedTotalBytes, int maxWorkers) {
        List<Integer> base = getDefaultWorkerCandidates(maxWorkers);
        long gb = estimatedTotalBytes / (1024 * 1024 * 1024);
        if (gb >= 100) {
            return base.stream().filter(w -> w >= 4).collect(Collectors.toList());
        }
        if (gb >= 10) {
            return base.stream().filter(w -> w >= 2).collect(Collectors.toList());
        }
        return base;
    }

    /**
     * Returns a reduced set of worker candidates: 1, 2, 3, 4, ... up to cap (linear +1, not multiply by 2).
     */
    public static List<Integer> getReducedWorkerCandidates(int maxWorkers) {
        return workersLinear(maxWorkers);
    }
}
