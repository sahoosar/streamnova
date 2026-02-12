package com.di.streamnova.agent.estimator;

import lombok.extern.slf4j.Slf4j;

/**
 * Sink-side throughput cap: BigQuery direct write vs load-from-GCS have different limits.
 * Used by Estimator to bound duration by sink capacity.
 */
@Slf4j
public final class SinkCap {

    /** BQ direct write / streaming insert: effective throughput cap (MB/s) per job. */
    private static final double BQ_DIRECT_CAP_MB_PER_SEC = 100.0;
    /** BQ load job from GCS: typically higher throughput (MB/s). */
    private static final double BQ_LOAD_FROM_GCS_CAP_MB_PER_SEC = 400.0;

    private SinkCap() {}

    /**
     * Returns the sink throughput cap in MB/s for the given load pattern.
     */
    public static double getCapMbPerSec(LoadPattern loadPattern) {
        return getCapMbPerSec(loadPattern, BQ_DIRECT_CAP_MB_PER_SEC, BQ_LOAD_FROM_GCS_CAP_MB_PER_SEC);
    }

    /**
     * Returns the sink throughput cap in MB/s for the given load pattern with configurable caps.
     */
    public static double getCapMbPerSec(LoadPattern loadPattern, double bqDirectCapMbPerSec, double gcsBqCapMbPerSec) {
        if (loadPattern == null) {
            return bqDirectCapMbPerSec;
        }
        switch (loadPattern) {
            case GCS_BQ:
                log.debug("[ESTIMATOR] Sink cap: GCS+BQ = {} MB/s", gcsBqCapMbPerSec);
                return gcsBqCapMbPerSec;
            case DIRECT:
            default:
                log.debug("[ESTIMATOR] Sink cap: BQ direct = {} MB/s", bqDirectCapMbPerSec);
                return bqDirectCapMbPerSec;
        }
    }
}
