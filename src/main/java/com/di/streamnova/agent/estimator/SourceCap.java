package com.di.streamnova.agent.estimator;

import lombok.extern.slf4j.Slf4j;

/**
 * Source-side throughput cap (e.g. Oracle read limit). Limits effective read MB/s
 * regardless of workers; used by Estimator to bound duration.
 */
@Slf4j
public final class SourceCap {

    /** Postgres: no hard cap; use measured throughput or default. */
    private static final double POSTGRES_CAP_MB_PER_SEC = 500.0;
    /** Oracle: typical lower single-connection/source limit (MB/s). */
    private static final double ORACLE_CAP_MB_PER_SEC = 80.0;

    private SourceCap() {}

    /**
     * Returns the source cap in MB/s for the given source type.
     * Oracle has a lower cap than Postgres by default.
     */
    public static double getCapMbPerSec(String sourceType) {
        if (sourceType == null || sourceType.isBlank()) {
            return POSTGRES_CAP_MB_PER_SEC;
        }
        String t = sourceType.trim().toLowerCase();
        if (t.contains("oracle")) {
            log.debug("[ESTIMATOR] Source cap: Oracle = {} MB/s", ORACLE_CAP_MB_PER_SEC);
            return ORACLE_CAP_MB_PER_SEC;
        }
        return POSTGRES_CAP_MB_PER_SEC;
    }

    /**
     * Effective source throughput: min(measured throughput, source cap).
     */
    public static double effectiveSourceThroughputMbPerSec(double measuredMbPerSec, String sourceType) {
        double cap = getCapMbPerSec(sourceType);
        double effective = Math.min(measuredMbPerSec > 0 ? measuredMbPerSec : Double.MAX_VALUE, cap);
        if (effective < measuredMbPerSec && measuredMbPerSec > 0) {
            log.debug("[ESTIMATOR] Source cap applied: {} MB/s (cap {})", effective, cap);
        }
        return effective > 0 ? effective : cap;
    }
}
