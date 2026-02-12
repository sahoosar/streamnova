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
        return getCapMbPerSec(sourceType, POSTGRES_CAP_MB_PER_SEC, ORACLE_CAP_MB_PER_SEC);
    }

    /**
     * Returns the source cap in MB/s for the given source type with configurable caps.
     */
    public static double getCapMbPerSec(String sourceType, double postgresCapMbPerSec, double oracleCapMbPerSec) {
        if (sourceType == null || sourceType.isBlank()) {
            return postgresCapMbPerSec;
        }
        String t = sourceType.trim().toLowerCase();
        if (t.contains("oracle")) {
            log.debug("[ESTIMATOR] Source cap: Oracle = {} MB/s", oracleCapMbPerSec);
            return oracleCapMbPerSec;
        }
        return postgresCapMbPerSec;
    }

    /**
     * Effective source throughput: min(measured throughput, source cap).
     */
    public static double effectiveSourceThroughputMbPerSec(double measuredMbPerSec, String sourceType) {
        return effectiveSourceThroughputMbPerSec(measuredMbPerSec, sourceType, POSTGRES_CAP_MB_PER_SEC, ORACLE_CAP_MB_PER_SEC);
    }

    /**
     * Effective source throughput with configurable caps: min(measured throughput, source cap).
     */
    public static double effectiveSourceThroughputMbPerSec(double measuredMbPerSec, String sourceType,
                                                           double postgresCapMbPerSec, double oracleCapMbPerSec) {
        double cap = getCapMbPerSec(sourceType, postgresCapMbPerSec, oracleCapMbPerSec);
        double effective = Math.min(measuredMbPerSec > 0 ? measuredMbPerSec : Double.MAX_VALUE, cap);
        if (effective < measuredMbPerSec && measuredMbPerSec > 0) {
            log.debug("[ESTIMATOR] Source cap applied: {} MB/s (cap {})", effective, cap);
        }
        return effective > 0 ? effective : cap;
    }
}
