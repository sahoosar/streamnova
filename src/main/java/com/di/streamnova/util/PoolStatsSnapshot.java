package com.di.streamnova.util;

/**
 * Snapshot of HikariCP pool statistics for a single connection pool.
 * Used for logging and REST exposure (e.g. /api/pipeline/pool-statistics).
 */
public record PoolStatsSnapshot(
    String poolName,
    int maxPoolSize,
    int minIdle,
    int activeConnections,
    int idleConnections,
    int totalConnections,
    int threadsAwaitingConnection
) {}
