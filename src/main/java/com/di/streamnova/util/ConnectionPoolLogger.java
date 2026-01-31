package com.di.streamnova.util;

import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;

/**
 * Logs HikariCP connection pool statistics for verification and debugging.
 * <p>Use at:
 * <ul>
 *   <li>Startup - pool creation (HikariDataSource)</li>
 *   <li>After setup phases - statistics, schema detection (PostgresStatisticsEstimator, PostgresSchemaDetector)</li>
 *   <li>After shard planning - startup summary (ShardPlanner)</li>
 *   <li>Per-worker init - call {@link #logPoolStats(DataSource, String)} with phase="ReadShardDoFn @Setup" in DoFn @Setup</li>
 *   <li>Per-shard - optionally call with phase="shard N" at DEBUG level for detailed verification</li>
 * </ul>
 */
@Slf4j
public final class ConnectionPoolLogger {

    private ConnectionPoolLogger() {}

    /**
     * Logs pool statistics if the DataSource is a HikariCP pool.
     *
     * @param dataSource the DataSource (must be com.zaxxer.hikari.HikariDataSource for stats)
     * @param phase      description of when this is being logged (e.g. "startup", "after statistics", "per-shard")
     */
    public static void logPoolStats(DataSource dataSource, String phase) {
        if (dataSource == null) {
            return;
        }
        if (!(dataSource instanceof com.zaxxer.hikari.HikariDataSource hikari)) {
            log.debug("Pool stats not available (not HikariCP): phase={}", phase);
            return;
        }
        try {
            String poolName = hikari.getPoolName();
            int active = hikari.getHikariPoolMXBean().getActiveConnections();
            int idle = hikari.getHikariPoolMXBean().getIdleConnections();
            int total = hikari.getHikariPoolMXBean().getTotalConnections();
            int waiting = hikari.getHikariPoolMXBean().getThreadsAwaitingConnection();

            log.info("[POOL] {} | pool={} | active={}, idle={}, total={}, waiting={} | "
                    + "Usage: 1 conn/shard when processing",
                    phase, poolName, active, idle, total, waiting);
        } catch (Exception e) {
            log.debug("Could not read pool stats for phase {}: {}", phase, e.getMessage());
        }
    }

    /**
     * Logs startup summary: pool config and expected connection usage.
     *
     * @param maxPoolSize   maximum pool size
     * @param minIdle       minimum idle connections
     * @param shardCount    number of shards (peak concurrent connections ≤ min(shardCount, maxPoolSize))
     * @param workerCount   number of workers (for context; connections are per-shard, not per-worker)
     */
    public static void logStartupSummary(int maxPoolSize, int minIdle, int shardCount, int workerCount) {
        int safeCap = (int) Math.floor(maxPoolSize * 0.8);
        int peakConnections = Math.min(shardCount, maxPoolSize);
        log.info("[POOL] Startup summary | maxPoolSize={}, minIdle={}, shardCount={}, workers={} | "
                + "Connections at startup: 0 (lazy); peak during read: ≤{} (1 per shard) | "
                + "Shards capped at {} by pool",
                maxPoolSize, minIdle, shardCount, workerCount, peakConnections, safeCap);
    }
}
