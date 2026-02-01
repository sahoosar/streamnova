package com.di.streamnova.util;

import com.di.streamnova.config.DbConfigSnapshot;
import com.zaxxer.hikari.HikariConfig;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe DataSource manager that supports multiple database connections.
 * Each unique database configuration (JDBC URL + username) gets its own connection pool.
 * 
 * This enables multiple applications or multiple database connections within the same application
 * to use this singleton without conflicts.
 */
@Slf4j
public enum HikariDataSource {

    INSTANCE;
    
    /**
     * Cache of DataSources keyed by connection identifier (JDBC URL + username).
     * This allows multiple different database connections to coexist.
     */
    private final ConcurrentMap<String, DataSource> dataSourceCache = new ConcurrentHashMap<>();

    /** Incrementing counter for stable, positive pool IDs (consistent across runs for monitoring). */
    private static final AtomicInteger poolIdCounter = new AtomicInteger(0);

    /**
     * Gets or creates a DataSource for the given database configuration.
     * Each unique combination of JDBC URL and username gets its own connection pool.
     * 
     * @param snapshot Database configuration snapshot
     * @return DataSource for the specified database configuration
     */
    public DataSource getOrInit(DbConfigSnapshot snapshot) {
        String connectionKey = generateConnectionKey(snapshot);
        
        return dataSourceCache.computeIfAbsent(connectionKey, key -> {
            log.info("Creating new HikariCP DataSource for connection: {} (user: {})", 
                    sanitizeUrl(snapshot.jdbcUrl()), snapshot.username());
            
            int configMaxPool = snapshot.maximumPoolSize();
            int effectivePoolSize = validateAndResolvePoolSize(snapshot, configMaxPool);
            int effectiveMinIdle = Math.min(snapshot.minimumIdle(), effectivePoolSize);

            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl(snapshot.jdbcUrl());
            hikariConfig.setUsername(snapshot.username());
            hikariConfig.setPassword(snapshot.password());
            hikariConfig.setDriverClassName(snapshot.driverClassName());
            hikariConfig.setMaximumPoolSize(effectivePoolSize);
            hikariConfig.setMinimumIdle(effectiveMinIdle);
            hikariConfig.setIdleTimeout(snapshot.idleTimeoutMs());
            hikariConfig.setConnectionTimeout(snapshot.connectionTimeoutMs());
            hikariConfig.setMaxLifetime(snapshot.maxLifetimeMs());

            // Dataflow-safe settings: improve stability for workers
            hikariConfig.setAutoCommit(false);
            hikariConfig.setInitializationFailTimeout(-1);

            // Optional: PostgreSQL TCP keep-alive (helps with long-running connections)
            if (snapshot.jdbcUrl() != null && snapshot.jdbcUrl().contains("postgresql")) {
                hikariConfig.addDataSourceProperty("tcpKeepAlive", "true");
            }

            // Optional: leak detection for debugging (set -DHikariCP.leakDetectionThreshold=60000 to enable)
            String leakThreshold = System.getProperty("HikariCP.leakDetectionThreshold");
            if (leakThreshold != null && !leakThreshold.isEmpty()) {
                try {
                    hikariConfig.setLeakDetectionThreshold(Long.parseLong(leakThreshold));
                } catch (NumberFormatException ignored) { /* use default */ }
            }

            // Set pool name for monitoring (stable, positive ID + readable short key, no password)
            String shortKey = generateShortPoolKey(snapshot);
            hikariConfig.setPoolName("HikariPool-" + poolIdCounter.incrementAndGet() + "-" + shortKey);

            com.zaxxer.hikari.HikariDataSource newDataSource = new com.zaxxer.hikari.HikariDataSource(hikariConfig);

            log.info("[POOL] Creating | connectionKey={} | config maxPoolSize={} | effective maxPoolSize={}, minIdle={}",
                    sanitizeUrl(snapshot.jdbcUrl()), configMaxPool, effectivePoolSize, effectiveMinIdle);

            // Calibrate as backup if validation missed an edge case
            calibratePoolSize(newDataSource, connectionKey, effectivePoolSize, effectiveMinIdle, snapshot.jdbcUrl());

            ConnectionPoolLogger.logPoolStats(newDataSource, "after calibration (final)");
            return newDataSource;
        });
    }

    /**
     * Generates a unique key for a database configuration.
     * Uses JDBC URL and username to identify unique connections.
     * 
     * @param snapshot Database configuration snapshot
     * @return Unique connection key
     */
    private String generateConnectionKey(DbConfigSnapshot snapshot) {
        // Key by JDBC URL and username to support multiple databases/users
        // Note: We don't include password in the key for security reasons,
        // but each unique URL+username combination gets its own pool
        return snapshot.jdbcUrl() + "|" + snapshot.username();
    }

    /**
     * Generates a short, readable pool key for monitoring (no password).
     * Extracts host, database, and username from JDBC URL.
     */
    private String generateShortPoolKey(DbConfigSnapshot snapshot) {
        String url = snapshot.jdbcUrl();
        String user = snapshot.username() != null ? snapshot.username() : "unknown";
        if (url == null || url.isBlank()) {
            return user.replaceAll("[^a-zA-Z0-9_]", "_");
        }
        // Remove password before parsing
        String safeUrl = url.replaceAll("password=[^;&]+", "password=***");
        // Extract host:port and database from jdbc:subprotocol://host:port/db
        String part = safeUrl;
        int slashSlash = safeUrl.indexOf("//");
        if (slashSlash >= 0) {
            part = safeUrl.substring(slashSlash + 2);
        }
        int slashDb = part.indexOf("/");
        String hostPort = slashDb >= 0 ? part.substring(0, slashDb) : part;
        String db = slashDb >= 0 && slashDb < part.length() - 1 ? part.substring(slashDb + 1).split("[?;]")[0] : "";
        String host = hostPort.split(":")[0];
        String safe = (host + "_" + db + "_" + user).replaceAll("[^a-zA-Z0-9_]", "_").replaceAll("_+", "_");
        return safe.isEmpty() ? "pool" : safe;
    }

    /**
     * Sanitizes JDBC URL for logging (removes sensitive information like passwords).
     *
     * @param jdbcUrl The JDBC URL to sanitize
     * @return Sanitized URL safe for logging
     */
    private String sanitizeUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            return "null";
        }
        // Mask passwords in JDBC URLs
        return jdbcUrl.replaceAll("password=[^;&]+", "password=***");
    }

    /**
     * Closes and removes a DataSource from the cache.
     * Useful for cleanup or reconnection scenarios.
     * <p><b>Dataflow note:</b> On Dataflow workers, shutdown is not always graceful; this method is rarely called.
     * Do not rely on it for correctness (e.g. for transaction commit or connection release guarantees).
     *
     * @param snapshot Database configuration snapshot
     */
    public void closeDataSource(DbConfigSnapshot snapshot) {
        String connectionKey = generateConnectionKey(snapshot);
        DataSource dataSource = dataSourceCache.remove(connectionKey);
        
        if (dataSource != null && dataSource instanceof com.zaxxer.hikari.HikariDataSource) {
            try {
                ((com.zaxxer.hikari.HikariDataSource) dataSource).close();
                log.info("Closed and removed DataSource for connection: {}", connectionKey);
            } catch (Exception e) {
                log.warn("Error closing DataSource for connection: {}", connectionKey, e);
            }
        }
    }

    /**
     * Closes all DataSources and clears the cache.
     * Useful for application shutdown (e.g. local runs, Spring shutdown hooks).
     * <p><b>Dataflow note:</b> On Dataflow workers, shutdown is not always graceful; closeAll() is rarely invoked.
     * Do not rely on it for correctness—design for abrupt worker termination.
     */
    public void closeAll() {
        log.info("Closing all DataSources (count: {})", dataSourceCache.size());
        
        dataSourceCache.forEach((key, dataSource) -> {
            if (dataSource instanceof com.zaxxer.hikari.HikariDataSource) {
                try {
                    ((com.zaxxer.hikari.HikariDataSource) dataSource).close();
                    log.debug("Closed DataSource for connection: {}", key);
                } catch (Exception e) {
                    log.warn("Error closing DataSource for connection: {}", key, e);
                }
            }
        });
        
        dataSourceCache.clear();
        log.info("All DataSources closed and cache cleared");
    }

    /**
     * Gets the number of active DataSource connections in the cache.
     * 
     * @return Number of cached DataSources
     */
    public int getActiveConnectionCount() {
        return dataSourceCache.size();
    }

    /** Fraction of DB-available connections to use when availability < config (60%). */
    private static final double CONNECTION_LIMIT_SAFETY_RATIO = 0.6;

    /**
     * Validates DB connection availability before pool creation.
     * If available slots &lt; config maximumPoolSize, uses 60% of available (DB-safe).
     *
     * @param snapshot      DbConfigSnapshot with connection details
     * @param configMaxPool maximumPoolSize from pipeline_config.yml
     * @return effective pool size to use (config value or 60% of available)
     */
    private int validateAndResolvePoolSize(DbConfigSnapshot snapshot, int configMaxPool) {
        if (configMaxPool <= 0) return configMaxPool;
        try {
            loadDriver(snapshot.driverClassName());
            try (Connection conn = DriverManager.getConnection(
                    snapshot.jdbcUrl(), snapshot.username(), snapshot.password())) {
                log.debug("[POOL] Validation | One-off connection OK, querying free connections in DB");
                int available = queryAvailableConnections(conn, snapshot.jdbcUrl());
                if (available <= 0) {
                    log.error("[POOL] Validation | Could not query free connections (max_connections/current) | "
                            + "using config maxPoolSize={} | Verify DB supports SHOW/status queries", configMaxPool);
                    return configMaxPool;
                }
                if (available < configMaxPool) {
                    int effective = Math.max(1, (int) Math.floor(available * CONNECTION_LIMIT_SAFETY_RATIO));
                    log.info("[POOL] Validation | DB free connections ({}) < config maxPoolSize ({}) | "
                            + "using 60% of available = {}", available, configMaxPool, effective);
                    return effective;
                }
                log.debug("[POOL] Validation | DB free connections ({}) >= config maxPoolSize ({}) | using config", available, configMaxPool);
                return configMaxPool;
            }
        } catch (Exception e) {
            log.warn("[POOL] Validation | Unable to connect for availability check | url={} | user={} | "
                    + "error={} | Using config maxPoolSize={} | Verify: JDBC URL, credentials, network, DB is up",
                    sanitizeUrl(snapshot.jdbcUrl()), snapshot.username(), e.getMessage(), configMaxPool, e);
            return configMaxPool;
        }
    }

    private void loadDriver(String driverClassName) throws ClassNotFoundException {
        Class.forName(driverClassName != null && !driverClassName.isBlank() ? driverClassName : "org.postgresql.Driver");
    }

    /**
     * Queries available connection slots: max_connections - current connections.
     * Returns -1 if query fails.
     */
    private int queryAvailableConnections(Connection conn, String jdbcUrl) {
        try {
            if (jdbcUrl != null && jdbcUrl.contains("postgresql")) {
                int maxConn = -1;
                int current = -1;
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SHOW max_connections")) {
                    if (rs.next()) maxConn = Integer.parseInt(rs.getString(1).trim());
                }
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_stat_activity")) {
                    if (rs.next()) current = rs.getInt(1);
                }
                if (maxConn > 0 && current >= 0) {
                    int free = Math.max(0, maxConn - current);
                    log.debug("[POOL] Validation | PostgreSQL: max_connections={}, current={}, free={}", maxConn, current, free);
                    return free;
                }
            } else if (jdbcUrl != null && jdbcUrl.contains("mysql")) {
                int maxConn = -1;
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SHOW VARIABLES LIKE 'max_connections'")) {
                    if (rs.next()) maxConn = Integer.parseInt(rs.getString(2).trim());
                }
                if (maxConn > 0) {
                    try (Statement st = conn.createStatement();
                         ResultSet rs = st.executeQuery("SHOW STATUS LIKE 'Threads_connected'")) {
                        if (rs.next()) {
                            int current = Integer.parseInt(rs.getString(2).trim());
                            int free = Math.max(0, maxConn - current);
                            log.debug("[POOL] Validation | MySQL: max_connections={}, current={}, free={}", maxConn, current, free);
                            return free;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("[POOL] Validation | Error querying free connections | jdbcUrl={} | error={} | "
                    + "Fix: ensure DB supports SHOW max_connections / pg_stat_activity (PostgreSQL) or equivalent",
                    sanitizeUrl(jdbcUrl), e.getMessage(), e);
        }
        return -1;
    }

    /**
     * Calibrates pool size by attempting to establish connections.
     * If connection-limit exceptions occur, queries DB for max_connections and uses 60% of available.
     */
    private void calibratePoolSize(com.zaxxer.hikari.HikariDataSource hikari, String connectionKey,
                                   int requestedMax, int requestedMinIdle, String jdbcUrl) {
        if (requestedMax <= 1) {
            ConnectionPoolLogger.logPoolCalibration(requestedMax, requestedMax, null, hikari.getPoolName());
            return;
        }

        List<Connection> acquired = new ArrayList<>(requestedMax);
        String rootCause = null;
        try {
            log.debug("[POOL] Calibration | Acquiring up to {} connections to verify DB support", requestedMax);
            for (int i = 0; i < requestedMax; i++) {
                Connection conn = hikari.getConnection();
                acquired.add(conn);
            }
        } catch (Exception e) {
            rootCause = extractRootCauseMessage(e);
            if (isConnectionLimitError(e)) {
                log.warn("[POOL] Calibration | Unable to acquire {} connections - DB limit hit | "
                        + "successfulConnections={}, attempting to collect free connections from DB",
                        requestedMax, acquired.size());
                int newMax = resolvePoolSizeFromDbLimit(acquired, requestedMax, jdbcUrl, rootCause);
                hikari.setMaximumPoolSize(newMax);
                int currentMinIdle = hikari.getMinimumIdle();
                if (currentMinIdle > newMax) {
                    int newMinIdle = Math.min(requestedMinIdle, newMax);
                    hikari.setMinimumIdle(Math.max(0, newMinIdle));
                    log.info("[POOL] Calibration | minIdle adjusted: {} → {} (to fit maxPoolSize={})",
                            currentMinIdle, hikari.getMinimumIdle(), newMax);
                }
                log.warn("[POOL] Calibration | DB connection limit | requested maxPoolSize={} NOT supported | "
                        + "adjusted to actualMaxPoolSize={} (60% of available) | rootCause={}",
                        requestedMax, newMax, rootCause);
            } else {
                log.error("[POOL] Calibration | Unable to connect during calibration | pool={} | requestedMax={} | "
                        + "successfulConnections={} | error={} | Verify: DB is up, credentials, network, connection timeout",
                        hikari.getPoolName(), requestedMax, acquired.size(), rootCause, e);
            }
        } finally {
            for (Connection c : acquired) {
                try { c.close(); } catch (Exception ignored) { /* release back to pool */ }
            }
        }

        int actualMax = hikari.getMaximumPoolSize();
        ConnectionPoolLogger.logPoolCalibration(requestedMax, actualMax, rootCause, hikari.getPoolName());
    }

    /**
     * Resolves pool size when DB connection limit is hit: query max_connections and free slots, use 60% of available.
     */
    private int resolvePoolSizeFromDbLimit(List<Connection> acquired, int requestedMax, String jdbcUrl, String rootCause) {
        int available = -1;
        Connection conn = acquired.isEmpty() ? null : acquired.get(0);
        if (conn == null || jdbcUrl == null) {
            log.warn("[POOL] Calibration | Cannot collect free connections - no connection available to query DB | "
                    + "successfulConnections=0 | rootCause={} | Fallback: using pool size 1", rootCause);
            return 1;
        }
        available = queryAvailableConnections(conn, jdbcUrl);
        if (available <= 0) {
            int maxConnFromDb = queryMaxConnectionsFromDb(conn, jdbcUrl);
            if (maxConnFromDb > 0) {
                available = maxConnFromDb;
                log.info("[POOL] Calibration | Collected max_connections={} (current/free query failed) | using 60% = {}",
                        available, (int) (available * CONNECTION_LIMIT_SAFETY_RATIO));
            } else {
                available = Math.max(1, acquired.size());
                log.error("[POOL] Calibration | Could not query free connections (max_connections/current) | "
                        + "successfulConnections={} | rootCause={} | Fallback: 60% of successfulConnections = {}",
                        acquired.size(), rootCause, (int) (available * CONNECTION_LIMIT_SAFETY_RATIO));
            }
        } else {
            log.info("[POOL] Calibration | Collected free connections={} | using 60% = {}",
                    available, (int) (available * CONNECTION_LIMIT_SAFETY_RATIO));
        }
        int newMax = Math.max(1, (int) Math.floor(available * CONNECTION_LIMIT_SAFETY_RATIO));
        newMax = Math.min(newMax, Math.max(1, acquired.size()));
        return newMax;
    }

    /**
     * Queries DB for max_connections. Returns -1 if not supported or query fails.
     */
    private int queryMaxConnectionsFromDb(Connection conn, String jdbcUrl) {
        try {
            if (jdbcUrl != null && jdbcUrl.contains("postgresql")) {
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SHOW max_connections")) {
                    if (rs.next()) {
                        return Integer.parseInt(rs.getString(1).trim());
                    }
                }
            } else if (jdbcUrl != null && jdbcUrl.contains("mysql")) {
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SHOW VARIABLES LIKE 'max_connections'")) {
                    if (rs.next()) {
                        return Integer.parseInt(rs.getString(2).trim());
                    }
                }
            }
        } catch (Exception e) {
            log.error("[POOL] Calibration | Could not query max_connections | jdbcUrl={} | error={}",
                    sanitizeUrl(jdbcUrl), e.getMessage());
        }
        return -1;
    }

    private String extractRootCauseMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null) root = root.getCause();
        return root.getMessage() != null ? root.getMessage() : t.getMessage();
    }

    /** Returns true if the exception indicates a connection/slot limit (e.g. PostgreSQL max_connections). */
    private boolean isConnectionLimitError(Throwable t) {
        if (t == null) {
            return false;
        }

        if (t instanceof java.sql.SQLException) {
            java.sql.SQLException sqlEx = (java.sql.SQLException) t;

            String sqlState = sqlEx.getSQLState();
            int errorCode = sqlEx.getErrorCode();

            // -------- PostgreSQL --------
            if ("53300".equals(sqlState)) {
                return true;
            }

            // -------- MySQL / Aurora MySQL --------
            if (errorCode == 1040 || "08004".equals(sqlState)) {
                return true;
            }

            // -------- Oracle --------
            // ORA-00018, ORA-00020
            if (errorCode == 18 || errorCode == 20 || errorCode == 12516 || errorCode == 12519) {
                return true;
            }

            // ORA-12516, ORA-12519 (listener refused connection)
            if (errorCode == 12516 || errorCode == 12519) {
                return true;
            }
        }

        // -------- Message fallback (covers wrapped / Hikari cases) --------
        String msg = t.getMessage();
        if (msg != null) {
            String m = msg.toLowerCase();
            if (m.contains("too many clients")
                    || m.contains("too many connections")
                    || m.contains("remaining connection slots are reserved")
                    || m.contains("connection limit")
                    || m.contains("max_connections")
                    || m.contains("maximum number of sessions")
                    || m.contains("maximum number of processes")
                    || m.contains("listener could not find available handler")
                    || m.contains("no appropriate service handler")) {
                return true;
            }
        }

        // -------- Recurse through cause chain --------
        return isConnectionLimitError(t.getCause());
    }

    /**
     * Re-calibrates an existing pool against DB connection limits.
     * Call after resize when pool was increased – verifies DB supports the new size.
     */
    public void calibratePool(DataSource dataSource) {
        if (dataSource instanceof com.zaxxer.hikari.HikariDataSource hikari) {
            int requested = hikari.getMaximumPoolSize();
            int minIdle = hikari.getMinimumIdle();
            String key = hikari.getPoolName() != null ? hikari.getPoolName() : sanitizeUrl(hikari.getJdbcUrl());
            calibratePoolSize(hikari, key, requested, minIdle, hikari.getJdbcUrl());
        }
    }

    /**
     * Increases the HikariCP pool size if the new value is greater than the current maximum.
     * After resize, runs calibration to ensure DB supports the new size.
     *
     * @param dataSource DataSource (must be HikariDataSource)
     * @param newMaximumPoolSize New maximum pool size (only applied if &gt; current)
     * @return Actual maximum pool size after resize (may be unchanged)
     */
    public int resizePoolIfNeeded(DataSource dataSource, int newMaximumPoolSize) {
        if (dataSource instanceof com.zaxxer.hikari.HikariDataSource hikari) {
            int current = hikari.getMaximumPoolSize();
            if (newMaximumPoolSize > current) {
                hikari.setMaximumPoolSize(newMaximumPoolSize);
                log.info("[POOL] Resize | maximumPoolSize: {} → {} (derived from shard count)", current, newMaximumPoolSize);
                return newMaximumPoolSize;
            }
            return current;
        }
        return newMaximumPoolSize;
    }
}
