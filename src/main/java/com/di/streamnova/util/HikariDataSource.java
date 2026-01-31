package com.di.streamnova.util;

import com.di.streamnova.config.DbConfigSnapshot;
import com.zaxxer.hikari.HikariConfig;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
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
            
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl(snapshot.jdbcUrl());
            hikariConfig.setUsername(snapshot.username());
            hikariConfig.setPassword(snapshot.password());
            hikariConfig.setDriverClassName(snapshot.driverClassName());
            hikariConfig.setMaximumPoolSize(snapshot.maximumPoolSize());
            hikariConfig.setMinimumIdle(snapshot.minimumIdle());
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

            DataSource newDataSource = new com.zaxxer.hikari.HikariDataSource(hikariConfig);
            log.info("[POOL] Startup | Created HikariCP pool: {} | maxSize={}, minIdle={} | "
                    + "Connections at creation: 0 (lazy - created on first use)",
                    connectionKey, snapshot.maximumPoolSize(), snapshot.minimumIdle());
            ConnectionPoolLogger.logPoolStats(newDataSource, "after pool creation");

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
}
