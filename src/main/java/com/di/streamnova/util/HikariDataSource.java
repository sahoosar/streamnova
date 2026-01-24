package com.di.streamnova.util;

import com.di.streamnova.config.DbConfigSnapshot;
import com.zaxxer.hikari.HikariConfig;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
            
            // Set pool name for monitoring/debugging
            hikariConfig.setPoolName("HikariPool-" + connectionKey.hashCode());
            
            DataSource newDataSource = new com.zaxxer.hikari.HikariDataSource(hikariConfig);
            log.info("Successfully Created HikariCP DataSource : {}", connectionKey);
            
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
     * Useful for application shutdown.
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
