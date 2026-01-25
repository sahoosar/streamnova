package com.di.streamnova.handler.jdbc.postgres;

import com.di.streamnova.config.PipelineConfigSource;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Estimates table statistics for PostgreSQL tables.
 * Provides row count and average row size estimates for shard planning.
 */
@Slf4j
public final class PostgresStatisticsEstimator {
    
    private PostgresStatisticsEstimator() {}
    
    /**
     * Statistics record containing row count and average row size.
     */
    public record TableStatistics(long rowCount, int avgRowSizeBytes) {}
    
    /**
     * Estimates table statistics (row count and average row size).
     * 
     * @param dataSource DataSource for database connection
     * @param config Pipeline configuration
     * @return TableStatistics with row count and average row size
     */
    public static TableStatistics estimateStatistics(DataSource dataSource, PipelineConfigSource config) {
        try (Connection conn = dataSource.getConnection()) {
            String tableName = config.getTable();
            if (tableName == null || tableName.isBlank()) {
                throw new IllegalArgumentException("Table name is required for statistics estimation");
            }
            
            // Parse schema and table name
            String[] parts = tableName.split("\\.");
            String schemaName = parts.length > 1 ? parts[0] : "public";
            String tableNameOnly = parts.length > 1 ? parts[1] : parts[0];
            
            long rowCount = estimateRowCount(conn, schemaName, tableNameOnly);
            int avgRowSize = estimateAverageRowSize(conn, schemaName, tableNameOnly);
            
            log.info("Table statistics estimated: table='{}.{}', rowCount={}, avgRowSizeBytes={}", 
                    schemaName, tableNameOnly, rowCount, avgRowSize);
            
            return new TableStatistics(rowCount, avgRowSize);
            
        } catch (SQLException e) {
            log.error("Failed to estimate table statistics", e);
            throw new RuntimeException("Failed to estimate table statistics: " + e.getMessage(), e);
        }
    }
    
    /**
     * Estimates row count using PostgreSQL statistics.
     * Falls back to COUNT(*) if statistics are not available.
     */
    private static long estimateRowCount(Connection conn, String schemaName, String tableName) throws SQLException {
        // Try to get row count from pg_class statistics (fast)
        String statsQuery = "SELECT reltuples::bigint AS row_count " +
                           "FROM pg_class c " +
                           "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                           "WHERE n.nspname = ? AND c.relname = ?";
        
        try (PreparedStatement ps = conn.prepareStatement(statsQuery)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long estimatedCount = rs.getLong("row_count");
                    if (estimatedCount > 0) {
                        log.debug("Row count from pg_class statistics: {}", estimatedCount);
                        return estimatedCount;
                    }
                }
            }
        }
        
        // Fallback to COUNT(*) if statistics not available
        log.debug("pg_class statistics not available, using COUNT(*) for row count estimation");
        String countQuery = "SELECT COUNT(*) AS row_count FROM \"" + schemaName + "\".\"" + tableName + "\"";
        try (PreparedStatement ps = conn.prepareStatement(countQuery);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                long count = rs.getLong("row_count");
                log.debug("Row count from COUNT(*): {}", count);
                return count;
            }
        }
        
        log.warn("Could not estimate row count, defaulting to 0");
        return 0L;
    }
    
    /**
     * Estimates average row size using PostgreSQL statistics.
     * Falls back to sampling if statistics are not available.
     */
    private static int estimateAverageRowSize(Connection conn, String schemaName, String tableName) throws SQLException {
        // Try to get average row size from pg_class statistics
        String statsQuery = "SELECT CASE WHEN reltuples > 0 THEN (relpages::bigint * 8192) / reltuples ELSE 0 END AS avg_row_size " +
                           "FROM pg_class c " +
                           "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                           "WHERE n.nspname = ? AND c.relname = ?";
        
        try (PreparedStatement ps = conn.prepareStatement(statsQuery)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int avgSize = rs.getInt("avg_row_size");
                    if (avgSize > 0) {
                        log.debug("Average row size from pg_class statistics: {} bytes", avgSize);
                        return avgSize;
                    }
                }
            }
        }
        
        // Fallback: sample first 1000 rows to estimate average size
        log.debug("pg_class statistics not available, sampling rows for average size estimation");
        String sampleQuery = "SELECT pg_column_size(t.*) AS row_size " +
                            "FROM \"" + schemaName + "\".\"" + tableName + "\" t " +
                            "LIMIT 1000";
        
        try (PreparedStatement ps = conn.prepareStatement(sampleQuery);
             ResultSet rs = ps.executeQuery()) {
            long totalSize = 0;
            int rowCount = 0;
            while (rs.next()) {
                totalSize += rs.getInt("row_size");
                rowCount++;
            }
            
            if (rowCount > 0) {
                int avgSize = (int) (totalSize / rowCount);
                log.debug("Average row size from sampling ({} rows): {} bytes", rowCount, avgSize);
                return Math.max(avgSize, 200); // Minimum 200 bytes
            }
        }
        
        log.warn("Could not estimate average row size, defaulting to 200 bytes");
        return 200; // Default average row size
    }
}
