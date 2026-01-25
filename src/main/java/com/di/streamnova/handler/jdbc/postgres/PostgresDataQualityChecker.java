package com.di.streamnova.handler.jdbc.postgres;

import com.di.streamnova.util.InputValidator;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Checks data quality for PostgreSQL tables.
 * Validates non-null columns in actual data to aid in sharding decisions.
 */
@Slf4j
public final class PostgresDataQualityChecker {
    
    private PostgresDataQualityChecker() {}
    
    /**
     * Checks which columns among the provided list have no null values in actual data.
     * This helps identify the best columns for sharding even if they're nullable in schema.
     * 
     * Uses efficient COUNT(*) query to check for null values without scanning all rows.
     * 
     * @param conn Database connection
     * @param schemaName Schema name (already validated)
     * @param tableNameOnly Table name without schema (already validated)
     * @param columnNames List of column names to check (typically first 3 columns)
     * @return List of column names that have no null values in the data
     */
    public static List<String> checkNonNullColumnsInData(Connection conn, String schemaName, 
                                                          String tableNameOnly, List<String> columnNames) {
        if (columnNames == null || columnNames.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<String> nonNullColumns = new ArrayList<>();
        
        // Build properly quoted table identifier
        String quotedTableName;
        if (schemaName.equals("public")) {
            quotedTableName = "\"" + tableNameOnly + "\"";
        } else {
            quotedTableName = "\"" + schemaName + "\".\"" + tableNameOnly + "\"";
        }
        
        for (String columnName : columnNames) {
            try {
                // Validate column name to prevent SQL injection
                InputValidator.validateColumnName(columnName);
                
                // Query to check if column has any null values
                // Efficient: Uses COUNT(*) with WHERE column IS NULL
                // If count is 0, the column has no null values
                String quotedColumnName = "\"" + columnName + "\"";
                String sql = "SELECT COUNT(*) as null_count " +
                            "FROM " + quotedTableName + " " +
                            "WHERE " + quotedColumnName + " IS NULL";
                
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            int nullCount = rs.getInt("null_count");
                            if (nullCount == 0) {
                                nonNullColumns.add(columnName);
                                log.debug("Column '{}' in table '{}' has no null values in data (verified: {} nulls found)", 
                                        columnName, quotedTableName, nullCount);
                            } else {
                                log.debug("Column '{}' in table '{}' has {} null value(s) in data", 
                                        columnName, quotedTableName, nullCount);
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                log.debug("Failed to check null values for column '{}' in table '{}': {}. " +
                        "Skipping data verification for this column.", 
                        columnName, quotedTableName, e.getMessage());
                // Continue checking other columns even if one fails
            } catch (IllegalArgumentException e) {
                log.debug("Invalid column name '{}' for null check: {}. Skipping.", 
                        columnName, e.getMessage());
                // Continue checking other columns
            }
        }
        
        return nonNullColumns;
    }
}
