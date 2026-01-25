package com.di.streamnova.handler.jdbc.postgres;

import com.di.streamnova.config.PipelineConfigSource;
import com.di.streamnova.util.DateFormatUtils;
import com.di.streamnova.util.InputValidator;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

/**
 * Formats partition values according to PostgreSQL column data types.
 * Handles date, timestamp, numeric, boolean, and string types appropriately.
 */
@Slf4j
public final class PostgresPartitionValueFormatter {
    
    private PostgresPartitionValueFormatter() {}
    
    /**
     * Gets the PostgreSQL data type of a column.
     * 
     * @param dataSource DataSource for database connection
     * @param config Pipeline configuration
     * @param columnName Column name to check
     * @return PostgreSQL data type (e.g., "date", "integer", "varchar"), or null if not found
     */
    public static String getColumnDataType(DataSource dataSource, PipelineConfigSource config, String columnName) {
        try (Connection conn = dataSource.getConnection()) {
            String tableName = config.getTable();
            if (tableName == null || tableName.isBlank()) {
                return null;
            }
            
            // Parse schema and table name
            String[] parts = tableName.split("\\.");
            String schemaName = parts.length > 1 ? parts[0] : "public";
            String tableNameOnly = parts.length > 1 ? parts[1] : parts[0];
            
            String sql = "SELECT data_type " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = ? " +
                        "  AND table_name = ? " +
                        "  AND column_name = ?";
            
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, schemaName);
                ps.setString(2, tableNameOnly);
                ps.setString(3, columnName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString("data_type");
                    }
                }
            }
        } catch (SQLException e) {
            log.warn("Failed to get data type for column '{}': {}", columnName, e.getMessage());
        }
        return null;
    }
    
    /**
     * Formats a partition value according to the column's data type.
     * Handles date, timestamp, numeric, and string types appropriately.
     * 
     * @param columnName Column name (for logging)
     * @param value Raw partition value from config
     * @param dataType PostgreSQL data type (e.g., "date", "integer", "varchar", "timestamp")
     * @return Formatted SQL value expression (without column name), or null if formatting fails
     */
    public static String formatPartitionValueByType(String columnName, String value, String dataType) {
        if (dataType == null || dataType.isBlank() || value == null || value.isBlank()) {
            return null;
        }
        
        String dataTypeLower = dataType.toLowerCase();
        
        // Date types
        if (dataTypeLower.equals("date")) {
            try {
                Date date = DateFormatUtils.convertToDate(value);
                if (date != null) {
                    // Format as DATE 'YYYY-MM-DD'
                    java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
                    String formattedDate = sdf.format(date);
                    log.debug("Formatted date partition value: '{}' -> DATE '{}'", value, formattedDate);
                    return "DATE '" + formattedDate + "'";
                }
            } catch (Exception e) {
                log.warn("Failed to parse date value '{}' for column '{}': {}", value, columnName, e.getMessage());
            }
        }
        
        // Timestamp types
        if (dataTypeLower.contains("timestamp")) {
            try {
                Date date = DateFormatUtils.convertToDate(value);
                if (date != null) {
                    // Format as TIMESTAMP 'YYYY-MM-DD HH:mm:ss'
                    java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String formattedTimestamp = sdf.format(date);
                    log.debug("Formatted timestamp partition value: '{}' -> TIMESTAMP '{}'", value, formattedTimestamp);
                    return "TIMESTAMP '" + formattedTimestamp + "'";
                }
            } catch (Exception e) {
                log.warn("Failed to parse timestamp value '{}' for column '{}': {}", value, columnName, e.getMessage());
            }
        }
        
        // Numeric types (integer, bigint, numeric, decimal, smallint, etc.)
        if (dataTypeLower.contains("int") || dataTypeLower.contains("numeric") || 
            dataTypeLower.contains("decimal") || dataTypeLower.contains("real") || 
            dataTypeLower.contains("double") || dataTypeLower.contains("float")) {
            try {
                // Try to parse as number (remove quotes if present)
                String numericValue = value.trim().replaceAll("^['\"]|['\"]$", "");
                Double.parseDouble(numericValue); // Validate it's a number
                log.debug("Formatted numeric partition value: '{}' -> {}", value, numericValue);
                return numericValue; // No quotes for numeric types
            } catch (NumberFormatException e) {
                log.warn("Failed to parse numeric value '{}' for column '{}': {}", value, columnName, e.getMessage());
            }
        }
        
        // Boolean types
        if (dataTypeLower.equals("boolean") || dataTypeLower.equals("bool")) {
            String boolValue = value.trim().toLowerCase();
            if (boolValue.equals("true") || boolValue.equals("1") || boolValue.equals("yes") || boolValue.equals("t")) {
                log.debug("Formatted boolean partition value: '{}' -> true", value);
                return "true";
            } else if (boolValue.equals("false") || boolValue.equals("0") || boolValue.equals("no") || boolValue.equals("f")) {
                log.debug("Formatted boolean partition value: '{}' -> false", value);
                return "false";
            } else {
                log.warn("Invalid boolean value '{}' for column '{}', treating as string", value, columnName);
            }
        }
        
        // String types (varchar, text, char, etc.) - default fallback
        // Escape single quotes and wrap in quotes
        String escapedValue = value.replace("'", "''");
        log.debug("Formatted string partition value: '{}' -> '{}'", value, escapedValue);
        return "'" + escapedValue + "'";
    }
    
    /**
     * Checks if a data type is numeric or timestamp (optimal for ordering/sharding).
     * 
     * @param dataType PostgreSQL data type
     * @return true if the type is numeric or timestamp
     */
    public static boolean isNumericOrTimestampType(String dataType) {
        if (dataType == null || dataType.isBlank()) {
            return false;
        }
        
        String dataTypeLower = dataType.toLowerCase();
        return dataTypeLower.contains("int") || 
               dataTypeLower.contains("numeric") || 
               dataTypeLower.contains("decimal") || 
               dataTypeLower.contains("real") || 
               dataTypeLower.contains("double") || 
               dataTypeLower.contains("float") || 
               dataTypeLower.contains("timestamp") || 
               dataTypeLower.equals("date");
    }
}
