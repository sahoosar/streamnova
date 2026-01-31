package com.di.streamnova.handler.jdbc.postgres;

import com.di.streamnova.config.PipelineConfigSource;
import com.di.streamnova.handler.jdbc.postgres.PostgresDataQualityChecker;
import com.di.streamnova.util.ConnectionPoolLogger;
import com.di.streamnova.util.InputValidator;
import com.di.streamnova.util.MetricsCollector;
import com.di.streamnova.util.TypeConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.schemas.Schema;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Detects and validates PostgreSQL table schemas.
 * Includes comprehensive validation for schema quality and sharding requirements.
 */
@Slf4j
public final class PostgresSchemaDetector {
    
    private static final int MIN_NON_NULL_REQUIRED = 2;
    private static final int MAX_NON_NULL_EXPECTED = 3;
    
    private PostgresSchemaDetector() {}
    
    /**
     * Detects schema for a PostgreSQL table with robust validation.
     * 
     * @param dataSource DataSource for database connection
     * @param config Pipeline configuration
     * @param metricsCollector Metrics collector for recording schema detection time
     * @return Detected Apache Beam Schema
     */
    public static Schema detectSchema(DataSource dataSource, PipelineConfigSource config, MetricsCollector metricsCollector) {
        long schemaStartTime = System.currentTimeMillis();
        
        try (Connection conn = dataSource.getConnection()) {
            String tableName = config.getTable();
            if (tableName == null || tableName.isBlank()) {
                throw new IllegalArgumentException("Table name is required");
            }
            
            // Validate table name format
            InputValidator.validateTableName(tableName);
            
            // Parse schema and table name
            String[] parts = tableName.split("\\.");
            String schemaName = parts.length > 1 ? parts[0] : "public";
            String tableNameOnly = parts.length > 1 ? parts[1] : parts[0];
            
            // Validate schema name if provided
            if (parts.length > 1) {
                InputValidator.validateSchemaName(schemaName);
            }
            InputValidator.validateIdentifier(tableNameOnly, "table name");
            
            // Verify table exists before querying columns
            if (!verifyTableExists(conn, schemaName, tableNameOnly)) {
                throw new IllegalArgumentException(
                    String.format("Table '%s' does not exist in schema '%s'", tableNameOnly, schemaName));
            }
            
            List<Schema.Field> fields = new ArrayList<>();
            List<String> fieldNames = new ArrayList<>();
            
            // Query to get column information
            String sql = "SELECT column_name, data_type, character_maximum_length, is_nullable, ordinal_position " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = ? AND table_name = ? " +
                        "ORDER BY ordinal_position";
            
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, schemaName);
                ps.setString(2, tableNameOnly);
                try (ResultSet rs = ps.executeQuery()) {
                    int maxColumns = config.getMaxColumns() > 0 ? config.getMaxColumns() : Integer.MAX_VALUE;
                    int columnCount = 0;
                    
                    while (rs.next() && columnCount < maxColumns) {
                        String columnName = rs.getString("column_name");
                        String dataType = rs.getString("data_type");
                        String isNullable = rs.getString("is_nullable");
                        
                        // Validate column name for security
                        InputValidator.validateColumnName(columnName);
                        
                        // Check for duplicate field names
                        if (fieldNames.contains(columnName)) {
                            log.warn("Duplicate column name '{}' detected in table '{}'. Skipping duplicate.", 
                                    columnName, tableName);
                            continue;
                        }
                        fieldNames.add(columnName);
                        
                        // Map PostgreSQL type to Beam type
                        Schema.FieldType fieldType = TypeConverter.mapPostgresTypeToBeamType(dataType);
                        
                        // Validate that the type is supported
                        if (fieldType == null) {
                            log.warn("Unsupported data type '{}' for column '{}' in table '{}'. Mapping to STRING.", 
                                    dataType, columnName, tableName);
                            fieldType = Schema.FieldType.STRING;
                        }
                        
                        // Use nullable information from database
                        boolean nullable = "YES".equalsIgnoreCase(isNullable);
                        fields.add(Schema.Field.of(columnName, fieldType).withNullable(nullable));
                        
                        // Best Practice Warning: Nullable columns in first positions
                        if (nullable && columnCount < 3) {
                            log.warn("Column '{}' (position {}) in table '{}' is nullable. " +
                                    "⚠️ Best Practice: First columns should ideally be non-null for optimal sharding/partitioning performance.",
                                    columnName, columnCount + 1, tableName);
                        }
                        
                        columnCount++;
                    }
                    
                    // Warn if maxColumns limit was reached
                    if (columnCount >= maxColumns && rs.next()) {
                        log.warn("⚠️ Schema Limitation: Table '{}' has more than {} columns (maxColumns limit). " +
                                "Only first {} columns will be included in schema.",
                                tableName, maxColumns, maxColumns);
                    }
                    
                    // Validate first 3 columns for sharding requirements
                    validateShardingRequirements(conn, schemaName, tableNameOnly, tableName, fields, columnCount);
                }
            }
            
            // Robust validation: Check minimum field count
            if (fields.isEmpty()) {
                throw new IllegalArgumentException(
                    String.format("No columns found for table '%s' in schema '%s'. " +
                                "Please verify table name and schema name are correct.", tableName, schemaName));
            }
            
            // Log detected fields for debugging
            if (log.isDebugEnabled()) {
                log.debug("Detected schema for table '{}': {} fields - {}", 
                        tableName, fields.size(), 
                        fieldNames.stream().limit(10).toList());
            }
            
            Schema schema = Schema.builder().addFields(fields).build();
            
            long duration = System.currentTimeMillis() - schemaStartTime;
            if (metricsCollector != null) {
                metricsCollector.recordSchemaDetection(duration);
            }
            
            log.info("Successfully detected schema for table '{}': {} fields detected in {}ms", 
                    tableName, fields.size(), duration);
            ConnectionPoolLogger.logPoolStats(dataSource, "after schema detection (1 conn used)");

            return schema;
            
        } catch (SQLException e) {
            log.error("Failed to detect schema for table '{}': {}", config.getTable(), e.getMessage(), e);
            throw new RuntimeException("Failed to detect schema: " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            log.error("Schema detection validation failed for table '{}': {}", config.getTable(), e.getMessage());
            throw e;
        }
    }
    
    /**
     * Validates sharding requirements for first 3 columns.
     */
    private static void validateShardingRequirements(Connection conn, String schemaName, String tableNameOnly, 
                                                     String tableName, List<Schema.Field> fields, int columnCount) {
        if (columnCount >= 3) {
            // Check schema-level nullable constraints
            long nullableInFirstThree = 0;
            List<String> firstThreeColumnNames = new ArrayList<>();
            for (int i = 0; i < Math.min(3, fields.size()); i++) {
                Schema.Field field = fields.get(i);
                boolean isNullable = field.getType().getNullable();
                if (isNullable) {
                    nullableInFirstThree++;
                }
                firstThreeColumnNames.add(field.getName());
            }
            
            // Check actual data for null values in first 3 columns
            List<String> nonNullColumns = PostgresDataQualityChecker.checkNonNullColumnsInData(
                    conn, schemaName, tableNameOnly, firstThreeColumnNames);
            int nonNullCount = nonNullColumns.size();
            
            // Validate minimum requirement
            if (nonNullCount < MIN_NON_NULL_REQUIRED) {
                log.error("❌ Sharding Requirement Failed: Only {} non-null column(s) found among first 3 columns in table '{}'. " +
                        "Required: Minimum {} non-null columns. Found: {}. " +
                        "Non-null columns: {}. " +
                        "This may severely impact sharding performance and data quality. " +
                        "Recommendation: Ensure at least {} non-null columns exist in the first 3 positions, " +
                        "or add NOT NULL constraints to key columns.",
                        nonNullCount, tableName, MIN_NON_NULL_REQUIRED, nonNullCount, 
                        nonNullColumns.isEmpty() ? "none" : nonNullColumns, MIN_NON_NULL_REQUIRED);
            } else if (nonNullCount == MIN_NON_NULL_REQUIRED) {
                log.warn("⚠️ Sharding Requirement Met (Minimum): {} non-null column(s) found among first 3 columns in table '{}': {}. " +
                        "This meets the minimum requirement ({}), but having {} non-null columns would be optimal. " +
                        "System will use these columns for sharding.",
                        nonNullCount, tableName, nonNullColumns, MIN_NON_NULL_REQUIRED, MAX_NON_NULL_EXPECTED);
            } else if (nonNullCount == MAX_NON_NULL_EXPECTED) {
                log.info("✅ Sharding Requirement Optimal: All {} first columns in table '{}' are non-null: {}. " +
                        "Excellent for sharding performance. System will select the best column from these for sharding.",
                        MAX_NON_NULL_EXPECTED, tableName, nonNullColumns);
            } else {
                log.info("✅ Sharding Quality: {} non-null column(s) found among first 3 columns in table '{}': {}. " +
                        "System will select the best column from these for sharding.",
                        nonNullCount, tableName, nonNullColumns);
            }
            
            // Additional schema-level validation logging
            logSchemaQualityIssues(tableName, nullableInFirstThree, nonNullCount, nonNullColumns);
        } else if (columnCount > 0) {
            log.warn("⚠️ Table '{}' has only {} column(s). " +
                    "Sharding requirement: Minimum {} non-null columns among first 3. " +
                    "Consider adding more columns or ensuring existing columns are non-null.",
                    tableName, columnCount, MIN_NON_NULL_REQUIRED);
        }
    }
    
    /**
     * Logs schema quality issues based on nullable constraints and data quality.
     */
    private static void logSchemaQualityIssues(String tableName, long nullableInFirstThree, 
                                             int nonNullCount, List<String> nonNullColumns) {
        if (nullableInFirstThree == 3) {
            if (nonNullColumns.isEmpty()) {
                log.error("❌ Critical Data Quality Issue: All first 3 columns in table '{}' are nullable in schema AND contain null values in data. " +
                        "Sharding requirement not met (minimum {} non-null columns required). " +
                        "This will severely impact sharding performance.",
                        tableName, MIN_NON_NULL_REQUIRED);
            } else if (nonNullCount < MIN_NON_NULL_REQUIRED) {
                log.error("❌ Schema Constraint Issue: All first 3 columns in table '{}' are nullable in schema, " +
                        "and only {} column(s) have no null values in actual data: {}. " +
                        "Sharding requirement not met (minimum {} non-null columns required). " +
                        "Consider adding NOT NULL constraints to at least {} columns.",
                        tableName, nonNullCount, nonNullColumns, MIN_NON_NULL_REQUIRED, MIN_NON_NULL_REQUIRED);
            } else {
                log.warn("⚠️ Schema Constraint Issue: All first 3 columns in table '{}' are nullable in schema, " +
                        "but {} column(s) have no null values in actual data: {}. " +
                        "Consider adding NOT NULL constraints to these columns for better sharding performance.",
                        tableName, nonNullCount, nonNullColumns);
            }
        } else if (nullableInFirstThree > 0) {
            if (nonNullCount >= MIN_NON_NULL_REQUIRED) {
                log.info("Schema quality check: {}/3 of first columns in table '{}' are nullable in schema. " +
                        "Columns with no null values in data: {} (meets minimum requirement of {}). " +
                        "System will auto-detect best shard column from non-null columns.",
                        nullableInFirstThree, tableName, nonNullColumns, MIN_NON_NULL_REQUIRED);
            } else {
                log.warn("Schema quality check: {}/3 of first columns in table '{}' are nullable in schema. " +
                        "Only {} non-null column(s) found in data: {}. " +
                        "Minimum {} non-null columns required for optimal sharding.",
                        nullableInFirstThree, tableName, nonNullCount, 
                        nonNullColumns.isEmpty() ? "none" : nonNullColumns, MIN_NON_NULL_REQUIRED);
            }
        } else {
            // All first 3 are non-nullable in schema
            if (nonNullCount >= MIN_NON_NULL_REQUIRED) {
                log.info("✅ Schema Quality: All first 3 columns in table '{}' are non-nullable in schema. " +
                        "Verified in data: {} column(s) have no null values: {}. " +
                        "Meets sharding requirement (minimum {}). Excellent for sharding performance.",
                        tableName, nonNullCount, nonNullColumns, MIN_NON_NULL_REQUIRED);
            } else {
                log.warn("⚠️ Schema vs Data Mismatch: All first 3 columns in table '{}' are non-nullable in schema, " +
                        "but only {} column(s) verified as non-null in data: {}. " +
                        "This may indicate data quality issues.",
                        tableName, nonNullCount, nonNullColumns);
            }
        }
    }
    
    /**
     * Verifies that a table exists in the specified schema.
     */
    private static boolean verifyTableExists(Connection conn, String schemaName, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) as table_count " +
                    "FROM information_schema.tables " +
                    "WHERE table_schema = ? AND table_name = ?";
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("table_count") > 0;
                }
            }
        }
        return false;
    }
}
