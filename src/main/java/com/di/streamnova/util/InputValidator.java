package com.di.streamnova.util;

import lombok.extern.slf4j.Slf4j;

import java.util.regex.Pattern;

/**
 * Input validation utility for security and data integrity.
 * Prevents SQL injection and validates input bounds.
 */
@Slf4j
public final class InputValidator {

    private InputValidator() {}

    // ============================================================================
    // SQL Identifier Validation Patterns
    // ============================================================================

    /**
     * Valid PostgreSQL identifier pattern:
     * - Starts with letter or underscore
     * - Followed by letters, digits, underscores, or dollar signs
     * - Max length: 63 characters (PostgreSQL limit)
     * - Allows quoted identifiers (handled separately)
     */
    private static final Pattern VALID_IDENTIFIER_PATTERN = Pattern.compile(
            "^[a-zA-Z_][a-zA-Z0-9_$]{0,62}$"
    );

    /**
     * Pattern to detect potentially dangerous SQL injection attempts:
     * - SQL keywords (SELECT, INSERT, UPDATE, DELETE, DROP, etc.)
     * - SQL operators (UNION, OR, AND, etc.)
     * - SQL comments (--, /*)
     * - Semicolons (statement terminators)
     * - Quotes (single or double)
     */
    private static final Pattern SQL_INJECTION_PATTERN = Pattern.compile(
            "(?i)(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|EXEC|EXECUTE|UNION|OR|AND|--|/\\*|\\*/|;|'|\")",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Maximum length for table/schema names (PostgreSQL limit is 63, but we use 64 for safety)
     */
    private static final int MAX_IDENTIFIER_LENGTH = 64;

    /**
     * Minimum length for identifiers
     */
    private static final int MIN_IDENTIFIER_LENGTH = 1;

    // ============================================================================
    // Numeric Input Validation Constants
    // ============================================================================

    private static final int MIN_SHARD_COUNT = 1;
    private static final int MAX_SHARD_COUNT = 10_000;  // Reasonable upper limit

    private static final int MIN_FETCH_SIZE = 1;
    private static final int MAX_FETCH_SIZE = 100_000;  // Reasonable upper limit

    private static final int MIN_POOL_SIZE = 1;
    private static final int MAX_POOL_SIZE = 1000;  // Reasonable upper limit

    private static final long MIN_ROW_COUNT = 0;
    private static final long MAX_ROW_COUNT = Long.MAX_VALUE;  // No practical limit

    private static final int MIN_ROW_SIZE_BYTES = 1;
    private static final int MAX_ROW_SIZE_BYTES = 10_000_000;  // 10MB per row max

    // ============================================================================
    // SQL Identifier Validation
    // ============================================================================

    /**
     * Validates a PostgreSQL identifier (table name, schema name, column name).
     * 
     * @param identifier The identifier to validate
     * @param identifierType Type of identifier for error messages (e.g., "table name", "schema name")
     * @return The validated identifier (trimmed)
     * @throws IllegalArgumentException if validation fails
     */
    public static String validateIdentifier(String identifier, String identifierType) {
        if (identifier == null) {
            throw new IllegalArgumentException(String.format("%s cannot be null", identifierType));
        }

        String trimmed = identifier.trim();
        
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(String.format("%s cannot be empty", identifierType));
        }

        if (trimmed.length() > MAX_IDENTIFIER_LENGTH) {
            throw new IllegalArgumentException(
                    String.format("%s exceeds maximum length of %d characters: %s", 
                            identifierType, MAX_IDENTIFIER_LENGTH, trimmed));
        }

        if (trimmed.length() < MIN_IDENTIFIER_LENGTH) {
            throw new IllegalArgumentException(
                    String.format("%s must be at least %d character long", 
                            identifierType, MIN_IDENTIFIER_LENGTH));
        }

        // Check for SQL injection patterns
        if (SQL_INJECTION_PATTERN.matcher(trimmed).find()) {
            log.warn("Potential SQL injection attempt detected in {}: {}", identifierType, trimmed);
            throw new IllegalArgumentException(
                    String.format("Invalid %s: contains potentially dangerous SQL patterns. " +
                            "Only alphanumeric characters, underscores, and dollar signs are allowed.", 
                            identifierType));
        }

        // Validate identifier pattern (for unquoted identifiers)
        // Note: Quoted identifiers are handled separately and should not contain SQL keywords
        if (!isQuotedIdentifier(trimmed) && !VALID_IDENTIFIER_PATTERN.matcher(trimmed).matches()) {
            throw new IllegalArgumentException(
                    String.format("Invalid %s format: '%s'. " +
                            "Must start with a letter or underscore, followed by letters, digits, underscores, or dollar signs.", 
                            identifierType, trimmed));
        }

        return trimmed;
    }

    /**
     * Validates a table name (can be schema.table or just table).
     * 
     * @param tableName The table name to validate
     * @return The validated table name
     * @throws IllegalArgumentException if validation fails
     */
    public static String validateTableName(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }

        String trimmed = tableName.trim();
        
        // Split schema and table if present
        String[] parts = trimmed.split("\\.", 2);
        
        if (parts.length == 2) {
            // Validate schema name
            validateIdentifier(parts[0], "Schema name");
            // Validate table name
            validateIdentifier(parts[1], "Table name");
        } else {
            // Validate table name only
            validateIdentifier(trimmed, "Table name");
        }

        return trimmed;
    }

    /**
     * Validates a schema name.
     * 
     * @param schemaName The schema name to validate
     * @return The validated schema name
     * @throws IllegalArgumentException if validation fails
     */
    public static String validateSchemaName(String schemaName) {
        return validateIdentifier(schemaName, "Schema name");
    }

    /**
     * Validates a column name.
     * 
     * @param columnName The column name to validate
     * @return The validated column name
     * @throws IllegalArgumentException if validation fails
     */
    public static String validateColumnName(String columnName) {
        return validateIdentifier(columnName, "Column name");
    }

    /**
     * Checks if an identifier is quoted (starts and ends with double quotes).
     * 
     * @param identifier The identifier to check
     * @return true if the identifier is quoted
     */
    private static boolean isQuotedIdentifier(String identifier) {
        return identifier.length() >= 2 
                && identifier.startsWith("\"") 
                && identifier.endsWith("\"");
    }

    // ============================================================================
    // Numeric Input Validation
    // ============================================================================

    /**
     * Validates shard count.
     * 
     * @param shardCount The shard count to validate
     * @return The validated shard count
     * @throws IllegalArgumentException if validation fails
     */
    public static int validateShardCount(int shardCount) {
        if (shardCount < MIN_SHARD_COUNT) {
            throw new IllegalArgumentException(
                    String.format("Shard count must be at least %d, got: %d", MIN_SHARD_COUNT, shardCount));
        }
        if (shardCount > MAX_SHARD_COUNT) {
            throw new IllegalArgumentException(
                    String.format("Shard count exceeds maximum of %d, got: %d", MAX_SHARD_COUNT, shardCount));
        }
        return shardCount;
    }

    /**
     * Validates fetch size.
     * 
     * @param fetchSize The fetch size to validate
     * @return The validated fetch size
     * @throws IllegalArgumentException if validation fails
     */
    public static int validateFetchSize(int fetchSize) {
        if (fetchSize < MIN_FETCH_SIZE) {
            throw new IllegalArgumentException(
                    String.format("Fetch size must be at least %d, got: %d", MIN_FETCH_SIZE, fetchSize));
        }
        if (fetchSize > MAX_FETCH_SIZE) {
            throw new IllegalArgumentException(
                    String.format("Fetch size exceeds maximum of %d, got: %d", MAX_FETCH_SIZE, fetchSize));
        }
        return fetchSize;
    }

    /**
     * Validates connection pool size.
     * 
     * @param poolSize The pool size to validate
     * @return The validated pool size
     * @throws IllegalArgumentException if validation fails
     */
    public static int validatePoolSize(Integer poolSize) {
        if (poolSize == null) {
            return poolSize; // null is allowed (unlimited)
        }
        if (poolSize < MIN_POOL_SIZE) {
            throw new IllegalArgumentException(
                    String.format("Pool size must be at least %d, got: %d", MIN_POOL_SIZE, poolSize));
        }
        if (poolSize > MAX_POOL_SIZE) {
            throw new IllegalArgumentException(
                    String.format("Pool size exceeds maximum of %d, got: %d", MAX_POOL_SIZE, poolSize));
        }
        return poolSize;
    }

    /**
     * Validates estimated row count.
     * 
     * @param rowCount The row count to validate
     * @return The validated row count
     * @throws IllegalArgumentException if validation fails
     */
    public static Long validateRowCount(Long rowCount) {
        if (rowCount == null) {
            return null; // null is allowed (unknown)
        }
        if (rowCount < MIN_ROW_COUNT) {
            throw new IllegalArgumentException(
                    String.format("Row count cannot be negative, got: %d", rowCount));
        }
        // No upper limit check - can be very large
        return rowCount;
    }

    /**
     * Validates average row size in bytes.
     * 
     * @param rowSizeBytes The row size to validate
     * @return The validated row size
     * @throws IllegalArgumentException if validation fails
     */
    public static Integer validateRowSizeBytes(Integer rowSizeBytes) {
        if (rowSizeBytes == null) {
            return null; // null is allowed (unknown)
        }
        if (rowSizeBytes < MIN_ROW_SIZE_BYTES) {
            throw new IllegalArgumentException(
                    String.format("Row size must be at least %d bytes, got: %d", 
                            MIN_ROW_SIZE_BYTES, rowSizeBytes));
        }
        if (rowSizeBytes > MAX_ROW_SIZE_BYTES) {
            throw new IllegalArgumentException(
                    String.format("Row size exceeds maximum of %d bytes, got: %d", 
                            MAX_ROW_SIZE_BYTES, rowSizeBytes));
        }
        return rowSizeBytes;
    }

    // ============================================================================
    // URL Validation
    // ============================================================================

    /**
     * Validates JDBC URL format (basic validation).
     * 
     * @param jdbcUrl The JDBC URL to validate
     * @return The validated JDBC URL
     * @throws IllegalArgumentException if validation fails
     */
    public static String validateJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("JDBC URL cannot be null or empty");
        }

        String trimmed = jdbcUrl.trim();
        
        // Basic validation: must start with jdbc:
        if (!trimmed.toLowerCase().startsWith("jdbc:")) {
            throw new IllegalArgumentException("JDBC URL must start with 'jdbc:'");
        }

        // Check for potentially dangerous patterns
        if (SQL_INJECTION_PATTERN.matcher(trimmed).find()) {
            log.warn("Potential SQL injection attempt detected in JDBC URL");
            throw new IllegalArgumentException("JDBC URL contains potentially dangerous patterns");
        }

        return trimmed;
    }

    // ============================================================================
    // Utility Methods
    // ============================================================================

    /**
     * Sanitizes a string for logging (removes sensitive information).
     * 
     * @param input The input to sanitize
     * @return Sanitized string safe for logging
     */
    public static String sanitizeForLogging(String input) {
        if (input == null) {
            return "null";
        }
        // Mask passwords in JDBC URLs
        if (input.contains("password=")) {
            return input.replaceAll("password=[^;&]+", "password=***");
        }
        return input;
    }
}
