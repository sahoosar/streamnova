package com.di.streamnova.aspect;

import java.sql.SQLException;

/**
 * Enumeration of error categories for transaction event logging.
 * 
 * <p>This enum provides a standardized way to categorize exceptions and errors
 * for better tracking, monitoring, and alerting. Each category represents a
 * specific type of error that can occur during transaction execution.
 * 
 * <p><strong>Usage:</strong>
 * <pre>{@code
 * ErrorCategory category = ErrorCategory.categorize(exception);
 * log.error("Transaction failed with category: {}", category);
 * }</pre>
 * 
 * <p><strong>HOW TO ADD NEW ERROR CATEGORIES:</strong>
 * 
 * <p><b>Step 1:</b> Add the enum constant at the end of the enum list (before UNKNOWN):
 * <pre>{@code
 * /**
 *  * Description of the new error type.
 *  *\/ 
 * NEW_ERROR_CATEGORY("Display Name", "Detailed description of when this error occurs"),
 * }</pre>
 * 
 * <p><b>Step 2:</b> Add categorization logic in the {@link #categorize(Throwable)} method.
 * Find the appropriate place in the method (order matters - most specific first):
 * <pre>{@code
 * // Add your check BEFORE APPLICATION_ERROR (which is the default)
 * if (isNewErrorCategory(exception)) {
 *     return NEW_ERROR_CATEGORY;
 * }
 * }</pre>
 * 
 * <p><b>Step 3:</b> Implement the helper method at the end of the class:
 * <pre>{@code
 * /**
 *  * Checks if the exception is a new error category type.
 *  *\/
 * private static boolean isNewErrorCategory(Throwable exception) {
 *     // Check exception type
 *     if (exception instanceof YourExceptionType) {
 *         return true;
 *     }
 *     
 *     // Or check error message patterns
 *     String message = exception.getMessage();
 *     if (message != null) {
 *         String lowerMsg = message.toLowerCase();
 *         return containsAny(lowerMsg, "keyword1", "keyword2");
 *     }
 *     
 *     return false;
 * }
 * }</pre>
 * 
 * <p><b>Example - Adding a FileNotFoundError category:</b>
 * <pre>{@code
 * // 1. Add enum constant
 * FILE_NOT_FOUND_ERROR("File not found", "Requested file or resource not found"),
 * 
 * // 2. Add check in categorize() method (before APPLICATION_ERROR)
 * if (isFileNotFoundError(exception)) {
 *     return FILE_NOT_FOUND_ERROR;
 * }
 * 
 * // 3. Add helper method
 * private static boolean isFileNotFoundError(Throwable exception) {
 *     return exception instanceof java.io.FileNotFoundException ||
 *            (exception.getMessage() != null && 
 *             exception.getMessage().toLowerCase().contains("file not found"));
 * }
 * }</pre>
 * 
 * <p><strong>IMPORTANT:</strong>
 * <ul>
 *   <li>Always add new categories BEFORE {@code APPLICATION_ERROR} (which is the default fallback)</li>
 *   <li>Order matters - most specific checks should come first</li>
 *   <li>Use {@code containsAny()} helper for message-based checks</li>
 *   <li>Test your new category with actual exceptions</li>
 * </ul>
 * 
 * @author StreamNova
 * @since 1.0
 */
public enum ErrorCategory {
    
    /**
     * Database connection errors (e.g., connection timeout, connection refused).
     * SQL State codes: 08xxx
     */
    CONNECTION_ERROR("Database connection error", "Failed to establish or maintain database connection"),
    
    /**
     * Database constraint violations (e.g., unique constraint, foreign key violation).
     * SQL State codes: 23xxx
     */
    CONSTRAINT_VIOLATION("Database constraint violation", "Database constraint check failed"),
    
    /**
     * SQL syntax or semantic errors.
     * SQL State codes: 42xxx
     */
    SQL_SYNTAX_ERROR("SQL syntax error", "Invalid SQL syntax or semantic error"),
    
    /**
     * Transaction rollback errors.
     * SQL State codes: 40xxx
     */
    TRANSACTION_ROLLBACK("Transaction rollback", "Transaction was rolled back"),
    
    /**
     * Database permission or access denied errors.
     */
    PERMISSION_ERROR("Permission denied", "Insufficient permissions to perform operation"),
    
    /**
     * General database-related errors.
     */
    DATABASE_ERROR("Database error", "General database operation error"),
    
    /**
     * Network-related errors (e.g., socket timeout, connection refused).
     */
    NETWORK_ERROR("Network error", "Network communication failure"),
    
    /**
     * Input validation errors (e.g., invalid arguments, illegal state).
     */
    VALIDATION_ERROR("Validation error", "Input validation or business rule violation"),
    
    /**
     * Configuration errors (e.g., missing configuration, invalid settings).
     */
    CONFIGURATION_ERROR("Configuration error", "Application configuration issue"),
    
    /**
     * Resource errors (e.g., out of memory, file system errors).
     */
    RESOURCE_ERROR("Resource error", "System resource exhaustion or unavailability"),
    
    /**
     * Authentication or authorization errors.
     */
    AUTHENTICATION_ERROR("Authentication error", "Authentication or authorization failure"),
    
    /**
     * Serialization or deserialization errors.
     */
    SERIALIZATION_ERROR("Serialization error", "Data serialization or deserialization failure"),
    
    /**
     * Timeout errors (operation exceeded time limit).
     */
    TIMEOUT_ERROR("Timeout error", "Operation exceeded maximum time limit"),
    
    /**
     * Unknown or unclassified errors.
     */
    APPLICATION_ERROR("Application error", "General application error"),
    
    /**
     * Unknown error category (fallback when categorization fails).
     */
    UNKNOWN("Unknown error", "Unclassified or unknown error type");

    private final String name;
    private final String description;

    /**
     * Creates a new ErrorCategory.
     * 
     * @param name the display name of the error category
     * @param description a detailed description of when this category applies
     */
    ErrorCategory(String name, String description) {
        this.name = name;
        this.description = description;
    }

    /**
     * Returns the display name of this error category.
     * 
     * @return the display name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the description of this error category.
     * 
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Categorizes a throwable exception into an appropriate ErrorCategory.
     * 
     * <p>This method analyzes the exception type, SQL state codes, and error messages
     * to determine the most appropriate category.
     * 
     * @param exception the exception to categorize (can be null)
     * @return the appropriate ErrorCategory, or UNKNOWN if exception is null or cannot be categorized
     */
    public static ErrorCategory categorize(Throwable exception) {
        if (exception == null) {
            return UNKNOWN;
        }

        // SQL Exception categorization (most specific first)
        if (exception instanceof SQLException) {
            return categorizeSqlException((SQLException) exception);
        }

        // Network errors
        if (isNetworkError(exception)) {
            return NETWORK_ERROR;
        }

        // Timeout errors
        if (isTimeoutError(exception)) {
            return TIMEOUT_ERROR;
        }

        // Validation errors
        if (isValidationError(exception)) {
            return VALIDATION_ERROR;
        }

        // Configuration errors
        if (isConfigurationError(exception)) {
            return CONFIGURATION_ERROR;
        }

        // Resource errors
        if (isResourceError(exception)) {
            return RESOURCE_ERROR;
        }

        // Authentication errors
        if (isAuthenticationError(exception)) {
            return AUTHENTICATION_ERROR;
        }

        // Serialization errors
        if (isSerializationError(exception)) {
            return SERIALIZATION_ERROR;
        }

        // ========== ADD NEW ERROR CATEGORIES HERE ==========
        // 
        // To add a new error category:
        // 1. Add enum constant above (before UNKNOWN)
        // 2. Add check here (before APPLICATION_ERROR)
        // 3. Add helper method below (see isSerializationError as example)
        //
        // Example:
        // if (isNewErrorType(exception)) {
        //     return NEW_ERROR_TYPE;
        // }
        // ====================================================

        // Default to application error (must be last)
        return APPLICATION_ERROR;
    }

    /**
     * Categorizes SQL exceptions based on SQL state codes and error messages.
     */
    private static ErrorCategory categorizeSqlException(SQLException sqlEx) {
        String sqlState = sqlEx.getSQLState();
        
        // Categorize by SQL state code
        if (sqlState != null) {
            if (sqlState.startsWith("08")) {
                return CONNECTION_ERROR;
            }
            if (sqlState.startsWith("23")) {
                return CONSTRAINT_VIOLATION;
            }
            if (sqlState.startsWith("42")) {
                return SQL_SYNTAX_ERROR;
            }
            if (sqlState.startsWith("40")) {
                return TRANSACTION_ROLLBACK;
            }
        }

        // Categorize by error message if SQL state is not available
        String message = sqlEx.getMessage();
        if (message != null) {
            String lowerMsg = message.toLowerCase();
            
            if (containsAny(lowerMsg, "connection", "timeout", "refused", "closed")) {
                return CONNECTION_ERROR;
            }
            if (containsAny(lowerMsg, "permission", "access denied", "unauthorized", "forbidden")) {
                return PERMISSION_ERROR;
            }
            if (containsAny(lowerMsg, "constraint", "unique", "foreign key", "check constraint")) {
                return CONSTRAINT_VIOLATION;
            }
            if (containsAny(lowerMsg, "syntax", "invalid", "parse error")) {
                return SQL_SYNTAX_ERROR;
            }
        }

        return DATABASE_ERROR;
    }

    /**
     * Checks if the exception is a network-related error.
     */
    private static boolean isNetworkError(Throwable exception) {
        return exception instanceof java.net.SocketTimeoutException ||
               exception instanceof java.net.ConnectException ||
               exception instanceof java.net.UnknownHostException ||
               exception instanceof java.net.SocketException ||
               (exception instanceof java.io.IOException && 
                !(exception instanceof java.io.FileNotFoundException));
    }

    /**
     * Checks if the exception is a timeout-related error.
     */
    private static boolean isTimeoutError(Throwable exception) {
        return exception instanceof java.util.concurrent.TimeoutException ||
               exception instanceof java.net.SocketTimeoutException ||
               (exception.getMessage() != null && 
                exception.getMessage().toLowerCase().contains("timeout"));
    }

    /**
     * Checks if the exception is a validation-related error.
     */
    private static boolean isValidationError(Throwable exception) {
        return exception instanceof IllegalArgumentException ||
               exception instanceof IllegalStateException ||
               exception instanceof java.util.NoSuchElementException ||
               exception instanceof IndexOutOfBoundsException;
    }

    /**
     * Checks if the exception is a configuration-related error.
     */
    private static boolean isConfigurationError(Throwable exception) {
        return exception instanceof org.springframework.beans.factory.BeanCreationException ||
               exception instanceof org.springframework.context.ApplicationContextException ||
               exception instanceof org.springframework.beans.factory.BeanDefinitionStoreException ||
               exception instanceof org.springframework.beans.factory.UnsatisfiedDependencyException;
    }

    /**
     * Checks if the exception is a resource-related error.
     */
    private static boolean isResourceError(Throwable exception) {
        return exception instanceof java.lang.OutOfMemoryError ||
               exception instanceof java.lang.StackOverflowError ||
               exception instanceof java.io.FileNotFoundException ||
               exception instanceof java.nio.file.FileSystemException ||
               (exception instanceof java.io.IOException && 
                exception.getMessage() != null &&
                exception.getMessage().toLowerCase().contains("no space"));
    }

    /**
     * Checks if the exception is an authentication-related error.
     */
    private static boolean isAuthenticationError(Throwable exception) {
        String message = exception.getMessage();
        if (message != null) {
            String lowerMsg = message.toLowerCase();
            return containsAny(lowerMsg, "authentication", "unauthorized", "forbidden", 
                             "access denied", "invalid credentials", "login failed");
        }
        // Check for Spring Security exceptions if available (optional dependency)
        String className = exception.getClass().getName();
        return className.contains("AuthenticationException") ||
               className.contains("AccessDeniedException");
    }

    /**
     * Checks if the exception is a serialization-related error.
     */
    private static boolean isSerializationError(Throwable exception) {
        return exception instanceof java.io.NotSerializableException ||
               exception instanceof java.io.InvalidClassException ||
               exception instanceof java.io.StreamCorruptedException ||
               exception instanceof java.io.OptionalDataException ||
               (exception.getClass().getName().contains("Serialization") &&
                exception.getMessage() != null &&
                exception.getMessage().toLowerCase().contains("serialization"));
    }

    // ========== ADD NEW ERROR CATEGORY HELPER METHODS HERE ==========
    //
    // Template for adding a new error category helper method:
    //
    // /**
    //  * Checks if the exception is a [your error type] error.
    //  */
    // private static boolean isYourErrorType(Throwable exception) {
    //     // Option 1: Check exception type
    //     if (exception instanceof YourExceptionClass) {
    //         return true;
    //     }
    //     
    //     // Option 2: Check error message patterns
    //     String message = exception.getMessage();
    //     if (message != null) {
    //         String lowerMsg = message.toLowerCase();
    //         return containsAny(lowerMsg, "keyword1", "keyword2", "keyword3");
    //     }
    //     
    //     // Option 3: Check exception class name (for optional dependencies)
    //     String className = exception.getClass().getName();
    //     if (className.contains("YourExceptionPattern")) {
    //         return true;
    //     }
    //     
    //     return false;
    // }
    //
    // ================================================================

    /**
     * Helper method to check if a string contains any of the given keywords.
     * Use this in your error category helper methods for message-based detection.
     * 
     * @param text the text to search in
     * @param keywords the keywords to search for
     * @return true if text contains any of the keywords
     */
    private static boolean containsAny(String text, String... keywords) {
        if (text == null) {
            return false;
        }
        for (String keyword : keywords) {
            if (text.contains(keyword)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a string representation of this error category.
     * 
     * @return the enum constant name
     */
    @Override
    public String toString() {
        return name();
    }
}
