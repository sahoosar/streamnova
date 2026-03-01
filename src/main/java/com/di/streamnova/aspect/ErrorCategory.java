package com.di.streamnova.aspect;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Standardized error categories for transaction event logging and alerting.
 * <p>Usage: {@code ErrorCategory category = ErrorCategory.categorize(exception);}
 * <p>To add a new category: add the enum constant (before UNKNOWN), add a matcher in
 * {@link #MATCHERS}, and optionally add a helper in the "Matcher helpers" section below.
 */
public enum ErrorCategory {

    CONNECTION_ERROR("Database connection error", "Failed to establish or maintain database connection"),
    CONSTRAINT_VIOLATION("Database constraint violation", "Database constraint check failed"),
    SQL_SYNTAX_ERROR("SQL syntax error", "Invalid SQL syntax or semantic error"),
    TRANSACTION_ROLLBACK("Transaction rollback", "Transaction was rolled back"),
    PERMISSION_ERROR("Permission denied", "Insufficient permissions to perform operation"),
    DATABASE_ERROR("Database error", "General database operation error"),
    NETWORK_ERROR("Network error", "Network communication failure"),
    VALIDATION_ERROR("Validation error", "Input validation or business rule violation"),
    CONFIGURATION_ERROR("Configuration error", "Application configuration issue"),
    RESOURCE_ERROR("Resource error", "System resource exhaustion or unavailability"),
    AUTHENTICATION_ERROR("Authentication error", "Authentication or authorization failure"),
    SERIALIZATION_ERROR("Serialization error", "Data serialization or deserialization failure"),
    TIMEOUT_ERROR("Timeout error", "Operation exceeded maximum time limit"),
    APPLICATION_ERROR("Application error", "General application error"),
    UNKNOWN("Unknown error", "Unclassified or unknown error type");

    private final String name;
    private final String description;

    ErrorCategory(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    /** Order matters: first match wins. Add new categories before APPLICATION_ERROR. */
    private static final Map<Predicate<Throwable>, ErrorCategory> MATCHERS = new LinkedHashMap<>();

    static {
        MATCHERS.put(ErrorCategory::isNetworkError, NETWORK_ERROR);
        MATCHERS.put(ErrorCategory::isTimeoutError, TIMEOUT_ERROR);
        MATCHERS.put(ErrorCategory::isValidationError, VALIDATION_ERROR);
        MATCHERS.put(ErrorCategory::isConfigurationError, CONFIGURATION_ERROR);
        MATCHERS.put(ErrorCategory::isResourceError, RESOURCE_ERROR);
        MATCHERS.put(ErrorCategory::isAuthenticationError, AUTHENTICATION_ERROR);
        MATCHERS.put(ErrorCategory::isSerializationError, SERIALIZATION_ERROR);
    }

    public static ErrorCategory categorize(Throwable exception) {
        if (exception == null) {
            return UNKNOWN;
        }
        if (exception instanceof SQLException) {
            return categorizeSqlException((SQLException) exception);
        }
        for (Map.Entry<Predicate<Throwable>, ErrorCategory> e : MATCHERS.entrySet()) {
            if (e.getKey().test(exception)) {
                return e.getValue();
            }
        }
        return APPLICATION_ERROR;
    }

    private static ErrorCategory categorizeSqlException(SQLException sqlEx) {
        String sqlState = sqlEx.getSQLState();
        if (sqlState != null) {
            ErrorCategory byState = SQL_STATE_PREFIX.get(sqlState.substring(0, Math.min(2, sqlState.length())));
            if (byState != null) {
                return byState;
            }
        }
        String msg = sqlEx.getMessage();
        if (msg != null) {
            String lower = msg.toLowerCase();
            if (containsAny(lower, "connection", "timeout", "refused", "closed")) return CONNECTION_ERROR;
            if (containsAny(lower, "permission", "access denied", "unauthorized", "forbidden")) return PERMISSION_ERROR;
            if (containsAny(lower, "constraint", "unique", "foreign key", "check constraint")) return CONSTRAINT_VIOLATION;
            if (containsAny(lower, "syntax", "invalid", "parse error")) return SQL_SYNTAX_ERROR;
        }
        return DATABASE_ERROR;
    }

    private static final Map<String, ErrorCategory> SQL_STATE_PREFIX = Map.of(
            "08", CONNECTION_ERROR,
            "23", CONSTRAINT_VIOLATION,
            "42", SQL_SYNTAX_ERROR,
            "40", TRANSACTION_ROLLBACK
    );

    // --- Matcher helpers (add new ones here when adding categories) ---

    private static boolean isNetworkError(Throwable t) {
        return t instanceof java.net.SocketTimeoutException
                || t instanceof java.net.ConnectException
                || t instanceof java.net.UnknownHostException
                || t instanceof java.net.SocketException
                || (t instanceof java.io.IOException && !(t instanceof java.io.FileNotFoundException));
    }

    private static boolean isTimeoutError(Throwable t) {
        return t instanceof java.util.concurrent.TimeoutException
                || t instanceof java.net.SocketTimeoutException
                || (t.getMessage() != null && t.getMessage().toLowerCase().contains("timeout"));
    }

    private static boolean isValidationError(Throwable t) {
        return t instanceof IllegalArgumentException
                || t instanceof IllegalStateException
                || t instanceof java.util.NoSuchElementException
                || t instanceof IndexOutOfBoundsException;
    }

    private static boolean isConfigurationError(Throwable t) {
        return t instanceof org.springframework.beans.factory.BeanCreationException
                || t instanceof org.springframework.context.ApplicationContextException
                || t instanceof org.springframework.beans.factory.BeanDefinitionStoreException
                || t instanceof org.springframework.beans.factory.UnsatisfiedDependencyException;
    }

    private static boolean isResourceError(Throwable t) {
        return t instanceof OutOfMemoryError
                || t instanceof StackOverflowError
                || t instanceof java.io.FileNotFoundException
                || t instanceof java.nio.file.FileSystemException
                || (t instanceof java.io.IOException && messageContains(t, "no space"));
    }

    private static boolean isAuthenticationError(Throwable t) {
        if (messageContains(t, "authentication", "unauthorized", "forbidden", "access denied", "invalid credentials", "login failed")) {
            return true;
        }
        String cn = t.getClass().getName();
        return cn.contains("AuthenticationException") || cn.contains("AccessDeniedException");
    }

    private static boolean isSerializationError(Throwable t) {
        return t instanceof java.io.NotSerializableException
                || t instanceof java.io.InvalidClassException
                || t instanceof java.io.StreamCorruptedException
                || t instanceof java.io.OptionalDataException
                || (t.getClass().getName().contains("Serialization") && messageContains(t, "serialization"));
    }

    private static boolean messageContains(Throwable t, String... keywords) {
        String msg = t.getMessage();
        return msg != null && containsAny(msg.toLowerCase(), keywords);
    }

    private static boolean containsAny(String text, String... keywords) {
        if (text == null) return false;
        for (String k : keywords) {
            if (text.contains(k)) return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return name();
    }
}
