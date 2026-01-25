package com.di.streamnova.aspect;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.net.SocketTimeoutException;
import java.net.ConnectException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for ErrorCategory enum.
 */
@DisplayName("ErrorCategory Tests")
class ErrorCategoryTest {

    // ============================================================================
    // Basic Enum Tests
    // ============================================================================

    @Test
    @DisplayName("Should have all expected error categories")
    void testErrorCategoryEnumValues() {
        ErrorCategory[] categories = ErrorCategory.values();
        assertTrue(categories.length > 0);
        
        // Verify key categories exist
        assertTrue(Stream.of(categories)
            .anyMatch(c -> c == ErrorCategory.CONNECTION_ERROR));
        assertTrue(Stream.of(categories)
            .anyMatch(c -> c == ErrorCategory.SQL_SYNTAX_ERROR));
        assertTrue(Stream.of(categories)
            .anyMatch(c -> c == ErrorCategory.UNKNOWN));
    }

    @Test
    @DisplayName("Should return correct name and description")
    void testGetNameAndDescription() {
        ErrorCategory category = ErrorCategory.CONNECTION_ERROR;
        assertNotNull(category.getName());
        assertNotNull(category.getDescription());
        assertFalse(category.getName().isEmpty());
        assertFalse(category.getDescription().isEmpty());
    }

    // ============================================================================
    // SQL Exception Categorization Tests
    // ============================================================================

    @Test
    @DisplayName("Should categorize SQL connection errors by SQL state")
    void testCategorize_SqlConnectionError() {
        SQLException sqlEx = new SQLException("Connection failed", "08001");
        ErrorCategory category = ErrorCategory.categorize(sqlEx);
        assertEquals(ErrorCategory.CONNECTION_ERROR, category);
    }

    @Test
    @DisplayName("Should categorize SQL constraint violations by SQL state")
    void testCategorize_SqlConstraintViolation() {
        SQLException sqlEx = new SQLException("Unique constraint violated", "23000");
        ErrorCategory category = ErrorCategory.categorize(sqlEx);
        assertEquals(ErrorCategory.CONSTRAINT_VIOLATION, category);
    }

    @Test
    @DisplayName("Should categorize SQL syntax errors by SQL state")
    void testCategorize_SqlSyntaxError() {
        SQLException sqlEx = new SQLException("Syntax error", "42000");
        ErrorCategory category = ErrorCategory.categorize(sqlEx);
        assertEquals(ErrorCategory.SQL_SYNTAX_ERROR, category);
    }

    @Test
    @DisplayName("Should categorize SQL transaction rollback by SQL state")
    void testCategorize_SqlTransactionRollback() {
        SQLException sqlEx = new SQLException("Transaction rolled back", "40001");
        ErrorCategory category = ErrorCategory.categorize(sqlEx);
        assertEquals(ErrorCategory.TRANSACTION_ROLLBACK, category);
    }

    @Test
    @DisplayName("Should categorize SQL errors by message when SQL state unavailable")
    void testCategorize_SqlErrorByMessage() {
        SQLException sqlEx = new SQLException("Connection timeout occurred");
        ErrorCategory category = ErrorCategory.categorize(sqlEx);
        assertEquals(ErrorCategory.CONNECTION_ERROR, category);
    }

    @Test
    @DisplayName("Should categorize permission errors by message")
    void testCategorize_PermissionError() {
        SQLException sqlEx = new SQLException("Access denied to table");
        ErrorCategory category = ErrorCategory.categorize(sqlEx);
        assertEquals(ErrorCategory.PERMISSION_ERROR, category);
    }

    // ============================================================================
    // Network Error Categorization Tests
    // ============================================================================

    @Test
    @DisplayName("Should categorize SocketTimeoutException as network error")
    void testCategorize_SocketTimeoutException() {
        SocketTimeoutException ex = new SocketTimeoutException("Connection timed out");
        ErrorCategory category = ErrorCategory.categorize(ex);
        assertEquals(ErrorCategory.NETWORK_ERROR, category);
    }

    @Test
    @DisplayName("Should categorize ConnectException as network error")
    void testCategorize_ConnectException() {
        ConnectException ex = new ConnectException("Connection refused");
        ErrorCategory category = ErrorCategory.categorize(ex);
        assertEquals(ErrorCategory.NETWORK_ERROR, category);
    }

    // ============================================================================
    // Timeout Error Categorization Tests
    // ============================================================================

    @Test
    @DisplayName("Should categorize TimeoutException as timeout error")
    void testCategorize_TimeoutException() {
        TimeoutException ex = new TimeoutException("Operation timed out");
        ErrorCategory category = ErrorCategory.categorize(ex);
        assertEquals(ErrorCategory.TIMEOUT_ERROR, category);
    }

    @Test
    @DisplayName("Should categorize exceptions with timeout message as timeout error")
    void testCategorize_TimeoutByMessage() {
        RuntimeException ex = new RuntimeException("Request timeout exceeded");
        ErrorCategory category = ErrorCategory.categorize(ex);
        assertEquals(ErrorCategory.TIMEOUT_ERROR, category);
    }

    // ============================================================================
    // Validation Error Categorization Tests
    // ============================================================================

    @Test
    @DisplayName("Should categorize IllegalArgumentException as validation error")
    void testCategorize_IllegalArgumentException() {
        IllegalArgumentException ex = new IllegalArgumentException("Invalid argument");
        ErrorCategory category = ErrorCategory.categorize(ex);
        assertEquals(ErrorCategory.VALIDATION_ERROR, category);
    }

    @Test
    @DisplayName("Should categorize IllegalStateException as validation error")
    void testCategorize_IllegalStateException() {
        IllegalStateException ex = new IllegalStateException("Invalid state");
        ErrorCategory category = ErrorCategory.categorize(ex);
        assertEquals(ErrorCategory.VALIDATION_ERROR, category);
    }

    // ============================================================================
    // Resource Error Categorization Tests
    // ============================================================================

    @Test
    @DisplayName("Should categorize OutOfMemoryError as resource error")
    void testCategorize_OutOfMemoryError() {
        OutOfMemoryError ex = new OutOfMemoryError("Java heap space");
        ErrorCategory category = ErrorCategory.categorize(ex);
        assertEquals(ErrorCategory.RESOURCE_ERROR, category);
    }

    // ============================================================================
    // Null and Unknown Error Tests
    // ============================================================================

    @Test
    @DisplayName("Should return UNKNOWN for null exception")
    void testCategorize_Null() {
        ErrorCategory category = ErrorCategory.categorize(null);
        assertEquals(ErrorCategory.UNKNOWN, category);
    }

    @Test
    @DisplayName("Should return APPLICATION_ERROR for unclassified exceptions")
    void testCategorize_UnclassifiedException() {
        RuntimeException ex = new RuntimeException("Generic error");
        ErrorCategory category = ErrorCategory.categorize(ex);
        assertEquals(ErrorCategory.APPLICATION_ERROR, category);
    }

    // ============================================================================
    // Helper Method Tests
    // ============================================================================

    @Test
    @DisplayName("Should return correct string representation")
    void testToString() {
        ErrorCategory category = ErrorCategory.CONNECTION_ERROR;
        assertEquals("CONNECTION_ERROR", category.toString());
    }

    @ParameterizedTest
    @MethodSource("provideErrorCategories")
    @DisplayName("Should have non-empty name and description for all categories")
    void testAllCategoriesHaveNameAndDescription(ErrorCategory category) {
        assertNotNull(category.getName());
        assertNotNull(category.getDescription());
        assertFalse(category.getName().isEmpty());
        assertFalse(category.getDescription().isEmpty());
    }

    static Stream<Arguments> provideErrorCategories() {
        return Stream.of(ErrorCategory.values())
            .map(Arguments::of);
    }
}
