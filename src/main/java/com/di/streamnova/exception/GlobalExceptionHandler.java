package com.di.streamnova.exception;

import com.di.streamnova.aspect.ErrorCategory;
import com.di.streamnova.util.TransactionEventLogger;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.di.streamnova.handler.jdbc.postgres.PostgresStatisticsEstimator;

import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Global exception handler for the application.
 * 
 * <p>This handler catches all unhandled exceptions and provides:
 * <ul>
 *   <li>Centralized exception handling</li>
 *   <li>Structured error logging using ErrorCategory</li>
 *   <li>Consistent error response format</li>
 *   <li>Integration with TransactionEventLogger</li>
 *   <li>Automatic error categorization</li>
 * </ul>
 * 
 * <p><strong>How It Works:</strong>
 * <ol>
 *   <li>Catches exceptions that escape other handlers</li>
 *   <li>Categorizes using {@link ErrorCategory}</li>
 *   <li>Logs structured error events</li>
 *   <li>Returns appropriate HTTP status and error details</li>
 * </ol>
 * 
 * <p><strong>Adding New Exception Handlers:</strong>
 * 
 * <p>To handle a specific exception type:
 * <pre>{@code
 * @ExceptionHandler(YourException.class)
 * @ResponseStatus(HttpStatus.BAD_REQUEST)
 * public ResponseEntity<ErrorResponse> handleYourException(YourException e) {
 *     ErrorCategory category = ErrorCategory.categorize(e);
 *     logError(category, e);
 *     return buildErrorResponse(category, e, HttpStatus.BAD_REQUEST);
 * }
 * }</pre>
 * 
 * @author StreamNova
 * @since 1.0
 */
@Slf4j
@ControllerAdvice
public class GlobalExceptionHandler {

    private final TransactionEventLogger eventLogger;

    @Autowired
    public GlobalExceptionHandler(TransactionEventLogger eventLogger) {
        this.eventLogger = eventLogger;
    }

    /**
     * Handles SQL exceptions (database errors).
     */
    @ExceptionHandler(SQLException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ResponseEntity<ErrorResponse> handleSqlException(SQLException e) {
        ErrorCategory category = ErrorCategory.categorize(e);
        logError("SQL_EXCEPTION", category, e);
        
        ErrorResponse errorResponse = buildErrorResponse(category, e, HttpStatus.INTERNAL_SERVER_ERROR);
        errorResponse.addDetail("sqlState", e.getSQLState());
        errorResponse.addDetail("errorCode", e.getErrorCode());
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }

    /**
     * Handles statistics estimation failures (connection, driver, validation).
     */
    @ExceptionHandler(PostgresStatisticsEstimator.StatisticsException.class)
    @ResponseStatus(HttpStatus.BAD_GATEWAY)
    public ResponseEntity<ErrorResponse> handleStatisticsException(PostgresStatisticsEstimator.StatisticsException e) {
        ErrorCategory category = ErrorCategory.categorize(e);
        logError("STATISTICS_EXCEPTION", category, e);
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(buildErrorResponse(category, e, HttpStatus.BAD_GATEWAY));
    }

    /**
     * Handles validation errors (IllegalArgumentException, IllegalStateException).
     */
    @ExceptionHandler({IllegalArgumentException.class, IllegalStateException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ResponseEntity<ErrorResponse> handleValidationException(Exception e) {
        ErrorCategory category = ErrorCategory.categorize(e);
        logError("VALIDATION_EXCEPTION", category, e);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(buildErrorResponse(category, e, HttpStatus.BAD_REQUEST));
    }

    /**
     * Handles network-related errors.
     */
    @ExceptionHandler({java.net.SocketTimeoutException.class, 
                      java.net.ConnectException.class,
                      java.net.UnknownHostException.class})
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public ResponseEntity<ErrorResponse> handleNetworkException(Exception e) {
        ErrorCategory category = ErrorCategory.categorize(e);
        logError("NETWORK_EXCEPTION", category, e);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(buildErrorResponse(category, e, HttpStatus.SERVICE_UNAVAILABLE));
    }

    /**
     * Handles timeout errors.
     */
    @ExceptionHandler(java.util.concurrent.TimeoutException.class)
    @ResponseStatus(HttpStatus.REQUEST_TIMEOUT)
    public ResponseEntity<ErrorResponse> handleTimeoutException(java.util.concurrent.TimeoutException e) {
        ErrorCategory category = ErrorCategory.categorize(e);
        logError("TIMEOUT_EXCEPTION", category, e);
        return ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body(buildErrorResponse(category, e, HttpStatus.REQUEST_TIMEOUT));
    }

    /**
     * Handles resource errors (OutOfMemoryError, file system errors).
     */
    @ExceptionHandler({java.lang.OutOfMemoryError.class,
                      java.nio.file.FileSystemException.class,
                      java.io.FileNotFoundException.class})
    @ResponseStatus(HttpStatus.INSUFFICIENT_STORAGE)
    public ResponseEntity<ErrorResponse> handleResourceException(Exception e) {
        ErrorCategory category = ErrorCategory.categorize(e);
        logError("RESOURCE_EXCEPTION", category, e);
        return ResponseEntity.status(HttpStatus.INSUFFICIENT_STORAGE).body(buildErrorResponse(category, e, HttpStatus.INSUFFICIENT_STORAGE));
    }

    /**
     * Handles all other unhandled exceptions (catch-all).
     */
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception e) {
        ErrorCategory category = ErrorCategory.categorize(e);
        logError("UNHANDLED_EXCEPTION", category, e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(buildErrorResponse(category, e, HttpStatus.INTERNAL_SERVER_ERROR));
    }

    /**
     * Handles throwable errors (including Error types like OutOfMemoryError).
     */
    @ExceptionHandler(Throwable.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ResponseEntity<ErrorResponse> handleThrowable(Throwable e) {
        ErrorCategory category = ErrorCategory.categorize(e);
        logError("THROWABLE_ERROR", category, e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(buildErrorResponse(category, e, HttpStatus.INTERNAL_SERVER_ERROR));
    }

    /**
     * Logs the error using TransactionEventLogger with full context.
     */
    private void logError(String eventType, ErrorCategory category, Throwable exception) {
        String transactionId = MDC.get("jobId");
        if (transactionId == null) {
            transactionId = MDC.get("transactionId");
        }
        if (transactionId == null) {
            transactionId = "global-handler-" + UUID.randomUUID().toString().substring(0, 8);
        }

        Map<String, Object> context = new HashMap<>();
        context.put("errorMessage", exception.getMessage() != null ? exception.getMessage() : exception.getClass().getSimpleName());
        context.put("errorType", exception.getClass().getName());
        context.put("errorCategory", category.name());
        context.put("errorCategoryName", category.getName());
        context.put("errorCategoryDescription", category.getDescription());
        context.put("handler", "GlobalExceptionHandler");
        context.put("threadId", Thread.currentThread().getId());
        context.put("threadName", Thread.currentThread().getName());

        // Add root cause information
        Throwable rootCause = getRootCause(exception);
        if (rootCause != exception) {
            context.put("rootCauseType", rootCause.getClass().getSimpleName());
            context.put("rootCauseMessage", rootCause.getMessage());
        }

        // Add SQL-specific details if applicable
        if (exception instanceof SQLException) {
            SQLException sqlEx = (SQLException) exception;
            context.put("sqlState", sqlEx.getSQLState());
            context.put("errorCode", sqlEx.getErrorCode());
        }

        eventLogger.logEvent(
            eventType,
            context,
            transactionId,
            Thread.currentThread(),
            "global_exception_handler",
            "StreamNova", // Will be overridden by actual applicationId in eventLogger
            exception
        );

        // Also log using SLF4J for immediate visibility
        log.error("GlobalExceptionHandler caught exception: {} [{}]", 
                exception.getClass().getSimpleName(), 
                category.getName(), 
                exception);
    }

    /**
     * Builds a structured error response.
     */
    private ErrorResponse buildErrorResponse(ErrorCategory category, Throwable exception, HttpStatus status) {
        ErrorResponse response = new ErrorResponse();
        response.setTimestamp(Instant.now().toString());
        response.setStatus(status.value());
        response.setError(status.getReasonPhrase());
        response.setMessage(exception.getMessage() != null ? exception.getMessage() : exception.getClass().getSimpleName());
        response.setErrorCategory(category.name());
        response.setErrorCategoryName(category.getName());
        response.setErrorCategoryDescription(category.getDescription());
        response.setPath(getRequestPath());
        
        // Add exception type
        response.addDetail("exceptionType", exception.getClass().getName());
        
        // Add root cause if different
        Throwable rootCause = getRootCause(exception);
        if (rootCause != exception) {
            response.addDetail("rootCauseType", rootCause.getClass().getName());
            response.addDetail("rootCauseMessage", rootCause.getMessage());
        }

        return response;
    }

    /**
     * Gets the root cause of an exception.
     */
    private Throwable getRootCause(Throwable exception) {
        if (exception == null) {
            return null;
        }
        Throwable cause = exception.getCause();
        if (cause == null || cause == exception) {
            return exception;
        }
        return getRootCause(cause);
    }

    /**
     * Gets the request path from MDC or returns default.
     */
    private String getRequestPath() {
        String path = MDC.get("requestPath");
        if (path == null) {
            path = MDC.get("path");
        }
        return path != null ? path : "/unknown";
    }

    /**
     * Structured error response for API endpoints.
     */
    public static class ErrorResponse {
        private String timestamp;
        private int status;
        private String error;
        private String message;
        private String errorCategory;
        private String errorCategoryName;
        private String errorCategoryDescription;
        private String path;
        private Map<String, Object> details = new HashMap<>();

        public String getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(String timestamp) {
            this.timestamp = timestamp;
        }

        public int getStatus() {
            return status;
        }

        public void setStatus(int status) {
            this.status = status;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getErrorCategory() {
            return errorCategory;
        }

        public void setErrorCategory(String errorCategory) {
            this.errorCategory = errorCategory;
        }

        public String getErrorCategoryName() {
            return errorCategoryName;
        }

        public void setErrorCategoryName(String errorCategoryName) {
            this.errorCategoryName = errorCategoryName;
        }

        public String getErrorCategoryDescription() {
            return errorCategoryDescription;
        }

        public void setErrorCategoryDescription(String errorCategoryDescription) {
            this.errorCategoryDescription = errorCategoryDescription;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public Map<String, Object> getDetails() {
            return details;
        }

        public void setDetails(Map<String, Object> details) {
            this.details = details;
        }

        public void addDetail(String key, Object value) {
            this.details.put(key, value);
        }
    }
}
