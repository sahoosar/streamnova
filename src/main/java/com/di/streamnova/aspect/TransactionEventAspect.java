package com.di.streamnova.aspect;

import com.di.streamnova.util.TransactionEventLogger;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Generic AOP Aspect for automatic transaction/operation event logging.
 * 
 * Automatically logs events when methods annotated with @LogTransaction are called.
 * Supports MDC (Mapped Diagnostic Context) for transaction tracking.
 * 
 * This is a generic mechanism that can be used for any transaction or operation:
 * - Data loading operations
 * - Database operations
 * - API calls
 * - Business transactions
 * - Any method that needs event logging
 * 
 * Features:
 * - Automatic event logging via annotations
 * - MDC support (reads and preserves transactionId, jobId, etc.)
 * - Method parameter extraction for context
 * - Exception handling with error events
 * - Duration tracking
 * - Error categorization
 * - Root cause analysis
 */
@Slf4j
@Aspect
@Component
public class TransactionEventAspect {

    private final String applicationId;
    private final TransactionEventLogger eventLogger;

    @Autowired
    public TransactionEventAspect(TransactionEventLogger eventLogger,
                                  @Value("${spring.application.name}") String applicationName) {
        this.eventLogger = eventLogger;
        this.applicationId = applicationName + "-" + UUID.randomUUID().toString().substring(0, 15);
        log.info("TransactionEventAspect initialized with applicationId: {}", applicationId);
    }

    /**
     * Intercepts methods annotated with @LogTransaction.
     * Automatically logs start, completion, and failure events.
     */
    @Around("@annotation(com.di.streamnova.aspect.LogTransaction)")
    public Object logTransaction(ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        LogTransaction annotation = method.getAnnotation(LogTransaction.class);
        
        String eventType = annotation.eventType();
        String transactionContext = annotation.transactionContext();
        String transactionIdKey = annotation.transactionIdKey();
        long startTime = System.currentTimeMillis();
        
        // Get MDC context (thread-local, automatically available)
        String transactionId = MDC.get(transactionIdKey);
        if (transactionId == null) {
            transactionId = MDC.get("jobId"); // Fallback to jobId
        }
        Thread currentThread = Thread.currentThread();
        
        // Extract method parameters for context
        Map<String, Object> context = extractContext(joinPoint, method, annotation);
        
        // Log transaction started event
        eventLogger.logEvent(
            eventType + "_STARTED", 
            context, 
            transactionId, 
            currentThread, 
            transactionContext,
            applicationId
        );
        
        try {
            // Execute the method
            Object result = joinPoint.proceed();
            
            // Calculate duration
            long durationMs = System.currentTimeMillis() - startTime;
            
            // Add result information to context
            if (result != null && annotation.includeResult()) {
                context.put("resultType", result.getClass().getSimpleName());
            }
            context.put("durationMs", durationMs);
            context.put("durationSeconds", durationMs / 1000.0);
            
            // Log transaction completed event
            eventLogger.logEvent(
                eventType + "_COMPLETED", 
                context, 
                transactionId, 
                currentThread, 
                transactionContext,
                applicationId
            );
            
            return result;
            
        } catch (Throwable e) {
            // Calculate duration before failure
            long durationMs = System.currentTimeMillis() - startTime;
            
            // Add error information to context
            ErrorCategory errorCategory = ErrorCategory.categorize(e);
            context.put("errorMessage", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            context.put("errorType", e.getClass().getSimpleName());
            context.put("errorCategory", errorCategory.name());
            context.put("errorCategoryName", errorCategory.getName());
            context.put("errorCategoryDescription", errorCategory.getDescription());
            context.put("durationMs", durationMs);
            context.put("durationSeconds", durationMs / 1000.0);
            
            // Add root cause information
            Throwable rootCause = getRootCause(e);
            if (rootCause != e) {
                context.put("rootCauseType", rootCause.getClass().getSimpleName());
                context.put("rootCauseMessage", rootCause.getMessage());
            }
            
            // Add SQL-specific error details
            if (e instanceof SQLException) {
                SQLException sqlEx = (SQLException) e;
                context.put("sqlState", sqlEx.getSQLState());
                context.put("errorCode", sqlEx.getErrorCode());
                if (sqlEx.getNextException() != null) {
                    context.put("hasChainedExceptions", true);
                }
            }
            
            // Log transaction failed event with full error details
            eventLogger.logEvent(
                eventType + "_FAILED", 
                context, 
                transactionId, 
                currentThread, 
                transactionContext,
                applicationId,
                e
            );
            
            // Re-throw the exception
            throw e;
        }
    }

    /**
     * Extracts context from method parameters based on annotation configuration.
     */
    private Map<String, Object> extractContext(ProceedingJoinPoint joinPoint, Method method, LogTransaction annotation) {
        Map<String, Object> context = new HashMap<>();
        
        Object[] args = joinPoint.getArgs();
        Parameter[] parameters = method.getParameters();
        String[] parameterNames = annotation.parameterNames();
        
        // Extract parameters by name if specified
        if (parameterNames.length > 0) {
            for (int i = 0; i < Math.min(parameters.length, parameterNames.length); i++) {
                if (i < args.length && parameterNames[i] != null && !parameterNames[i].isEmpty()) {
                    Object value = args[i];
                    String paramName = parameterNames[i].toLowerCase();
                    // Sanitize sensitive parameter names (check parameter name, not value)
                    // This prevents false positives from values containing "password" as substring
                    if (paramName.contains("password") || paramName.contains("pwd") || 
                        paramName.contains("secret") || paramName.contains("credential")) {
                        context.put(parameterNames[i], "***");
                    } else {
                        // Use sanitizeValue which handles JDBC URLs correctly
                        context.put(parameterNames[i], sanitizeValue(value));
                    }
                }
            }
        } else {
            // Extract by parameter name (if available via reflection)
            for (int i = 0; i < parameters.length && i < args.length; i++) {
                String paramName = parameters[i].getName();
                String paramNameLower = paramName.toLowerCase();
                // Skip common parameters that aren't useful
                if (!paramName.equals("pipeline") && !paramName.equals("config")) {
                    // Sanitize sensitive parameter names (check parameter name, not value)
                    if (paramNameLower.contains("password") || paramNameLower.contains("pwd") || 
                        paramNameLower.contains("secret") || paramNameLower.contains("credential")) {
                        context.put(paramName, "***");
                    } else {
                        context.put(paramName, sanitizeValue(args[i]));
                    }
                }
            }
        }
        
        // Add method information
        context.put("method", method.getName());
        context.put("className", method.getDeclaringClass().getSimpleName());
        
        return context;
    }

    /**
     * Sanitizes values for logging (removes sensitive information).
     */
    private Object sanitizeValue(Object value) {
        if (value == null) {
            return null;
        }
        
        if (value instanceof String) {
            String str = (String) value;
            // Sanitize JDBC URLs
            if (str.startsWith("jdbc:")) {
                return sanitizeJdbcUrl(str);
            }
        }
        
        return value;
    }

    /**
     * Sanitizes JDBC URL for logging.
     */
    private String sanitizeJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            return "null";
        }
        String sanitized = jdbcUrl;
        if (sanitized.contains("password=")) {
            int passwordIndex = sanitized.indexOf("password=");
            int endIndex = sanitized.indexOf("&", passwordIndex);
            if (endIndex == -1) {
                endIndex = sanitized.length();
            }
            sanitized = sanitized.substring(0, passwordIndex + 9) + "***" + 
                       (endIndex < sanitized.length() ? sanitized.substring(endIndex) : "");
        }
        return sanitized;
    }

    /**
     * Gets the root cause of an exception by traversing the exception chain.
     * 
     * @param exception the exception to analyze
     * @return the root cause exception, or the original exception if no cause exists
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
}
