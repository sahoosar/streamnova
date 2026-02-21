package com.di.streamnova.util;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Generic transaction event logger for any operation or transaction.
 * 
 * Logs structured events at key milestones during any transaction:
 * - Transaction started
 * - Transaction completed
 * - Transaction failed
 * 
 * This is a generic mechanism that can be used for:
 * - Data loading operations
 * - Database operations
 * - API calls
 * - Business transactions
 * - Any operation that needs event logging
 * 
 * Supports:
 * - Multi-application: Each application has unique applicationId
 * - Multi-threading: Each thread has unique threadId and threadName
 * - Unique transaction tracking: Each transaction has unique transactionId (from MDC)
 * - Timestamps: ISO 8601 formatted timestamps
 */
@Slf4j
@Component
public class TransactionEventLogger {

    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT;
    
    // Application ID - unique per application instance
    private final String applicationId;
    
    /**
     * Constructor with optional application name injection.
     * If not provided, generates a unique ID per instance.
     */
    public TransactionEventLogger(
            @Value("${spring.application.name}") String applicationName) {
        // Generate unique application ID: appName-instanceId
        // This ensures each application instance has a unique ID
        this.applicationId = applicationName + "-" + UUID.randomUUID().toString().substring(0, 8);
        log.info("[TX] TransactionEventLogger initialized with applicationId: {}", applicationId);
    }

    /**
     * Generic method to log any transaction event.
     * 
     * Thread-safe: Each thread's MDC context is automatically used.
     * Multi-application: Each application instance has unique applicationId.
     * 
     * @param eventType The type of event (e.g., "POSTGRES_READ_STARTED", "DATA_PROCESSING_COMPLETED")
     * @param context Additional context data for the event
     * @param transactionId Transaction ID from MDC (e.g., jobId, transactionId)
     * @param thread Current thread
     * @param transactionContext Context about the transaction (e.g., "postgres_read", "data_processing")
     * @param applicationId Application ID
     */
    public void logEvent(String eventType, Map<String, Object> context, String transactionId,
                        Thread thread, String transactionContext, String applicationId) {
        logEvent(eventType, context, transactionId, thread, transactionContext, applicationId, null);
    }

    /**
     * Generic method to log any transaction event with exception.
     * 
     * @param eventType The type of event
     * @param context Additional context data
     * @param transactionId Transaction ID from MDC
     * @param thread Current thread
     * @param transactionContext Context about the transaction
     * @param applicationId Application ID
     * @param exception Exception if any (for error events)
     */
    public void logEvent(String eventType, Map<String, Object> context, String transactionId,
                        Thread thread, String transactionContext, String applicationId, Throwable exception) {
        Map<String, Object> event = new HashMap<>();
        event.put("eventType", eventType);
        event.put("timestamp", ISO_FORMATTER.format(Instant.now()));
        event.put("applicationId", applicationId);
        event.put("transactionId", transactionId != null ? transactionId : "unknown");
        event.put("threadId", thread.getId());
        event.put("threadName", thread.getName());
        
        if (transactionContext != null && !transactionContext.isEmpty()) {
            context.put("transactionContext", transactionContext);
        }
        
        if (exception != null) {
            // Add stack trace summary (first 5 lines)
            String stackTrace = getStackTraceSummary(exception, 5);
            if (stackTrace != null && !stackTrace.isEmpty()) {
                context.put("stackTraceSummary", stackTrace);
            }
        }
        
        if (context != null && !context.isEmpty()) {
            event.put("context", context);
        }
        
        // Log as structured JSON-like format for easy parsing
        String eventJson = formatEvent(event);
        log.info("[TX] EVENT: {}", eventJson);
    }

    /**
     * Formats event as JSON-like string for logging.
     */
    private String formatEvent(Map<String, Object> event) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : event.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append("\"").append(entry.getKey()).append("\": ");
            Object value = entry.getValue();
            if (value instanceof String) {
                sb.append("\"").append(escapeJson((String) value)).append("\"");
            } else if (value instanceof Number || value instanceof Boolean) {
                sb.append(value);
            } else if (value instanceof Map) {
                sb.append(formatMap((Map<?, ?>) value));
            } else {
                sb.append("\"").append(escapeJson(String.valueOf(value))).append("\"");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Formats a nested map for JSON-like output.
     */
    private String formatMap(Map<?, ?> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append("\"").append(entry.getKey()).append("\": ");
            Object value = entry.getValue();
            if (value instanceof String) {
                sb.append("\"").append(escapeJson((String) value)).append("\"");
            } else if (value instanceof Number || value instanceof Boolean) {
                sb.append(value);
            } else {
                sb.append("\"").append(escapeJson(String.valueOf(value))).append("\"");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Escapes special characters for JSON-like output.
     */
    private String escapeJson(String str) {
        if (str == null) {
            return "";
        }
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }

    /**
     * Gets a summary of the stack trace (first N lines).
     */
    private String getStackTraceSummary(Throwable exception, int maxLines) {
        if (exception == null) {
            return null;
        }
        
        try {
            java.io.StringWriter sw = new java.io.StringWriter();
            java.io.PrintWriter pw = new java.io.PrintWriter(sw);
            exception.printStackTrace(pw);
            String fullTrace = sw.toString();
            
            String[] lines = fullTrace.split("\n");
            int linesToInclude = Math.min(maxLines, lines.length);
            StringBuilder summary = new StringBuilder();
            
            for (int i = 0; i < linesToInclude; i++) {
                if (i > 0) summary.append(" | ");
                summary.append(lines[i].trim());
            }
            
            if (lines.length > maxLines) {
                summary.append(" | ... (").append(lines.length - maxLines).append(" more lines)");
            }
            
            return summary.toString();
        } catch (Exception e) {
            // Fallback if stack trace extraction fails
            return exception.getClass().getName() + ": " + exception.getMessage();
        }
    }
}
