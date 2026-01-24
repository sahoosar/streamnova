package com.di.streamnova.aspect;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Generic annotation to mark methods for automatic transaction/operation event logging.
 * 
 * When a method is annotated with @LogTransaction, the TransactionEventAspect
 * will automatically:
 * - Log transaction started event before method execution
 * - Log transaction completed event after successful execution
 * - Log transaction failed event if exception occurs
 * - Extract method parameters for event context
 * - Track duration and performance metrics
 * - Preserve MDC context (jobId, transactionId, etc.)
 * 
 * This is a generic mechanism that can be used for any transaction or operation.
 * 
 * Example:
 * <pre>
 * {@code
 * @LogTransaction(
 *     eventType = "POSTGRES_READ",
 *     transactionContext = "postgres_read",
 *     parameterNames = {"tableName", "jdbcUrl"}
 * )
 * public PCollection<Row> read(Pipeline pipeline, PipelineConfigSource config) {
 *     // Method implementation - events logged automatically!
 * }
 * }
 * </pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface LogTransaction {
    
    /**
     * The event type prefix (e.g., "POSTGRES_READ" will generate 
     * "POSTGRES_READ_STARTED", "POSTGRES_READ_COMPLETED", "POSTGRES_READ_FAILED").
     */
    String eventType();
    
    /**
     * Context about what transaction/operation is being performed 
     * (e.g., "postgres_read", "schema_detection", "statistics_estimation", "data_processing").
     */
    String transactionContext() default "";
    
    /**
     * Names of parameters to include in event context.
     * If empty, all parameters will be included (except common ones like "pipeline", "config").
     */
    String[] parameterNames() default {};
    
    /**
     * Whether to include result information in completed event.
     */
    boolean includeResult() default false;
    
    /**
     * Whether to extract transaction ID from MDC (e.g., "transactionId", "jobId").
     * If true, will look for transactionId in MDC, otherwise uses jobId.
     */
    String transactionIdKey() default "jobId";
}
