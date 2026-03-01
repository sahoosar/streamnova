package com.di.streamnova.agent.metrics;

import java.util.List;
import java.util.Optional;

/**
 * Persistence for Metrics & Learning Store: estimates vs actuals, throughput profiles,
 * execution status. Implementations can be in-memory or JDBC (see AGENT_TABLES_SCHEMA.sql).
 */
public interface MetricsLearningStore {

    // --- Estimates vs actuals ---
    void saveEstimateVsActual(EstimateVsActual record);
    List<EstimateVsActual> findEstimatesVsActualsByRunId(String runId);
    List<EstimateVsActual> findRecentEstimatesVsActuals(String sourceType, String schemaName, String tableName, int limit);

    // --- Throughput profiles ---
    void saveThroughputProfile(ThroughputProfile profile);
    Optional<ThroughputProfile> findThroughputProfileByRunId(String runId);
    List<ThroughputProfile> findRecentThroughputProfiles(String sourceType, String schemaName, String tableName, int limit);

    // --- Execution status ---
    void saveExecutionStatus(ExecutionStatus status);
    void updateExecutionStatus(String runId, String status, java.time.Instant finishedAt, String jobId, String message);
    Optional<ExecutionStatus> findExecutionStatusByRunId(String runId);
    List<ExecutionStatus> findRecentExecutionStatuses(int limit);
    List<ExecutionStatus> findExecutionStatusesByStatus(String status, int limit);
    /** Find runs triggered by a given caller agent (e.g. agent-1). Empty or null callerAgentId returns recent runs. */
    List<ExecutionStatus> findExecutionStatusesByCallerAgentId(String callerAgentId, int limit);
}
