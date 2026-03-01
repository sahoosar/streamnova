package com.di.streamnova.agent.metrics;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;

/**
 * Status of an agent run (plan → recommend → execute). Tracks lifecycle for
 * reporting and correlation with estimates vs actuals.
 */
@Value
@Builder
public class ExecutionStatus {
    public static final String PLANNED = "PLANNED";
    public static final String RUNNING = "RUNNING";
    public static final String SUCCESS = "SUCCESS";
    public static final String FAILED = "FAILED";

    String runId;
    String profileRunId;
    String mode;
    String loadPattern;
    String sourceType;
    String schemaName;
    String tableName;
    String status;
    Instant startedAt;
    Instant finishedAt;
    Instant createdAt;
    String jobId;
    String message;
    /** Optional: id of the external agent that triggered this run (e.g. agent-1, agent-2). For tracking when multiple agents call the webhook. */
    String callerAgentId;

    public Instant getCreatedAt() {
        return createdAt != null ? createdAt : Instant.now();
    }
}
