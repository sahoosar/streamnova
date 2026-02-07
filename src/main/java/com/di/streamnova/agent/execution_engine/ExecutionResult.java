package com.di.streamnova.agent.execution_engine;

import lombok.Builder;
import lombok.Value;

/**
 * Result of an execution request (for when ExecutionEngine is implemented).
 */
@Value
@Builder
public class ExecutionResult {
    boolean success;
    String jobId;
    String message;
}
