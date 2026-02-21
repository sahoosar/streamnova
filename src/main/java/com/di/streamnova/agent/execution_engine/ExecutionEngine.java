package com.di.streamnova.agent.execution_engine;

import com.di.streamnova.agent.execution_planner.ExecutionPlanOption;
import com.di.streamnova.config.HandlerOverrides;

/**
 * Contract for the agent Execution Engine: take a recommended plan (ExecutionPlanOption)
 * and run a Dataflow/Beam job with that configuration.
 *
 * <p>Handler selection: pass optional {@link HandlerOverrides} (source, intermediate, target)
 * to choose which handlers to use when multiple are present (2-stage: source + target; 3-stage: source + intermediate + target).
 */
public interface ExecutionEngine {

    /**
     * Execute with config default handlers (no overrides).
     */
    ExecutionResult execute(ExecutionPlanOption candidate);

    /**
     * Execute with optional handler overrides so the user/agent can choose source, intermediate, and/or target for this run.
     */
    ExecutionResult execute(ExecutionPlanOption candidate, HandlerOverrides overrides);

    /**
     * Execute with overrides and optional request (schema, table, extraction params) for template-based execution.
     * When a template applies (e.g. postgres→gcs), config is built from the template and request; otherwise uses event-config merge.
     */
    ExecutionResult execute(ExecutionPlanOption candidate, HandlerOverrides overrides, ExecuteRequest request);
}
