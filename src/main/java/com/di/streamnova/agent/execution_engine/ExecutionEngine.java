package com.di.streamnova.agent.execution_engine;

import com.di.streamnova.agent.adaptive_execution_planner.ExecutionPlanOption;

/**
 * Contract for the agent Execution Engine: take a recommended plan (ExecutionPlanOption)
 * and run a Dataflow/Beam job with that configuration.
 *
 * <p><b>Status:</b> Not yet implemented. Dataflow job execution already exists in
 * {@link com.di.streamnova.runner.DataflowRunnerService} (Beam pipeline run from YAML config).
 * This engine should (1) accept a ExecutionPlanOption from the Recommender, (2) build
 * PipelineOptions (e.g. DataflowPipelineOptions with workerMachineType, maxNumWorkers),
 * (3) invoke the runner with those options so the job uses the recommended machine type,
 * workers, and shards. See EXECUTION_ENGINE_STATUS.md in the project root.
 */
public interface ExecutionEngine {

    /**
     * Execute a load job with the given candidate (machine type, workers, shards).
     * Implementation should build PipelineOptions from the candidate and call
     * DataflowRunnerService (or equivalent) with those options.
     *
     * @param candidate recommended plan from the Recommender
     * @return execution result (job id, status, or throw on failure)
     */
    ExecutionResult execute(ExecutionPlanOption candidate);
}
