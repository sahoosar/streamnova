package com.di.streamnova.agent.execution_engine;

import com.di.streamnova.agent.adaptive_execution_planner.ExecutionPlanOption;
import com.di.streamnova.runner.DataflowRunnerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * ExecutionEngine implementation: delegates to DataflowRunnerService with the recommended
 * candidate. No Beam/PipelineOptions in the agent layer; the service builds options and runs the pipeline.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ExecutionEngineService implements ExecutionEngine {

    private final DataflowRunnerService dataflowRunnerService;

    @Override
    public ExecutionResult execute(ExecutionPlanOption candidate) {
        if (candidate == null) {
            return ExecutionResult.builder()
                    .success(false)
                    .jobId(null)
                    .message("Invalid candidate: null")
                    .build();
        }
        if (candidate.getWorkerCount() < 1) {
            return ExecutionResult.builder()
                    .success(false)
                    .jobId(null)
                    .message("Invalid candidate: workerCount must be >= 1")
                    .build();
        }
        try {
            Optional<String> jobId = dataflowRunnerService.runPipeline(candidate);
            return ExecutionResult.builder()
                    .success(true)
                    .jobId(jobId.orElse(null))
                    .message("Pipeline completed successfully")
                    .build();
        } catch (Exception e) {
            log.error("[EXECUTION] Pipeline run failed for candidate {}: {}", candidate.getLabelOrDefault(), e.getMessage(), e);
            return ExecutionResult.builder()
                    .success(false)
                    .jobId(null)
                    .message(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName())
                    .build();
        }
    }
}
