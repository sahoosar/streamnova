package com.di.streamnova.agent.execution_engine;

import com.di.streamnova.agent.execution_planner.ExecutionPlanOption;
import com.di.streamnova.agent.workflow.PipelineTemplateService;
import com.di.streamnova.config.HandlerOverrides;
import com.di.streamnova.config.LoadConfig;
import com.di.streamnova.config.PipelineConfigService;
import com.di.streamnova.config.PipelineHandlerResolver;
import com.di.streamnova.config.PipelineHandlerSelection;
import com.di.streamnova.guardrail.GuardrailService;      // ← replaces @Value + parseAllowedMachineTypes
import com.di.streamnova.runner.DataflowRunnerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * ExecutionEngine implementation — delegates to DataflowRunnerService with the recommended candidate.
 *
 * <p>Machine-type guardrail enforcement has been extracted to {@link GuardrailService}.
 * The duplicate {@code @Value allowedMachineTypesConfig} field and
 * {@code getAllowedMachineTypesList()} helper have been removed; one call to
 * {@link GuardrailService#assertMachineTypeAllowed(String)} replaces them both,
 * controlled by {@code streamnova.guardrails.enforce-on-execute} in YAML.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ExecutionEngineService implements ExecutionEngine {

    private final DataflowRunnerService   dataflowRunnerService;
    private final PipelineConfigService   pipelineConfigService;
    private final PipelineTemplateService pipelineTemplateService;
    private final PipelineHandlerResolver pipelineHandlerResolver;
    private final GuardrailService        guardrailService;   // replaces @Value + helper method

    @Override
    public ExecutionResult execute(ExecutionPlanOption candidate) {
        return execute(candidate, HandlerOverrides.none());
    }

    @Override
    public ExecutionResult execute(ExecutionPlanOption candidate, HandlerOverrides overrides) {
        ExecutionResult validation = validateCandidate(candidate);
        if (validation != null) return validation;

        HandlerOverrides effective = overrides != null ? overrides : HandlerOverrides.none();
        logOverrides(effective);

        Map<String, String> selection = buildResolvedSelection(effective);
        int stages = stageCount(effective);

        try {
            Optional<String> jobId = runPipelineWithTemplateOrResolver(candidate, effective, null);
            return ExecutionResult.builder()
                    .success(true).jobId(jobId.orElse(null))
                    .message("Pipeline completed successfully")
                    .stages(stages).selection(selection).build();
        } catch (Exception e) {
            log.error("[EXECUTION] Pipeline run failed for candidate {}: {}",
                    candidate.getLabelOrDefault(), e.getMessage(), e);
            return ExecutionResult.builder()
                    .success(false).jobId(null)
                    .message(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName())
                    .stages(stages).selection(selection).build();
        }
    }

    @Override
    public ExecutionResult execute(ExecutionPlanOption candidate, HandlerOverrides overrides,
                                   ExecuteRequest request) {
        ExecutionResult validation = validateCandidate(candidate);
        if (validation != null) return validation;

        HandlerOverrides effective = overrides != null ? overrides : HandlerOverrides.none();
        logOverrides(effective);

        Map<String, String> selection = buildResolvedSelection(effective);
        int stages = stageCount(effective);

        try {
            Optional<String> jobId = runPipelineWithTemplateOrResolver(candidate, effective, request);
            return ExecutionResult.builder()
                    .success(true).jobId(jobId.orElse(null))
                    .message("Pipeline completed successfully")
                    .stages(stages).selection(selection).build();
        } catch (Exception e) {
            log.error("[EXECUTION] Pipeline run failed for candidate {}: {}",
                    candidate.getLabelOrDefault(), e.getMessage(), e);
            return ExecutionResult.builder()
                    .success(false).jobId(null)
                    .message(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName())
                    .stages(stages).selection(selection).build();
        }
    }

    // ------------------------------------------------------------------ //
    // Private helpers                                                     //
    // ------------------------------------------------------------------ //

    /**
     * Common candidate validation: null check, workerCount, machine-type guardrail.
     * Returns a failure result when invalid; null when valid.
     */
    private ExecutionResult validateCandidate(ExecutionPlanOption candidate) {
        if (candidate == null) {
            return ExecutionResult.builder().success(false).jobId(null)
                    .message("Invalid candidate: null").build();
        }
        if (candidate.getWorkerCount() < 1) {
            return ExecutionResult.builder().success(false).jobId(null)
                    .message("Invalid candidate: workerCount must be >= 1").build();
        }
        // Delegate machine-type enforcement to GuardrailService (YAML-driven)
        // Throws GuardrailViolationException → caught by GlobalExceptionHandler → 400
        guardrailService.assertMachineTypeAllowed(candidate.getMachineType());
        return null; // valid
    }

    /** Tries template-based config first; falls back to event-config merge. */
    private Optional<String> runPipelineWithTemplateOrResolver(
            ExecutionPlanOption candidate, HandlerOverrides overrides, ExecuteRequest request) {
        if (pipelineTemplateService != null
                && overrides.getSource() != null && overrides.getTarget() != null) {
            LoadConfig templateConfig = pipelineTemplateService.resolveConfigFromTemplate(
                    overrides.getSource(), overrides.getIntermediate(), overrides.getTarget(),
                    candidate, request);
            if (templateConfig != null && pipelineHandlerResolver != null) {
                PipelineHandlerSelection selection =
                        pipelineHandlerResolver.resolveWithLoadConfig(templateConfig);
                log.info("[EXECUTION] Template-rendered config for source={} target={}",
                        overrides.getSource(), overrides.getTarget());
                return dataflowRunnerService.runPipeline(candidate, selection);
            }
        }
        return dataflowRunnerService.runPipeline(candidate, overrides);
    }

    private Map<String, String> buildResolvedSelection(HandlerOverrides overrides) {
        Map<String, String> map = new LinkedHashMap<>();
        String sourceKey = overrides.getSource() != null && !overrides.getSource().isBlank()
                ? overrides.getSource().trim()
                : (pipelineConfigService != null ? pipelineConfigService.getDefaultSourceKey() : null);
        String intermediateKey = overrides.getIntermediate() != null
                && !overrides.getIntermediate().isBlank()
                ? overrides.getIntermediate().trim() : null;
        String targetKey = overrides.getTarget() != null && !overrides.getTarget().isBlank()
                ? overrides.getTarget().trim()
                : (pipelineConfigService != null ? pipelineConfigService.getDefaultDestinationKey() : null);
        map.put("source", sourceKey);
        map.put("intermediate", intermediateKey);
        map.put("target", targetKey);
        return map;
    }

    private static int stageCount(HandlerOverrides overrides) {
        return (overrides.getIntermediate() != null
                && !overrides.getIntermediate().isBlank()) ? 3 : 2;
    }

    private static void logOverrides(HandlerOverrides eff) {
        if (eff.getSource() != null || eff.getIntermediate() != null || eff.getTarget() != null) {
            log.info("[EXECUTION] Handler overrides: source={} intermediate={} target={}",
                    eff.getSource(), eff.getIntermediate(), eff.getTarget());
        }
    }
}
