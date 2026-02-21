package com.di.streamnova.agent.execution_engine;

import com.di.streamnova.agent.execution_planner.ExecutionPlanOption;
import com.di.streamnova.agent.recommender.Guardrails;
import com.di.streamnova.agent.workflow.PipelineTemplateService;
import com.di.streamnova.config.HandlerOverrides;
import com.di.streamnova.config.LoadConfig;
import com.di.streamnova.config.PipelineConfigService;
import com.di.streamnova.config.PipelineHandlerResolver;
import com.di.streamnova.config.PipelineHandlerSelection;
import com.di.streamnova.runner.DataflowRunnerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * ExecutionEngine implementation: delegates to DataflowRunnerService with the recommended
 * candidate. No Beam/PipelineOptions in the agent layer; the service builds options and runs the pipeline.
 * <p>
 * When using event configs (use-event-configs-only): call execute with the <b>same</b> source, intermediate,
 * and target used for GET actions-mapping and POST create-datasource, so the pipeline receives the same
 * merged event config and can trigger SOURCE_READ, INTERMEDIATE_WRITE, TARGET_WRITE in order.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ExecutionEngineService implements ExecutionEngine {

    private final DataflowRunnerService dataflowRunnerService;
    private final PipelineConfigService pipelineConfigService;
    private final PipelineTemplateService pipelineTemplateService;
    private final PipelineHandlerResolver pipelineHandlerResolver;

    @Value("${streamnova.guardrails.allowed-machine-types}")
    private String allowedMachineTypesConfig;

    private List<String> getAllowedMachineTypesList() {
        if (allowedMachineTypesConfig == null || allowedMachineTypesConfig.isBlank()) return List.of();
        return Arrays.stream(allowedMachineTypesConfig.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    @Override
    public ExecutionResult execute(ExecutionPlanOption candidate) {
        return execute(candidate, HandlerOverrides.none());
    }

    @Override
    public ExecutionResult execute(ExecutionPlanOption candidate, HandlerOverrides overrides) {
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
        List<String> allowed = getAllowedMachineTypesList();
        if (!allowed.isEmpty() && !Guardrails.isMachineTypeAllowed(candidate.getMachineType(), allowed)) {
            String msg = "machineType " + candidate.getMachineType() + " not in allowed list: " + allowed;
            log.warn("[EXECUTION] Rejected: {}", msg);
            return ExecutionResult.builder()
                    .success(false)
                    .jobId(null)
                    .message(msg)
                    .build();
        }
        HandlerOverrides effective = overrides != null ? overrides : HandlerOverrides.none();
        if (effective.getSource() != null || effective.getIntermediate() != null || effective.getTarget() != null) {
            log.info("[EXECUTION] Handler overrides for this run: source={}, intermediate={}, target={}", effective.getSource(), effective.getIntermediate(), effective.getTarget());
        }

        Map<String, String> selection = buildResolvedSelection(effective);
        int stages = (effective.getIntermediate() != null && !effective.getIntermediate().isBlank()) ? 3 : 2;

        try {
            Optional<String> jobId = runPipelineWithTemplateOrResolver(candidate, effective, null);
            return ExecutionResult.builder()
                    .success(true)
                    .jobId(jobId.orElse(null))
                    .message("Pipeline completed successfully")
                    .stages(stages)
                    .selection(selection)
                    .build();
        } catch (Exception e) {
            log.error("[EXECUTION] Pipeline run failed for candidate {}: {}", candidate.getLabelOrDefault(), e.getMessage(), e);
            return ExecutionResult.builder()
                    .success(false)
                    .jobId(null)
                    .message(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName())
                    .stages(stages)
                    .selection(selection)
                    .build();
        }
    }

    @Override
    public ExecutionResult execute(ExecutionPlanOption candidate, HandlerOverrides overrides, ExecuteRequest request) {
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
        List<String> allowed = getAllowedMachineTypesList();
        if (!allowed.isEmpty() && !Guardrails.isMachineTypeAllowed(candidate.getMachineType(), allowed)) {
            String msg = "machineType " + candidate.getMachineType() + " not in allowed list: " + allowed;
            log.warn("[EXECUTION] Rejected: {}", msg);
            return ExecutionResult.builder()
                    .success(false)
                    .jobId(null)
                    .message(msg)
                    .build();
        }
        HandlerOverrides effective = overrides != null ? overrides : HandlerOverrides.none();
        if (effective.getSource() != null || effective.getIntermediate() != null || effective.getTarget() != null) {
            log.info("[EXECUTION] Handler overrides for this run: source={}, intermediate={}, target={}", effective.getSource(), effective.getIntermediate(), effective.getTarget());
        }
        Map<String, String> selection = buildResolvedSelection(effective);
        int stages = (effective.getIntermediate() != null && !effective.getIntermediate().isBlank()) ? 3 : 2;
        try {
            Optional<String> jobId = runPipelineWithTemplateOrResolver(candidate, effective, request);
            return ExecutionResult.builder()
                    .success(true)
                    .jobId(jobId.orElse(null))
                    .message("Pipeline completed successfully")
                    .stages(stages)
                    .selection(selection)
                    .build();
        } catch (Exception e) {
            log.error("[EXECUTION] Pipeline run failed for candidate {}: {}", candidate.getLabelOrDefault(), e.getMessage(), e);
            return ExecutionResult.builder()
                    .success(false)
                    .jobId(null)
                    .message(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName())
                    .stages(stages)
                    .selection(selection)
                    .build();
        }
    }

    /**
     * Tries template-based config first (e.g. postgres→gcs); if no template or template returns null, uses resolver (event-config merge).
     */
    private Optional<String> runPipelineWithTemplateOrResolver(ExecutionPlanOption candidate, HandlerOverrides overrides, ExecuteRequest request) {
        if (pipelineTemplateService != null && overrides.getSource() != null && overrides.getTarget() != null) {
            LoadConfig templateConfig = pipelineTemplateService.resolveConfigFromTemplate(
                    overrides.getSource(), overrides.getIntermediate(), overrides.getTarget(),
                    candidate, request);
            if (templateConfig != null && pipelineHandlerResolver != null) {
                PipelineHandlerSelection selection = pipelineHandlerResolver.resolveWithLoadConfig(templateConfig);
                log.info("[EXECUTION] Using template-rendered config for source={}, target={}", overrides.getSource(), overrides.getTarget());
                return dataflowRunnerService.runPipeline(candidate, selection);
            }
        }
        return dataflowRunnerService.runPipeline(candidate, overrides);
    }

    /** Builds resolved selection map (source, intermediate, target keys) for execute response JSON. */
    private Map<String, String> buildResolvedSelection(HandlerOverrides overrides) {
        Map<String, String> map = new LinkedHashMap<>();
        String sourceKey = overrides.getSource() != null && !overrides.getSource().isBlank()
                ? overrides.getSource().trim()
                : (pipelineConfigService != null ? pipelineConfigService.getDefaultSourceKey() : null);
        String intermediateKey = overrides.getIntermediate() != null && !overrides.getIntermediate().isBlank()
                ? overrides.getIntermediate().trim()
                : null;
        String targetKey = overrides.getTarget() != null && !overrides.getTarget().isBlank()
                ? overrides.getTarget().trim()
                : (pipelineConfigService != null ? pipelineConfigService.getDefaultDestinationKey() : null);
        map.put("source", sourceKey);
        map.put("intermediate", intermediateKey);
        map.put("target", targetKey);
        return map;
    }
}
