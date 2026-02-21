package com.di.streamnova.config;

import com.di.streamnova.handler.SourceHandler;
import com.di.streamnova.handler.SourceHandlerRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Resolves pipeline handler selection from config and optional overrides.
 * No fixed handler for source, intermediate, or target — user selects per stage (2-stage: source + target; 3-stage: source + intermediate + target).
 * Validation is type-driven (e.g. gcs → path; bigquery → dataset+table) for intermediate and target.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PipelineHandlerResolver {

    private final PipelineConfigService pipelineConfigService;
    private final EventConfigLoaderService eventConfigLoaderService;
    private final PipelineConfigFilesProperties configFilesProperties;
    private final SourceHandlerRegistry sourceHandlerRegistry;

    /**
     * Resolves which handlers to use for this run from config and optional overrides.
     * When use-event-configs-only: config is merged from database_event_config.yml, gcs_event_config.yml, bq_event_config.yml.
     * Otherwise: config from streamnova.pipeline.config-file (single pipeline config YAML).
     *
     * @param sourceKeyOverride      source config key (e.g. postgres, gcs); required when use-event-configs-only
     * @param intermediateKeyOverride optional intermediate key (e.g. gcs) for 3-stage
     * @param targetKeyOverride      destination config key (e.g. bigquery, gcs); required when use-event-configs-only
     */
    public PipelineHandlerSelection resolve(String sourceKeyOverride, String intermediateKeyOverride, String targetKeyOverride) {
        log.info("[HANDLER-SELECTION] User provided: source={}, intermediate={}, target={}",
                sourceKeyOverride != null ? sourceKeyOverride : "(config)",
                intermediateKeyOverride != null ? intermediateKeyOverride : "(config)",
                targetKeyOverride != null ? targetKeyOverride : "(config)");

        LoadConfig loadConfig;
        if (configFilesProperties != null && configFilesProperties.isUseEventConfigsOnly()
                && eventConfigLoaderService != null) {
            loadConfig = eventConfigLoaderService.getMergedConfig(
                    trimToNull(sourceKeyOverride),
                    trimToNull(intermediateKeyOverride),
                    trimToNull(targetKeyOverride));
        } else {
            loadConfig = pipelineConfigService.getEffectiveLoadConfig(
                    trimToNull(sourceKeyOverride),
                    trimToNull(intermediateKeyOverride),
                    trimToNull(targetKeyOverride));
        }

        if (loadConfig == null) {
            if (configFilesProperties != null && configFilesProperties.isUseEventConfigsOnly()) {
                throw new IllegalStateException("Pipeline configuration is null. Provide source and target (and intermediate for 3-stage). Check database_event_config.yml, gcs_event_config.yml, bq_event_config.yml.");
            }
            throw new IllegalStateException("Pipeline configuration is null. Check streamnova.pipeline.config-file and pipeline config YAML.");
        }
        if (loadConfig.getSource() == null) {
            throw new IllegalStateException("Pipeline source is null. Set defaultSource or pass source in execute request. Check pipeline.config.sources/connections in YAML.");
        }

        PipelineHandlerRequirements requirements = pipelineConfigService.getPipelineHandlerRequirements(loadConfig);
        if (requirements == null) {
            throw new IllegalStateException("Could not resolve pipeline handler requirements from config.");
        }

        if (!sourceHandlerRegistry.hasHandler(requirements.getSourceHandlerType())) {
            throw new IllegalStateException(
                    "No handler for source type '" + requirements.getSourceHandlerType() + "'. Available: " + sourceHandlerRegistry.getRegisteredTypes());
        }

        SourceHandler<?> sourceHandler = sourceHandlerRegistry.getHandler(requirements.getSourceHandlerType());
        validateStageByType(loadConfig, requirements);

        PipelineHandlerSelection selection = PipelineHandlerSelection.builder()
                .loadConfig(loadConfig)
                .requirements(requirements)
                .sourceHandler(sourceHandler)
                .build();

        // Resolved execution details for tracking — user can find all details later
        String stageMode = requirements.hasIntermediate() ? "3-stage (source→intermediate→target)" : "2-stage (source→target)";
        log.info("[EXECUTION-TRACKING] Resolved execution: {} | source={} ({}), intermediate={}, target={} | sourceTable={}",
                stageMode,
                requirements.getSourceHandlerType(), sourceHandler.getClass().getSimpleName(),
                requirements.hasIntermediate() ? requirements.getIntermediateHandlerType() : "none",
                requirements.getTargetHandlerType() != null ? requirements.getTargetHandlerType() : "none",
                loadConfig.getSource() != null ? loadConfig.getSource().getTable() : "null");

        return selection;
    }

    /**
     * Resolves handler selection from an existing LoadConfig (e.g. from template rendering).
     * Does not load or merge config; uses the given config to derive requirements and look up the source handler.
     */
    public PipelineHandlerSelection resolveWithLoadConfig(LoadConfig loadConfig) {
        if (loadConfig == null) {
            throw new IllegalArgumentException("LoadConfig cannot be null.");
        }
        if (loadConfig.getSource() == null) {
            throw new IllegalStateException("Pipeline source is null in provided LoadConfig.");
        }
        PipelineHandlerRequirements requirements = pipelineConfigService.getPipelineHandlerRequirements(loadConfig);
        if (requirements == null) {
            throw new IllegalStateException("Could not resolve pipeline handler requirements from config.");
        }
        if (!sourceHandlerRegistry.hasHandler(requirements.getSourceHandlerType())) {
            throw new IllegalStateException(
                    "No handler for source type '" + requirements.getSourceHandlerType() + "'. Available: " + sourceHandlerRegistry.getRegisteredTypes());
        }
        SourceHandler<?> sourceHandler = sourceHandlerRegistry.getHandler(requirements.getSourceHandlerType());
        validateStageByType(loadConfig, requirements);
        PipelineHandlerSelection selection = PipelineHandlerSelection.builder()
                .loadConfig(loadConfig)
                .requirements(requirements)
                .sourceHandler(sourceHandler)
                .build();
        String stageMode = requirements.hasIntermediate() ? "3-stage (source→intermediate→target)" : "2-stage (source→target)";
        log.info("[EXECUTION-TRACKING] Resolved from config: {} | source={} ({}), target={} | sourceTable={}",
                stageMode, requirements.getSourceHandlerType(), sourceHandler.getClass().getSimpleName(),
                requirements.getTargetHandlerType(),
                loadConfig.getSource() != null ? loadConfig.getSource().getTable() : "null");
        return selection;
    }

    private static String trimToNull(String s) {
        return s != null && !s.isBlank() ? s.trim() : null;
    }

    /**
     * Validates intermediate and target by type only — no hardcoding which stage is gcs or bigquery.
     * User can choose e.g. target=gcs, intermediate=bigquery; validation applies by type.
     */
    private void validateStageByType(LoadConfig loadConfig, PipelineHandlerRequirements requirements) {
        // Intermediate: validate by selected type
        if (requirements.hasIntermediate()) {
            String it = requirements.getIntermediateHandlerType();
            if ("gcs".equals(it)) {
                if (loadConfig.getIntermediate() == null || loadConfig.getIntermediate().getGcsPath() == null || loadConfig.getIntermediate().getGcsPath().isBlank()) {
                    throw new IllegalStateException("Intermediate handler type is 'gcs' but pipeline.config.intermediate.gcsPath is not set.");
                }
            } else if ("bigquery".equals(it)) {
                if (loadConfig.getIntermediate() == null) {
                    throw new IllegalStateException("Intermediate handler type is 'bigquery' but pipeline.config.intermediate is not set.");
                }
                if (loadConfig.getIntermediate().getDataset() == null || loadConfig.getIntermediate().getDataset().isBlank()) {
                    throw new IllegalStateException("Intermediate handler type is 'bigquery' but pipeline.config.intermediate.dataset is not set.");
                }
                if (loadConfig.getIntermediate().getTable() == null || loadConfig.getIntermediate().getTable().isBlank()) {
                    throw new IllegalStateException("Intermediate handler type is 'bigquery' but pipeline.config.intermediate.table is not set.");
                }
            }
        }
        // Target: validate by selected type
        String tt = requirements.getTargetHandlerType();
        if (tt == null || tt.isBlank()) return;
        if ("bigquery".equals(tt)) {
            if (loadConfig.getTarget() == null) {
                throw new IllegalStateException("Target handler type is 'bigquery' but pipeline.config.target is not set.");
            }
            if (loadConfig.getTarget().getDataset() == null || loadConfig.getTarget().getDataset().isBlank()) {
                throw new IllegalStateException("Target handler type is 'bigquery' but pipeline.config.target.dataset is not set.");
            }
            if (loadConfig.getTarget().getTable() == null || loadConfig.getTarget().getTable().isBlank()) {
                throw new IllegalStateException("Target handler type is 'bigquery' but pipeline.config.target.table is not set.");
            }
        } else if ("gcs".equals(tt)) {
            if (loadConfig.getTarget() == null) {
                throw new IllegalStateException("Target handler type is 'gcs' but pipeline.config.target is not set.");
            }
            if (loadConfig.getTarget().getGcsPath() == null || loadConfig.getTarget().getGcsPath().isBlank()) {
                throw new IllegalStateException("Target handler type is 'gcs' but pipeline.config.target.gcsPath is not set.");
            }
        }
    }
}
