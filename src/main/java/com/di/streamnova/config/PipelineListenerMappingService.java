package com.di.streamnova.config;

import com.di.streamnova.agent.execution_engine.ActionMappingEntry;
import com.di.streamnova.agent.execution_engine.ActionsMappingResponse;
import com.di.streamnova.agent.execution_engine.CreateDatasourceResponse;
import com.di.streamnova.agent.execution_engine.DatasourceConnectionDetailsResponse;
import com.di.streamnova.agent.execution_engine.HandlerValidationException;
import com.di.streamnova.agent.execution_engine.StageConnectionDetail;
import com.di.streamnova.agent.execution_engine.StagesMappingResponse;
import com.di.streamnova.agent.workflow.PipelineTemplateRegistry;
import com.di.streamnova.handler.SourceHandlerRegistry;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import com.di.streamnova.util.HikariDataSource;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Maps pipeline listener config (2-stage / 3-stage) to user-selected handlers and provides
 * stages mapping, actions mapping, datasource details, and pool creation for the pipeline-listener API.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PipelineListenerMappingService {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final String SOURCE_READ = "SOURCE_READ";
    private static final String INTERMEDIATE_WRITE = "INTERMEDIATE_WRITE";
    private static final String TARGET_WRITE = "TARGET_WRITE";

    private final EventConfigLoaderService eventConfigLoaderService;
    private final PipelineConfigFilesProperties configFilesProperties;
    private final PipelineConfigService pipelineConfigService;
    private final PipelineTemplateRegistry templateRegistry;
    private final ResourceLoader resourceLoader;
    private final SourceHandlerRegistry sourceHandlerRegistry;

    /**
     * Returns pipeline_listener_config details for the requested stage(s).
     * When stage is 2 or 3, returns only that mode; when null, returns both 2-stage and 3-stage (and other modes if present).
     */
    public StagesMappingResponse getStagesMapping(Integer stage) {
        String entrypoint = configFilesProperties != null ? configFilesProperties.getListenerConfigFilePath() : "classpath:pipeline_listener_config.yml";
        Map<String, StageActionsConfig> listeners = loadListeners(entrypoint);
        if (listeners == null || listeners.isEmpty()) {
            return StagesMappingResponse.builder()
                    .entrypoint(entrypoint)
                    .stages(List.of())
                    .message("No listener config found at " + entrypoint)
                    .build();
        }

        List<StagesMappingResponse.StageDetailDto> stages = new ArrayList<>();
        for (Map.Entry<String, StageActionsConfig> e : listeners.entrySet()) {
            String modeKey = e.getKey();
            StageActionsConfig cfg = e.getValue();
            int stageValue = stageValueFromKey(modeKey);
            if (stage != null && stage != stageValue) {
                if (stageValue == 2 || stageValue == 3) continue;
            }
            List<StagesMappingResponse.StageActionEntry> actions = cfg.getActions() == null ? List.of() : cfg.getActions().stream()
                    .map(a -> StagesMappingResponse.StageActionEntry.builder()
                            .action(a.getAction())
                            .eventType(a.getEventType())
                            .build())
                    .collect(Collectors.toList());
            stages.add(StagesMappingResponse.StageDetailDto.builder()
                    .stageValue(stageValue)
                    .mode(modeKey)
                    .stages(cfg.getStages())
                    .actions(actions)
                    .build());
        }
        return StagesMappingResponse.builder()
                .entrypoint(entrypoint)
                .stages(stages)
                .build();
    }

    /**
     * Returns actions mapping for the given source/intermediate/target.
     * Validates prerequisites (merged config, handler registered, expected-handler, JDBC connection config)
     * and throws HandlerValidationException if invalid. Uses listener fallback when listener config has no actions.
     */
    public ActionsMappingResponse getActionsMapping(String source, String intermediate, String target) {
        source = normalizeSourceKey(source);
        intermediate = normalizeIntermediateKey(intermediate);
        target = normalizeTargetKey(target);
        LoadConfig merged = validateActionMappingOrThrow(source, intermediate, target);
        Map<String, StageActionsConfig> listeners = loadListeners(configFilesProperties.getListenerConfigFilePath());
        int stageCount = StringUtils.hasText(intermediate) ? 3 : 2;
        String mode = stageCount == 3 ? "3-stage" : "2-stage";
        StageActionsConfig modeConfig = listeners != null ? listeners.get(mode) : null;
        List<ActionMappingEntry> actionsMapping = new ArrayList<>();
        if (modeConfig != null && modeConfig.getActions() != null && !modeConfig.getActions().isEmpty()) {
            for (PipelineActionConfig a : modeConfig.getActions()) {
                String handler = mapActionToHandler(a.getAction(), source, intermediate, target, stageCount);
                actionsMapping.add(ActionMappingEntry.builder()
                        .action(a.getAction())
                        .handler(handler)
                        .eventType(a.getEventType() != null ? a.getEventType() : "LOAD_" + a.getAction())
                        .build());
            }
        } else {
            // Listener fallback: same defaults as PipelineConfigService.defaultListenerActionsForMode
            for (PipelineActionConfig a : defaultListenerActionsForMode(stageCount == 2)) {
                String handler = mapActionToHandler(a.getAction(), source, intermediate, target, stageCount);
                actionsMapping.add(ActionMappingEntry.builder()
                        .action(a.getAction())
                        .handler(handler)
                        .eventType(a.getEventType() != null ? a.getEventType() : "LOAD_" + a.getAction())
                        .build());
            }
        }
        String workflowType = templateRegistry.getWorkflowType(
                source != null ? source.trim() : null,
                StringUtils.hasText(intermediate) ? intermediate.trim() : null,
                target != null ? target.trim() : null);
        return ActionsMappingResponse.builder()
                .stages(stageCount)
                .mode(mode)
                .actionsMapping(actionsMapping)
                .entrypoint(configFilesProperties.getListenerConfigFilePath())
                .workflowType(workflowType)
                .build();
    }

    /**
     * Returns connection details per listener stage. When use-event-configs-only, ensures JDBC pool
     * for SOURCE_READ exists so the pipeline can use it.
     */
    public DatasourceConnectionDetailsResponse getDatasourceConnectionDetails(String source, String intermediate, String target) {
        source = normalizeSourceKey(source);
        intermediate = normalizeIntermediateKey(intermediate);
        target = normalizeTargetKey(target);
        LoadConfig merged = validateActionMappingOrThrow(source, intermediate, target);
        int stageCount = StringUtils.hasText(intermediate) ? 3 : 2;
        String mode = stageCount == 3 ? "3-stage" : "2-stage";
        List<StageConnectionDetail> stageDetails = buildStageConnectionDetails(merged, source, intermediate, target, stageCount, true);
        return DatasourceConnectionDetailsResponse.builder()
                .stages(stageCount)
                .mode(mode)
                .stageDetails(stageDetails)
                .build();
    }

    /**
     * Creates datasource pool(s) for all listener stages. Validates source/intermediate/target (same as actions-mapping);
     * for 2-stage: SOURCE_READ + TARGET_WRITE; for 3-stage: SOURCE_READ + INTERMEDIATE_WRITE + TARGET_WRITE.
     * Returns per-stage success/error so each object (source, intermediate, target) has its own status.
     */
    public CreateDatasourceResponse createDatasourcePoolsForMapping(String source, String intermediate, String target) {
        source = normalizeSourceKey(source);
        intermediate = normalizeIntermediateKey(intermediate);
        target = normalizeTargetKey(target);
        LoadConfig merged = validateActionMappingOrThrow(source, intermediate, target);
        int stageCount = StringUtils.hasText(intermediate) ? 3 : 2;
        List<String> failedEvents = new ArrayList<>();
        List<StageConnectionDetail> stageDetails = new ArrayList<>();
        List<String> errorMessages = new ArrayList<>();

        // SOURCE_READ: create JDBC pool when source is a DB
        String sourceHandler = source != null ? source.trim() : merged.getDefaultSource();
        if (merged.getSource() != null && merged.getSource().getJdbcUrl() != null && !merged.getSource().getJdbcUrl().isBlank()) {
            try {
                HikariDataSource.INSTANCE.getOrInit(merged.getSource().toDbConfigSnapshot());
                stageDetails.add(StageConnectionDetail.builder()
                        .action(SOURCE_READ)
                        .handler(sourceHandler)
                        .connectionDetails(toConnectionDetailsMasked(merged.getSource()))
                        .success(true)
                        .build());
            } catch (Exception e) {
                log.warn("Failed to create pool for SOURCE_READ: {}", e.getMessage());
                failedEvents.add(SOURCE_READ);
                String err = SOURCE_READ + ": " + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
                errorMessages.add(err);
                stageDetails.add(StageConnectionDetail.builder()
                        .action(SOURCE_READ)
                        .handler(sourceHandler)
                        .connectionDetails(toConnectionDetailsMasked(merged.getSource()))
                        .success(false)
                        .error(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName())
                        .build());
            }
        } else {
            stageDetails.add(StageConnectionDetail.builder()
                    .action(SOURCE_READ)
                    .handler(sourceHandler)
                    .connectionDetails(merged.getSource() != null ? toConnectionDetailsMasked(merged.getSource()) : null)
                    .success(true)
                    .build());
        }

        // INTERMEDIATE_WRITE (3-stage only): validate config; report success or error
        if (stageCount == 3) {
            String intermediateHandler = intermediate != null ? intermediate.trim() : merged.getDefaultIntermediate();
            if (merged.getIntermediate() == null) {
                failedEvents.add(INTERMEDIATE_WRITE);
                String err = INTERMEDIATE_WRITE + ": No intermediate config loaded. Check event config for intermediate=" + intermediateHandler + ".";
                errorMessages.add(err);
                stageDetails.add(StageConnectionDetail.builder()
                        .action(INTERMEDIATE_WRITE)
                        .handler(intermediateHandler)
                        .success(false)
                        .error("No intermediate config loaded. Check event config for intermediate=" + intermediateHandler + ".")
                        .build());
            } else {
                String intermediateError = validateIntermediateConfig(merged.getIntermediate());
                boolean ok = intermediateError == null;
                if (!ok) {
                    failedEvents.add(INTERMEDIATE_WRITE);
                    String err = INTERMEDIATE_WRITE + ": " + intermediateError;
                    errorMessages.add(err);
                }
                Map<String, Object> details = new LinkedHashMap<>();
                details.put("type", "intermediate");
                if (merged.getIntermediate().getBucket() != null) details.put("gcsBucket", merged.getIntermediate().getBucket());
                if (merged.getIntermediate().getGcsPath() != null) details.put("gcsPath", merged.getIntermediate().getGcsPath());
                if (merged.getIntermediate().getBasePath() != null) details.put("basePath", merged.getIntermediate().getBasePath());
                stageDetails.add(StageConnectionDetail.builder()
                        .action(INTERMEDIATE_WRITE)
                        .handler(intermediateHandler)
                        .connectionDetails(details)
                        .success(ok)
                        .error(ok ? null : intermediateError)
                        .build());
            }
        }

        // TARGET_WRITE: validate config; report success or error
        if (merged.getTarget() != null) {
            String targetHandler = target != null ? target.trim() : merged.getDefaultDestination();
            String targetError = validateTargetConfig(merged.getTarget());
            boolean ok = targetError == null;
            if (!ok) {
                failedEvents.add(TARGET_WRITE);
                String err = TARGET_WRITE + ": " + targetError;
                errorMessages.add(err);
            }
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("type", "target");
            if (merged.getTarget().getProject() != null) details.put("project", merged.getTarget().getProject());
            if (merged.getTarget().getDataset() != null) details.put("dataset", merged.getTarget().getDataset());
            if (merged.getTarget().getTable() != null) details.put("table", merged.getTarget().getTable());
            if (merged.getTarget().getBucket() != null) details.put("gcsBucket", merged.getTarget().getBucket());
            if (merged.getTarget().getGcsPath() != null) details.put("gcsPath", merged.getTarget().getGcsPath());
            if (merged.getTarget().getBasePath() != null) details.put("basePath", merged.getTarget().getBasePath());
            stageDetails.add(StageConnectionDetail.builder()
                    .action(TARGET_WRITE)
                    .handler(targetHandler)
                    .connectionDetails(details)
                    .success(ok)
                    .error(ok ? null : targetError)
                    .build());
        } else {
            failedEvents.add(TARGET_WRITE);
            String err = TARGET_WRITE + ": No target config loaded.";
            errorMessages.add(err);
            stageDetails.add(StageConnectionDetail.builder()
                    .action(TARGET_WRITE)
                    .handler(target != null ? target.trim() : null)
                    .success(false)
                    .error("No target config loaded.")
                    .build());
        }

        String message = errorMessages.isEmpty() ? null : String.join(". ", errorMessages);
        return CreateDatasourceResponse.builder()
                .success(failedEvents.isEmpty())
                .failedEvents(failedEvents.isEmpty() ? null : failedEvents)
                .stageDetails(stageDetails)
                .message(message)
                .build();
    }

    /**
     * Validates action mapping (merged config + handler registration + expected-handler + JDBC connection config).
     * Returns merged LoadConfig or throws HandlerValidationException.
     */
    private LoadConfig validateActionMappingOrThrow(String source, String intermediate, String target) {
        String srcKey = trimToNull(source);
        String tgtKey = trimToNull(target);
        if (srcKey == null || tgtKey == null) {
            throw new HandlerValidationException(
                    "Source and target are required. Use query params: ?source=postgres&target=gcs (or source=oracle, target=bigquery, etc.). " +
                            "For 3-stage add &intermediate=gcs. Allowed source keys: postgres, oracle, mysql, gcs, bq. Target keys: gcs, bq (bigquery).");
        }
        LoadConfig merged = eventConfigLoaderService.getMergedConfig(srcKey, trimToNull(intermediate), tgtKey);
        if (merged == null || merged.getSource() == null || merged.getTarget() == null) {
            throw new HandlerValidationException(
                    "Could not load config for source='" + source + "' and target='" + target + "'. " +
                            "Check: (1) application.yml streamnova.pipeline.event-config-files has entries for '" + srcKey + "' and '" + tgtKey + "'; " +
                            "(2) DB sources: database_event_config.yml has pipeline.config.sources." + srcKey + "; " +
                            "GCS/BQ as source: gcs_event_config.yml has pipeline.config.sources.gcs or sources.gcs_inputs; bq_event_config.yml has pipeline.config.sources.bigquery; " +
                            "(3) Destinations: gcs_event_config.yml has pipeline.config.destinations.gcs for target=gcs; bq_event_config.yml has pipeline.config.destinations.bigquery for target=bq/bigquery.");
        }
        PipelineHandlerRequirements req = pipelineConfigService.getPipelineHandlerRequirements(merged);
        if (req == null) {
            throw new HandlerValidationException(
                    "Could not resolve pipeline handler requirements. Ensure source has type set in event config.");
        }
        if (sourceHandlerRegistry != null && !sourceHandlerRegistry.hasHandler(req.getSourceHandlerType())) {
            String available = sourceHandlerRegistry.getRegisteredTypes() != null && !sourceHandlerRegistry.getRegisteredTypes().isEmpty()
                    ? sourceHandlerRegistry.getRegisteredTypes().toString()
                    : "none";
            throw new HandlerValidationException(
                    "Source handler '" + req.getSourceHandlerType() + "' is not registered. Available: " + available + ". Use these for source in actions-mapping and execute.");
        }
        String expectedHandler = configFilesProperties != null ? configFilesProperties.getExpectedHandler() : null;
        if (expectedHandler != null && !expectedHandler.isBlank()) {
            String expected = expectedHandler.trim().toLowerCase();
            String resolvedSourceKey = merged.getDefaultSource() != null ? merged.getDefaultSource().trim().toLowerCase() : (source != null ? source.trim().toLowerCase() : "");
            if (!resolvedSourceKey.equals(expected)) {
                throw new HandlerValidationException(
                        "Pipeline is configured for source '" + expectedHandler.trim() + "' (streamnova.pipeline.expected-handler) but request has source '" + (source != null ? source.trim() : merged.getDefaultSource()) + "'.");
            }
        }
        PipelineConfigSource src = merged.getSource();
        if (src != null && src.getJdbcUrl() != null && !src.getJdbcUrl().isBlank()) {
            List<String> missing = src.getMissingAttributesForConnection();
            if (missing != null && !missing.isEmpty()) {
                throw new HandlerValidationException(
                        "Source connection config is incomplete: " + String.join(", ", missing) + ". Fix pipeline.config.sources." + (merged.getDefaultSource() != null ? merged.getDefaultSource() : "source") + " in database_event_config.yml (or the event config for this source).");
            }
        }
        return merged;
    }

    private static List<PipelineActionConfig> defaultListenerActionsForMode(boolean twoStage) {
        List<PipelineActionConfig> list = new ArrayList<>();
        if (twoStage) {
            list.add(actionConfig("SOURCE_READ", "LOAD_SOURCE_READ"));
            list.add(actionConfig("TARGET_WRITE", "LOAD_TARGET_WRITE"));
        } else {
            list.add(actionConfig("SOURCE_READ", "LOAD_SOURCE_READ"));
            list.add(actionConfig("INTERMEDIATE_WRITE", "LOAD_INTERMEDIATE_WRITE"));
            list.add(actionConfig("TARGET_WRITE", "LOAD_TARGET_WRITE"));
        }
        return list;
    }

    private static PipelineActionConfig actionConfig(String action, String eventType) {
        PipelineActionConfig c = new PipelineActionConfig();
        c.setAction(action);
        c.setEventType(eventType);
        return c;
    }

    private static String mapActionToHandler(String action, String source, String intermediate, String target, int stageCount) {
        if (SOURCE_READ.equals(action)) return source != null ? source.trim() : "";
        if (TARGET_WRITE.equals(action)) return target != null ? target.trim() : "";
        if (INTERMEDIATE_WRITE.equals(action) && stageCount == 3) return intermediate != null ? intermediate.trim() : "";
        return "";
    }

    private List<StageConnectionDetail> buildStageConnectionDetails(LoadConfig merged, String source, String intermediate, String target, int stageCount, boolean ensurePool) {
        List<StageConnectionDetail> list = new ArrayList<>();
        if (merged.getSource() != null) {
            PipelineConfigSource src = merged.getSource();
            if (src.getJdbcUrl() != null && !src.getJdbcUrl().isBlank()) {
                if (ensurePool) {
                    try {
                        HikariDataSource.INSTANCE.getOrInit(src.toDbConfigSnapshot());
                    } catch (Exception e) {
                        log.debug("Could not ensure pool for SOURCE_READ: {}", e.getMessage());
                    }
                }
            }
            list.add(StageConnectionDetail.builder()
                    .action(SOURCE_READ)
                    .handler(source != null ? source.trim() : merged.getDefaultSource())
                    .connectionDetails(toSourceConnectionDetails(src))
                    .build());
        }
        if (stageCount == 3 && merged.getIntermediate() != null) {
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("type", "intermediate");
            if (merged.getIntermediate().getBucket() != null) details.put("gcsBucket", merged.getIntermediate().getBucket());
            if (merged.getIntermediate().getGcsPath() != null) details.put("gcsPath", merged.getIntermediate().getGcsPath());
            if (merged.getIntermediate().getBasePath() != null) details.put("basePath", merged.getIntermediate().getBasePath());
            list.add(StageConnectionDetail.builder()
                    .action(INTERMEDIATE_WRITE)
                    .handler(intermediate != null ? intermediate.trim() : merged.getDefaultIntermediate())
                    .connectionDetails(details)
                    .build());
        }
        if (merged.getTarget() != null) {
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("type", "target");
            if (merged.getTarget().getProject() != null) details.put("project", merged.getTarget().getProject());
            if (merged.getTarget().getDataset() != null) details.put("dataset", merged.getTarget().getDataset());
            if (merged.getTarget().getTable() != null) details.put("table", merged.getTarget().getTable());
            if (merged.getTarget().getBucket() != null) details.put("gcsBucket", merged.getTarget().getBucket());
            if (merged.getTarget().getGcsPath() != null) details.put("gcsPath", merged.getTarget().getGcsPath());
            if (merged.getTarget().getBasePath() != null) details.put("basePath", merged.getTarget().getBasePath());
            list.add(StageConnectionDetail.builder()
                    .action(TARGET_WRITE)
                    .handler(target != null ? target.trim() : merged.getDefaultDestination())
                    .connectionDetails(details)
                    .build());
        }
        return list;
    }

    /** Connection details for SOURCE_READ: JDBC (masked) or GCS/BigQuery (type, path, project, etc.). */
    private static Map<String, Object> toSourceConnectionDetails(PipelineConfigSource src) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (src == null) return m;
        m.put("type", src.getType());
        if (src.getJdbcUrl() != null && !src.getJdbcUrl().isBlank()) {
            m.put("jdbcUrl", src.getJdbcUrl());
            if (src.getUsername() != null) m.put("username", src.getUsername());
            m.put("password", "***");
        } else {
            if (src.getGcsPath() != null) m.put("gcsPath", src.getGcsPath());
            if (src.getGcsPaths() != null) m.put("gcsPaths", src.getGcsPaths());
            if (src.getProject() != null) m.put("project", src.getProject());
            if (src.getDataset() != null) m.put("dataset", src.getDataset());
            if (src.getTable() != null) m.put("table", src.getTable());
            if (src.getLocation() != null) m.put("location", src.getLocation());
            if (src.getQuery() != null && !src.getQuery().isBlank()) m.put("query", src.getQuery());
        }
        return m;
    }

    private static Map<String, Object> toConnectionDetailsMasked(PipelineConfigSource src) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", src.getType());
        if (src.getJdbcUrl() != null) m.put("jdbcUrl", src.getJdbcUrl());
        if (src.getUsername() != null) m.put("username", src.getUsername());
        m.put("password", "***");
        return m;
    }

    /** Returns null if valid; otherwise error message for intermediate (GCS) config. */
    private static String validateIntermediateConfig(IntermediateConfig cfg) {
        if (cfg == null) return "Intermediate config is null.";
        boolean hasGcsPath = cfg.getGcsPath() != null && !cfg.getGcsPath().isBlank();
        boolean hasBucket = cfg.getBucket() != null && !cfg.getBucket().isBlank();
        if (!hasGcsPath && !hasBucket) return "Missing gcsPath or bucket for intermediate (GCS). Set in gcs_event_config.yml pipeline.config.intermediates.gcs_stage.";
        return null;
    }

    /** Returns null if valid; otherwise error message for target config (BigQuery or GCS). */
    private static String validateTargetConfig(TargetConfig cfg) {
        if (cfg == null) return "Target config is null.";
        String type = cfg.getType() != null ? cfg.getType().trim().toLowerCase() : "";
        if ("bigquery".equals(type)) {
            if (cfg.getDataset() == null || cfg.getDataset().isBlank()) return "Missing dataset for BigQuery target. Set in bq_event_config.yml pipeline.config.destinations.bigquery.";
            if (cfg.getTable() == null || cfg.getTable().isBlank()) return "Missing table for BigQuery target. Set in bq_event_config.yml pipeline.config.destinations.bigquery.";
        } else if ("gcs".equals(type)) {
            boolean hasGcsPath = cfg.getGcsPath() != null && !cfg.getGcsPath().isBlank();
            boolean hasBucket = cfg.getBucket() != null && !cfg.getBucket().isBlank();
            if (!hasGcsPath && !hasBucket) return "Missing gcsPath or bucket for GCS target. Set in gcs_event_config.yml pipeline.config.destinations.gcs.";
        }
        return null;
    }

    private Map<String, StageActionsConfig> loadListeners(String path) {
        if (path == null || path.isBlank()) return null;
        try {
            Resource resource = resourceLoader.getResource(path.trim());
            if (!resource.exists()) return null;
            try (InputStream in = resource.getInputStream()) {
                PipelineYamlRoot root = YAML_MAPPER.readValue(in, PipelineYamlRoot.class);
                if (root == null || root.getPipeline() == null || root.getPipeline().getConfig() == null) return null;
                return root.getPipeline().getConfig().getListeners();
            }
        } catch (Exception e) {
            log.debug("Could not load listener config {}: {}", path, e.getMessage());
            return null;
        }
    }

    private static int stageValueFromKey(String modeKey) {
        if ("2-stage".equals(modeKey)) return 2;
        if ("3-stage".equals(modeKey)) return 3;
        return 0;
    }

    private static String trimToNull(String s) {
        return s != null && !s.isBlank() ? s.trim() : null;
    }

    /** Normalizes target key for config lookup (e.g. bq → bigquery). Event config and templates use "bigquery". */
    private static String normalizeTargetKey(String target) {
        if (target == null) return null;
        String t = target.trim();
        if (t.isEmpty()) return target;
        if ("bq".equalsIgnoreCase(t)) return "bigquery";
        return t;
    }

    /** Normalizes source key for config lookup (trim only). */
    private static String normalizeSourceKey(String source) {
        return source == null ? null : source.trim();
    }

    /** Normalizes intermediate key for config lookup (trim only). */
    private static String normalizeIntermediateKey(String intermediate) {
        return intermediate == null ? null : intermediate.trim();
    }
}
