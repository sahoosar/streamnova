package com.di.streamnova.agent.workflow;

import com.di.streamnova.agent.execution_engine.ExecuteRequest;
import com.di.streamnova.agent.execution_planner.ExecutionPlanOption;
import com.di.streamnova.agent.workflow.dto.DbToGcsTemplateContext;
import com.di.streamnova.agent.workflow.dto.TemplateContextView;
import com.di.streamnova.agent.workflow.dto.TemplateDetailsRequest;
import com.di.streamnova.agent.workflow.dto.TemplateDetailsResponse;
import com.di.streamnova.config.EventConfigLoaderService;
import com.di.streamnova.config.LoadConfig;
import com.di.streamnova.config.PipelineConfigSource;
import com.di.streamnova.config.PipelineYamlRoot;
import com.di.streamnova.config.TargetConfig;
import com.di.streamnova.config.TemplateDefaultsProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds template context from event configs and candidate, renders pipeline template YAML,
 * and returns a normalized LoadConfig for execution.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PipelineTemplateService {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{([a-zA-Z0-9_-]+)}");
    private static final long DEFAULT_CONNECTION_TIMEOUT_MS = 300_000L;
    private static final long DEFAULT_IDLE_TIMEOUT_MS = 300_000L;
    private static final long DEFAULT_MAX_LIFETIME_MS = 1_800_000L;

    private final ResourceLoader resourceLoader;
    private final EventConfigLoaderService eventConfigLoaderService;
    private final PipelineTemplateRegistry templateRegistry;
    private final TemplateDefaultsProperties templateDefaults;

    /**
     * Resolves config for execution: if a template exists for (source, intermediate, target),
     * builds context from merged event config + candidate + request, renders template, and returns
     * normalized LoadConfig. Otherwise returns null (caller uses event-config merge).
     */
    public LoadConfig resolveConfigFromTemplate(String sourceKey, String intermediateKey, String targetKey,
                                                 ExecutionPlanOption candidate, ExecuteRequest request) {
        String templatePath = templateRegistry.selectTemplate(sourceKey, intermediateKey, targetKey);
        if (templatePath == null || templatePath.isBlank()) {
            return null;
        }
        if (!templatePath.contains("db-to-gcs-template") || templatePath.contains("to-bq")) {
            return null;
        }
        LoadConfig merged = eventConfigLoaderService.getMergedConfig(
                sourceKey != null ? sourceKey.trim() : null,
                intermediateKey != null && !intermediateKey.isBlank() ? intermediateKey.trim() : null,
                targetKey != null ? targetKey.trim() : null);
        if (merged == null || merged.getSource() == null || merged.getTarget() == null) {
            log.warn("[TEMPLATE] Cannot build context: merged config null or missing source/target");
            return null;
        }
        DbToGcsTemplateContext context = buildContext(merged, candidate, request, sourceKey, targetKey);
        return renderAndParse(templatePath, context);
    }

    /**
     * Returns template details and all calculated placeholder values for the given request.
     * Use this so the user can see the resolved values before executing. Password is masked in the response.
     */
    public TemplateDetailsResponse getTemplateDetails(TemplateDetailsRequest request) {
        if (request == null || request.getSource() == null || request.getSource().isBlank()
                || request.getTarget() == null || request.getTarget().isBlank()) {
            return TemplateDetailsResponse.builder()
                    .message("source and target are required")
                    .build();
        }
        String sourceKey = request.getSource().trim();
        String intermediateKey = request.getIntermediate() != null && !request.getIntermediate().isBlank() ? request.getIntermediate().trim() : null;
        String targetKey = request.getTarget().trim();

        String templatePath = templateRegistry.selectTemplate(sourceKey, intermediateKey, targetKey);
        if (templatePath == null || templatePath.isBlank()) {
            return TemplateDetailsResponse.builder()
                    .message("No template defined for source=" + sourceKey + ", target=" + targetKey + (intermediateKey != null ? ", intermediate=" + intermediateKey : ""))
                    .build();
        }
        LoadConfig merged = eventConfigLoaderService.getMergedConfig(sourceKey, intermediateKey, targetKey);
        if (merged == null || merged.getSource() == null || merged.getTarget() == null) {
            String workflowType = templateRegistry.getWorkflowType(sourceKey, intermediateKey, targetKey);
            return TemplateDetailsResponse.builder()
                    .templatePath(templatePath)
                    .workflowType(workflowType)
                    .message("Could not load merged config for source/target. Check event config files.")
                    .build();
        }
        ExecutionPlanOption candidate = toExecutionPlanOption(request.getCandidate());
        ExecuteRequest executeRequest = toExecuteRequest(request);
        DbToGcsTemplateContext context = buildContext(merged, candidate, executeRequest, sourceKey, targetKey);
        String workflowType = templateRegistry.getWorkflowType(sourceKey, intermediateKey, targetKey);
        if (workflowType == null) {
            log.debug("[TEMPLATE] No workflow type for source={}, intermediate={}, target={}", sourceKey, intermediateKey, targetKey);
        }
        return TemplateDetailsResponse.builder()
                .workflowType(workflowType)
                .templatePath(templatePath)
                .context(TemplateContextView.from(context))
                .build();
    }

    private static ExecutionPlanOption toExecutionPlanOption(TemplateDetailsRequest.CandidateBody c) {
        if (c == null) return null;
        int workers = c.getWorkerCount() != null && c.getWorkerCount() > 0 ? c.getWorkerCount() : 1;
        int shards = c.getShardCount() != null && c.getShardCount() > 0 ? c.getShardCount() : 1;
        int vcpus = c.getVirtualCpus() != null && c.getVirtualCpus() > 0 ? c.getVirtualCpus() : 4;
        int poolSize = c.getSuggestedPoolSize() != null && c.getSuggestedPoolSize() >= 0 ? c.getSuggestedPoolSize() : 0;
        return ExecutionPlanOption.builder()
                .machineType(c.getMachineType() != null ? c.getMachineType().trim() : "n2-standard-4")
                .workerCount(workers)
                .shardCount(shards)
                .virtualCpus(vcpus)
                .suggestedPoolSize(poolSize)
                .label(c.getLabel())
                .build();
    }

    private static ExecuteRequest toExecuteRequest(TemplateDetailsRequest r) {
        if (r == null) return null;
        ExecuteRequest out = new ExecuteRequest();
        out.setSourceSchema(r.getSourceSchema());
        out.setSourceTable(r.getSourceTable());
        out.setSourceQuery(r.getSourceQuery());
        out.setExtractionMode(r.getExtractionMode());
        out.setIncrementalColumn(r.getIncrementalColumn());
        out.setWatermarkFrom(r.getWatermarkFrom());
        out.setWatermarkTo(r.getWatermarkTo());
        return out;
    }

    /**
     * Builds DbToGcsTemplateContext from merged event config, candidate, and optional request.
     */
    public DbToGcsTemplateContext buildContext(LoadConfig merged, ExecutionPlanOption candidate,
                                               ExecuteRequest request, String sourceKey, String targetKey) {
        PipelineConfigSource src = merged.getSource();
        TargetConfig tgt = merged.getTarget();
        String runId = request != null && request.getExecutionRunId() != null && !request.getExecutionRunId().isBlank()
                ? request.getExecutionRunId()
                : "run-" + UUID.randomUUID();

        String schema = null;
        String table = null;
        String query = null;
        String extractionMode = "FULL";
        String incrementalColumn = null;
        String watermarkFrom = null;
        String watermarkTo = null;
        if (request != null) {
            if (request.getSourceSchema() != null && !request.getSourceSchema().isBlank()) schema = request.getSourceSchema().trim();
            if (request.getSourceTable() != null && !request.getSourceTable().isBlank()) table = request.getSourceTable().trim();
            if (request.getSourceQuery() != null && !request.getSourceQuery().isBlank()) query = request.getSourceQuery().trim();
            if (request.getExtractionMode() != null && !request.getExtractionMode().isBlank()) extractionMode = request.getExtractionMode().trim();
            if (request.getIncrementalColumn() != null && !request.getIncrementalColumn().isBlank()) incrementalColumn = request.getIncrementalColumn().trim();
            if (request.getWatermarkFrom() != null && !request.getWatermarkFrom().isBlank()) watermarkFrom = request.getWatermarkFrom().trim();
            if (request.getWatermarkTo() != null && !request.getWatermarkTo().isBlank()) watermarkTo = request.getWatermarkTo().trim();
        }
        if (table == null && src.getTable() != null && !src.getTable().isBlank()) table = src.getTable().trim();
        if (schema == null && table != null && table.contains(".")) {
            int dot = table.indexOf('.');
            schema = table.substring(0, dot);
            table = table.substring(dot + 1);
        } else if (schema == null) schema = "";
        if (table == null) table = "";

        int workers = 1;
        int shards = 1;
        int maxDbConnections = src.getMaximumPoolSize() > 0 ? src.getMaximumPoolSize() : (src.getFallbackPoolSize() > 0 ? src.getFallbackPoolSize() : 8);
        if (candidate != null) {
            workers = Math.max(1, candidate.getWorkerCount());
            shards = Math.max(1, candidate.getShardCount());
            if (candidate.getSuggestedPoolSize() > 0) maxDbConnections = candidate.getSuggestedPoolSize();
        } else {
            if (src.getWorkers() != null && src.getWorkers() > 0) workers = src.getWorkers();
            if (src.getShards() != null && src.getShards() > 0) shards = src.getShards();
        }

        long connectionTimeoutMs = src.getConnectionTimeout() > 0 ? src.getConnectionTimeout() : DEFAULT_CONNECTION_TIMEOUT_MS;
        long idleTimeoutMs = src.getIdleTimeout() > 0 ? src.getIdleTimeout() : DEFAULT_IDLE_TIMEOUT_MS;
        long maxLifetimeMs = src.getMaxLifetime() > 0 ? src.getMaxLifetime() : DEFAULT_MAX_LIFETIME_MS;

        String defaultBytesPerRowBytes = "204800";
        if (src.getDefaultBytesPerRow() != null && !src.getDefaultBytesPerRow().isBlank()) {
            defaultBytesPerRowBytes = parseBytesToNumber(src.getDefaultBytesPerRow());
        }

        String outputBucket = "";
        String outputBasePath = "";
        String fileFormat = templateDefaults != null && templateDefaults.getFileFormat() != null && !templateDefaults.getFileFormat().isBlank() ? templateDefaults.getFileFormat().trim() : "PARQUET";
        String compression = templateDefaults != null && templateDefaults.getCompression() != null && !templateDefaults.getCompression().isBlank() ? templateDefaults.getCompression().trim() : "SNAPPY";
        // targetFileSizeMb: optional target hint (actual file sizes may vary). 0 = no target, use default.
        int targetFileSizeMb = templateDefaults != null && templateDefaults.getTargetFileSizeMb() > 0 ? templateDefaults.getTargetFileSizeMb() : 64;
        String writeMode = templateDefaults != null && templateDefaults.getWriteMode() != null && !templateDefaults.getWriteMode().isBlank() ? templateDefaults.getWriteMode().trim() : "APPEND";
        boolean traceEnabled = templateDefaults != null ? templateDefaults.isTraceEnabled() : true;
        if (tgt != null) {
            if (tgt.getBucket() != null && !tgt.getBucket().isBlank()) {
                outputBucket = tgt.getBucket().trim();
                outputBasePath = tgt.getBasePath() != null ? tgt.getBasePath().trim() : "";
            } else if (tgt.getGcsPath() != null && !tgt.getGcsPath().isBlank()) {
                String path = tgt.getGcsPath().trim();
                if (path.startsWith("gs://")) {
                    path = path.substring(5);
                    int slash = path.indexOf('/');
                    if (slash > 0) {
                        outputBucket = path.substring(0, slash);
                        outputBasePath = path.substring(slash + 1);
                    } else {
                        outputBucket = path;
                    }
                }
            }
            if (tgt.getFileFormat() != null && !tgt.getFileFormat().isBlank()) fileFormat = tgt.getFileFormat().trim();
            if (tgt.getCompression() != null && !tgt.getCompression().isBlank()) compression = tgt.getCompression().trim();
            if (tgt.getTargetFileSizeMb() != null && tgt.getTargetFileSizeMb() > 0) targetFileSizeMb = tgt.getTargetFileSizeMb();
            if (tgt.getWriteMode() != null && !tgt.getWriteMode().isBlank()) writeMode = tgt.getWriteMode().trim();
            if (tgt.getTraceEnabled() != null) traceEnabled = tgt.getTraceEnabled();
        }

        String sourceKeyDisplay = "postgres".equalsIgnoreCase(sourceKey != null ? sourceKey.trim() : "")
                ? "postgre_db" : ("oracle".equalsIgnoreCase(sourceKey != null ? sourceKey.trim() : "") ? "oracle_db" : (sourceKey != null ? sourceKey : ""));

        return DbToGcsTemplateContext.builder()
                .runId(runId)
                .datePartition(LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE))
                .sourceKey(sourceKeyDisplay)
                .sourceType(src.getType() != null ? src.getType().trim() : "")
                .driverClass(src.getDriver() != null ? src.getDriver() : "")
                .jdbcUrl(src.getJdbcUrl() != null ? src.getJdbcUrl() : "")
                .username(src.getUsername() != null ? src.getUsername() : "")
                .password(src.getPassword() != null ? src.getPassword() : "")
                .sourceSchema(schema != null ? schema : "")
                .sourceTable(table != null ? table : "")
                .sourceQuery(query != null ? query : "")
                .fetchSize(src.getFetchSize() > 0 ? src.getFetchSize() : 5000)
                .maxDbConnections(maxDbConnections)
                .minimumIdle(src.getMinimumIdle() > 0 ? src.getMinimumIdle() : 2)
                .connectionTimeoutMs(connectionTimeoutMs)
                .idleTimeoutMs(idleTimeoutMs)
                .maxLifetimeMs(maxLifetimeMs)
                .workers(workers)
                .shards(shards)
                .extractionMode(extractionMode)
                .incrementalColumn(incrementalColumn)
                .watermarkFrom(watermarkFrom)
                .watermarkTo(watermarkTo)
                .defaultBytesPerRowBytes(defaultBytesPerRowBytes)
                .enableSourceRowCount(false)
                .sourceCountQuery("")
                .outputBucket(outputBucket)
                .outputBasePath(outputBasePath)
                .fileFormat(fileFormat)
                .compression(compression)
                .targetFileSizeMb(targetFileSizeMb)
                .writeMode(writeMode)
                .traceEnabled(traceEnabled)
                .build();
    }

    /**
     * Loads template from path, substitutes placeholders from context, parses YAML, and returns normalized LoadConfig.
     */
    public LoadConfig renderAndParse(String templatePath, DbToGcsTemplateContext context) {
        try {
            Resource resource = resourceLoader.getResource(templatePath.trim());
            if (!resource.exists()) {
                log.warn("[TEMPLATE] Template not found: {}", templatePath);
                return null;
            }
            String raw;
            try (InputStream in = resource.getInputStream()) {
                raw = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
            String rendered = substitutePlaceholders(raw, context);
            PipelineYamlRoot root = YAML_MAPPER.readValue(rendered, PipelineYamlRoot.class);
            if (root == null || root.getPipeline() == null || root.getPipeline().getConfig() == null) {
                log.warn("[TEMPLATE] Parsed YAML has no pipeline.config");
                return null;
            }
            LoadConfig config = root.getPipeline().getConfig();
            return normalizeTemplateLoadConfig(config);
        } catch (Exception e) {
            log.error("[TEMPLATE] Failed to render or parse template {}: {}", templatePath, e.getMessage(), e);
            return null;
        }
    }

    private String substitutePlaceholders(String raw, DbToGcsTemplateContext ctx) {
        StringBuilder out = new StringBuilder(raw.length());
        Matcher m = PLACEHOLDER.matcher(raw);
        while (m.find()) {
            String key = m.group(1);
            String value = getPlaceholderValue(key, ctx);
            m.appendReplacement(out, Matcher.quoteReplacement(value));
        }
        m.appendTail(out);
        return out.toString();
    }

    private String getPlaceholderValue(String key, DbToGcsTemplateContext ctx) {
        switch (key) {
            case "runId": return ctx.getRunIdOrEmpty();
            case "yyyy-MM-dd": return ctx.getDatePartitionOrEmpty();
            case "sourceKey": return ctx.getSourceKeyOrEmpty();
            case "sourceType": return ctx.getSourceTypeOrEmpty();
            case "driverClass": return ctx.getDriverClassOrEmpty();
            case "jdbcUrl": return ctx.getJdbcUrlOrEmpty();
            case "username": return ctx.getUsernameOrEmpty();
            case "password": return ctx.getPasswordOrEmpty();
            case "sourceSchema": return ctx.getSourceSchemaOrEmpty();
            case "sourceTable": return ctx.getSourceTableOrEmpty();
            case "sourceQuery": return ctx.getSourceQueryOrEmpty();
            case "fetchSize": return String.valueOf(ctx.getFetchSize());
            case "maxDbConnections": return String.valueOf(ctx.getMaxDbConnections());
            case "minimumIdle": return String.valueOf(ctx.getMinimumIdle());
            case "connectionTimeoutMs": return String.valueOf(ctx.getConnectionTimeoutMs());
            case "idleTimeoutMs": return String.valueOf(ctx.getIdleTimeoutMs());
            case "maxLifetimeMs": return String.valueOf(ctx.getMaxLifetimeMs());
            case "workers": return String.valueOf(ctx.getWorkers());
            case "shards": return String.valueOf(ctx.getShards());
            case "extractionMode": return ctx.getExtractionModeOrEmpty();
            case "incrementalColumn": return ctx.getIncrementalColumnOrEmpty();
            case "watermarkFrom": return ctx.getWatermarkFromOrEmpty();
            case "watermarkTo": return ctx.getWatermarkToOrEmpty();
            case "defaultBytesPerRowBytes": return ctx.getDefaultBytesPerRowBytesOrDefault();
            case "enableSourceRowCount": return String.valueOf(ctx.isEnableSourceRowCount());
            case "sourceCountQuery": return ctx.getSourceCountQueryOrEmpty();
            case "outputBucket": return ctx.getOutputBucketOrEmpty();
            case "outputBasePath": return ctx.getOutputBasePathOrEmpty();
            case "fileFormat": return ctx.getFileFormatOrEmpty();
            case "compression": return ctx.getCompressionOrEmpty();
            case "targetFileSizeMb": return String.valueOf(ctx.getTargetFileSizeMb());
            case "writeMode": return ctx.getWriteModeOrEmpty();
            case "traceEnabled": return String.valueOf(ctx.isTraceEnabled());
            default: return "{" + key + "}";
        }
    }

    /**
     * Ensures LoadConfig has single source and target set from template keys (db_source, gcs_stage).
     */
    private LoadConfig normalizeTemplateLoadConfig(LoadConfig config) {
        if (config.getSources() != null && config.getSources().containsKey("db_source")) {
            config.setSource(config.getSources().get("db_source"));
            config.setDefaultSource("db_source");
        }
        if (config.getDestinations() != null && config.getDestinations().containsKey("gcs_stage")) {
            TargetConfig gcs = config.getDestinations().get("gcs_stage");
            if (gcs != null && (gcs.getGcsPath() == null || gcs.getGcsPath().isBlank()) && gcs.getBucket() != null) {
                String path = "gs://" + gcs.getBucket();
                if (gcs.getBasePath() != null && !gcs.getBasePath().isBlank()) {
                    path += "/" + gcs.getBasePath();
                }
                gcs.setGcsPath(path);
            }
            config.setTarget(gcs);
            config.setDefaultDestination("gcs_stage");
        }
        return config;
    }

    private static String parseBytesToNumber(String value) {
        if (value == null || value.isBlank()) return "204800";
        String v = value.trim().toUpperCase();
        try {
            if (v.endsWith("KB")) {
                long num = Long.parseLong(v.substring(0, v.length() - 2).trim());
                return String.valueOf(num * 1024);
            }
            if (v.endsWith("MB")) {
                long num = Long.parseLong(v.substring(0, v.length() - 2).trim());
                return String.valueOf(num * 1024 * 1024);
            }
            return v;
        } catch (NumberFormatException e) {
            return "204800";
        }
    }
}
