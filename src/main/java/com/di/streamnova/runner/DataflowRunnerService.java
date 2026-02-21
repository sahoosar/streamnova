package com.di.streamnova.runner;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.DoFn.FinishBundleContext;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.MDC;

import com.di.streamnova.agent.execution_planner.ExecutionPlanOption;
import com.di.streamnova.aspect.LogTransaction;
import com.di.streamnova.config.*;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.springframework.stereotype.Service;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.PCollection;
import com.di.streamnova.handler.SourceHandler;
import com.di.streamnova.handler.SourceHandlerRegistry;
import com.di.streamnova.util.TransactionEventLogger;
import com.di.streamnova.util.HikariDataSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.StreamSupport;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;


@Service
@Slf4j
public class DataflowRunnerService {

    private final PipelineConfigService pipelineConfigService;
    private final SourceHandlerRegistry registry;
    private final ApplicationContext applicationContext;
    private final PipelineConfigFilesProperties pipelineConfigFilesProperties;
    private final EventConfigLoaderService eventConfigLoaderService;
    private final PipelineHandlerResolver pipelineHandlerResolver;
    private final TransactionEventLogger transactionEventLogger; // optional: may be null in tests

    @Value("${streamnova.dataflow.project:}")
    private String dataflowProject;
    @Value("${streamnova.dataflow.region:}")
    private String dataflowRegion;

    public DataflowRunnerService(PipelineConfigService pipelineConfigService, SourceHandlerRegistry registry,
                                 ApplicationContext applicationContext,
                                 PipelineConfigFilesProperties pipelineConfigFilesProperties,
                                 EventConfigLoaderService eventConfigLoaderService,
                                 PipelineHandlerResolver pipelineHandlerResolver,
                                 TransactionEventLogger transactionEventLogger) {
        this.pipelineConfigService = pipelineConfigService;
        this.registry = registry;
        this.applicationContext = applicationContext;
        this.pipelineConfigFilesProperties = pipelineConfigFilesProperties != null ? pipelineConfigFilesProperties : new PipelineConfigFilesProperties();
        this.eventConfigLoaderService = eventConfigLoaderService != null ? eventConfigLoaderService : null;
        this.pipelineHandlerResolver = pipelineHandlerResolver != null ? pipelineHandlerResolver
                : new PipelineHandlerResolver(pipelineConfigService, eventConfigLoaderService, this.pipelineConfigFilesProperties, registry);
        this.transactionEventLogger = transactionEventLogger;
    }

    /** Call through proxy so @LogTransaction aspect runs (avoids self-invocation). */
    private DataflowRunnerService getSelf() {
        return applicationContext.getBean(DataflowRunnerService.class);
    }

    public void runPipeline() {

        String jobId = "job-" + java.util.UUID.randomUUID();
        MDC.put("jobId", jobId);

        try {
            // Resolve handler selection from config (source, optional intermediate, destination) — one place maps choice to handlers
            PipelineHandlerSelection selection = pipelineHandlerResolver.resolve(null, null, null);
            ensureExpectedHandlerMatches(selection.getSourceHandlerType());

            Pipeline pipeline = Pipeline.create();
            PCollection<Row> sourceData = getSelf().startLoadOperation(pipeline, MDC.get("jobId"), selection);

            // Total rows metric (pass-through)
            sourceData.apply("CountTotalRows", ParDo.of(new CountRowsFn()));

            // Per-shard counts
            PCollection<KV<Integer, Long>> perShardCounts =
                    sourceData
                            .apply("KeyByShard",
                                    MapElements.into(
                                                    TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.booleans()))
                                            .via((SerializableFunction<Row, KV<Integer, Boolean>>)
                                                    r -> KV.of(r.getInt32("shard_id"), Boolean.TRUE)))
                            .apply("CountPerShard", Count.perKey());

            // Log per-shard counts (static DoFn — no outer capture)
            perShardCounts.apply("LogPerShard", ParDo.of(new LogPerShardFn(jobId)));
            
            // Validate row count completeness (optional - can be enabled via config)
            // This validates that all shards together read the expected number of rows
            perShardCounts.apply("ValidateRowCount", ParDo.of(new ValidateRowCountFn(jobId)));
            
            // now run it
            PipelineResult result = pipeline.run();
            result.waitUntilFinish();

            // Retrieve total rows metric
            MetricQueryResults mqr = result.metrics().queryMetrics(
                    MetricsFilter.builder()
                            .addNameFilter(MetricNameFilter.named("streamnova", "rows_total"))
                            .build());

            long total =
                    StreamSupport.stream(mqr.getCounters().spliterator(), false)
                            .map(MetricResult::getCommitted)      // or getAttempted()
                            .filter(Objects::nonNull)
                            .mapToLong(Long::longValue)
                            .sum();

            log.info("[RUNNER] Total records read from source: {}", total);

        } finally {
            MDC.remove("jobId");
        }
    }

    /**
     * Runs the pipeline with the given agent candidate (machine type, workers, shards).
     * Builds DataflowPipelineOptions from the candidate, applies candidate to source config,
     * and runs the pipeline. Returns the Dataflow job ID when run on Dataflow, or empty when run locally.
     *
     * @param candidate recommended execution plan from the agent (AdaptiveExecutionPlanner / Recommender)
     * @return optional Dataflow job ID (present when run on Dataflow)
     * @throws IllegalStateException if config is missing or invalid
     * @throws RuntimeException      if pipeline execution fails
     */
    /**
     * Runs the pipeline with the given candidate. Uses config defaults for handler selection.
     */
    public Optional<String> runPipeline(ExecutionPlanOption candidate) {
        return runPipeline(candidate, HandlerOverrides.none());
    }

    /**
     * Runs the pipeline with the given candidate. When multiple handlers are present, {@code overrides}
     * let the user choose: two-stage (source + target) or three-stage (source + intermediate + target).
     */
    public Optional<String> runPipeline(ExecutionPlanOption candidate, HandlerOverrides overrides) {
        if (candidate == null) {
            throw new IllegalArgumentException("ExecutionPlanOption candidate cannot be null");
        }
        if (overrides == null) {
            overrides = HandlerOverrides.none();
        }
        PipelineHandlerSelection selection = pipelineHandlerResolver.resolve(
                overrides.getSource(), overrides.getIntermediate(), overrides.getTarget());
        return runPipeline(candidate, selection);
    }

    /**
     * Runs the pipeline with a pre-resolved selection (e.g. from template rendering).
     * Applies candidate overrides (workers, shards, pool size) to the selection's source config.
     */
    public Optional<String> runPipeline(ExecutionPlanOption candidate, PipelineHandlerSelection selection) {
        if (candidate == null) {
            throw new IllegalArgumentException("ExecutionPlanOption candidate cannot be null");
        }
        if (selection == null) {
            throw new IllegalArgumentException("PipelineHandlerSelection cannot be null");
        }
        ensureExpectedHandlerMatches(selection.getSourceHandlerType());
        String jobId = "job-" + java.util.UUID.randomUUID();
        MDC.put("jobId", jobId);
        try {
            DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
            options.setWorkerMachineType(candidate.getMachineType());
            options.setMaxNumWorkers(candidate.getWorkerCount());
            if (dataflowProject != null && !dataflowProject.isBlank()) {
                options.setProject(dataflowProject.trim());
            }
            if (dataflowRegion != null && !dataflowRegion.isBlank()) {
                options.setRegion(dataflowRegion.trim());
            }
            Pipeline pipeline = Pipeline.create(options);
            LoadConfig baseConfig = selection.getLoadConfig();
            PipelineConfigSource baseSource = baseConfig.getSource();
            PipelineConfigSource overriddenSource = new PipelineConfigSource();
            BeanUtils.copyProperties(baseSource, overriddenSource);
            // Execution plan: use only the candidate. Do not rely on pipeline config for shards, workers, machineType, or pool.
            // Pipeline config (YAML) is used only for connection, table, and other read settings (jdbcUrl, table, credentials, fetchSize, etc.).
            overriddenSource.setMachineType(candidate.getMachineType());
            overriddenSource.setWorkers(candidate.getWorkerCount());
            overriddenSource.setShards(candidate.getShardCount());
            overriddenSource.setMaximumPoolSize(candidate.getSuggestedPoolSize() > 0 ? candidate.getSuggestedPoolSize() : baseSource.getMaximumPoolSize());
            log.info("[RUNNER] Using candidate for execution plan: machineType={}, workers={}, shards={}, poolSize={}; connection/table from pipeline config",
                    candidate.getMachineType(), candidate.getWorkerCount(), candidate.getShardCount(),
                    candidate.getSuggestedPoolSize() > 0 ? candidate.getSuggestedPoolSize() : baseSource.getMaximumPoolSize());

            LoadConfig overriddenConfig = new LoadConfig();
            overriddenConfig.setSource(overriddenSource);
            overriddenConfig.setIntermediate(baseConfig.getIntermediate());
            overriddenConfig.setTarget(baseConfig.getTarget());

            PipelineHandlerSelection overriddenSelection = PipelineHandlerSelection.builder()
                    .loadConfig(overriddenConfig)
                    .requirements(selection.getRequirements())
                    .sourceHandler(selection.getSourceHandler())
                    .build();

            boolean twoStage = !selection.hasIntermediate();
            if (twoStage) {
                log.info("[RUNNER] 2-stage mode: SOURCE_READ -> TARGET_WRITE");
            } else {
                log.info("[RUNNER] 3-stage mode: SOURCE_READ -> INTERMEDIATE_WRITE -> TARGET_WRITE");
            }

            // Ensure JDBC pool exists for source before running (idempotent). Clear error if create-datasource was not called.
            PipelineConfigSource sourceForPool = overriddenConfig.getSource();
            if (sourceForPool != null && sourceForPool.getType() != null
                    && !"gcs".equals(sourceForPool.getType().trim().toLowerCase())) {
                try {
                    HikariDataSource.INSTANCE.getOrInit(sourceForPool.toDbConfigSnapshot());
                } catch (Exception e) {
                    throw new IllegalStateException(
                            "JDBC connection pool could not be created for source. Call POST /api/agent/pipeline-listener/create-datasource with same source, target (and intermediate for 3-stage), or check connection config: " + e.getMessage(),
                            e);
                }
            }

            List<PipelineActionConfig> actionConfigs = pipelineConfigService.getListenerActionsForMode(twoStage);
            PCollection<Row> sourceData = null;
            for (PipelineActionConfig actionConfig : actionConfigs) {
                String actionName = actionConfig.getAction() != null ? actionConfig.getAction() : "";
                String eventType = (actionConfig.getEventType() != null && !actionConfig.getEventType().isBlank())
                        ? actionConfig.getEventType()
                        : "LOAD_" + actionName;
                firePipelineActionEvent(eventType + "_STARTED", jobId, actionName);
                try {
                    switch (actionName.toUpperCase()) {
                        case "SOURCE_READ":
                            sourceData = getSelf().startLoadOperation(pipeline, jobId, overriddenSelection);
                            break;
                        case "INTERMEDIATE_WRITE":
                            getSelf().placeholderIntermediateWrite(overriddenSelection);
                            break;
                        case "TARGET_WRITE":
                            getSelf().placeholderTargetWrite(overriddenSelection);
                            break;
                        default:
                            log.debug("[RUNNER] Unknown pipeline action: {}", actionName);
                    }
                    firePipelineActionEvent(eventType + "_COMPLETED", jobId, actionName);
                } catch (Exception e) {
                    if (transactionEventLogger != null) {
                        Map<String, Object> ctx = new HashMap<>();
                        ctx.put("action", actionName);
                        ctx.put("error", e.getMessage());
                        transactionEventLogger.logEvent(eventType + "_FAILED", ctx, jobId, Thread.currentThread(), "load_operation", "StreamNova", e);
                    }
                    throw e;
                }
            }
            if (sourceData == null) {
                throw new IllegalStateException("SOURCE_READ action did not run or return data. Check pipeline.config.listeners for " + (twoStage ? "2-stage" : "3-stage") + " actions.");
            }

            sourceData.apply("CountTotalRows", ParDo.of(new CountRowsFn()));
            PCollection<KV<Integer, Long>> perShardCounts = sourceData
                    .apply("KeyByShard",
                            MapElements.into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.booleans()))
                                    .via((SerializableFunction<Row, KV<Integer, Boolean>>) r -> KV.of(r.getInt32("shard_id"), Boolean.TRUE)))
                    .apply("CountPerShard", Count.perKey());
            perShardCounts.apply("LogPerShard", ParDo.of(new LogPerShardFn(jobId)));
            perShardCounts.apply("ValidateRowCount", ParDo.of(new ValidateRowCountFn(jobId)));

            PipelineResult result = pipeline.run();
            result.waitUntilFinish();

            MetricQueryResults mqr = result.metrics().queryMetrics(
                    MetricsFilter.builder().addNameFilter(MetricNameFilter.named("streamnova", "rows_total")).build());
            long total = StreamSupport.stream(mqr.getCounters().spliterator(), false)
                    .map(MetricResult::getCommitted)
                    .filter(Objects::nonNull)
                    .mapToLong(Long::longValue)
                    .sum();
            log.info("[RUNNER] Total records read from source: {}", total);

            if (result instanceof DataflowPipelineJob dataflowJob) {
                return Optional.ofNullable(dataflowJob.getJobId());
            }
            return Optional.empty();
        } finally {
            MDC.remove("jobId");
        }
    }

    public static class CountRowsFn extends DoFn<Row, Void> {
        private final Counter counter = Metrics.counter("streamnova", "rows_total");

        @ProcessElement
        public void process(@Element Row r) {
            counter.inc();
            // no output
        }
    }

    // LogPerShardFn to print, record counts per shard
    public static class LogPerShardFn extends DoFn<KV<Integer, Long>, Void> {
        private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger("PerShard");
        private final String jobId; // serializable

        public LogPerShardFn(String jobId) {
            this.jobId = jobId;
        }

        @ProcessElement
        public void process(@Element KV<Integer, Long> kv) {
            LOG.info("jobId={} shard_id={} count={}", jobId, kv.getKey(), kv.getValue());
        }
    }
    
    // ValidateRowCountFn to validate completeness of data read across all shards
    // Note: This is a simplified validation - for full validation, expected count
    // would need to be passed through the pipeline
    public static class ValidateRowCountFn extends DoFn<KV<Integer, Long>, Void> {
        private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger("RowCountValidation");
        private final String jobId;

        public ValidateRowCountFn(String jobId) {
            this.jobId = jobId;
        }

        @ProcessElement
        public void process(@Element KV<Integer, Long> kv) {
            LOG.debug("jobId={} shard_id={} count={}", jobId, kv.getKey(), kv.getValue());
            // Validation logic can be added here if expected count is available
        }
    }
    

    /**
     * Starts the load using resolved handler selection (modular: one handler per stage — source; optional intermediate; destination).
     */
    @LogTransaction(
        eventType = "LOAD_OPERATION",
        transactionContext = "load_operation_start"
    )
    public PCollection<Row> startLoadOperation(Pipeline pipeline, String jobId, PipelineHandlerSelection selection) {
        if (selection == null || selection.getLoadConfig() == null || selection.getSourceHandler() == null) {
            throw new IllegalStateException("PipelineHandlerSelection is required (use PipelineHandlerResolver to resolve from config and overrides).");
        }
        LoadConfig loadConfig = selection.getLoadConfig();
        PipelineConfigSource pipelineSrcConfig = loadConfig.getSource();
        // Log all details for tracking — user can find what was executed later
        String stages = selection.hasIntermediate() ? "3-stage (source→intermediate→target)" : "2-stage (source→target)";
        log.info("[EXECUTION-TRACKING] Starting pipeline: {} | source={} ({}), intermediate={}, target={}",
                stages,
                selection.getSourceHandlerType(), selection.getSourceHandler().getClass().getSimpleName(),
                selection.hasIntermediate() ? selection.getIntermediateHandlerType() : "none",
                selection.getTargetHandlerType() != null ? selection.getTargetHandlerType() : "none");
        log.info("[RUNNER] Source table={}, jdbcUrl={}", pipelineSrcConfig.getTable(),
                pipelineSrcConfig.getJdbcUrl() != null ? pipelineSrcConfig.getJdbcUrl().replaceAll("password=[^;&]+", "password=***") : "null");

        @SuppressWarnings("unchecked")
        SourceHandler<PipelineConfigSource> handler = (SourceHandler<PipelineConfigSource>) selection.getSourceHandler();
        PCollection<Row> sourceData = handler.read(pipeline, pipelineSrcConfig);

        if (sourceData == null) {
            throw new IllegalStateException("Failed to read data from source: " + pipelineSrcConfig.getType());
        }
        return sourceData;
    }

    /** @deprecated Use {@link #startLoadOperation(Pipeline, String, PipelineHandlerSelection)}; builds selection from loadConfig. */
    @Deprecated
    public PCollection<Row> startLoadOperation(Pipeline pipeline, String jobId, LoadConfig loadConfig) {
        if (loadConfig == null) {
            loadConfig = pipelineConfigService.getEffectiveLoadConfig();
        }
        if (loadConfig == null || loadConfig.getSource() == null) {
            throw new IllegalStateException("Pipeline configuration or source is null. Check pipeline config YAML.");
        }
        PipelineHandlerRequirements requirements = pipelineConfigService.getPipelineHandlerRequirements(loadConfig);
        if (requirements == null || !registry.hasHandler(requirements.getSourceHandlerType())) {
            throw new IllegalStateException("No handler for source type. Available: " + registry.getRegisteredTypes());
        }
        PipelineHandlerSelection selection = PipelineHandlerSelection.builder()
                .loadConfig(loadConfig)
                .requirements(requirements)
                .sourceHandler(registry.getHandler(requirements.getSourceHandlerType()))
                .build();
        return startLoadOperation(pipeline, jobId, selection);
    }

    /** Fires a pipeline action event via TransactionEventLogger when configured. */
    private void firePipelineActionEvent(String eventType, String jobId, String actionName) {
        if (transactionEventLogger == null) return;
        Map<String, Object> context = new HashMap<>();
        context.put("action", actionName != null ? actionName : "");
        context.put("jobId", jobId);
        transactionEventLogger.logEvent(eventType, context, jobId, Thread.currentThread(), "load_operation", "StreamNova");
    }

    /** Placeholder for INTERMEDIATE_WRITE (3-stage). Not yet implemented in Beam graph. */
    public void placeholderIntermediateWrite(PipelineHandlerSelection selection) {
        log.info("[RUNNER] PipelineAction INTERMEDIATE_WRITE: placeholder (not yet implemented in Beam graph). Config: intermediate type={}",
                selection.hasIntermediate() ? selection.getIntermediateHandlerType() : "none");
    }

    /** Placeholder for TARGET_WRITE. Not yet implemented in Beam graph. */
    public void placeholderTargetWrite(PipelineHandlerSelection selection) {
        String targetType = selection.getTargetHandlerType() != null ? selection.getTargetHandlerType() : "none";
        log.info("[RUNNER] PipelineAction TARGET_WRITE: placeholder (not yet implemented in Beam graph). Config: target type={}", targetType);
    }

    /**
     * When streamnova.pipeline.expected-handler is set, verifies the resolved source type matches; otherwise throws.
     */
    private void ensureExpectedHandlerMatches(String sourceType) {
        String expectedHandler = pipelineConfigFilesProperties.getExpectedHandler();
        if (expectedHandler == null || expectedHandler.isBlank()) {
            return;
        }
        String expected = expectedHandler.trim().toLowerCase();
        String actual = sourceType == null ? "" : sourceType.trim().toLowerCase();
        if (!actual.equals(expected)) {
            log.error("[RUNNER] Pipeline expects handler '{}' (streamnova.pipeline.expected-handler) but config resolved to '{}'. Set defaultSource to {} or run with expected-handler={}.",
                    expectedHandler, sourceType, expectedHandler, sourceType);
            throw new IllegalStateException(
                    String.format("Pipeline is configured for handler '%s' but resolved source is '%s'. Set defaultSource to %s or set streamnova.pipeline.expected-handler=%s.",
                            expectedHandler, sourceType, expectedHandler, sourceType));
        }
        log.info("[RUNNER] Executing with expected handler: {}", expectedHandler.trim());
    }

}
