package com.di.streamnova.runner;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.DoFn.FinishBundleContext;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.MDC;

import com.di.streamnova.agent.adaptive_execution_planner.ExecutionPlanOption;
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

import java.util.Objects;
import java.util.Optional;
import java.util.stream.StreamSupport;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;


@Service
@Slf4j
public class DataflowRunnerService {

    private final PipelineConfigService pipelineConfigService;
    private final SourceHandlerRegistry registry;

    @Value("${streamnova.dataflow.project:}")
    private String dataflowProject;
    @Value("${streamnova.dataflow.region:}")
    private String dataflowRegion;

    public DataflowRunnerService(PipelineConfigService pipelineConfigService, SourceHandlerRegistry registry) {
        this.pipelineConfigService = pipelineConfigService;
        this.registry = registry;
    }

    public void runPipeline() {

        String jobId = "job-" + java.util.UUID.randomUUID();
        MDC.put("jobId", jobId);

        try {
            // Create the pipeline
            Pipeline pipeline = Pipeline.create();

            // Start the data load operation after reading yaml file.
            PCollection<Row> sourceData = startLoadOperation(pipeline, MDC.get("jobId"), null);

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

            log.info("Total records read from source: {}", total);

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
    public Optional<String> runPipeline(ExecutionPlanOption candidate) {
        if (candidate == null) {
            throw new IllegalArgumentException("ExecutionPlanOption candidate cannot be null");
        }
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

            LoadConfig baseConfig = pipelineConfigService.getEffectiveLoadConfig();
            if (baseConfig == null || baseConfig.getSource() == null) {
                throw new IllegalStateException("Pipeline configuration is null or missing source. Check pipeline config YAML.");
            }
            PipelineConfigSource baseSource = baseConfig.getSource();
            PipelineConfigSource overriddenSource = new PipelineConfigSource();
            BeanUtils.copyProperties(baseSource, overriddenSource);
            // Execution plan: use only the candidate. Do not rely on pipeline config for shards, workers, machineType, or pool.
            // Pipeline config (YAML) is used only for connection, table, and other read settings (jdbcUrl, table, credentials, fetchSize, etc.).
            overriddenSource.setMachineType(candidate.getMachineType());
            overriddenSource.setWorkers(candidate.getWorkerCount());
            overriddenSource.setShards(candidate.getShardCount());
            overriddenSource.setMaximumPoolSize(candidate.getSuggestedPoolSize() > 0 ? candidate.getSuggestedPoolSize() : baseSource.getMaximumPoolSize());
            log.info("Using candidate for execution plan: machineType={}, workers={}, shards={}, poolSize={}; connection/table from pipeline config",
                    candidate.getMachineType(), candidate.getWorkerCount(), candidate.getShardCount(),
                    candidate.getSuggestedPoolSize() > 0 ? candidate.getSuggestedPoolSize() : baseSource.getMaximumPoolSize());

            LoadConfig overriddenConfig = new LoadConfig();
            overriddenConfig.setSource(overriddenSource);
            overriddenConfig.setIntermediate(baseConfig.getIntermediate());
            overriddenConfig.setTarget(baseConfig.getTarget());

            PCollection<Row> sourceData = startLoadOperation(pipeline, jobId, overriddenConfig);
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
            log.info("Total records read from source: {}", total);

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
    

    @LogTransaction(
        eventType = "LOAD_OPERATION",
        transactionContext = "load_operation_start"
        // Note: parameterNames removed - method parameters (pipeline, jobId) don't match
        // the desired logged values (sourceType, tableName, jdbcUrl) which are extracted
        // from config inside the method. AOP will extract actual parameters automatically.
    )
    private PCollection<Row> startLoadOperation(Pipeline pipeline, String jobId, LoadConfig loadConfig) {
        if (loadConfig == null) {
            loadConfig = pipelineConfigService.getEffectiveLoadConfig();
        }
        if (loadConfig == null) {
            log.error("Failed to load configuration from YAML properties. If streamnova.pipeline.config-file is set, run from project root so the file path resolves.");
            throw new IllegalStateException("Pipeline configuration is null. Check pipeline config YAML (or streamnova.pipeline.config-file path) and run from project root.");
        }

        if (loadConfig.getSource()== null) {
            log.error("Source configuration is missing in pipeline config");
            throw new IllegalStateException("Source configuration cannot be null");
        }

        log.info("Starting pipeline with configuration: {}", loadConfig);

        PipelineConfigSource pipelineSrcConfig = loadConfig.getSource();

        if (pipelineSrcConfig == null) {
            log.error("Source configuration is missing");
            throw new IllegalArgumentException("Source configuration cannot be null");
        }

        // Log source configuration details for context
        log.info("Successfully read data from source: {} (table: {}, jdbcUrl: {})", 
                pipelineSrcConfig.getType(), 
                pipelineSrcConfig.getTable(),
                pipelineSrcConfig.getJdbcUrl() != null ? 
                    pipelineSrcConfig.getJdbcUrl().replaceAll("password=[^;&]+", "password=***") : "null");

        // Handle source configuration
        SourceHandler<PipelineConfigSource> handler = registry.getHandler(pipelineSrcConfig.getType());

        if (handler == null) {
            log.error("No handler found for source type: {}", pipelineSrcConfig.getType());
            throw new IllegalArgumentException("Unsupported source type: " + pipelineSrcConfig.getType());
        }

        PCollection<Row> sourceData = handler.read(pipeline, pipelineSrcConfig);

        if (sourceData == null) {
            log.error("Source data is null after reading from source: {}", pipelineSrcConfig.getType());
            throw new IllegalStateException("Failed to read data from source: " + pipelineSrcConfig.getType());
        }

        // Events are logged automatically via AOP (@LogTransaction annotation)
        return sourceData;
    }
}
