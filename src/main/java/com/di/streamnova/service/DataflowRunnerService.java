package com.di.streamnova.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.MDC;

import com.di.streamnova.aspect.LogTransaction;
import com.di.streamnova.config.*;
import com.di.streamnova.util.Dlq;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.schemas.Schema;
import org.springframework.stereotype.Service;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.PCollection;
import com.di.streamnova.handler.SourceHandler;
import com.di.streamnova.handler.SourceHandlerRegistry;

import java.util.Objects;
import java.util.stream.StreamSupport;


@Service
@Slf4j
public class DataflowRunnerService {

    private final YamlPipelineProperties yamlProps;
    private final SourceHandlerRegistry registry;

    public DataflowRunnerService(YamlPipelineProperties yamlProps, SourceHandlerRegistry registry) {
        this.yamlProps = yamlProps;
        this.registry = registry;
    }

    public void runPipeline() {

        String jobId = "job-" + java.util.UUID.randomUUID();
        MDC.put("jobId", jobId);

        try {
            // Create the pipeline
            Pipeline pipeline = Pipeline.create();

            //Start the data load operation after reading yaml file.
            PCollection<Row> sourceData = startLoadOperation(pipeline,MDC.get("jobId"));

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
    

    @LogTransaction(
        eventType = "LOAD_OPERATION",
        transactionContext = "load_operation_start",
        parameterNames = {"sourceType", "tableName", "jdbcUrl"}
    )
    private PCollection<Row> startLoadOperation(Pipeline pipeline, String jobId ) {
        // Get the full configuration
        LoadConfig loadConfig = yamlProps.getConfig();

        if (loadConfig == null) {
            log.error("Failed to load configuration from YAML properties");
            throw new IllegalStateException("Configuration cannot be null");
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

        log.info("Successfully read data from source: {}", pipelineSrcConfig.getType());

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
