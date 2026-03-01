package com.di.streamnova.ai.tool;

import com.di.streamnova.agent.execution_engine.ExecuteRequest;
import com.di.streamnova.agent.execution_engine.ExecutionEngine;
import com.di.streamnova.agent.execution_engine.ExecutionResult;
import com.di.streamnova.agent.execution_planner.ExecutionPlanOption;
import com.di.streamnova.config.HandlerOverrides;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Spring AI Tool — wraps {@link ExecutionEngine} so that Gemini can trigger
 * a Dataflow pipeline execution autonomously when the user asks
 * "run the pipeline now" or "execute a postgres-to-gcs load".
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "streamnova.ai.enabled", havingValue = "true", matchIfMissing = true)
public class PipelineExecutionTool {

    private final ExecutionEngine executionEngine;

    /**
     * Executes a data pipeline with the given machine type and routing.
     */
    @Tool(description = """
            Executes a StreamNova data pipeline (Apache Beam / Dataflow) with the specified
            machine type, worker count, shard count, source, and target.
            Source types: postgres | oracle | mysql | gcs | bigquery.
            Target types: gcs | bigquery.
            Intermediate (optional): gcs — required for 3-stage pipelines (DB → GCS → BigQuery).
            Returns jobId and success status.
            Only call this after confirming parameters with the user.
            """)
    public String executePipeline(
            @ToolParam(description = "GCP machine type, e.g. n2d-standard-4")  String machineType,
            @ToolParam(description = "Number of Dataflow workers, e.g. 2")      int workerCount,
            @ToolParam(description = "Number of extraction shards, e.g. 4")     int shardCount,
            @ToolParam(description = "Source handler: postgres|oracle|mysql|gcs|bigquery") String source,
            @ToolParam(description = "Target handler: gcs|bigquery")            String target,
            @ToolParam(description = "Intermediate handler (optional, for 3-stage): gcs") String intermediate
    ) {
        log.info("[TOOL:execute] machine={} workers={} shards={} {}→{}→{}",
                machineType, workerCount, shardCount, source, intermediate, target);

        ExecutionPlanOption candidate = ExecutionPlanOption.builder()
                .machineType(machineType)
                .workerCount(workerCount)
                .shardCount(shardCount)
                .virtualCpus(4)
                .build();

        HandlerOverrides overrides = HandlerOverrides.builder()
                .source(nullIfBlank(source))
                .intermediate(nullIfBlank(intermediate))
                .target(nullIfBlank(target))
                .build();

        ExecuteRequest request = new ExecuteRequest();
        request.setSource(nullIfBlank(source));
        request.setIntermediate(nullIfBlank(intermediate));
        request.setTarget(nullIfBlank(target));

        try {
            ExecutionResult result = executionEngine.execute(candidate, overrides, request);
            if (result.isSuccess()) {
                return String.format(
                        "Pipeline started successfully. jobId=%s stages=%d source=%s target=%s",
                        result.getJobId(), result.getStages(), source, target);
            } else {
                return "Pipeline execution failed: " + result.getMessage();
            }
        } catch (Exception ex) {
            log.error("[TOOL:execute] failed: {}", ex.getMessage());
            return "Pipeline execution error: " + ex.getMessage();
        }
    }

    /**
     * Describes available pipeline templates (useful for Gemini to pick a route).
     */
    @Tool(description = """
            Lists the available StreamNova pipeline templates with their source→target routes.
            Call this when the user asks which pipeline types are available.
            """)
    public String listPipelineTemplates() {
        return """
                Available Pipeline Templates:
                  1. db-to-gcs            → Database (postgres/oracle/mysql) → GCS
                  2. db-to-gcs-to-bq      → Database → GCS (staging) → BigQuery  [3-stage]
                  3. gcs-to-bq            → GCS → BigQuery
                  4. gcs-to-gcs           → GCS → GCS (copy/transform)
                  5. warehouse-to-gcs-to-bq → Hive/Impala → GCS → BigQuery  [3-stage]
                  6. bq-to-gcs            → BigQuery → GCS
                """;
    }

    private static String nullIfBlank(String s) {
        return (s == null || s.isBlank()) ? null : s.trim();
    }
}
