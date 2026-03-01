package com.di.streamnova.agent.execution_engine;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request body for POST /api/agent/execute. Supplies the candidate to run and optional executionRunId
 * so that execution status can be updated after run (fixes status staying RUNNING if client doesn't POST execution-outcome).
 * <p>
 * Handler selection: when you set {@link #source} and {@link #target}, the pipeline loads the respective
 * event config and assigns the handler for each stage. Example: {@code "source": "postgres", "target": "gcs"}
 * loads source from database_event_config.yml (PostgresHandler) and target from gcs_event_config.yml (GCS destination).
 * <ul>
 *   <li>2-stage: set source and target (source → destination).</li>
 *   <li>3-stage: set source, intermediate, and target (source → staging → destination).</li>
 * </ul>
 * Omitted values fall back to pipeline config. See GET /api/agent/pipeline-handlers.
 */
@Data
@NoArgsConstructor
public class ExecuteRequest {
    /** Candidate to run (machine type, workers, shards, etc.). */
    private CandidateBody candidate;
    /** Optional run ID from recommend response; when set, execution status is updated to SUCCESS/FAILED after run. */
    @JsonAlias("execution_run_id")
    private String executionRunId;
    /** Optional source handler/connection (e.g. postgres, oracle, gcs). Maps to source handler for this run. */
    private String source;
    /** Optional intermediate handler type (e.g. gcs). For 3-stage: source → intermediate → target. */
    private String intermediate;
    /** Optional target/destination handler type (e.g. bigquery). Maps to destination for this run. */
    private String target;

    /** Optional source schema for template (e.g. public). */
    @JsonAlias("source_schema")
    private String sourceSchema;
    /** Optional source table for template (e.g. my_table or schema.my_table). */
    @JsonAlias("source_table")
    private String sourceTable;
    /** Optional custom source query; if empty, engine uses schema+table. */
    @JsonAlias("source_query")
    private String sourceQuery;
    /** Optional extraction mode: FULL, INCREMENTAL. */
    @JsonAlias("extraction_mode")
    private String extractionMode;
    /** Optional incremental column (e.g. updated_at). */
    @JsonAlias("incremental_column")
    private String incrementalColumn;
    /** Optional watermark from (e.g. 2026-02-01T00:00:00Z). */
    @JsonAlias("watermark_from")
    private String watermarkFrom;
    /** Optional watermark to (e.g. 2026-02-15T00:00:00Z). */
    @JsonAlias("watermark_to")
    private String watermarkTo;

    /**
     * When true, this run reserves the full connection pool (no other run can start until this one finishes).
     * When false, run shares the pool; limited by max-concurrent-runs guardrail.
     */
    private Boolean exclusive;

    /** Optional caller/agent id for audit; when set, list audit via GET /api/agent/audit?callerAgentId=... */
    @JsonAlias("caller_agent_id")
    private String callerAgentId;

    @Data
    @NoArgsConstructor
    public static class CandidateBody {
        @JsonAlias("machine_type")
        private String machineType;
        @JsonAlias("worker_count")
        private Integer workerCount;
        @JsonAlias("shard_count")
        private Integer shardCount;
        @JsonAlias("partition_count")
        private Integer partitionCount;
        @JsonAlias("max_concurrent_shards")
        private Integer maxConcurrentShards;
        @JsonAlias("virtual_cpus")
        private Integer virtualCpus;
        @JsonAlias("suggested_pool_size")
        private Integer suggestedPoolSize;
        private String label;
    }
}
