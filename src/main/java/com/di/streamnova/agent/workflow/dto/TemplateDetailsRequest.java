package com.di.streamnova.agent.workflow.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request body for GET/POST template-details: same shape as execute (source, target, optional intermediate, candidate, schema/table).
 * Candidate is optional; when omitted, default workers/shards are used for the calculated context.
 */
@Data
@NoArgsConstructor
public class TemplateDetailsRequest {

    private String source;
    private String intermediate;
    private String target;

    /** Optional candidate (workers, shards, etc.); when omitted, defaults are used for preview. */
    private CandidateBody candidate;

    @JsonAlias("source_schema")
    private String sourceSchema;
    @JsonAlias("source_table")
    private String sourceTable;
    @JsonAlias("source_query")
    private String sourceQuery;
    @JsonAlias("extraction_mode")
    private String extractionMode;
    @JsonAlias("incremental_column")
    private String incrementalColumn;
    @JsonAlias("watermark_from")
    private String watermarkFrom;
    @JsonAlias("watermark_to")
    private String watermarkTo;

    @Data
    @NoArgsConstructor
    public static class CandidateBody {
        @JsonAlias("machine_type")
        private String machineType;
        @JsonAlias("worker_count")
        private Integer workerCount;
        @JsonAlias("shard_count")
        private Integer shardCount;
        @JsonAlias("virtual_cpus")
        private Integer virtualCpus;
        @JsonAlias("suggested_pool_size")
        private Integer suggestedPoolSize;
        private String label;
    }
}
