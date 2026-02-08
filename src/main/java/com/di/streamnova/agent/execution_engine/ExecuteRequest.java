package com.di.streamnova.agent.execution_engine;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request body for POST /api/agent/execute. Supplies the candidate to run and optional executionRunId
 * so that execution status can be updated after run (fixes status staying RUNNING if client doesn't POST execution-outcome).
 */
@Data
@NoArgsConstructor
public class ExecuteRequest {
    /** Candidate to run (machine type, workers, shards, etc.). */
    private CandidateBody candidate;
    /** Optional run ID from recommend response; when set, execution status is updated to SUCCESS/FAILED after run. */
    @JsonAlias("execution_run_id")
    private String executionRunId;

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
