package com.di.streamnova.agent.trigger;

import com.di.streamnova.agent.estimator.LoadPattern;
import com.di.streamnova.agent.recommender.UserMode;
import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Request body for POST /api/agent/webhook/trigger.
 * Triggers the agent flow: recommend (profile → candidates → estimate → recommend), then optionally execute.
 */
@Data
@NoArgsConstructor
public class WebhookTriggerRequest {

    /** Source handler key (e.g. postgres, oracle). Optional; falls back to pipeline config. */
    private String source;
    /** Target handler key (e.g. gcs, bigquery). Optional for recommend-only; required for execute. */
    private String target;
    /** Intermediate handler key for 3-stage (e.g. gcs). Optional. */
    private String intermediate;

    /** Recommendation mode: COST_OPTIMAL, FAST_LOAD, BALANCED. Default BALANCED. */
    @JsonAlias("mode")
    private UserMode mode = UserMode.BALANCED;
    /** If true, after recommend runs execute with the recommended candidate. Default true. */
    @JsonAlias("execute_immediately")
    private boolean executeImmediately = true;

    /** Run warm-up throughput discovery. Default true. */
    @JsonAlias("warm_up")
    private Boolean warmUp = true;
    /** Load pattern: DIRECT or GCS_BQ. Optional. */
    @JsonAlias("load_pattern")
    private LoadPattern loadPattern;

    /** Optional guardrail overrides. */
    @JsonAlias("max_cost_usd")
    private Double maxCostUsd;
    @JsonAlias("max_duration_sec")
    private Double maxDurationSec;
    @JsonAlias("min_throughput_mb_per_sec")
    private Double minThroughputMbPerSec;
    @JsonAlias("allowed_machine_types")
    private List<String> allowedMachineTypes;
    @JsonAlias("database_pool_max_size")
    private Integer databasePoolMaxSize;

    /** Optional idempotency key for deduplication (future use). */
    @JsonAlias("idempotency_key")
    private String idempotencyKey;

    /** Optional caller agent id (e.g. agent-1, agent-2). Used to track which of the 10 agents triggered this request; persisted with the run for filtering and reporting. */
    @JsonAlias("caller_agent_id")
    private String callerAgentId;

    /** When true, this run reserves the full connection pool (exclusive). When false, run shares the pool (subject to max-concurrent-runs). */
    private Boolean exclusive;
}
