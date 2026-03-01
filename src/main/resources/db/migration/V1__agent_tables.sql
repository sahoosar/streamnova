-- Agent tables: profiler output and Metrics & Learning Store.
-- Profiler: table profiles, throughput discovery; agent runs; estimates vs actuals for learning.

-- ---------------------------------------------------------------------------
-- Profiler: table profiles (row count, row size, complexity)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_table_profiles (
    run_id              VARCHAR(64)  NOT NULL PRIMARY KEY,
    source_type         VARCHAR(32)  NOT NULL,
    schema_name         VARCHAR(128) NOT NULL,
    table_name          VARCHAR(128) NOT NULL,
    row_count_estimate  BIGINT       NOT NULL,
    avg_row_size_bytes  INT          NOT NULL,
    estimated_total_bytes BIGINT     NOT NULL,
    complexity          VARCHAR(32),
    profiled_at         TIMESTAMP    NOT NULL,
    created_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_agent_table_profiles_table
    ON agent_table_profiles (source_type, schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_agent_table_profiles_profiled_at
    ON agent_table_profiles (profiled_at DESC);

-- ---------------------------------------------------------------------------
-- Profiler: throughput discovery (warm-up read results)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_throughput_discovery (
    run_id              VARCHAR(64)  NOT NULL PRIMARY KEY,
    source_type         VARCHAR(32)  NOT NULL,
    schema_name         VARCHAR(128) NOT NULL,
    table_name          VARCHAR(128) NOT NULL,
    bytes_read          BIGINT       NOT NULL,
    duration_ms         BIGINT       NOT NULL,
    rows_read           INT          NOT NULL,
    throughput_mb_per_sec DOUBLE PRECISION NOT NULL,
    sampled_at          TIMESTAMP    NOT NULL,
    created_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_agent_throughput_table
    ON agent_throughput_discovery (source_type, schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_agent_throughput_sampled_at
    ON agent_throughput_discovery (sampled_at DESC);

-- ---------------------------------------------------------------------------
-- Agent runs (plan → recommend → execute)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_runs (
    run_id              VARCHAR(64)  NOT NULL PRIMARY KEY,
    profile_run_id      VARCHAR(64),
    mode                VARCHAR(32)  NOT NULL,
    load_pattern        VARCHAR(32)  NOT NULL,
    source_type         VARCHAR(32)  NOT NULL,
    schema_name         VARCHAR(128) NOT NULL,
    table_name          VARCHAR(128) NOT NULL,
    status              VARCHAR(32)  NOT NULL,
    started_at          TIMESTAMP,
    finished_at         TIMESTAMP,
    created_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_agent_runs_status ON agent_runs (status);
CREATE INDEX IF NOT EXISTS idx_agent_runs_table ON agent_runs (source_type, schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_agent_runs_started ON agent_runs (started_at DESC);

-- ---------------------------------------------------------------------------
-- Estimates vs actuals (self-learning)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_estimates_vs_actuals (
    id                  BIGSERIAL   PRIMARY KEY,
    run_id              VARCHAR(64)  NOT NULL,
    estimated_duration_sec DOUBLE PRECISION,
    actual_duration_sec   DOUBLE PRECISION,
    estimated_cost_usd    DOUBLE PRECISION,
    actual_cost_usd       DOUBLE PRECISION,
    machine_type        VARCHAR(64),
    worker_count        INT,
    shard_count         INT,
    recorded_at         TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_agent_estimates_run ON agent_estimates_vs_actuals (run_id);
