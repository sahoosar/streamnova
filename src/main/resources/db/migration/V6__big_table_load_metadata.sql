-- ============================================================
-- V6 : Big-Table Load Metadata
-- Stores every run, shard, GCS manifest, and validation row
-- for the 3-stage Postgres → GCS → BigQuery loader.
-- ============================================================

-- ---------------------------------------------------------
-- load_runs  :  one row per triggered load operation
-- ---------------------------------------------------------
CREATE TABLE load_runs (
    id                    VARCHAR(36)   NOT NULL PRIMARY KEY,
    table_schema          VARCHAR(128)  NOT NULL,
    table_name            VARCHAR(256)  NOT NULL,
    partition_col         VARCHAR(128)  NOT NULL,

    source_row_count      BIGINT,
    avg_row_bytes         INTEGER,
    total_data_bytes      BIGINT,

    machine_type          VARCHAR(64),
    worker_count          INTEGER,
    shard_count           INTEGER,
    max_concurrent        INTEGER,
    pool_size             INTEGER       NOT NULL DEFAULT 10,

    iteration_count       INTEGER       NOT NULL DEFAULT 0,
    status                VARCHAR(32)   NOT NULL DEFAULT 'PLANNING',
    -- PLANNING | STAGE1_RUNNING | STAGE1_DONE
    -- | STAGE2_RUNNING | STAGE2_DONE
    -- | STAGE3_RUNNING | STAGE3_DONE
    -- | PROMOTING | PROMOTED | FAILED

    gcs_bucket            VARCHAR(256),
    gcs_prefix            VARCHAR(512),
    bq_project            VARCHAR(128),
    bq_dataset            VARCHAR(256),
    bq_table              VARCHAR(256),
    write_disposition     VARCHAR(32)   DEFAULT 'WRITE_TRUNCATE',

    error_message         TEXT,

    started_at            TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    stage1_started_at     TIMESTAMPTZ,
    stage1_completed_at   TIMESTAMPTZ,
    stage2_completed_at   TIMESTAMPTZ,
    stage3_started_at     TIMESTAMPTZ,
    stage3_completed_at   TIMESTAMPTZ,
    promoted_at           TIMESTAMPTZ,

    -- JSON array of IterativePlanResult snapshots (all iterations)
    plan_iterations_json  TEXT
);

CREATE INDEX idx_load_runs_status ON load_runs (status);
CREATE INDEX idx_load_runs_table  ON load_runs (table_schema, table_name);

-- ---------------------------------------------------------
-- load_shards  :  one row per shard / partition range
-- ---------------------------------------------------------
CREATE TABLE load_shards (
    id                    VARCHAR(36)   NOT NULL PRIMARY KEY,
    run_id                VARCHAR(36)   NOT NULL REFERENCES load_runs (id),
    shard_index           INTEGER       NOT NULL,
    iteration_born        INTEGER       NOT NULL DEFAULT 1,   -- which planning iteration defined this shard

    partition_col         VARCHAR(128),
    min_key               BIGINT,
    max_key               BIGINT,

    estimated_rows        BIGINT,
    actual_row_count      BIGINT,

    gcs_path              VARCHAR(512),
    gcs_file_size_bytes   BIGINT,

    status                VARCHAR(32)   NOT NULL DEFAULT 'PENDING',
    -- PENDING | RUNNING | DONE | FAILED | SKIPPED (pilot-only shards recomputed in iter-3)

    attempt               INTEGER       NOT NULL DEFAULT 1,
    error_message         TEXT,

    started_at            TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ,
    duration_ms           BIGINT,

    UNIQUE (run_id, shard_index)
);

CREATE INDEX idx_load_shards_run_id        ON load_shards (run_id);
CREATE INDEX idx_load_shards_run_status    ON load_shards (run_id, status);

-- ---------------------------------------------------------
-- load_manifests  :  manifest written after stage 1 and stage 3
-- ---------------------------------------------------------
CREATE TABLE load_manifests (
    id                    VARCHAR(36)   NOT NULL PRIMARY KEY,
    run_id                VARCHAR(36)   NOT NULL REFERENCES load_runs (id),
    stage                 VARCHAR(32)   NOT NULL,   -- STAGE1 | STAGE3
    manifest_gcs_path     VARCHAR(512),
    total_files           INTEGER,
    total_rows            BIGINT,
    total_bytes           BIGINT,
    created_at            TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_load_manifests_run_id ON load_manifests (run_id);

-- ---------------------------------------------------------
-- load_validations  :  row-count reconciliation results
-- ---------------------------------------------------------
CREATE TABLE load_validations (
    id                    VARCHAR(36)   NOT NULL PRIMARY KEY,
    run_id                VARCHAR(36)   NOT NULL REFERENCES load_runs (id),
    stage                 VARCHAR(32)   NOT NULL,   -- AFTER_STAGE1 | AFTER_STAGE3

    expected_rows         BIGINT,
    actual_rows           BIGINT,
    delta_rows            BIGINT,
    delta_pct             NUMERIC(12,6),
    threshold_pct         NUMERIC(12,6),

    passed                BOOLEAN       NOT NULL,
    detail                TEXT,
    validated_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_load_validations_run_id ON load_validations (run_id);
