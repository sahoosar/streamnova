-- Dataflow job metadata: track submitted jobs and their status even when the
-- Cloud Run request times out (e.g. before result.waitUntilFinish() returns).
-- Apply with your migration tool (Flyway/Liquibase) or run manually.
--
-- Flow:
-- 1. On pipeline.run() → INSERT one row with dataflow_job_id, status SUBMITTED/RUNNING.
-- 2. If waitUntilFinish() returns in the same request → UPDATE status to DONE/FAILED.
-- 3. If the request expires (e.g. Cloud Run 1h timeout) → row stays SUBMITTED/RUNNING
--    until a separate poller (scheduler) calls Dataflow API and UPDATEs the row.
-- So clients can GET status by run_id or dataflow_job_id regardless of request lifetime.

-- ---------------------------------------------------------------------------
-- Dataflow job metadata (survives Cloud Run request timeout)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dataflow_job_metadata (
    run_id              VARCHAR(64)  NOT NULL PRIMARY KEY,  -- our execution run id (e.g. job-<uuid>)
    dataflow_job_id     VARCHAR(128) NOT NULL,               -- GCP Dataflow job id from pipeline.run()
    status              VARCHAR(32)  NOT NULL,               -- SUBMITTED, RUNNING, DONE, FAILED, UNKNOWN, REQUEST_TIMEOUT
    submitted_at        TIMESTAMP    NOT NULL,
    updated_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at         TIMESTAMP,                           -- set when status becomes DONE/FAILED
    message             VARCHAR(1024),                      -- error or status detail from Dataflow
    -- Optional context (for display / correlation with agent_runs)
    execution_run_id    VARCHAR(64),                        -- if same as run_id or link to agent execution
    source_type         VARCHAR(32),
    schema_name         VARCHAR(128),
    table_name          VARCHAR(128),
    machine_type        VARCHAR(64),
    worker_count        INT,
    shard_count         INT,
    created_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dataflow_job_id UNIQUE (dataflow_job_id)
);

CREATE INDEX IF NOT EXISTS idx_dataflow_job_metadata_status
    ON dataflow_job_metadata (status);
CREATE INDEX IF NOT EXISTS idx_dataflow_job_metadata_submitted
    ON dataflow_job_metadata (submitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_dataflow_job_metadata_updated
    ON dataflow_job_metadata (updated_at DESC);

-- Optional: add job_id and message to agent_runs so ExecutionStatus (MetricsLearningStore)
-- can persist Dataflow job id and outcome in the same place. Uncomment if you use agent_runs
-- as the single source of truth and want to sync back from dataflow_job_metadata.
-- ALTER TABLE agent_runs ADD COLUMN IF NOT EXISTS job_id VARCHAR(128);
-- ALTER TABLE agent_runs ADD COLUMN IF NOT EXISTS message VARCHAR(1024);
