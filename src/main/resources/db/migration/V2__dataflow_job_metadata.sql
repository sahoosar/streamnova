-- Dataflow job metadata: track submitted jobs and status when the request times out.
-- Clients can GET status by run_id or dataflow_job_id.

CREATE TABLE IF NOT EXISTS dataflow_job_metadata (
    run_id              VARCHAR(64)   NOT NULL PRIMARY KEY,
    dataflow_job_id     VARCHAR(128)  NOT NULL,
    status              VARCHAR(32)   NOT NULL,
    submitted_at        TIMESTAMP     NOT NULL,
    updated_at           TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at         TIMESTAMP,
    message             VARCHAR(1024),
    execution_run_id     VARCHAR(64),
    source_type          VARCHAR(32),
    schema_name          VARCHAR(128),
    table_name           VARCHAR(128),
    machine_type         VARCHAR(64),
    worker_count         INT,
    shard_count          INT,
    created_at           TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dataflow_job_id UNIQUE (dataflow_job_id)
);

CREATE INDEX IF NOT EXISTS idx_dataflow_job_metadata_status
    ON dataflow_job_metadata (status);
CREATE INDEX IF NOT EXISTS idx_dataflow_job_metadata_submitted
    ON dataflow_job_metadata (submitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_dataflow_job_metadata_updated
    ON dataflow_job_metadata (updated_at DESC);
