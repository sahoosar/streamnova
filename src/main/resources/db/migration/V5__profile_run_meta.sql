-- Metadata for every profile run (success or failure). Links to agent_table_profiles and agent_throughput_discovery when present.
CREATE TABLE IF NOT EXISTS profile_run_meta (
    run_id          VARCHAR(64)  NOT NULL PRIMARY KEY,
    completed_at    TIMESTAMP    NOT NULL,
    error_message   TEXT,
    source_type     VARCHAR(32),
    schema_name     VARCHAR(128),
    table_name      VARCHAR(128)
);

CREATE INDEX IF NOT EXISTS idx_profile_run_meta_completed ON profile_run_meta (completed_at DESC);
CREATE INDEX IF NOT EXISTS idx_profile_run_meta_table ON profile_run_meta (source_type, schema_name, table_name);
