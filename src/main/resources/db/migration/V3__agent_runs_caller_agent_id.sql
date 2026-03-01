-- Track which external agent triggered each run (for multi-agent / webhook tracking).
ALTER TABLE agent_runs ADD COLUMN IF NOT EXISTS caller_agent_id VARCHAR(64);
CREATE INDEX IF NOT EXISTS idx_agent_runs_caller_agent_id ON agent_runs (caller_agent_id);
-- Optional: job_id and message for correlation with execution-outcome (if not already present).
ALTER TABLE agent_runs ADD COLUMN IF NOT EXISTS job_id VARCHAR(128);
ALTER TABLE agent_runs ADD COLUMN IF NOT EXISTS message VARCHAR(1024);
