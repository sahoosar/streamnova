-- Persistent audit of external agent request/response per invocation (for tracking and support).
CREATE TABLE IF NOT EXISTS agent_invocation_audit (
    id                  BIGSERIAL   PRIMARY KEY,
    caller_agent_id     VARCHAR(64) NOT NULL,
    run_id              VARCHAR(64),
    endpoint            VARCHAR(256) NOT NULL,
    request_summary     TEXT,
    response_status     INT,
    response_summary    TEXT,
    created_at         TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_agent_invocation_audit_caller ON agent_invocation_audit (caller_agent_id);
CREATE INDEX IF NOT EXISTS idx_agent_invocation_audit_run ON agent_invocation_audit (run_id);
CREATE INDEX IF NOT EXISTS idx_agent_invocation_audit_created ON agent_invocation_audit (created_at DESC);
