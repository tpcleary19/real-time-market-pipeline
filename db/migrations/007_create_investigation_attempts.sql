-- Forward-only accounting: earlier jobs and their usage are not reconstructed.
CREATE TABLE IF NOT EXISTS investigation_attempts (
    claim_token TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES investigation_jobs(event_id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    budget_hour TIMESTAMPTZ NOT NULL REFERENCES investigation_hourly_budget(hour_start),
    trigger TEXT NOT NULL CHECK (trigger IN ('current_hour', 'backfill', 'manual')),
    provider TEXT,
    requested_model TEXT,
    response_model TEXT,
    response_service_tier TEXT,
    response_id TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    model_request_started_at TIMESTAMPTZ,
    response_received_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    duration_ms NUMERIC CHECK (duration_ms >= 0),
    model_duration_ms NUMERIC CHECK (model_duration_ms >= 0),
    outcome TEXT NOT NULL DEFAULT 'running'
        CHECK (outcome IN ('running', 'completed', 'skipped', 'failed', 'expired')),
    error_class TEXT,
    usage_status TEXT NOT NULL DEFAULT 'unknown'
        CHECK (usage_status IN ('unknown', 'reported', 'not_requested')),
    raw_usage JSONB,
    raw_tool_usage JSONB,
    input_tokens BIGINT CHECK (input_tokens >= 0),
    output_tokens BIGINT CHECK (output_tokens >= 0),
    cache_read_tokens BIGINT CHECK (cache_read_tokens >= 0),
    cache_write_tokens BIGINT CHECK (cache_write_tokens >= 0),
    reasoning_tokens BIGINT CHECK (reasoning_tokens >= 0),
    billable_searches BIGINT CHECK (billable_searches >= 0),
    estimated_cost_usd NUMERIC(20, 10) CHECK (estimated_cost_usd >= 0),
    cost_status TEXT NOT NULL DEFAULT 'unknown'
        CHECK (cost_status IN ('unknown', 'estimated', 'not_requested')),
    cost_unknown_reason TEXT,
    pricing_snapshot JSONB,
    UNIQUE (event_id, attempt_number)
);

CREATE INDEX IF NOT EXISTS idx_investigation_attempts_started
    ON investigation_attempts (started_at);
CREATE INDEX IF NOT EXISTS idx_investigation_attempts_running
    ON investigation_attempts (started_at) WHERE outcome = 'running';
