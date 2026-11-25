-- Durable inbox for price-spikes; a separate worker will investigate pending jobs.
CREATE TABLE IF NOT EXISTS investigation_jobs (
    event_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    spike JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_investigation_jobs_pending
    ON investigation_jobs (created_at) WHERE status = 'pending';
