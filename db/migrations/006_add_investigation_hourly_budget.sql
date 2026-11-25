CREATE TABLE investigation_hourly_budget (
    hour_start TIMESTAMPTZ PRIMARY KEY,
    started_count INTEGER NOT NULL DEFAULT 0
        CHECK (started_count BETWEEN 0 AND 3)
);

ALTER TABLE investigation_jobs
    ADD COLUMN budget_hour TIMESTAMPTZ;