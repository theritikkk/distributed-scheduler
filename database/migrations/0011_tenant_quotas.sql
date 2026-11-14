-- 0011_tenant_quotas.sql
--
-- Per-user (tenant) admission quotas. PostgreSQL remains authoritative; the
-- API caches this row for a short TTL and falls back to configured defaults
-- when a user has no row or the cache is unavailable.
--
-- weight is a multiplier on tasks_per_min and burst. A weight of 2 gives the
-- tenant twice the configured admission rate. This is weighted admission
-- fairness, not a replacement for the FIFO work queue.

CREATE TABLE IF NOT EXISTS tenant_quotas (
    user_id       UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    weight        NUMERIC(6, 3) NOT NULL DEFAULT 1.000
                  CHECK (weight > 0 AND weight <= 100),
    tasks_per_min INTEGER NOT NULL DEFAULT 60
                  CHECK (tasks_per_min >= 1 AND tasks_per_min <= 1000000),
    burst         INTEGER NOT NULL DEFAULT 10
                  CHECK (burst >= 1 AND burst <= 1000000),
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tenant_quotas_enabled
    ON tenant_quotas (enabled)
    WHERE enabled;

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tenant_quotas_updated_at ON tenant_quotas;
CREATE TRIGGER tenant_quotas_updated_at BEFORE UPDATE ON tenant_quotas
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at();
