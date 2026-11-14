-- 0009_outbox.sql
--
-- Transactional outbox: state changes and the events they produce commit in
-- the same PostgreSQL transaction. A single relay publishes pending rows to
-- RabbitMQ with publisher confirms.

CREATE TABLE IF NOT EXISTS outbox (
    id             BIGSERIAL PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id   UUID NOT NULL,
    event_type     TEXT NOT NULL,
    payload        JSONB NOT NULL,
    headers        JSONB NOT NULL DEFAULT '{}',
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'published', 'failed')),
    attempts       INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at   TIMESTAMPTZ,
    last_error     TEXT
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox (status, available_at, id)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_outbox_aggregate
    ON outbox (aggregate_type, aggregate_id, id);

CREATE INDEX IF NOT EXISTS idx_outbox_published
    ON outbox (published_at)
    WHERE status = 'published';
