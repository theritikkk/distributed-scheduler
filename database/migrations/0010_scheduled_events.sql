-- 0010_scheduled_events.sql
--
-- DB-backed timers. The transactional outbox materializes task.scheduled and
-- task.retry events into this table; a poller publishes due rows to RabbitMQ.
-- This removes per-message TTL from the critical path and makes delayed work
-- inspectable in PostgreSQL. It is a prerequisite for quorum queues.

CREATE TABLE IF NOT EXISTS scheduled_events (
    id             BIGSERIAL PRIMARY KEY,
    aggregate_type TEXT NOT NULL DEFAULT 'task',
    aggregate_id   UUID NOT NULL,
    kind           TEXT NOT NULL CHECK (kind IN ('wakeup', 'retry')),
    destination    TEXT NOT NULL
                   CHECK (destination IN ('scheduler.due', 'scheduler.tasks')),
    dedupe_key     TEXT NOT NULL,
    payload        JSONB NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'published', 'failed', 'cancelled')),
    attempts       INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at   TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at   TIMESTAMPTZ,
    cancelled_at   TIMESTAMPTZ,
    last_error     TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_scheduled_events_dedupe
    ON scheduled_events (dedupe_key);

CREATE INDEX IF NOT EXISTS idx_scheduled_events_due
    ON scheduled_events (available_at, id)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_scheduled_events_aggregate
    ON scheduled_events (aggregate_type, aggregate_id, status);

CREATE INDEX IF NOT EXISTS idx_scheduled_events_cleanup
    ON scheduled_events (published_at)
    WHERE status IN ('published', 'cancelled');
