-- 0013_scheduled_event_headers.sql
--
-- Preserve W3C trace context across DB-backed timers. The outbox already
-- carries headers; the timer table needs the same field so a trace survives
-- the PostgreSQL delay boundary.

ALTER TABLE scheduled_events
    ADD COLUMN IF NOT EXISTS headers JSONB NOT NULL DEFAULT '{}';
