-- 0012_archive_tables.sql
--
-- Online archival targets for terminal history. The live tables keep serving
-- the scheduling hot path; a bounded batch job copies old terminal rows here
-- and then deletes them from the live table in the same transaction.
--
-- Live-table declarative partitioning is intentionally not performed here:
-- task_executions has (task_id, scheduled_for) and active-execution unique
-- indexes that cannot be preserved unchanged under a time partition key. The
-- archive tables are the compatible, online alternative.

CREATE TABLE IF NOT EXISTS task_execution_archive (
    id              UUID PRIMARY KEY,
    task_id         UUID NOT NULL,
    worker_id       VARCHAR(255),
    scheduled_for   TIMESTAMPTZ NOT NULL,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    status          VARCHAR(20) NOT NULL,
    output          TEXT,
    error_message   TEXT,
    lease_id        UUID,
    lease_expires_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ,
    attempt         INTEGER NOT NULL,
    created_at      TIMESTAMPTZ,
    archived_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_task_execution_archive_task
    ON task_execution_archive (task_id, scheduled_for);

CREATE INDEX IF NOT EXISTS idx_task_execution_archive_archived_at
    ON task_execution_archive (archived_at);

CREATE TABLE IF NOT EXISTS task_execution_attempts_archive (
    id              UUID PRIMARY KEY,
    execution_id    UUID NOT NULL,
    attempt         INTEGER NOT NULL,
    worker_id       VARCHAR(255),
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    status          TEXT NOT NULL,
    error_message   TEXT,
    archived_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_task_execution_attempts_archive_execution
    ON task_execution_attempts_archive (execution_id, attempt);

CREATE INDEX IF NOT EXISTS idx_task_execution_attempts_archive_archived_at
    ON task_execution_attempts_archive (archived_at);

CREATE TABLE IF NOT EXISTS outbox_archive (
    id             BIGINT PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id   UUID NOT NULL,
    event_type     TEXT NOT NULL,
    payload        JSONB NOT NULL,
    headers        JSONB NOT NULL DEFAULT '{}',
    status         TEXT NOT NULL,
    attempts       INTEGER NOT NULL,
    available_at   TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    published_at   TIMESTAMPTZ,
    last_error     TEXT,
    archived_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbox_archive_aggregate
    ON outbox_archive (aggregate_type, aggregate_id, id);

CREATE INDEX IF NOT EXISTS idx_outbox_archive_archived_at
    ON outbox_archive (archived_at);

CREATE INDEX IF NOT EXISTS idx_outbox_archive_status
    ON outbox_archive (status, archived_at);

-- Bounded scans for the archive job. These are partial/expression indexes so
-- the hot pending path is unaffected.
CREATE INDEX IF NOT EXISTS idx_task_executions_archive_scan
    ON task_executions (COALESCE(completed_at, created_at))
    WHERE status IN ('completed', 'failed', 'timeout', 'cancelled');

CREATE INDEX IF NOT EXISTS idx_outbox_published_scan
    ON outbox (published_at)
    WHERE status = 'published';

CREATE INDEX IF NOT EXISTS idx_outbox_failed_scan
    ON outbox (created_at)
    WHERE status = 'failed';
