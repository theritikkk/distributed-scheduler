-- Distributed Task Scheduler - Database Schema
-- PostgreSQL 15+

-- Users table for authentication
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_users_email ON users(email);

-- Tasks table: task definitions and schedules
CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    task_name VARCHAR(255) NOT NULL,
    command_payload JSONB NOT NULL,
    schedule_type VARCHAR(20) NOT NULL CHECK (schedule_type IN ('one-time', 'recurring')),
    cron_expression VARCHAR(100),
    next_execution_time TIMESTAMPTZ NOT NULL,
    dispatch_at TIMESTAMPTZ NOT NULL,
    jitter_seconds INTEGER NOT NULL DEFAULT 0,
    jitter_policy VARCHAR(10) NOT NULL DEFAULT 'stable',
    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'scheduled', 'running', 'completed', 'failed', 'cancelled')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT cron_required_for_recurring CHECK (
        (schedule_type = 'one-time' AND cron_expression IS NULL) OR
        (schedule_type = 'recurring' AND cron_expression IS NOT NULL)
    ),
    CONSTRAINT tasks_jitter_seconds_range CHECK (jitter_seconds BETWEEN 0 AND 3600),
    CONSTRAINT tasks_jitter_policy_valid CHECK (jitter_policy IN ('none', 'stable')),
    CONSTRAINT tasks_jitter_policy_none_zero CHECK (jitter_policy <> 'none' OR jitter_seconds = 0),
    CONSTRAINT tasks_dispatch_at_not_before_next CHECK (dispatch_at >= next_execution_time)
);

CREATE INDEX idx_tasks_user_id ON tasks(user_id);
CREATE INDEX idx_tasks_status ON tasks(status);
CREATE INDEX idx_tasks_next_execution_time ON tasks(next_execution_time) WHERE status IN ('pending', 'scheduled');
CREATE INDEX idx_tasks_dispatch_at_due ON tasks(dispatch_at) WHERE status IN ('pending', 'scheduled');
CREATE INDEX idx_tasks_user_status ON tasks(user_id, status);

-- Task executions: history of each run
CREATE TABLE IF NOT EXISTS task_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    worker_id VARCHAR(255),
    scheduled_for TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'timeout', 'cancelled')),
    output TEXT,
    error_message TEXT,
    lease_id UUID,
    lease_expires_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ,
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_task_executions_task_id ON task_executions(task_id);
CREATE INDEX idx_task_executions_started_at ON task_executions(started_at);
CREATE INDEX idx_task_executions_status ON task_executions(status);
CREATE INDEX idx_task_executions_retry ON task_executions(next_attempt_at) WHERE status = 'pending';
CREATE INDEX idx_task_executions_lease ON task_executions(lease_expires_at) WHERE status = 'running';
CREATE UNIQUE INDEX uq_task_executions_slot ON task_executions(task_id, scheduled_for);
CREATE UNIQUE INDEX uq_task_executions_active
    ON task_executions(task_id) WHERE status IN ('pending', 'running');

-- Execution attempts: one row per worker claim / lease
CREATE TABLE IF NOT EXISTS task_execution_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    execution_id UUID NOT NULL REFERENCES task_executions(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    worker_id VARCHAR(255),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'timeout', 'cancelled')),
    error_message TEXT,
    UNIQUE (execution_id, attempt)
);

CREATE INDEX idx_task_execution_attempts_execution
    ON task_execution_attempts(execution_id, attempt);

-- Durable API idempotency keys
CREATE TABLE IF NOT EXISTS idempotency_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('in_progress', 'completed', 'failed')),
    response_status INTEGER,
    response_body JSONB,
    resource_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '24 hours',
    UNIQUE (user_id, key)
);

CREATE INDEX idx_idempotency_expires_at ON idempotency_keys(expires_at);

-- Per-user admission quotas. The API caches these briefly; PostgreSQL remains
-- authoritative. weight is a multiplier on the configured rate and burst.
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

-- Transactional outbox: state changes and their RabbitMQ events commit together
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

CREATE INDEX idx_outbox_pending
    ON outbox (status, available_at, id) WHERE status = 'pending';
CREATE INDEX idx_outbox_aggregate ON outbox (aggregate_type, aggregate_id, id);
CREATE INDEX idx_outbox_published
    ON outbox (published_at) WHERE status = 'published';

CREATE INDEX idx_outbox_published_scan
    ON outbox (published_at) WHERE status = 'published';
CREATE INDEX idx_outbox_failed_scan
    ON outbox (created_at) WHERE status = 'failed';

-- Terminal history archive. A bounded batch job copies rows here before
-- deleting them from the live tables; no pending work is ever archived.
CREATE TABLE IF NOT EXISTS task_execution_archive (
    id               UUID PRIMARY KEY,
    task_id          UUID NOT NULL,
    worker_id        VARCHAR(255),
    scheduled_for    TIMESTAMPTZ NOT NULL,
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    status           VARCHAR(20) NOT NULL,
    output           TEXT,
    error_message    TEXT,
    lease_id         UUID,
    lease_expires_at TIMESTAMPTZ,
    next_attempt_at  TIMESTAMPTZ,
    attempt          INTEGER NOT NULL,
    created_at       TIMESTAMPTZ,
    archived_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_task_execution_archive_task
    ON task_execution_archive (task_id, scheduled_for);
CREATE INDEX idx_task_execution_archive_archived_at
    ON task_execution_archive (archived_at);

CREATE TABLE IF NOT EXISTS task_execution_attempts_archive (
    id           UUID PRIMARY KEY,
    execution_id UUID NOT NULL,
    attempt      INTEGER NOT NULL,
    worker_id    VARCHAR(255),
    started_at   TIMESTAMPTZ NOT NULL,
    finished_at  TIMESTAMPTZ,
    status       TEXT NOT NULL,
    error_message TEXT,
    archived_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_task_execution_attempts_archive_execution
    ON task_execution_attempts_archive (execution_id, attempt);
CREATE INDEX idx_task_execution_attempts_archive_archived_at
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

CREATE INDEX idx_outbox_archive_aggregate
    ON outbox_archive (aggregate_type, aggregate_id, id);
CREATE INDEX idx_outbox_archive_archived_at
    ON outbox_archive (archived_at);

CREATE INDEX idx_task_executions_archive_scan
    ON task_executions (COALESCE(completed_at, created_at))
    WHERE status IN ('completed', 'failed', 'timeout', 'cancelled');

-- DB-backed timers: outbox relay materializes schedule/retry events here and a
-- poller publishes them when due, avoiding per-message TTL delay queues.
CREATE TABLE IF NOT EXISTS scheduled_events (
    id             BIGSERIAL PRIMARY KEY,
    aggregate_type TEXT NOT NULL DEFAULT 'task',
    aggregate_id   UUID NOT NULL,
    kind           TEXT NOT NULL CHECK (kind IN ('wakeup', 'retry')),
    destination    TEXT NOT NULL
                   CHECK (destination IN ('scheduler.due', 'scheduler.tasks')),
    dedupe_key     TEXT NOT NULL,
    payload        JSONB NOT NULL,
    headers        JSONB NOT NULL DEFAULT '{}',
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'published', 'failed', 'cancelled')),
    attempts       INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at   TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at   TIMESTAMPTZ,
    cancelled_at   TIMESTAMPTZ,
    last_error     TEXT
);

CREATE UNIQUE INDEX uq_scheduled_events_dedupe
    ON scheduled_events (dedupe_key);
CREATE INDEX idx_scheduled_events_due
    ON scheduled_events (available_at, id) WHERE status = 'pending';
CREATE INDEX idx_scheduled_events_aggregate
    ON scheduled_events (aggregate_type, aggregate_id, status);
CREATE INDEX idx_scheduled_events_cleanup
    ON scheduled_events (published_at) WHERE status IN ('published', 'cancelled');

-- Workers: registered worker nodes
CREATE TABLE IF NOT EXISTS workers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    worker_id VARCHAR(255) UNIQUE NOT NULL,
    worker_address VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'busy', 'offline', 'draining')),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    registered_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_workers_status ON workers(status);
CREATE INDEX idx_workers_last_heartbeat ON workers(last_heartbeat);
CREATE UNIQUE INDEX idx_workers_worker_id ON workers(worker_id);

-- Trigger to update updated_at
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at();
CREATE TRIGGER tasks_updated_at BEFORE UPDATE ON tasks
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at();
CREATE TRIGGER workers_updated_at BEFORE UPDATE ON workers
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at();
CREATE TRIGGER tenant_quotas_updated_at BEFORE UPDATE ON tenant_quotas
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at();
