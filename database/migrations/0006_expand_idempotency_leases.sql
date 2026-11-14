-- 0006_expand_idempotency_leases.sql
--
-- Expand phase for Phase 2: durable API idempotency keys, execution leases,
-- retry timing and per-attempt history. Old code can keep running while this
-- migration is applied: every change is additive, pending executions are
-- simply not created by the old code, and started_at remains writable.

ALTER TABLE task_executions
  ADD COLUMN IF NOT EXISTS lease_id UUID,
  ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 0;

ALTER TABLE task_executions
  ALTER COLUMN started_at DROP NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'task_executions_attempt_check'
      AND conrelid = 'task_executions'::regclass
  ) THEN
    ALTER TABLE task_executions
      ADD CONSTRAINT task_executions_attempt_check CHECK (attempt >= 0);
  END IF;
END
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'task_executions_status_check'
      AND conrelid = 'task_executions'::regclass
  ) THEN
    ALTER TABLE task_executions
      DROP CONSTRAINT task_executions_status_check;
  END IF;
END
$$;

ALTER TABLE task_executions
  ADD CONSTRAINT task_executions_status_check
  CHECK (status IN ('pending', 'running', 'completed', 'failed', 'timeout', 'cancelled'));

CREATE INDEX IF NOT EXISTS idx_task_executions_retry
  ON task_executions (next_attempt_at)
  WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_task_executions_lease
  ON task_executions (lease_expires_at)
  WHERE status = 'running';

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

CREATE INDEX IF NOT EXISTS idx_task_execution_attempts_execution
  ON task_execution_attempts (execution_id, attempt);

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

CREATE INDEX IF NOT EXISTS idx_idempotency_expires_at
  ON idempotency_keys (expires_at);
