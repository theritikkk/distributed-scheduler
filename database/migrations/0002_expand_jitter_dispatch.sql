-- 0002_expand_jitter_dispatch.sql
--
-- Expand phase: additive-only columns and indexes. Old code can keep running
-- while this migration is applied; it ignores the new fields and can still
-- insert NULL dispatch_at / scheduled_for values.
--
-- The application is deployed after this migration and writes the new fields.
-- The contract migration (0005) runs after the application rollout.

ALTER TABLE tasks
  ADD COLUMN IF NOT EXISTS dispatch_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS jitter_seconds INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS jitter_policy VARCHAR(10) NOT NULL DEFAULT 'stable';

ALTER TABLE task_executions
  ADD COLUMN IF NOT EXISTS scheduled_for TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_tasks_dispatch_at_due
  ON tasks (dispatch_at)
  WHERE status IN ('pending', 'scheduled');

CREATE INDEX IF NOT EXISTS idx_task_executions_slot
  ON task_executions (task_id, scheduled_for);
