-- Rollback for the additive expand migration.
-- Run this only after the application that reads these columns has been rolled
-- back and after the contract/unique migrations have been rolled back.

DROP INDEX IF EXISTS idx_task_executions_slot;
DROP INDEX IF EXISTS idx_tasks_dispatch_at_due;

ALTER TABLE task_executions DROP COLUMN IF EXISTS scheduled_for;
ALTER TABLE tasks
  DROP COLUMN IF EXISTS jitter_policy,
  DROP COLUMN IF EXISTS jitter_seconds,
  DROP COLUMN IF EXISTS dispatch_at;
