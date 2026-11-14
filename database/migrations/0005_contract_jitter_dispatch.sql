-- 0005_contract_jitter_dispatch.sql
--
-- Run after the new API/coordinator code is deployed. The constraints enforce
-- the invariants that the application now writes.

ALTER TABLE tasks
  ALTER COLUMN dispatch_at SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'tasks_jitter_seconds_range'
  ) THEN
    ALTER TABLE tasks
      ADD CONSTRAINT tasks_jitter_seconds_range
      CHECK (jitter_seconds BETWEEN 0 AND 3600);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'tasks_jitter_policy_valid'
  ) THEN
    ALTER TABLE tasks
      ADD CONSTRAINT tasks_jitter_policy_valid
      CHECK (jitter_policy IN ('none', 'stable'));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'tasks_jitter_policy_none_zero'
  ) THEN
    ALTER TABLE tasks
      ADD CONSTRAINT tasks_jitter_policy_none_zero
      CHECK (jitter_policy <> 'none' OR jitter_seconds = 0);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'tasks_dispatch_at_not_before_next'
  ) THEN
    ALTER TABLE tasks
      ADD CONSTRAINT tasks_dispatch_at_not_before_next
      CHECK (dispatch_at >= next_execution_time);
  END IF;
END $$;

ALTER TABLE task_executions
  ALTER COLUMN scheduled_for SET NOT NULL;
