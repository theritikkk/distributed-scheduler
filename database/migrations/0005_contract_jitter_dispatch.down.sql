ALTER TABLE tasks
  DROP CONSTRAINT IF EXISTS tasks_dispatch_at_not_before_next,
  DROP CONSTRAINT IF EXISTS tasks_jitter_policy_none_zero,
  DROP CONSTRAINT IF EXISTS tasks_jitter_policy_valid,
  DROP CONSTRAINT IF EXISTS tasks_jitter_seconds_range;

ALTER TABLE tasks
  ALTER COLUMN dispatch_at DROP NOT NULL;

ALTER TABLE task_executions
  ALTER COLUMN scheduled_for DROP NOT NULL;
