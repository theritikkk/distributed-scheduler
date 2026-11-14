-- 0007_backfill_attempts.sql
--
-- Backfill attempt numbers and attempt history for executions created before
-- Phase 2. New Phase 2 executions start at attempt 0 and create their first
-- attempt row when a worker claims them.

UPDATE task_executions
SET attempt = 1
WHERE attempt = 0
  AND started_at IS NOT NULL
  AND status IN ('running', 'completed', 'failed', 'timeout', 'cancelled');

INSERT INTO task_execution_attempts (
  execution_id,
  attempt,
  worker_id,
  started_at,
  finished_at,
  status,
  error_message
)
SELECT
  e.id,
  1,
  e.worker_id,
  e.started_at,
  CASE
    WHEN e.status IN ('completed', 'failed', 'timeout', 'cancelled')
      THEN COALESCE(e.completed_at, e.started_at)
    ELSE NULL
  END,
  e.status,
  e.error_message
FROM task_executions e
WHERE e.attempt = 1
  AND e.started_at IS NOT NULL
  AND e.status IN ('running', 'completed', 'failed', 'timeout', 'cancelled')
  AND NOT EXISTS (
    SELECT 1
    FROM task_execution_attempts a
    WHERE a.execution_id = e.id
      AND a.attempt = 1
  );

-- A task may have at most one pending/running execution. Older releases could
-- leave duplicates behind; fail with an actionable message rather than hiding
-- data loss when the concurrent unique index is built.
DO $$
DECLARE
  duplicate_task UUID;
  active_count INTEGER;
BEGIN
  SELECT task_id, count(*)
  INTO duplicate_task, active_count
  FROM task_executions
  WHERE status IN ('pending', 'running')
  GROUP BY task_id
  HAVING count(*) > 1
  LIMIT 1;

  IF duplicate_task IS NOT NULL THEN
    RAISE EXCEPTION
      'task % has % active executions; resolve duplicates before applying 0008',
      duplicate_task,
      active_count;
  END IF;
END
$$;
