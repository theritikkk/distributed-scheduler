-- 0008_active_execution_unique.sql
--
-- Enforce at most one pending/running execution per task. The reaper and worker
-- claim protocol rely on this invariant. Run outside a transaction so normal
-- traffic is not blocked by the concurrent index build.

-- migrate:no-transaction
-- If an earlier concurrent build failed, PostgreSQL leaves an invalid index
-- with this name. Drop it before retrying:
--   DROP INDEX CONCURRENTLY uq_task_executions_active;
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_task_executions_active
  ON task_executions (task_id)
  WHERE status IN ('pending', 'running');
