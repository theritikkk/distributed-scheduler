-- 0004_slot_unique.sql
--
-- The unique slot index is safe for old code: old code inserts
-- task_executions.scheduled_for as NULL, and PostgreSQL allows multiple NULLs
-- in a unique index. New code uses ON CONFLICT (task_id, scheduled_for).
--
-- Run outside a transaction so normal production traffic is not blocked by the
-- index build. The migration runner recognizes this marker.

-- migrate:no-transaction
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_task_executions_slot
  ON task_executions (task_id, scheduled_for);
