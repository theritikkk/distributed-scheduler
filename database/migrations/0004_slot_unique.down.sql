-- migrate:no-transaction
DROP INDEX CONCURRENTLY IF EXISTS uq_task_executions_slot;
