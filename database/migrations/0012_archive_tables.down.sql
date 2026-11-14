DROP INDEX IF EXISTS idx_outbox_failed_scan;
DROP INDEX IF EXISTS idx_outbox_published_scan;
DROP INDEX IF EXISTS idx_task_executions_archive_scan;
DROP TABLE IF EXISTS outbox_archive;
DROP TABLE IF EXISTS task_execution_attempts_archive;
DROP TABLE IF EXISTS task_execution_archive;
