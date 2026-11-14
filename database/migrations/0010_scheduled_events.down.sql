-- 0010_scheduled_events.down.sql
--
-- Scheduled events are additive and intentionally retained on rollback. Stop
-- the poller or disable the feature before rolling back code. Pending rows must
-- not be deleted: reconciliation and the reaper recover affected tasks, but the
-- delay is shorter if operators drain the table first.
--
-- To remove the table after all rows are drained (manual, not part of rollback):
--   DROP TABLE IF EXISTS scheduled_events;

SELECT 1;
