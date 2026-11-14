-- 0009_outbox.down.sql
--
-- The outbox is additive and intentionally retained on rollback. Stop the relay
-- or set OUTBOX_ENABLED=false in the services first; pending rows must never be
-- deleted as part of a code rollback.

SELECT 1;
