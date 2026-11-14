-- Rollback intentionally keeps additive Phase 2 tables and columns.
--
-- Execution attempt history and idempotency records are data, not schema
-- artifacts. Old Phase 1 code can run with the added nullable/defaulted
-- columns and the pending status value, so no destructive rollback is needed.
SELECT 1;
