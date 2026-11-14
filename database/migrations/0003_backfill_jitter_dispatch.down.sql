-- Backfill is intentionally not reversible: restoring historical NULLs would
-- lose the only durable slot identity for executions. Roll back the application
-- and the later contract/unique migrations first; the additive columns can then
-- be removed by the 0002 rollback.
SELECT 1;
