-- 0001_baseline.sql
--
-- Baseline marker for databases created by database/init.sql.
--
-- This migration intentionally does nothing. The original schema is applied by
-- database/init.sql (and by scripts/init_db.py for local/CI bootstrap), then this
-- version is recorded with:
--
--     python scripts/migrate.py baseline
--
-- Future schema changes start at 0002 and are applied with:
--
--     python scripts/migrate.py up
--
-- Keeping the original bootstrap separate avoids a second copy of the schema and
-- avoids surprise changes for deployments that already have data.

SELECT 1;
