-- Disposable PostgreSQL instance for the PgBouncer harness.
-- The role is intentionally non-superuser: the pooler, not the application,
-- must own connection reuse and authentication.
CREATE ROLE scheduler LOGIN PASSWORD 'scheduler_secret' NOSUPERUSER NOCREATEDB;
CREATE DATABASE distributed_scheduler OWNER scheduler;
