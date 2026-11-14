# PgBouncer integration harness

Starts a disposable PostgreSQL 16 instance, an `edoburu/pgbouncer` transaction
pooler, and the Prometheus PgBouncer exporter. The tests use the host's
`psycopg2`, exactly like the coordinator and worker.

```bash
./tests/pgbouncer/run.sh
KEEP=1 ./tests/pgbouncer/run.sh
```

Ports: PostgreSQL `55432`, PgBouncer `56432`, exporter `59127`. They do not
overlap the local Docker E2E stack.

## What is asserted

1. **SCRAM end to end.** PostgreSQL stores a `SCRAM-SHA-256$...` verifier;
   PgBouncer accepts the correct password and rejects a wrong one.
2. **Transaction pooling.** Two concurrent transactions get two distinct
   backends; after one commits, a later transaction reuses the released backend.
3. **Advisory transaction locks.** The outbox relay's
   `pg_try_advisory_xact_lock(hashtext(...))` pattern acquires, refuses a second
   holder, and releases on commit.
4. **Session reset.** With `server_reset_query_always=1`, `SET application_name`
   and a temporary table from one client are gone for the next client.
5. **Pooler admin view.** `SHOW CONFIG` reports transaction mode and
   `DISCARD ALL`; `SHOW POOLS` contains the scheduler database.
6. **Exporter metrics.** `pgbouncer_pools_{client,server}_active_connections`,
   `pgbouncer_pools_client_waiting_connections`, and
   `pgbouncer_config_max_client_connections` are present and carry the
   `database="distributed_scheduler"` label.

The harness mirrors `deploy/ha-lab/ha/pgbouncer.ini.tmpl` with a deliberately
small pool (`default_pool_size=2`) so reuse and reset behavior are observable.
The production template uses larger pools, but the same transaction mode,
SCRAM auth, and reset policy.
