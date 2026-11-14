# PITR restore drill

Starts a disposable PostgreSQL 16 primary with WAL archiving, takes a
`pg_basebackup`, inserts two batches of rows with a WAL switch after each batch, and
then restores the same base backup twice to two different target times.

```bash
./tests/pitr/run.sh
KEEP=1 ./tests/pitr/run.sh      # keep the primary and backup volume
```

Ports: primary `55433`, restore `55434`. They do not overlap the local Docker E2E
stack.

## What the drill asserts

| Restore target | Expected rows | Max id | Post-target rows | Promotion |
| --- | --- | --- | --- | --- |
| After batch 1 | 20 | 20 | 0 | `pg_is_in_recovery()` must be false |
| After batch 2 | 30 | 30 | 0 | `pg_is_in_recovery()` must be false |

Rows 31–40 are inserted only after both target times, so their absence proves replay
stopped at the requested time rather than following the WAL to the end.

## How it works

- The primary runs with `wal_level=replica`, `archive_mode=on`,
  `archive_command='test ! -f /backup/archive/%f && cp %p /backup/archive/%f'`, and
  `archive_timeout=1`.
- `run.sh` creates `base/` and `archive/` on a Docker volume and runs
  `pg_basebackup -Fp -Xs` while the primary is live.
- Each batch records `clock_timestamp()` in UTC, calls `pg_switch_wal()`, and waits for
  `pg_stat_archiver.archived_count` to advance.
- `restore-entrypoint.sh` copies the base backup into a fresh `PGDATA`, writes
  `restore_command`, `recovery_target_time`, and `recovery_target_action=promote` into
  `postgresql.auto.conf`, and hands off to the official PostgreSQL entrypoint.
- The restore container is started with `docker compose run` and removed after each
  target.

This is a local mechanics drill. It does not cover S3 durability/encryption, WAL-G or
pgBackRest, restore throughput, or Patroni cutover. See the production procedure in
[`docs/runbooks/failover.md`](../../docs/runbooks/failover.md#pitr-restore-validation-and-production-procedure).

## Requirements

- Docker with a running daemon
- Host `psql`/`pg_basebackup` are not required; the drill runs them inside the
  containers
- `bash` (standard on macOS/Linux)
