# Implementation Plan — Reliability and Scale

- **Status:** Phases 0–6 implemented and validated; Phase 7 implemented and locally validated; live AWS HA lab not re-provisioned
- **Date:** 2026-11-04
- **Scope:** Idempotency, scheduling correctness, outbox, rate limiting, backpressure,
  concurrency, database/broker HA, optional Redis scaling
- **Design references:**
  - [01 — Jitter and `dispatch_at`](01-jitter-and-dispatch-time.md)
  - [02 — Rate limiting and Redis fallback](02-rate-limiting-and-redis-fallback.md)
  - [03 — Coordinator backpressure](03-coordinator-backpressure.md)

---

## 1. Program goal

Move the scheduler from “works on a demo deployment” to “survives duplicates, restarts,
overload, and dependency failures without losing task state.”

The program has five outcomes:

1. **Correct scheduling.** One execution per scheduled slot, stable cron cadence, jittered
   load.
2. **Exactly-once task state.** At-least-once delivery plus idempotent transitions; no
   duplicate side effects from retries or reconnects.
3. **Durable publication.** The transactional outbox makes DB state and RabbitMQ
   publication converge, even across crashes.
4. **Overload protection.** Rate limits, admission control, queue-depth backpressure, and
   worker concurrency prevent cascading failure.
5. **HA.** Postgres and RabbitMQ survive node loss; Redis is never the source of truth.

### Explicit non-goals

- Exactly-once execution of arbitrary shell commands. That is impossible without
  idempotent commands; the platform can guarantee exactly-once **state transitions** and
  at-least-once **delivery**.
- Multi-region active-active.
- Replacing RabbitMQ with Kafka, Redis Streams, or a database queue.
- A full sandbox for user commands. The current worker runs commands as root with access
  to environment secrets; that is a separate security project and should be prioritized
  independently.
- Kubernetes or autoscaling infrastructure.

---

## 2. Phase overview

| Phase | Outcome | Estimate | Depends on | Main risk |
|-------|---------|----------|------------|-----------|
| 0 | Safe foundations: migrations, config, tests, CI, metrics | 2–4 days | — | Migration tooling churn |
| 1 | Jitter, `dispatch_at`, execution-slot uniqueness | 4–6 days | 0 | Backfill/index locks |
| 2 | DB-first idempotency, state machine, leases, attempts | 5–8 days | 1 | State-machine edge cases |
| 3 | Transactional outbox and relay | 5–7 days | 2 | Ordering and duplicate publishes |
| 4 | Rate limiting, Redis fallback, admission, worker concurrency | 6–8 days | 1, 2 | False positives / Redis outage |
| 5 | Coordinator backpressure and reconnect | 4–6 days | 2, 3 | Pika pause/reconnect correctness |
| 6 | Postgres and RabbitMQ HA/replication | 1–2 weeks | 3, 5 | Failover data loss / split brain |
| 7 | Optional Redis KV/cache/fairness scaling | 3–5 days | 4, 6 | Scope creep; cache coherence |

Estimates assume one experienced engineer including tests and documentation. Phases 4 and
5 can run in parallel after Phase 2; Phase 6 can start once the outbox is stable.
A two-engineer team should target 5–7 weeks rather than the sum of the estimates.

### Dependency graph

```text
Phase 0
   │
   ▼
Phase 1 ──────────────┐
   │                  │
   ▼                  ▼
Phase 2 ────────▶ Phase 4
   │                  │
   ▼                  ▼
Phase 3 ────────▶ Phase 5
   │                  │
   └────────┬─────────┘
            ▼
         Phase 6
            │
            ▼
         Phase 7 (optional, implemented)
```

### Decision log

| Decision | Rationale |
|----------|-----------|
| Postgres is the source of truth | Durable, transactional, already required |
| Idempotency keys live in Postgres | Redis loss must not create duplicates |
| Outbox is for event publication, not DB replication | It solves the DB↔broker dual-write gap |
| Redis is ephemeral (rate limits, cache, optional locks) | Never let a cache outage corrupt state |
| Stable per-task jitter | Spreads load without changing each task's cadence |
| Redis rate limiter with a local fallback | Global limits when healthy, safe degradation when not |
| No Postgres rate-limit fallback | Would turn a Redis outage into a DB outage |
| Coordinator uses local dispatch pacing + queue depth | Redis is not needed in the dispatch hot path |
| Pull loop for the due consumer | Makes pause/resume and ack rules explicit |
| Single outbox relay initially | Preserves per-aggregate order; scale later with locks |
| Thread-per-consumer worker concurrency initially | Pika is not thread-safe; one channel per thread is safe |
| RabbitMQ quorum queues in HA phase | Survive broker loss without losing messages |

---

## 3. Phase 0 — Foundations

### Goal

Make it safe to ship schema and behavior changes: a migration runner, validated config,
real tests in CI, and baseline metrics.

> **Implementation status (2026-11-09): Phases 0–7 implemented; Phase 7 locally
> validated.** Migration
> runner/bootstrap, validated config, API unit + PostgreSQL integration tests, Python
> unit tests, CI lint/test/migration/coverage jobs, Prometheus scrape fixes, target-down
> alert, coordinator metric identity, concurrency coverage, and API-only + full-stack
> baselines are in place. Coordinator integration tests use a disposable PostgreSQL from
> CI (`TEST_DATABASE_URL`) and a real RabbitMQ service for topology coverage; worker
> retry/idempotency paths use a fake AMQP channel. Phases 1 and 2 status blocks document
> the migration/rollout details; Phases 3–7 status blocks follow their sections. Phase 6
> is deployed in the three-node `deploy/ha-lab/` stack and passed the PostgreSQL,
> RabbitMQ, Redis, and load-test drills; the single-node free-tier demo still passes the
> timer/outbox path. Phase 7 adds opt-in caches, tenant quotas, sharded ingress, W3C
> tracing, and archival; it was validated locally against disposable PostgreSQL and the
> repository test suites, not by re-provisioning AWS.

### Tasks

#### Migrations

- [x] Add a migration runner. Implemented as `scripts/migrate.py` (chosen over `dbmate` to
      avoid an extra binary dependency); it supports a `schema_migrations` checksum table,
      baseline adoption, fresh-database bootstrap, rollback, and non-transactional
      migrations for `CREATE INDEX CONCURRENTLY`.
- [x] Create `database/migrations/` and a `Makefile` target:
      `make migrate`, `make migrate-status`, `make migrate-rollback`.
- [x] Baseline existing databases. For a fresh database, `database/init.sql` remains the
      bootstrap; for existing databases, mark the baseline in `schema_migrations` after
      verifying the schema.
- [x] Add a CI job that starts an ephemeral Postgres, runs all migrations from scratch, and
      runs the test suite.
- [x] Add a migration smoke test: apply, insert seed data, roll back one step, re-apply.

#### Config

- [x] Add a central config module for the API (`zod` or equivalent) and the coordinator
      (a small typed settings object). Fail fast on missing/invalid env vars.
- [x] Remove insecure fallback credentials from production paths. Local defaults may remain
      for development only and must log a warning.
- [x] Document every new environment variable in `.env.example`.

#### Tests

- [x] API: Vitest + Supertest; integration tests against a disposable Postgres.
- [x] Coordinator/worker: pytest + a disposable Postgres and RabbitMQ (Testcontainers or
      Compose profiles).
- [x] Add at least one test per critical path before changing behavior: task creation,
      dispatch, result application, idempotent skip, retry.
- [x] Add a concurrency test harness (N parallel requests / coordinators).

#### CI

- [x] Remove `|| true` from lint/security steps. The current lint failure must be fixed or
      the lint config added; CI must not hide it.
- [x] Add Python lint/format/test jobs (`ruff`, `pytest`).
- [x] Add a migration job.
- [x] Add coverage reporting, with a modest initial threshold; ratchet over time.

#### Metrics and alerting

- [x] Add `prom-client` to the API; expose `/metrics`.
- [x] Fix `monitoring/prometheus.yml` to scrape every worker and API replica, not only
      workers 1–3.
- [x] Add an `up == 0` alert and a queue-depth dashboard.
- [x] Ensure coordinator metrics include the process/consumer identity.

#### Load test baseline

- [x] Fix `tests/production-test.example.js` to use a valid local JWT and a payload with
      `command`.
- [x] Record baseline: requests/s, p95/p99, DB connections, queue depth, worker
      utilization, dispatch lag.
- [x] Commit the baseline numbers and the test command in the plan or README.

#### Documentation

- [x] Update the README's “exactly-once”, worker-count, and port claims to match reality.
- [x] Add a short “current limitations” section.

### Exit criteria

- `make migrate` works on a clean database and on a copy of the current schema.
- CI runs lint, build, Python tests, API tests, and migrations; failures are visible.
- Baseline load test runs locally and produces numbers.
- No production docs claim exactly-once execution.

---

## 4. Phase 1 — Schedule correctness

### Goal

Implement design 01: stable jitter, `dispatch_at`, and database-enforced execution slots.

### Tasks

#### Schema

- [x] Add `tasks.jitter_seconds` (`0..MAX_JITTER_SEC`).
- [x] Add `tasks.jitter_policy` (`none|stable`).
- [x] Add `tasks.dispatch_at` (expand nullable, backfill, contract NOT NULL).
- [x] Add `task_executions.scheduled_for` (expand nullable, backfill from `started_at`,
      contract NOT NULL).
- [x] Add `idx_tasks_dispatch_at_due` and the unique
      `uq_task_executions_slot (task_id, scheduled_for)`.
- [x] Pre-flight duplicate check and reviewed cleanup for existing executions.
- [x] Backfill in bounded 10k-row `UPDATE` batches. The migration runner still wraps
      one migration file in a transaction; very large installations should run the
      equivalent reviewed maintenance SQL before applying the migration.

#### API

- [x] Generate `task_id` in the API with `randomUUID()`.
- [x] Add `cron-parser`; validate cron expressions and compute `next_execution_time`
      server-side for recurring tasks.
- [x] Accept `jitter_seconds` / `jitter_policy`; validate bounds.
- [x] Compute `dispatch_at` from `task_id`, base time, jitter, and cron period.
- [x] Publish the wakeup for `dispatch_at`.
- [x] Recompute `dispatch_at` on update when cron/time/jitter changes.
- [x] Return `dispatch_at` and jitter fields in create/update/get/list responses.

#### Coordinator

- [x] Read `COALESCE(dispatch_at, next_execution_time)` during rollout.
- [x] Compare due time with database `NOW()`, not the application clock.
- [x] Insert `task_executions` with `scheduled_for = next_execution_time` and
      `ON CONFLICT (task_id, scheduled_for) DO NOTHING`.
- [x] Include `scheduledFor` and `dispatchAt` in the task message.
- [x] Compute the next recurring slot from `scheduled_for`, not `now()`.
- [x] Skip missed slots by default; document the policy.
- [x] Guard recurring rescheduling with `status <> 'cancelled'`.
- [x] Reconciliation queries `dispatch_at` and jitters its interval.

#### Worker

- [x] Replace deterministic retry backoff with full jitter, with an env-selected
      `full|equal|none` mode.
- [x] Record retry backoff in metrics.

### Tests

- [x] 10k same-slot tasks spread over the window.
- [x] Duplicate wakeups create one execution.
- [x] Two coordinators process the same wakeup concurrently.
- [x] Recurring cadence does not drift.
- [x] Cancellation is not undone by a late result.
- [x] Migration backfill and rollback.
- [x] TS/Python stable-offset parity.

### Exit criteria

- Jitter disabled produces byte-for-byte behavior parity.
- With jitter enabled, dispatch is spread and no slot duplicates exist.
- `p95(schedule_lag)` is within the configured window plus queue wait.
- The migration is additive and rollback-safe.

### Rollback

Set `SCHEDULER_JITTER_ENABLED=false`; code writes `dispatch_at = next_execution_time`.
Schema remains.

### Status

Phase 1 is implemented on the current branch. Migrations `0002`–`0005` provide the
expand/backfill/unique-index/contract sequence; API, coordinator, and worker code and
tests cover stable offsets, duplicate slots, recurring cadence, cancellation, missed
slots, and retry jitter. See `Benchmark.md` for the deterministic 10k-offset
verification.

---

## 5. Phase 2 — Idempotency, state machine, leases

### Goal

Make task state transitions idempotent and crash-safe, and make the API's
`Idempotency-Key` a durable contract.

### 5.1 Schema

```sql
-- Idempotency keys
CREATE TABLE idempotency_keys (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    key             text NOT NULL,
    request_hash    text NOT NULL,
    state           text NOT NULL CHECK (state IN ('in_progress', 'completed', 'failed')),
    response_status integer,
    response_body   jsonb,
    resource_id     uuid,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz NOT NULL DEFAULT now() + interval '24 hours',
    UNIQUE (user_id, key)
);

CREATE INDEX idx_idempotency_expires_at ON idempotency_keys (expires_at);

-- Execution attempts
CREATE TABLE task_execution_attempts (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    execution_id   uuid NOT NULL REFERENCES task_executions(id) ON DELETE CASCADE,
    attempt        integer NOT NULL,
    worker_id      varchar(255),
    started_at     timestamptz NOT NULL DEFAULT now(),
    finished_at    timestamptz,
    status         text NOT NULL,
    error_message  text,
    UNIQUE (execution_id, attempt)
);

-- Lease, retry, and ownership columns
ALTER TABLE task_executions
    ADD COLUMN lease_id uuid,
    ADD COLUMN lease_expires_at timestamptz,
    ADD COLUMN next_attempt_at timestamptz,
    ADD COLUMN attempt integer NOT NULL DEFAULT 0;

-- Add 'pending' to execution statuses if the state machine uses it.
ALTER TABLE task_executions DROP CONSTRAINT task_executions_status_check;
ALTER TABLE task_executions
    ADD CONSTRAINT task_executions_status_check
    CHECK (status IN ('pending', 'running', 'completed', 'failed', 'timeout', 'cancelled'));

CREATE INDEX idx_task_executions_retry
    ON task_executions (next_attempt_at)
    WHERE status = 'pending';

-- One active execution per task (after cleanup/reaper exists).
CREATE UNIQUE INDEX CONCURRENTLY uq_task_executions_active
    ON task_executions (task_id)
    WHERE status IN ('pending', 'running');
```

The active unique index is intentionally in Phase 2, not Phase 1: it requires the reaper
and cleanup of any existing duplicate running rows.

### 5.2 State machine

Tasks:

```text
scheduled ──▶ running ──▶ scheduled   (recurring success)
    │            ├──────▶ completed   (one-time success)
    │            └──────▶ failed      (final failure)
    ├──────────────────▶ cancelled
    └──────────────────▶ failed        (non-retryable failure)
```

Executions:

```text
pending ──▶ running ──▶ completed
   │           ├──────▶ failed
   │           ├──────▶ timeout
   │           └──────▶ cancelled
   └──────────────────▶ cancelled
running ── lease expired ──▶ pending   (reap, same execution, new attempt)
running ── retry scheduled ─▶ pending  (next_attempt_at in the future)
```

Rules:

- Every transition is a conditional `UPDATE ... WHERE status = expected`.
- A result update also requires the matching `lease_id`.
- A cancelled task is terminal; late results must not reschedule it.
- A stale worker cannot overwrite a newer lease.
- A duplicate result is a no-op (`completed_at IS NULL`).

### 5.3 API idempotency

- [x] Accept `Idempotency-Key` on `POST /tasks` (and bulk endpoints when added).
- [x] Canonicalize the request body and store `SHA-256` in `request_hash`.
- [x] `INSERT ... ON CONFLICT (user_id, key) DO NOTHING` to claim the key.
- [x] If the key exists with a different hash, return `409 Conflict`.
- [x] If `state = completed`, replay `response_status` + `response_body`.
- [x] If `state = in_progress`, return `409` with `Retry-After` (or `202` if the API is
      changed to async creation).
- [x] Write the idempotency row, task row, and outbox event in one transaction (outbox
      arrives in Phase 3; until then, use the same DB transaction and publish after
      commit with a documented dual-write risk).
- [x] Do not commit an `in_progress` key before the task row. A crash must roll back the
      key claim, not leave a stuck key. With `ON CONFLICT DO NOTHING`, a concurrent
      request blocks on the unique index until the first transaction commits, then sees
      `completed`.
- [x] Store the response in the same transaction as the task/outbox write; a replay
      returns the stored response without re-running the handler.
- [x] Clean up expired rows in a background job.
- [x] Do not cache sensitive response data longer than necessary.

### 5.4 Worker lease and heartbeat

- [x] Due consumer creates an execution as `pending` with `scheduled_for`.
- [x] Worker claims:
      `UPDATE task_executions SET status='running', worker_id=%s, lease_id=%s,
      lease_expires_at=NOW()+interval '60 seconds', started_at=NOW(), attempt=attempt+1,
      next_attempt_at=NULL
      WHERE id=%s AND status='pending'
        AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
      RETURNING ...`.
- [x] If the claim returns zero rows, republish a not-yet-due retry with its remaining
      TTL, or ack and skip when another worker owns the execution or it is terminal.
- [x] Start a heartbeat thread that extends the lease every 15–20s while the command runs.
- [x] On completion, publish a result containing `executionId`, `leaseId`, `workerId`,
      and `attempt`.
- [x] The result consumer applies:
      `WHERE id=%s AND lease_id=%s AND status='running' AND completed_at IS NULL`.
- [x] Insert an attempt row for each claim; close it on result or lease expiry.
- [x] When scheduling a retry, update the execution to `pending`, clear `lease_id` /
      `lease_expires_at`, and set `next_attempt_at = NOW() + backoff` before publishing
      the retry. The retry message must not be delivered before `next_attempt_at`.
- [x] Reaper every 15s:
      `SELECT ... WHERE (
         status = 'pending'
         AND COALESCE(next_attempt_at, created_at) < NOW() - interval '5 minutes'
       ) OR (
         status = 'running' AND lease_expires_at < NOW()
       )
      FOR UPDATE SKIP LOCKED LIMIT 100`.
      Mark the attempt failed, clear the lease, and either return the execution to
      `pending` for another attempt or fail it after `max_attempts`.
- [x] A delayed retry with a future `next_attempt_at` must not be reaped; otherwise a
      legitimate backoff is mistaken for a lost worker.
- [x] Stale `pending` handling covers the crash window between inserting the execution
      and publishing the task; otherwise the active unique index can block the task
      forever.
- [x] Populate `worker_id` on executions (currently never set).
- [x] Publish the result before acking; Phase 3 adds publisher confirms/outbox for a
      fully durable handoff.

### 5.5 Tests

- [x] Same idempotency key concurrently: one task, one response.
- [x] Same key, different body: `409`.
- [x] Crash after claiming the key but before insert: retry recovers.
- [x] Crash after task insert but before response: replay returns the stored response.
- [x] Duplicate result with the same lease: applied once.
- [x] Result with an old lease: rejected.
- [x] Worker crash: lease expires, reaper retries, no duplicate execution.
- [x] Delayed retry with a future `next_attempt_at` is not reaped; a lost retry after the
      grace period is republished.
- [x] Cancellation during execution: late result cannot reschedule.
- [x] State transition matrix tests (allowed and rejected).

### Exit criteria

- A client can retry `POST /tasks` safely with the same key.
- A worker crash does not leave a task stuck in `running`.
- A stale result cannot overwrite a newer attempt.
- The execution history shows every attempt and worker.
- No transition is applied without a status/lease condition.

### Rollback

- Keep the additive columns/tables.
- Disable enforcement with `IDEMPOTENCY_ENFORCE=false` and leave lease checks in
  “log only” mode for one release if needed.
- Do not drop execution history.

### Status

Phase 2 is implemented on the current branch. Migrations `0006`–`0008` provide the
expand/backfill/active-index rollout. The API stores canonical request hashes and
responses in `idempotency_keys`; the worker claims executions with leases and attempt
rows; the result consumer requires the matching lease; and the coordinator reaper
recovers expired leases and stale pending dispatches. The reaper also deletes expired
idempotency keys. `0007` refuses to proceed if a task has duplicate active executions,
and `0008` builds the partial unique index concurrently. Rollback keeps additive history;
`IDEMPOTENCY_ENFORCE=false` disables key enforcement, and `0008` is the only down
migration that removes an index.

Validation covered API build/lint and PostgreSQL integration tests (including concurrent
same-key creation), coordinator dispatch/result/reaper tests, worker claim/retry and
lease-guard tests, migration upgrade/rollback tests, and the existing Phase 0/1 suites.

---

## 6. Phase 3 — Transactional outbox

### Goal

Remove the DB↔RabbitMQ dual-write gap: every state change and its event commit together.

> The outbox is **not** database replication. It does not copy tables or replace a read
> replica. It durably records events that still need to be published. Postgres
> replication/HA is Phase 6.

### 6.1 Schema

```sql
CREATE TABLE outbox (
    id             bigserial PRIMARY KEY,
    aggregate_type text NOT NULL,
    aggregate_id   uuid NOT NULL,
    event_type     text NOT NULL,
    payload        jsonb NOT NULL,
    headers        jsonb NOT NULL DEFAULT '{}',
    status         text NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'published', 'failed')),
    attempts       integer NOT NULL DEFAULT 0,
    available_at   timestamptz NOT NULL DEFAULT now(),
    created_at     timestamptz NOT NULL DEFAULT now(),
    published_at   timestamptz,
    last_error     text
);

CREATE INDEX idx_outbox_pending
    ON outbox (status, available_at, id)
    WHERE status = 'pending';

CREATE INDEX idx_outbox_aggregate
    ON outbox (aggregate_type, aggregate_id, id);
```

### 6.2 Write path

- [x] API task creation: insert task + `outbox(event_type='task.scheduled')` in one
      transaction. Remove the direct publish from the request path.
- [x] API update/cancel: insert task update + outbox event in one transaction.
- [x] Due consumer: insert execution + outbox `task.dispatch` in one transaction.
- [x] Result consumer: update execution/task + insert next wakeup event in one transaction.
- [x] Worker result/retry/DLQ publication uses a worker-side transaction. Final result and
      DLQ events commit before the worker acks.
- [x] Never publish before commit.
- [x] Never commit a state change without its event row.

### 6.3 Relay

Routing map:

| Event type | Destination |
|------------|-------------|
| `task.scheduled` | `scheduler.due` |
| `task.dispatch` | `scheduler.tasks` |
| `task.result` | `scheduler.results` |
| `task.retry` | `scheduler.retry_delay` |
| `task.dead_letter` | `scheduler.dlx` → `scheduler.tasks.dlq` |

- [x] Poll `pending` rows with `FOR UPDATE SKIP LOCKED LIMIT 100`, ordered by `id`.
- [x] Publish with publisher confirms and `mandatory=True`.
- [x] On confirm, set `status='published', published_at=NOW()`.
- [x] On failure, increment `attempts`, set `available_at = NOW() + backoff`, store
      `last_error`; after N attempts, mark `failed` and alert.
- [x] Backoff with full jitter.
- [x] Pace publishing with a local token bucket. Queue-depth backpressure is tracked in
      Phase 5.
- [x] Use per-aggregate `pg_try_advisory_xact_lock(hashtext(aggregate_id))` claims so
      multiple relays preserve order; the deployment runs one relay by default.
- [x] Clean up published rows older than 7 days (configurable).
- [x] Metrics: pending rows, oldest pending age, publish rate, failures, lag.

### 6.4 Tests

- [x] Crash after publish, before marking published: relay rolls back and republishes;
      idempotent consumers absorb the duplicate.
- [x] Crash after DB commit, before publish: pending rows remain and the relay drains them
      after restart.
- [x] Broker down: connection failures leave rows pending (attempts are not burned); the
      relay reconnects and drains on recovery.
- [x] Two relay instances: advisory-lock claims prevent double processing and preserve
      per-aggregate order.
- [x] Outbox backlog, lag, and terminal-failure alerts exist.
- [x] Cleanup does not delete unpublished or failed rows.

### Exit criteria

- No task state change is published directly from an API request.
- A broker outage cannot strand a committed task without an outbox row.
- Relay lag and failures are visible.
- Duplicate publishes are harmless because consumers are idempotent.

### Rollback

- Set `OUTBOX_ENABLED=false` and return to direct publish with confirms in the API,
  coordinator, and worker.
- The outbox table remains; the relay can be stopped independently.
- Do not delete pending rows.

### Implementation status (Phase 3)

`0009_outbox` adds the table, pending/aggregate indexes, and published-retention index.
The API writes task creation/update/cancel events in the same transaction as idempotency
and task state. The coordinator writes dispatch and recurring wakeup events in its
transaction, and the reaper writes requeue events. The worker commits retry, final result,
and DLQ events before acking. The coordinator relay uses confirms, `mandatory=True`,
per-aggregate advisory locks, full-jitter retry backoff, a token bucket, and retention
cleanup. `OUTBOX_ENABLED=false` is the explicit direct-confirm rollback path.

The special case of a future wakeup is preserved: `task.scheduled` is published to
`scheduler.due`, and the due consumer redelivers a future task directly to
`scheduler.delay` with the remaining TTL. That redelivery is a control-plane wakeup, not
a task-state transition; reconciliation remains the safety net.

---

## 7. Phase 4 — Overload protection and worker concurrency

### Goal

Enforce global/per-user limits, degrade safely when Redis fails, and let each worker process
more than one task without corrupting Pika state.

### 7.1 API rate limiting (design 02)

- [x] Add the Redis token-bucket module and Lua script.
- [x] Add the local token-bucket fallback and circuit breaker.
- [x] Add `RATE_LIMIT_MODE=observe`; ship without rejecting.
- [x] Add login limiting before bcrypt.
- [x] Add per-user task creation budgets (idempotent replays pass through the same budget).
- [x] Add global ingress and global task-admission limits.
- [x] Add `X-RateLimit-*`, `Retry-After`, and `429` responses.
- [x] Add DB pool and queue-depth admission (`503`/`429`).
- [ ] Add Nginx `limit_req` / `limit_conn` at the edge. (Documented in the rate-limit
      runbook; not applied to the sample Nginx config yet.)
- [x] Add metrics, alerts, and a runbook.

### 7.2 Worker concurrency

Current worker: one Pika connection/channel, one task at a time, blocking
`subprocess.run`. Recommended first step: **thread-per-consumer**, not a shared callback
thread pool.

- [x] Add `WORKER_CONCURRENCY` (default 1, cap 8).
- [x] Each consumer thread opens its own `BlockingConnection`, channel, and
      `basic_qos(prefetch_count=1)`.
- [x] Each thread gets a unique ID: `${WORKER_ID}-${slot}`.
- [x] Use a DB connection pool per process (`psycopg2.pool.ThreadedConnectionPool`);
      task checks no longer open a fresh session.
- [x] Keep `subprocess.run` with an explicit timeout and no shell; add `MAX_OUTPUT_BYTES`
      truncation for combined stdout/stderr.
- [x] Add a per-worker concurrency gauge and task duration labels by slot
      (`CONSUMER_UP` and task metrics use the slot ID).
- [x] Heartbeat one lease per in-flight execution (one per slot); the DB pool bounds
      heartbeat connections.
- [x] Test graceful shutdown: SIGTERM stops consuming via `add_callback_threadsafe`;
      callbacks get a bounded drain window, and any unacked work is redelivered after the
      process exits.

Why not a shared thread pool:

- Pika's `BlockingConnection`/channel is not thread-safe.
- Acking from a worker thread after a callback returns is unsafe unless you marshal the
  ack back to the consumer thread.
- One connection per consumer thread is simple and scales to a handful of tasks per
  worker.

If higher concurrency is needed later, migrate to `aio-pika` with an asyncio semaphore and
`asyncio.create_subprocess_shell`.

### 7.3 Tests

- [ ] Redis up: limits are shared across 3 API replicas. (Single-replica Redis path is
      E2E-tested; 3-replica shared-bucket test remains.)
- [x] Redis down: local fallback, bounded 429 (unit + live E2E: fallback active, circuit
      open, then half-open recovery after Redis returned).
- [x] Redis latency: circuit opens, API p99 bounded (unit: circuit breaker and timeout).
- [ ] DB pool saturation: 503 before queries pile up. (Admission logic is unit-covered;
      full pool-saturation integration run remains.)
- [x] Worker concurrency: N tasks in parallel, no cross-thread Pika errors (E2E with
      `WORKER_CONCURRENCY=2`; each slot owns its connection/channel).
- [x] Worker crash: in-flight tasks are redelivered and idempotently skipped (Phase 2
      lease tests plus restart E2E).
- [x] Queue backlog: admission rejects new work with 429/503 (live E2E: 10-message
      backlog produced `429 queue_backlog` with `Retry-After: 2`; creation resumed after
      workers drained it).
- [x] Observe mode does not change behavior.

### Exit criteria

- No API path writes rate-limit state to Postgres.
- Redis outage does not increase DB write rate above capacity.
- A worker can process `WORKER_CONCURRENCY` tasks concurrently without corruption.
- 429/503 responses carry `Retry-After`.
- Alerts exist for fallback active, circuit open, and DB pool saturation.

### Rollback

- `RATE_LIMIT_MODE=observe` or `RATE_LIMIT_ENABLED=false`.
- `WORKER_CONCURRENCY=1` and restart workers.
- Admission control can be disabled independently with `ADMISSION_ENABLED=false`.

### Implementation status (Phase 4)

Redis token buckets use a Lua script and `ioredis`; a per-process local bucket and
circuit breaker provide bounded fallback. Global ingress, login (before bcrypt), and
per-user task creation limits are wired with `X-RateLimit-*` and `Retry-After`.
Admission rejects task creation with 503/429 based on DB pool utilization/waiters and
queue depth. The API samples `scheduler.tasks` through a dedicated channel on a 5-second
interval. All limits are off/observe by default.

Workers now run `WORKER_CONCURRENCY` slots (cap 8), each with its own connection, confirm
channel, and prefetch of one. IDs are `${WORKER_ID}-${slot}`. DB access uses
`psycopg2.pool.ThreadedConnectionPool`; command output is capped by `MAX_OUTPUT_BYTES`;
SIGTERM stops consumers with `add_callback_threadsafe`. The API queue publisher and
depth monitor also reconnect with capped backoff, so outbox-disabled rollback mode does
not silently lose wakeups after a broker restart. Rate-limit and worker metrics, alerts,
and [runbooks](../runbooks/rate-limiting.md) are in place.

Live validation covered Redis fallback/circuit recovery, 20 parallel tasks across all
worker slots, live queue-backlog admission, and graceful worker shutdown. Remaining
validation: three-replica shared-bucket test, live DB-pool saturation, and edge Nginx
limits.

---

## 8. Phase 5 — Coordinator backpressure and reconnect

### Goal

Implement design 03: stop creating work when the system is saturated, never ack after a
failure, and reconnect cleanly.

### Tasks

- [x] Add a coordinator DB connection gate. It bounds concurrent sessions and exposes
      active/waiting stats; it is not a long-lived `psycopg_pool` (documented).
- [x] Add `QueueDepthMonitor` with a dedicated Pika connection and passive declares.
- [x] Add `BackpressureController` with hysteresis and `observe|enforce` modes.
- [x] Add `DbGate` from connection-gate stats.
- [x] Add local `DispatchGate` token bucket.
- [x] Convert the due consumer to an explicit `basic_get` loop (design 03, §5).
- [x] Ack only after a successful DB commit and publish/outbox write.
- [x] Nack/requeue on transient errors with jittered backoff; invalid JSON is dropped.
- [x] Keep heartbeats alive during pauses with `process_data_events`.
- [x] Add reconnect with exponential backoff and topology re-declaration.
- [x] Convert the result consumer to the same pull/reconnect model with a DB/result-depth
      gate. (Batch>1 prefetch is not used because each result requires its own transaction.)
- [x] Make reconciliation skip when paused or when `scheduler.due` is backed up.
- [x] Make the outbox relay respect task/result queue depth.
- [x] Add metrics, alerts, and runbook entries.

### Tests

- [x] Fill `scheduler.tasks` above high; due dispatch stops; resumes below low (unit +
      live E2E: paused at 10 queued messages, resumed after workers restarted).
- [ ] Slow DB: pause, heartbeats alive, no ack on error.
- [x] Kill RabbitMQ: reconnect with jitter, no duplicate executions (live E2E: pending
      outbox row drained after recovery; consumer reconnect counters incremented).
- [ ] Two coordinators: both pause/resume; unique slot prevents duplicates.
- [ ] Result backlog: due dispatch pauses; resumes after results drain.
- [x] Observe mode: identical behavior, “would pause” metrics only (default mode during
      normal E2E; enforcement tested separately).
- [ ] 2× capacity load test: DB pool stays below 80%, queue depth bounded.

### Exit criteria

- No unbounded DB write burst when workers are saturated.
- No message loss on DB/broker errors.
- Pause/resume does not flap for 1 hour under a steady backlog.
- Reconnect is automatic and observable.
- Rollback to `observe` is one env change.

### Implementation status (Phase 5)

The coordinator samples `scheduler.tasks`, `scheduler.results`, and `scheduler.due` with
a dedicated passive-declare connection. A hysteresis controller combines queue depth, a
bounded DB connection gate, and sample freshness. In `enforce` mode, due/result/
reconciliation/outbox consumers pause; `observe` only records state.

The due and result consumers were converted to pull loops with manual acks, jittered
transient nack/requeue, reconnect and topology re-declaration, and `process_data_events`
during pauses. The outbox relay and reconciliation respect the gate. Metrics, alerts, and
a [backpressure runbook](../runbooks/backpressure.md) are in place.

The coordinator DB gate is a semaphore around short-lived sessions rather than a
long-lived `psycopg_pool`. This still bounds concurrency and exposes active/waiting stats,
but it does not eliminate connection setup cost. Live validation paused at a 10-message
backlog and resumed cleanly, and a RabbitMQ stop/start drained a pending outbox row and
completed the task. Full 2× load and two-coordinator pause tests remain staging work.

---

## 9. Phase 6 — Database and broker HA

### Goal

Survive the loss of a Postgres primary or a RabbitMQ node without losing task state or
silently duplicating work.

### 9.1 Postgres

Choose one:

- **Managed** (RDS/Aurora/Cloud SQL): preferred if the deployment is already cloud-based.
  Use Multi-AZ, automated backups, PITR, and a read replica.
- **Self-managed** (Patroni + etcd/Consul): if the project must remain portable. Use a
  synchronous standby for RPO 0, a witness, and PgBouncer.

Tasks:

- [x] Provision primary + standby (and optionally a read replica). The lab uses Patroni +
      etcd with one leader, one synchronous standby, and one async replica. Checklist:
      [docs/HA.md](../HA.md).
- [x] Add PgBouncer (transaction pooling) in front of the primary; size pools per service.
      One pooler runs per node on `:6432`; services connect locally and PgBouncer follows
      HAProxy `/primary`. Transaction pooling, SCRAM, `DISCARD ALL`, and advisory locks
      passed the local harness. Not yet re-run in AWS.
- [x] Use transaction-scoped advisory locks (`pg_advisory_xact_lock`) where locking is
      needed; session-scoped locks are unsafe behind transaction pooling. (Outbox relay
      already does this.)
- [x] Configure `max_connections`, `statement_timeout`, `lock_timeout`, and
      `idle_in_transaction_session_timeout`. The lab Patroni template sets 200 / 5s / 2s
      / 60s; the coordinator also sets per-session timeouts.
- [x] Route writes to the primary. Services connect through HAProxy `:5433`, which uses
      Patroni's `/primary` health check; `:5434` serves replicas for read-only checks.
- [x] Backups + PITR; restore drill quarterly. Production design uses pgBackRest/WAL-G
      to S3; the local drill restored one base backup to two target times and both
      restores promoted. A production bucket and quarterly operational drill remain
      deployment work.
- [x] Monitor replication lag, failover events, connection saturation, disk. Prometheus
      now scrapes Patroni, HAProxy, PgBouncer, and Redis; HA alert rules cover these
      signals. Alertmanager routing still needs a pager test.
- [x] Document the failover runbook. (Runbook:
      [docs/runbooks/failover.md](../runbooks/failover.md).)
- [x] Test the failover runbook in staging. Drill 1 in the lab: node1 promoted in ~2.8 s,
      RPO 0, 89/89 accepted tasks completed exactly once.
- [x] Ensure migrations run against the primary and are safe under load
      (`CONCURRENTLY`, no long locks). `deploy/ha-lab/deploy.sh` applies migrations
      through HAProxy `:5433` before starting the services.

RPO/RTO target:

- Synchronous standby: RPO 0, RTO minutes (failover time).
- Asynchronous replica: RPO seconds, RTO minutes; document the trade-off.

### 9.2 RabbitMQ

#### Quorum-queue prerequisite: remove per-message TTL delays

RabbitMQ quorum queues do not support per-message TTL (and have other feature
differences; verify against the target RabbitMQ version). The current
`scheduler.delay` and `scheduler.retry_delay` queues depend on per-message TTL, so they
**cannot** be converted to quorum queues as-is.

Choose one delay mechanism before the HA rollout:

- **DB-backed scheduled events (implemented):** `scheduled_events` stores
  `(id, aggregate_id, kind, destination, dedupe_key, payload, available_at, status,
  attempts)`; the outbox relay materializes timers and a poller publishes due
  wakeups/retries to `scheduler.due`/`scheduler.tasks`. This removes the TTL delay
  queues from the primary path and gives the scheduler a durable, inspectable timer.
- **Delayed-message exchange:** not selected.
- **Queue-level TTL buckets:** not selected.

The classic TTL queues remain only for the `OUTBOX_ENABLED=false` rollback, so they
must be drained and deleted before the broker can be declared quorum-HA.

#### Quorum-queue tasks

- [x] 3-node cluster across availability zones (lab: `ap-south-1a/b/c`).
- [x] Replace per-message TTL delay queues with DB-backed scheduled events.
- [x] Implement quorum declaration for `scheduler.tasks`, `scheduler.due`,
      `scheduler.results`, and `scheduler.tasks.dlq` via `RABBITMQ_QUEUE_TYPE=quorum`.
- [x] Provision the cluster and enable `RABBITMQ_QUEUE_TYPE=quorum` after draining the
      old TTL queues. All four queues have three members.
- [x] Keep publisher confirms and consumer manual acks.
- [x] Configure queue-length and DLQ alerts.
- [x] Configure broker disk/memory alarms for the managed cluster.
      `rabbitmq.conf.tmpl` sets resource alarms, `pause_minority`, and consumer-timeout
      protection; Prometheus alerts on RabbitMQ alarms and partitions.
- [x] Use reconnect backoff in clients; the lab exposes RabbitMQ through per-node
      HAProxy `:5673` (no NLB).
- [x] Test broker loss: messages survive, consumers reconnect, no duplicate executions.
      Drill 2: 301-message backlog retained with the leader down; 300/300 exactly once.
- [x] Test network partition: quorum queues require a majority; document the behavior.
      Local harness: one-broker isolation kept the majority serving; two-broker
      isolation paused the minority; 160/160 messages consumed exactly once after
      recovery.
- [x] Set intentional retention for DB timers and outbox rows; do not let them grow
      forever.
- [x] Configure broker queue/message TTLs and alarms for the managed cluster. Quorum
      declarations carry `x-message-ttl` and `x-delivery-limit`; changing them requires
      draining and recreating the queues.

> Current status (2026-11-08): the v1.4.0 lab was deployed and validated in a
> self-managed three-node AWS lab (`deploy/ha-lab/`). Patroni PostgreSQL 16.6 (sync
> standby), 3-node RabbitMQ with quorum queues, Redis Sentinel, and per-node HAProxy
> ran there. Drills passed: PostgreSQL RPO 0 / ~2.8 s promotion, 301 queued messages
> retained on broker leader loss, Redis local fallback with no 5xx, and 8,040 load-test
> tasks completed exactly once. The lab was then destroyed to stop charges.
>
> v1.5.0 hardening was validated locally/at configuration level, not by re-provisioning
> AWS: Redis Cluster, quorum TTL/delivery limits and partition behavior, PgBouncer,
> PITR restore, nginx load balancing, and Patroni/HAProxy/PgBouncer/Redis metrics and
> alerts. The free-tier single-node demo remains non-HA by design. Remaining production
> gaps: no live post-v1.5.0 AWS rerun, no managed LB or DNS endpoint, no production S3
> backup bucket, and no pager-route test. See [docs/HA.md](../HA.md) and the
> [evolution record](../evolution/v1.5.0-data-plane-hardening.md).

### 9.3 Redis (if used for rate limits/cache)

- [x] Managed Redis or Sentinel; not a single point of failure. The lab runs one master,
      two replicas, three Sentinels, and a sentinel-aware proxy.
- [x] Persistence is optional because state is ephemeral; document that loss is
      acceptable. The lab disables RDB/AOF and Redis is never authoritative.
- [x] If Redis Cluster is required for horizontal scaling, test the Lua script and
      client in cluster mode. `REDIS_CLUSTER_NODES` selects ioredis `Cluster`; the
      single-key script needs no hash tag. A six-node local harness passed, including a
      forced master failover.
- [x] Redis remains non-authoritative. Postgres still enforces idempotency and state.

### 9.4 Failover drills

- [x] Kill the Postgres primary during dispatch; measure RTO, verify no duplicate
      executions, verify the outbox drains after failover. Drill 1: RPO 0, ~2.8 s
      promotion, 89/89 exactly once.
- [x] Kill a RabbitMQ node during a backlog; verify quorum queues and consumer recovery.
      Drill 2: 301 messages retained, 300/300 exactly once.
- [x] Kill the Redis master while rate limiting is enforced; verify local fallback and a
      bounded circuit-breaker recovery. Drill 3: 59 fallback requests, no 5xx.
- [x] Partition the network between coordinator and DB; verify pause/backoff, not ack/loop.
      The quorum partition harness covers broker-side majority loss; the coordinator
      DB-gate/reconnect path is covered by its unit and integration tests. A combined
      live DB+browser partition drill was not re-run in AWS.
- [x] Restore from PITR into a staging database and verify row counts/invariants. The
      local drill restored to two target times and verified post-target rows were absent;
      the production staging restore remains an operational step.
- [x] Record results in `docs/runbooks/failover.md`.

### Status

Phase 6 is deployed and validated in the lab:

- Migration `0010_scheduled_events` plus the bootstrap table and indexes.
- Outbox relay materializes `task.scheduled`, `task.cancelled`, and `task.retry` in the
  same transaction that marks the outbox row published.
- `scheduled_event_poller.py` drains due rows with `FOR UPDATE SKIP LOCKED`, confirms,
  `mandatory=True`, full-jitter backoff, terminal failure, retention cleanup, and
  backpressure gating.
- `RABBITMQ_QUEUE_TYPE=classic|quorum` topology support in API, coordinator, and worker;
  quorum mode skips the TTL queues and requires outbox + DB timers.
- Worker duplicate-retry path no longer republishes TTL messages when the outbox is on.
- Metrics, three alerts, Grafana panels, HA guide, and rollback flags are documented.
- Coordinator/worker/API tests cover materialization, dedupe, cancellation, timer poll,
  failure/backoff, cleanup, config validation, and quorum topology.
- `deploy/ha-lab/` provisions three Spot nodes across AZs with etcd + Patroni,
  RabbitMQ quorum queues, Redis Sentinel, per-node HAProxy and PgBouncer, optional nginx,
  and Prometheus/Grafana.
- The deploy script reuses `.env` across reruns and enforces quorum queue membership
  after application startup.
- v1.5.0 adds Redis Cluster selection, quorum TTL/delivery limits, PgBouncer transaction
  pooling, PITR restore design + local drill, nginx load balancing, and
  Patroni/HAProxy/PgBouncer/Redis scrape jobs and HA alerts.
- Drills 1–3 and the HA load test passed in the v1.4.0 lab; results are recorded in
  [docs/runbooks/failover.md](../runbooks/failover.md#drill-log). Redis Cluster, quorum
  partition, PgBouncer, PITR, and nginx were then validated with local harnesses.

Not complete: a live post-v1.5.0 AWS rerun, a managed load balancer or DNS-based stable
endpoint, a production S3/WAL-G backup deployment, and an alert pager-route test.
Phase 7 is implemented and locally validated; its accelerators remain opt-in.

### Exit criteria

- A primary failover does not lose committed tasks or duplicate executions.
- A broker node loss does not lose queued messages.
- The outbox drains after recovery.
- Alerts and runbooks are current.
- RTO/RPO are measured, not assumed.

---

## 10. Phase 7 — Optional Redis KV/cache/fairness scaling

Redis and OpenTelemetry are accelerators/observers; PostgreSQL remains the
scheduler's source of truth. The Phase 4 token bucket is still the only
Redis dependency on the admission path, and it degrades to a smaller local
budget rather than falling back to the database.

> **Status: implemented and locally validated (v1.6.0).** Migrations `0011`–`0013`
> add `tenant_quotas`, the archive tables, and `scheduled_events.headers`. Every
> feature is default-off and fails open to PostgreSQL. No live AWS lab was
> re-provisioned; validation used disposable PostgreSQL, the local API/worker
> suites, and local harnesses. See the
> [v1.6.0 record](../evolution/v1.6.0-optional-scaling.md) and
> [Phase 7 internals](../internals/09-cache-quotas-tracing-archival.md).

### Tasks

- [x] Cache task reads / execution history with short TTLs and explicit invalidation.
      `CACHE_ENABLED`, `CACHE_TTL_SEC`, `CACHE_REDIS_TIMEOUT_MS`; task writes
      invalidate the task key and bump a per-task execution generation.
- [x] Cache completed idempotency responses; PostgreSQL remains authoritative.
      `IDEMPOTENCY_CACHE_ENABLED`; cached responses carry the request hash, and a
      mismatch still returns `409`.
- [x] Add per-tenant quotas and weighted fairness. Migration `0011`;
      `TENANT_QUOTAS_ENABLED`; effective rate/burst are base × `weight`; disabled
      or missing rows use configured defaults, never unlimited access.
- [x] Shard the global rate limiter across Redis keys if a single key is hot.
      `GLOBAL_INGRESS_SHARDS` (default 16); FNV-1a client routing; total burst is
      divided across shards.
- [x] Add OpenTelemetry traces across API → outbox → coordinator → worker → result.
      `OTEL_ENABLED`; W3C `traceparent` in outbox headers, RabbitMQ headers, and
      `scheduled_events.headers` (migration `0013`); Python helpers no-op when the
      SDK is absent.
- [x] Add partitioning/archival for `task_executions` and `outbox`. Migration
      `0012`; bounded copy/delete of terminal rows only; coordinator loop plus
      `scripts/archive_data.py` / `make archive-data`.
- [x] Consider a delayed-exchange or DB-driven scheduler to replace the TTL delay
      queue. Completed in Phase 6 (`scheduled_events`); the TTL path remains only
      for rollback.

### Acceptance criteria

- [x] API suite passes: 63 passed / 20 skipped; build clean.
- [x] Coordinator and worker pytest suites pass against disposable PostgreSQL.
- [x] Root pytest passes with `--ignore=tests/pgbouncer` (the PgBouncer harness
      owns its own ports and is not part of the default run).
- [x] Migrations `0011`–`0013` apply from bootstrap, roll back, and re-apply.
- [x] Archival dry-run, real run, and `make archive-data` move only old terminal
      rows; pending and recent rows stay.
- [x] Prometheus rules validate (54 rules) and the Grafana dashboard is valid
      JSON (45 panels, Phase 7 IDs 116–126).
- [x] Cache/tenant/shard tests cover hit/miss, invalidation, defaults, weighted
      effective values, observe/enforce, and shard selection.
- [x] Tracing tests cover header propagation across outbox and timer rows.
- [ ] Live AWS rerun and production cache/quota calibration. Not claimed; the lab
      remains destroyed.

### Schema

| Migration | Change | Down behavior |
| --- | --- | --- |
| `0011_tenant_quotas` | `tenant_quotas` table, constraints, `updated_at` trigger | drops the table |
| `0012_archive_tables` | archive tables, archive indexes, live-table scan indexes | drops archive tables and scan indexes |
| `0013_scheduled_event_headers` | `scheduled_events.headers JSONB NOT NULL DEFAULT '{}'` | drops the column |

### Rollout

1. Apply `0011`–`0013` with `make migrate`.
2. Deploy the new images with every Phase 7 flag off; verify the existing path is
   unchanged.
3. Enable `CACHE_ENABLED` on one API replica; watch hit/miss/error/latency.
4. Enable `IDEMPOTENCY_CACHE_ENABLED`; replay the same key and a mismatched body.
5. Add `tenant_quotas` rows for canary tenants, enable `TENANT_QUOTAS_ENABLED` with
   `RATE_LIMIT_MODE=observe`, then move one replica to `enforce`.
6. Raise `GLOBAL_INGRESS_SHARDS` only after confirming the aggregate burst math;
   `1` is the compatibility rollback.
7. Roll tracing out API → coordinator → workers; verify one end-to-end trace.
8. Run archival dry-run, then a manual bounded run, then enable `ARCHIVE_ENABLED`.

Do **not**:

- Move task state to Redis.
- Use Redis locks as the only correctness mechanism.
- Let cache loss create duplicate tasks or executions.
- Treat a quota weight as worker/broker priority; dispatch remains FIFO.
- Partition `task_executions` by time without redesigning the unique
  `(task_id, scheduled_for)` and active-execution indexes.

### Exit criteria

- All Phase 7 features are default-off or fail open; disabling any flag restores
  the prior behavior without schema rollback.
- PostgreSQL remains authoritative for tasks, slots, leases, idempotency, outbox,
  and timers.
- Terminal history can be archived without touching pending/running work.
- Traces survive the outbox and database-timer boundaries when enabled.
- Alerts and the runbook cover cache, quota, tracing, and archival failures.

---

## 11. Cross-cutting test strategy

| Layer | Tooling | What it proves |
|-------|---------|----------------|
| Unit | Vitest, pytest | Algorithms, state transitions, validation |
| Integration | Supertest + Postgres, pytest + Postgres/RabbitMQ | SQL, transactions, queue behavior |
| Contract | Message schema tests | Producer/consumer compatibility |
| Concurrency | Parallel requests/coordinators | Idempotency, unique slots, leases |
| Chaos | Toxiproxy, kill containers | Redis/DB/broker failure behavior |
| Load | k6 | Throughput, p95/p99, backpressure |
| Migration | Ephemeral Postgres | Expand/backfill/contract and rollback |

Required CI gates:

- Lint, format, typecheck, build.
- Unit + integration tests.
- Migration from scratch and one-step rollback.
- No masked failures.
- Container image build.

Before each phase’s production rollout:

- Load test at 2× expected peak.
- Chaos test the dependency that phase introduces.
- Verify dashboards and alerts.
- Write/refresh the runbook.

---

## 12. Rollout strategy

Every phase follows the same pattern:

1. **Expand:** additive schema, nullable columns, dual-read/dual-write.
2. **Deploy:** ship code behind a feature flag in `observe` mode where applicable.
3. **Backfill:** batched, resumable, monitored.
4. **Contract:** enforce constraints/indexes after data is clean.
5. **Enable:** turn on behavior for a canary tenant/coordinator.
6. **Measure:** compare against the baseline; roll back on regression.

Feature flags:

```env
SCHEDULER_JITTER_ENABLED=false
IDEMPOTENCY_ENFORCE=false
OUTBOX_ENABLED=false
RATE_LIMIT_ENABLED=false
RATE_LIMIT_MODE=observe
ADMISSION_ENABLED=false
BACKPRESSURE_MODE=observe
WORKER_CONCURRENCY=1
CACHE_ENABLED=false
IDEMPOTENCY_CACHE_ENABLED=false
TENANT_QUOTAS_ENABLED=false
GLOBAL_INGRESS_SHARDS=16
OTEL_ENABLED=false
ARCHIVE_ENABLED=false
```

Canary order:

- Phase 1: one coordinator + one API replica.
- Phase 2: one tenant / one API replica.
- Phase 3: one coordinator/relay; compare outbox lag and duplicate publishes.
- Phase 4: login limits first, then task creation; one API replica.
- Phase 5: observe mode for a full day, then enforce on one coordinator.
- Phase 6: staging failover first; production during a low-traffic window.
- Phase 7: cache on one API replica, quotas in observe for canary tenants, tracing
  API → coordinator → workers, then archival dry-run and one manual bounded run.

Rollback rules:

- Prefer flags over schema rollback.
- Never drop columns/tables in the same release that stops using them.
- Keep the outbox rows; stopping the relay is safe.
- Keep execution history; mark bad rows instead of deleting them.

---

## 13. Risk register

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Migration/index lock on large tables | Medium | High | `CONCURRENTLY`, batched backfill, off-peak, lock timeout |
| Duplicate running executions block active index | Medium | High | Pre-flight query, reviewed cleanup, reaper first |
| Cron parser behavior differs from coordinator | Medium | Medium | Shared test vectors; compute next slot server-side |
| Jitter pushes runs past next slot | Low | Medium | Cap window at `period - 1`; invariant test |
| Idempotency key leaks response data | Low | Medium | TTL, no secrets in response body, access control |
| Stale worker overwrites new attempt | Medium | High | Lease ID in conditional update; attempt history |
| Outbox duplicate publish | Medium | Low | Consumers idempotent; duplicate metrics |
| Outbox order inversion with multiple relays | Medium | High | Single relay initially; advisory locks per aggregate |
| Outbox table growth | Medium | Medium | Cleanup job, lag alert, bounded archival |
| Cache serves stale task data | Medium | Low | Short TTL, write-through update, generation invalidation |
| Cache outage adds DB load | Medium | High | Fail-open timeout, admission control, error-rate alert |
| Tenant quota defaults surprise low-volume users | Medium | Medium | Observe mode, `X-Quota-*` headers, per-tenant rows before enforce |
| Quota weights mistaken for worker priority | Medium | Medium | Document FIFO dispatch; quotas gate admission only |
| Archive job competes with live traffic | Low | Medium | Small batches, sleep, `SKIP LOCKED`, off-peak schedule |
| Archive moves a row still needed | Low | High | Terminal-only predicate, one transaction, dry-run, retention window |
| Trace context bloats messages/rows | Low | Low | Header-only JSONB, sampler, no payload duplication |
| Redis outage causes DB overload | Medium | High | Local conservative limiter, DB admission, no DB fallback |
| Rate limit false positives | Medium | Medium | Observe mode, per-route calibration, canary |
| Pika thread-safety bug | Medium | High | One connection/channel per consumer thread |
| Coordinator pause flapping | Medium | Medium | Hysteresis, consecutive samples, observe mode |
| DB failover loses async writes | Low/Medium | High | Synchronous standby or documented RPO; PITR |
| RabbitMQ quorum queue unavailability | Low | High | 3 nodes, majority, alarms, reconnect |
| Worker command security | High | Critical | Separate project: sandbox, non-root, resource limits, no host secrets |
| CI still hides failures | Medium | High | Remove `|| true`, required checks, branch protection |

The worker command security risk is called out because it is more severe than any
reliability issue in this plan: the worker executes user-provided commands as root with
database and JWT secrets in its environment. Reliability work must not delay that fix.

---

## 14. Definition of done (per phase)

- [ ] Code merged behind a flag; default is safe.
- [ ] Unit, integration, concurrency, and migration tests pass in CI.
- [ ] Load test at 2× expected peak passes or has a documented exception.
- [ ] Metrics and alerts are live; dashboard updated.
- [ ] Runbook entry exists (symptoms, diagnosis, rollback).
- [ ] Backfill completed and verified; constraints enforced.
- [ ] Rollback tested in staging.
- [ ] README/architecture docs updated; no stale exactly-once claims.
- [ ] No new hardcoded secrets, ports, public IPs, or environment-specific defaults.

---

## 15. First two weeks (Phase 0 + Phase 1, complete)

### Week 1

- [x] Add the migration runner, `database/migrations/`, Makefile targets.
- [x] Add ephemeral Postgres/RabbitMQ test harness and one smoke test.
- [x] Add config validation; remove production fallback credentials.
- [x] Add API metrics endpoint and fix Prometheus scrape targets.
- [x] Fix CI: lint config, remove `|| true`, add Python tests.
- [x] Capture baseline load test numbers.

### Week 2

- [x] Add migration `0002_expand_jitter_dispatch.sql`.
- [x] Add `stable_offset` + `compute_dispatch_at` in TS and Python with parity tests.
- [x] Add API fields and `cron-parser` validation.
- [x] API writes `dispatch_at`; coordinator reads `COALESCE`.
- [x] Due consumer uses `dispatch_at` and `scheduled_for` with `ON CONFLICT`.
- [x] Add the unique slot index after backfill.
- [x] Add worker retry jitter.
- [x] Add metrics and the Phase 1 acceptance test.

### Suggested PR sequence

| PR | Contents | Depends on |
|----|----------|------------|
| 1 | Migration runner, Makefile, CI migration job | — |
| 2 | Config validation + test harness | 1 |
| 3 | Baseline metrics/load test/docs fixes | 2 |
| 4 | Expand + backfill migrations | 1 |
| 5 | Jitter helpers + tests (TS/Python) | 4 |
| 6 | API jitter/dispatch_at/cron validation | 5 |
| 7 | Coordinator dispatch/result/reconciliation | 6 |
| 8 | Worker retry jitter + metrics | 5 |
| 9 | Contract migration (unique slot index lands before code rollout) | 7 |
| 10 | Phase 1 load test + runbook | 9 |

Keep PRs small and independently deployable. Do not combine the contract migration with
the code that first writes the new columns.

---

## 16. Open decisions

1. **Default recurring jitter (resolved):** 0, opt-in per task until tenants can see and
   control `dispatch_at`.
2. **Misfire policy (resolved):** skip missed recurring slots; a one-time task whose
   time is already past dispatches immediately.
3. **Idempotency scope:** required for `POST /tasks` or optional? Recommendation: optional
   initially, required for bulk operations.
4. **Outbox ordering:** one relay forever, or per-aggregate advisory locks? Recommendation:
   one relay until throughput requires more; then per-aggregate locks.
5. **HA topology:** managed Postgres/RabbitMQ vs self-managed Patroni/quorum queues.
   Recommendation: managed if the deployment target supports it.
6. **Worker concurrency:** thread-per-consumer vs `aio-pika`. Recommendation: thread-per-
   consumer first, cap at 4–8, revisit after profiling.
7. **Redis topology:** single/Sentinel vs Cluster. Recommendation: Sentinel or managed
   initially; Cluster only if the rate-limit key is hot.
8. **Worker sandbox:** should it be a prerequisite for exposing the scheduler to untrusted
   users? Recommendation: yes; treat it as a separate P0 security workstream.
