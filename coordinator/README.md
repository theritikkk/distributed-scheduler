# Coordinator Service

The coordinator is the **scheduling brain** of the distributed task scheduler. It does not run user commands; it moves work from PostgreSQL → RabbitMQ → workers and merges results back into the database.

---

## What is `__pycache__` / `*.cpython-313.pyc`?

When you run Python, it compiles `.py` files to **bytecode** for faster startup next time.

| Path | Meaning |
|------|---------|
| `__pycache__/` | Folder Python creates next to your source files |
| `__init__.cpython-313.pyc` | Compiled bytecode of `__init__.py` for **Python 3.13** (`313` = version) |

**You should not edit or commit these.** They are regenerated automatically. This repo ignores them via `.gitignore` (`__pycache__/`, `*.pyc`).

To remove them locally:

```bash
find coordinator -type d -name __pycache__ -exec rm -rf {} +
```

---

## Folder layout

```
coordinator/
├── main.py                 # Entry point: starts metrics, threads, due consumer loop
├── metrics.py              # Prometheus metric definitions (counters/gauges)
├── requirements.txt
├── Dockerfile
├── README.md               # This file
└── internal/
    ├── config.py           # Validated environment configuration
    ├── archive.py          # Bounded archival of terminal executions/attempts/outbox
    ├── tracing.py          # Optional W3C/OpenTelemetry helpers (no-op fallback)
    ├── outbox.py           # Outbox row types/helpers
    ├── scheduled_events.py # Materialize/cancel/clean DB-backed wakeup and retry timers
    ├── backpressure/       # Queue-depth monitor, DB gate, controller, dispatch gate
    ├── registry/           # Worker health in PostgreSQL
    │   ├── __init__.py
    │   └── worker_registry.py
    └── queue/              # All RabbitMQ + scheduling logic
        ├── __init__.py     # Re-exports run_* functions
        ├── connection.py   # Blocking AMQP connection helper
        ├── topology.py     # Queue names + classic/quorum declaration
        ├── publisher.py    # Rollback-only TTL wakeups → scheduler.due
        ├── outbox_relay.py # Committed outbox rows → broker or scheduled_events
        ├── scheduled_event_poller.py # due DB timers → confirmed broker publishes
        ├── due_consumer.py       # scheduler.due → create pending execution → scheduler.tasks
        ├── result_consumer.py    # scheduler.results → lease-guarded DB update + reschedule recurring
        ├── reaper.py             # stale pending/expired lease recovery + idempotency cleanup
        └── reconciliation.py     # Slow safety scan → nudge missed tasks to due queue
```

---

## How the coordinator runs (`main.py`)

1. **Metrics HTTP server** on port `9090` (`/metrics` for Prometheus).
2. **Heartbeat checker** (background): marks workers `offline` if no heartbeat for 2 minutes.
3. **Queue-depth monitor and backpressure controller** (background): samples `scheduler.tasks`, `scheduler.results`, and `scheduler.due` with a dedicated passive-declare connection; combines them with the bounded DB gate. In `enforce` mode it pauses due/result/reconciliation/outbox/timer work; default is `observe`.
4. **Result consumer** (background): pull loop over `scheduler.results`, applying only results whose `leaseId` matches the running execution and closing the matching attempt row.
5. **Reaper** (background): every `REAPER_INTERVAL_SEC` (default 15s) recovers expired leases and stale pending dispatches, republishing the same execution or failing it after max attempts. Also deletes expired API idempotency keys.
6. **Reconciliation poller** (background): every `SCHEDULER_RECONCILE_INTERVAL_SEC` (default 120s), finds due `scheduled` tasks and publishes to `scheduler.due` (safety net if timers were missed). It skips while paused or when the due queue is backed up.
7. **Outbox relay** (background): claims committed rows with `FOR UPDATE SKIP LOCKED`, preserves per-aggregate order with `pg_advisory_xact_lock`, publishes with confirms and `mandatory=True`, and retries with backoff. Schedule/retry events are materialized into `scheduled_events` in the same transaction.
8. **Timer poller** (background, `SCHEDULED_EVENTS_ENABLED=true`): claims due `scheduled_events` with `FOR UPDATE SKIP LOCKED`, publishes to `scheduler.due`/`scheduler.tasks` with confirms and `mandatory=True`, and retries with backoff.
9. **Due consumer** (foreground): pull loop over `scheduler.due`, locks the task row, creates a `pending` `task_executions` row, writes the dispatch outbox event in the same transaction, and publishes to `scheduler.tasks`.
10. **Archivist** (background, `ARCHIVE_ENABLED=true`): every `ARCHIVE_INTERVAL_SEC` copies and deletes old terminal executions/attempts and published/failed outbox rows in bounded transactions. Pending/running work and pending timers are never selected.

---

## File-by-file

### `main.py`

- Loads `DATABASE_URL`, `RABBITMQ_URL` from environment.
- Verifies DB connectivity.
- Starts Prometheus on **9090**.
- Spawns background threads for registry + result consumer + reaper + reconciliation.
- Blocks on `run_due_consumer()`.

### `metrics.py`

Prometheus instruments used by queue modules, for example:

- Tasks dispatched from due consumer
- Results applied / stale-lease rejected / skipped (idempotent duplicate)
- Executions reaped and idempotency keys cleaned
- Reconciliation nudges

### `internal/registry/worker_registry.py`

- `Registry.run_heartbeat_checker()`: periodic SQL `UPDATE workers SET status='offline' WHERE last_heartbeat < NOW() - 2 minutes`.

### `internal/queue/topology.py`

Declares durable queues and DLX bindings in `classic` or `quorum` mode:

| Queue | Role |
|-------|------|
| `scheduler.due` | “Run this task now” wakeups from DB timers/reconciliation |
| `scheduler.tasks` | Work for workers |
| `scheduler.tasks.dlq` | Poison / max-retries / bad messages |
| `scheduler.results` | Worker completion reports |
| `scheduler.delay` | Rollback only: per-message TTL → `scheduler.due`; not declared in quorum mode |
| `scheduler.retry_delay` | Rollback only: backoff TTL → `scheduler.tasks`; not declared in quorum mode |

`RABBITMQ_QUEUE_TYPE=quorum` adds `x-queue-type=quorum` and skips the TTL queues. It is
rejected unless `OUTBOX_ENABLED=true` and `SCHEDULED_EVENTS_ENABLED=true`.

### `internal/queue/publisher.py`

- `publish_schedule_wakeup(channel, task_id, next_execution_time)` — rollback-only TTL message on `scheduler.delay`, or immediate publish to `scheduler.due`.
- `publish_schedule_wakeup_url(...)` — opens a short-lived connection for the direct fallback.

### `internal/queue/due_consumer.py`

1. Pull from `scheduler.due` with `basic_get`; check the backpressure gate first.
2. JSON body: `{ "taskId": "..." }`.
3. `SELECT ... FOR UPDATE` on task; skip if not `scheduled`.
4. If `next_execution_time`/`dispatch_at` is still in the future → leave the authoritative DB timer in place (classic mode re-publishes a TTL wakeup).
5. `INSERT task_executions` (`pending`), `UPDATE tasks` → `running`, and write the outbox dispatch event in one transaction.
6. Ack only after commit/publish; transient errors nack/requeue with jitter; keep heartbeats alive with `process_data_events` while paused.

### `internal/queue/result_consumer.py`

1. Pulls worker result JSON containing `executionId`, `leaseId`, `workerId`, and `attempt`.
2. Updates the execution only if `lease_id` matches, `status = 'running'`, and `completed_at IS NULL`; closes the matching attempt row.
3. Updates task status; for recurring tasks computes the next cron slot and writes a schedule outbox event in the same transaction.
4. Stale leases and duplicate results are no-ops, so an old worker cannot overwrite a newer attempt.
5. Results continue to drain when only the task queue is deep; they pause when the DB or result queue is saturated.

### `internal/queue/reconciliation.py`

- Slow loop: `SELECT id FROM tasks WHERE status='scheduled' AND COALESCE(dispatch_at, next_execution_time) <= NOW() LIMIT 100`.
- Publishes `{ taskId }` to `scheduler.due` for each (recovery path).

---

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | local postgres URL | PostgreSQL |
| `RABBITMQ_URL` | local amqp URL | RabbitMQ |
| `TASK_MAX_RETRIES` | `3` | Passed to workers in task messages; reaper max attempts = retries + 1 |
| `SCHEDULER_RECONCILE_INTERVAL_SEC` | `120` | Reconciliation period |
| `REAPER_ENABLED` | `true` | Enable stale execution recovery |
| `REAPER_INTERVAL_SEC` | `15` | Reaper scan interval |
| `PENDING_RETRY_GRACE_SEC` | `300` | Grace after a pending dispatch/retry becomes due before republishing |
| `IDEMPOTENCY_CLEANUP_INTERVAL_SEC` | `3600` | Expired idempotency key cleanup interval |
| `METRICS_PORT` | `9090` | Prometheus scrape port |
| `OUTBOX_ENABLED` | `true` | Write/publish through the transactional outbox |
| `OUTBOX_POLL_INTERVAL_SEC` | `1` | Relay poll interval |
| `OUTBOX_BATCH_SIZE` | `100` | Max rows claimed per relay pass |
| `OUTBOX_MAX_ATTEMPTS` | `10` | Terminal failure threshold |
| `SCHEDULED_EVENTS_ENABLED` | `true` | Materialize/publish DB-backed timers |
| `SCHEDULED_EVENT_POLL_INTERVAL_SEC` | `1.0` | Timer poll interval |
| `SCHEDULED_EVENT_BATCH_SIZE` | `100` | Max due timers claimed per pass |
| `SCHEDULED_EVENT_MAX_ATTEMPTS` | `10` | Terminal timer publish failure threshold |
| `SCHEDULED_EVENT_BACKOFF_BASE_SEC` / `_MAX_SEC` | `1` / `60` | Timer publish retry backoff |
| `SCHEDULED_EVENT_RETENTION_DAYS` | `7` | Published/cancelled timer retention |
| `RABBITMQ_QUEUE_TYPE` | `classic` | `classic` or `quorum` (requires outbox + DB timers) |
| `BACKPRESSURE_MODE` | `observe` | `observe` or `enforce` |
| `BACKPRESSURE_TASKS_HIGH` / `_LOW` | `5000` / `1000` | Task queue watermarks |
| `BACKPRESSURE_RESULTS_HIGH` / `_LOW` | `2000` / `500` | Result queue watermarks |
| `DB_POOL_MAX` | `20` | Coordinator connection gate limit |
| `DISPATCH_RATE_PER_SEC` / `DISPATCH_BURST` | `200` / `20` | Local dispatch pacing |
| `RECONCILE_BATCH` / `RECONCILE_PACE_SEC` | `100` / `0.01` | Reconciliation batching and pacing |
| `OTEL_ENABLED` | `false` | Enable W3C tracing spans/export |
| `OTEL_SERVICE_NAME` | `coordinator` | Trace service name |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP default | OTLP/HTTP trace endpoint |
| `OTEL_SAMPLER_ARG` | `0.1` | Root sampler ratio |
| `ARCHIVE_ENABLED` | `false` | Enable the terminal-history archive loop |
| `ARCHIVE_RETENTION_DAYS` | `30` | Keep terminal rows newer than this |
| `ARCHIVE_BATCH_SIZE` | `500` | Rows copied/deleted per batch |
| `ARCHIVE_INTERVAL_SEC` | `3600` | Archive loop interval |
| `ARCHIVE_MAX_BATCHES` | `20` | Bounded batches per cycle |
| `ARCHIVE_SLEEP_MS` | `50` | Pause between batches |

---

## Metrics (`metrics.py` + `internal/queue/rabbitmq_metrics.py`)

- HTTP server on **`:9090/metrics`** (see `METRICS_PORT`).
- Counters for due dispatch, results applied/skipped, reconciliation nudges, workers marked offline.
- Optional RabbitMQ queue depth gauges via management API poll (`RABBITMQ_METRICS_POLL_SEC`, default 15s).
- Phase 3 outbox pending/age/failure metrics.
- Phase 6 scheduled-event pending/failed/lag, materialized/cancelled/published, publish failures/duration, and cleanup metrics.
- Phase 5 `scheduler_backpressure_state`, transitions, paused seconds, queue depths, DB pool utilization/waiters, consumer reconnects/errors, and dispatch gate blocks.
- Phase 7 `scheduler_archive_rows_total`, `_errors_total`, `_runs_total`, `_duration_seconds`, and `_oldest_row_age_seconds` for bounded archival.

---

## Related docs

- [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) — design trade-offs
- [../README.md#monitoring-stack](../README.md#monitoring-stack) — Prometheus, Grafana, Loki, alerts
- [../docs/runbooks/phase7-scaling.md](../docs/runbooks/phase7-scaling.md) — cache, quotas, tracing, archival
- [../docs/DEMO.md](../docs/DEMO.md) — step-by-step demo
