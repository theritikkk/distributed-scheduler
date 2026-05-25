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
    ├── registry/           # Worker health in PostgreSQL
    │   ├── __init__.py
    │   └── worker_registry.py
    └── queue/              # All RabbitMQ + scheduling logic
        ├── __init__.py     # Re-exports run_* functions
        ├── connection.py   # Blocking AMQP connection helper
        ├── topology.py     # Queue names + declare_scheduler_topology()
        ├── publisher.py    # Delay TTL wakeups → scheduler.due
        ├── due_consumer.py       # scheduler.due → create execution → scheduler.tasks
        ├── result_consumer.py    # scheduler.results → update DB + reschedule recurring
        └── reconciliation.py     # Slow safety scan → nudge missed tasks to due queue
```

---

## How the coordinator runs (`main.py`)

1. **Metrics HTTP server** on port `9090` (`/metrics` for Prometheus).
2. **Heartbeat checker** (background): marks workers `offline` if no heartbeat for 2 minutes.
3. **Result consumer** (background): reads `scheduler.results`, updates `task_executions` / `tasks` (idempotent by `execution_id`).
4. **Reconciliation poller** (background): every `SCHEDULER_RECONCILE_INTERVAL_SEC` (default 120s), finds due `scheduled` tasks and publishes to `scheduler.due` (safety net if TTL wakeups were missed).
5. **Due consumer** (foreground): consumes `scheduler.due`, locks task row, creates `task_executions`, publishes payload to `scheduler.tasks`.

---

## File-by-file

### `main.py`

- Loads `DATABASE_URL`, `RABBITMQ_URL` from environment.
- Verifies DB connectivity.
- Starts Prometheus on **9090**.
- Spawns background threads for registry + result consumer + reconciliation.
- Blocks on `run_due_consumer()`.

### `metrics.py`

Prometheus instruments used by queue modules, for example:

- Tasks dispatched from due consumer
- Results applied / skipped (idempotent duplicate)
- Reconciliation nudges

### `internal/registry/worker_registry.py`

- `Registry.run_heartbeat_checker()`: periodic SQL `UPDATE workers SET status='offline' WHERE last_heartbeat < NOW() - 2 minutes`.

### `internal/queue/topology.py`

Declares all durable queues and DLX bindings:

| Queue | Role |
|-------|------|
| `scheduler.delay` | Per-message TTL; expires → `scheduler.due` |
| `scheduler.due` | “Run this task now” wakeups |
| `scheduler.tasks` | Work for workers |
| `scheduler.retry_delay` | Failed task backoff; expires → `scheduler.tasks` |
| `scheduler.tasks.dlq` | Poison / max-retries / bad messages |
| `scheduler.results` | Worker completion reports |

### `internal/queue/publisher.py`

- `publish_schedule_wakeup(channel, task_id, next_execution_time)` — TTL message on `scheduler.delay` or immediate publish to `scheduler.due`.
- `publish_schedule_wakeup_url(...)` — opens short-lived connection (used after recurring task completes).

### `internal/queue/due_consumer.py`

1. JSON body: `{ "taskId": "..." }`.
2. `SELECT ... FOR UPDATE` on task; skip if not `scheduled`.
3. If `next_execution_time` still in future → re-publish wakeup.
4. `INSERT task_executions` (`running`), `UPDATE tasks` → `running`.
5. Publish full message to `scheduler.tasks` (`taskId`, `executionId`, `payload`, `retryCount`, `maxRetries`).

### `internal/queue/result_consumer.py`

1. Consumes worker result JSON.
2. Updates execution only if `completed_at IS NULL` (idempotency).
3. Updates task status; for recurring tasks computes next cron time and calls `publish_schedule_wakeup_url`.

### `internal/queue/reconciliation.py`

- Slow loop: `SELECT id FROM tasks WHERE status='scheduled' AND next_execution_time <= NOW() LIMIT 100`.
- Publishes `{ taskId }` to `scheduler.due` for each (recovery path).

---

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | local postgres URL | PostgreSQL |
| `RABBITMQ_URL` | local amqp URL | RabbitMQ |
| `TASK_MAX_RETRIES` | `3` | Passed to workers in task payload |
| `SCHEDULER_RECONCILE_INTERVAL_SEC` | `120` | Reconciliation period |
| `METRICS_PORT` | `9090` | Prometheus scrape port |

---

## Metrics (`metrics.py` + `internal/queue/rabbitmq_metrics.py`)

- HTTP server on **`:9090/metrics`** (see `METRICS_PORT`).
- Counters for due dispatch, results applied/skipped, reconciliation nudges, workers marked offline.
- Optional RabbitMQ queue depth gauges via management API poll (`RABBITMQ_METRICS_POLL_SEC`, default 15s).

---

## Related docs

- [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) — design trade-offs
- [../docs/MONITORING.md](../docs/MONITORING.md) — Prometheus, Grafana, Loki, alerts
- [../docs/DEMO.md](../docs/DEMO.md) — step-by-step demo
