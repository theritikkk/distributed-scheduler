# Project Overview

This document explains what the distributed scheduler does, how the pieces fit together, and the tech stack — in plain terms.

---

## What problem does it solve?

You want to run scheduled tasks ("every hour", "once at 3pm") in a reliable, distributed way:

- Multiple workers run tasks in parallel so you can scale.
- If a worker dies, tasks are not lost — they stay in a queue.
- You create, list, update, and delete tasks via a REST API and see execution history.

It's cron + a message queue + an API: **cron-as-a-service**, distributed and fault-tolerant.

---

## Architecture

```
  Client
    │
    ▼
┌──────────────────────────-───────────────┐
│  API Gateway (Node.js / TypeScript)      │
│  JWT auth · CRUD · rate limiting         │
└──────────────┬─────────-─────────────────┘
               │
       ┌───────┴────────┐
       ▼                ▼
  PostgreSQL         RabbitMQ
  users              scheduler.due
  tasks              scheduler.tasks
  task_executions    scheduler.results
  workers            scheduler.tasks.dlq
  outbox             (TTL delay queues are
  scheduled_events    rollback-only)
       │
       └───────┬───────
               ▼
┌──────────────────────────────────────────┐
│  Coordinator (Python)                    │
│  · Outbox → DB timers → due queue        │
│  · Dispatch tasks to workers             │
│  · Consume results → update DB           │
│  · Slow reconciliation every 120s        │
│  · Timer poller + worker heartbeat check │
└──────────────┬───────────────────────────┘
               │
       ┌───────┼───────┐
       ▼       ▼       ▼
  worker-1  worker-2  worker-3  …  worker-10
  (Python — each runs independently,
   exposes its own metrics endpoint)
```

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| API Gateway | Node.js, Express, TypeScript |
| Coordinator | Python 3.12 |
| Workers | Python 3.12 (10 named replicas) |
| Database | PostgreSQL 15 |
| Message broker | RabbitMQ 3 |
| Metrics | Prometheus + Grafana |
| Log aggregation | Loki + Promtail |
| Alerting | Alertmanager |
| Deployment | Docker Compose on AWS EC2 |

---

## What each component does

### API Gateway

- REST API for auth (register, login with JWT) and task management (CRUD + execution history).
- Validates input and applies JWT auth on all task endpoints.
- Supports layered Redis token buckets (global ingress, login, per-user task creation) with a bounded local fallback and opt-in admission control. Defaults are off/observe. The global bucket can be sharded across `GLOBAL_INGRESS_SHARDS` keys.
- Supports opt-in short-TTL task/execution caches, a completed idempotency-response cache, and weighted `tenant_quotas` admission. PostgreSQL remains authoritative; all Phase 7 caches fail open. W3C trace headers are written into the outbox and RabbitMQ messages when `OTEL_ENABLED=true`.
- Writes task state and its outbox event in one transaction; the coordinator relay materializes the timer and publishes it when due. `OUTBOX_ENABLED=false` restores direct confirmed TTL publishing.
- Exposes `/metrics` for Prometheus.

### Coordinator

Eight responsibilities running in parallel threads:

1. **Outbox relay** — publishes committed outbox rows with confirms and per-aggregate advisory locks. Schedule/retry events are materialized into `scheduled_events` in the same transaction that marks the outbox row published.
2. **Timer poller** — selects due `scheduled_events` with `FOR UPDATE SKIP LOCKED`, publishes them to `scheduler.due` or `scheduler.tasks` with confirms and `mandatory=True`, and retries failures with backoff. Multiple coordinators can run it safely.
3. **Due consumer** — consumes `scheduler.due`. For each: locks the task row, checks `dispatch_at` against database `NOW()`, inserts exactly one `pending` `task_executions` record per `(task_id, scheduled_for)`, and publishes the full payload to `scheduler.tasks`.
4. **Result consumer** — consumes `scheduler.results`. Applies the result only when the message's `leaseId` matches the execution's current lease and the execution is still running. For recurring tasks, advances the base slot from `scheduled_for` (skipping missed slots), computes the next `dispatch_at` with stable jitter, and reschedules. Cancelled tasks are not rescheduled.
5. **Reconciliation poller** — every 120s (with ±20% jitter) scans PostgreSQL for any `scheduled` tasks with `dispatch_at <= NOW()` and nudges them to `scheduler.due`. Recovery mechanism for missed timers.
6. **Execution reaper** — every 15s recovers pending dispatches that were never claimed and running executions whose lease expired. It re-publishes the same execution for another attempt or fails it after max attempts, and deletes expired idempotency keys.
7. **Heartbeat checker** — marks workers `offline` in DB if `last_heartbeat` is stale by more than 2 minutes.
8. **Archivist** — when `ARCHIVE_ENABLED=true`, copies old terminal executions/attempts and published/failed outbox rows to archive tables in bounded transactions. Pending/running work and pending timers are never selected.

### Workers (worker-1 … worker-10)

- On startup: registers the base worker ID in the `workers` table.
- Runs `WORKER_CONCURRENCY` slots (cap 8), each with its own connection, confirm channel, and prefetch of one. Slot IDs are `${WORKER_ID}-${slot}`.
- Background thread: sends the registration heartbeat to DB every 20 seconds.
- Each slot consumes from `scheduler.tasks`, atomically claims the pending execution with a lease and attempt row, starts a lease-heartbeat thread, runs the command, and publishes a lease-guarded result to `scheduler.results`.
- Uses a bounded `psycopg2.pool.ThreadedConnectionPool`; command output is capped by `MAX_OUTPUT_BYTES`.
- On failure: updates the execution to `pending` with `next_attempt_at`, closes the attempt row, and commits a `task.retry` outbox event with an exponential ceiling plus jitter (`RETRY_JITTER_MODE`, default `full`). The timer poller republishes it when due. After `TASK_MAX_RETRIES` failures, routes to `scheduler.tasks.dlq`.
- Each worker exposes its own Prometheus metrics endpoint (ports 9101–9110) so Grafana can show per-replica utilization.

### RabbitMQ

All queues are durable with persistent messages — tasks survive broker and worker restarts. Task/execution state, the outbox, and DB-backed timers live in PostgreSQL; the broker is a delivery mechanism, not the source of truth. With a multi-node broker, `RABBITMQ_QUEUE_TYPE=quorum` declares quorum queues for the non-TTL queues.

| Queue | Purpose |
|-------|---------|
| `scheduler.due` | Due wakeups from the DB timer poller. |
| `scheduler.tasks` | Full payloads consumed by workers. |
| `scheduler.results` | Worker results consumed by coordinator. |
| `scheduler.tasks.dlq` | Poison messages after max retries. |
| `scheduler.delay` | Rollback only (`OUTBOX_ENABLED=false`): TTL wakeups. |
| `scheduler.retry_delay` | Rollback only: TTL retries. |

### Monitoring stack

- **Prometheus** scrapes API gateway, coordinator, all 10 workers, and rabbitmq-exporter every 15s.
- **Grafana** has two pre-provisioned datasources (Prometheus and Loki) and the Distributed Scheduler v2 dashboard auto-loads on startup.
- **Promtail** uses Docker socket discovery to collect logs from every container automatically and ships them to Loki with `service`, `container`, and `stream` labels.
- **Alertmanager** receives fired alerts from Prometheus. Configure Slack/email in `monitoring/alertmanager/alertmanager.yml`.

---

## Task lifecycle (full example)

1. Client sends `POST /api/v1/tasks` with JWT and an optional `Idempotency-Key`.
2. API canonicalizes the request and atomically writes the idempotency record, task (`status=scheduled`), and outbox wakeup event.
3. The coordinator relay materializes the wakeup into `scheduled_events` at `dispatch_at`. If the broker or coordinator is down, the timer stays in PostgreSQL and is retried.
4. When due, the timer poller publishes to `scheduler.due`; the coordinator locks the task row, inserts one `pending` `task_executions` row per `(task_id, scheduled_for)`, and publishes to `scheduler.tasks`.
5. One worker claims the execution (lease + attempt row), starts lease heartbeats, and runs the command.
6. Worker publishes a lease-guarded result to `scheduler.results`; on failure it commits the DB retry state and `task.retry` outbox event first, and the timer poller republishes it when due.
7. Coordinator applies the result only if the lease matches, updates `task_executions` and `tasks`, and closes the attempt. If recurring, it advances the next base slot from `scheduled_for` and reschedules.
8. If the worker dies, the reaper returns the same execution to `pending` after lease expiry. A late stale result is rejected.
9. Client calls `GET /api/v1/tasks/:id/executions` and the attempts endpoint to see the result, including `scheduled_for`, worker, and every attempt.

---

## Project structure

```
distributed-scheduler/
├── api-gateway/          Node.js/TypeScript REST API
│   └── src/
│       ├── routes/       auth.ts, tasks.ts
│       ├── middleware/   auth, errorHandler, logger
│       ├── queue.ts      RabbitMQ publish helpers
│       ├── db.ts         PostgreSQL pool
│       └── metrics.ts    Prometheus metrics
├── coordinator/          Python coordinator
│   └── internal/
│       ├── queue/        due_consumer, result_consumer,
│       │                 reconciliation, publisher, topology
│       └── registry/     worker heartbeat checker
├── worker/               Python worker
│   ├── main.py           Task consumer + execution logic
│   ├── metrics.py        Prometheus counters and histograms
│   └── rabbit_topology.py Queue/exchange declarations
├── database/
│   ├── init.sql          Bootstrap schema for a fresh database
│   └── migrations/       Versioned schema changes (scripts/migrate.py)
├── scripts/              Migration and database bootstrap tools
├── monitoring/
│   ├── prometheus.yml    Scrape config (all 10 workers listed)
│   ├── prometheus/
│   │   └── alerts.yml    26 alert rules
│   ├── grafana/
│   │   ├── provisioning/ Datasources + dashboard provider
│   │   └── dashboards/   scheduler-overview.json (v2)
│   ├── loki/             loki-config.yml
│   ├── promtail/         promtail-config.yml
│   └── alertmanager/     alertmanager.yml
├── docs/                 Architecture, demo guide, API spec, runbooks,
│                         versioned evolution, and internals diagrams
├── docker-compose.yml
└── .env.example
```
