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
  users              scheduler.delay
  tasks              scheduler.due
  task_executions    scheduler.tasks
  workers            scheduler.results
       │              scheduler.retry_delay
       └───────┬───────scheduler.tasks.dlq
               ▼
┌──────────────────────────────────────────┐
│  Coordinator (Python)                    │
│  · TTL wakeups → due queue               │
│  · Dispatch tasks to workers             │
│  · Consume results → update DB           │
│  · Slow reconciliation every 120s        │
│  · Worker heartbeat checker              │
└──────────────┬───────────────────────────┘
               │
       ┌───────┼───────┐
       ▼       ▼       ▼
  worker-1  worker-2  worker-3
  (Python — each runs independently,
   exposes its own metrics endpoint)
```

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| API Gateway | Node.js, Express, TypeScript |
| Coordinator | Python 3.12 |
| Workers | Python 3.12 (3 named replicas) |
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
- Validates input, enforces rate limits (10,000 req/15min), and applies JWT auth on all task endpoints.
- On task create/update, publishes a TTL wakeup message to `scheduler.delay` in RabbitMQ.
- Exposes `/metrics` for Prometheus.

### Coordinator

Three responsibilities running in parallel threads:

1. **Due consumer** — consumes `scheduler.due` (messages whose TTL has expired). For each: locks the task row, inserts a `task_executions` record, publishes the full payload to `scheduler.tasks`.
2. **Result consumer** — consumes `scheduler.results`. Updates `task_executions` with status/output/error. For recurring tasks, computes next run time via cron expression and reschedules.
3. **Reconciliation poller** — every 120s scans PostgreSQL for any `scheduled` tasks already past due and nudges them to `scheduler.due`. Recovery mechanism for missed TTL wakeups.
4. **Heartbeat checker** — marks workers `offline` in DB if `last_heartbeat` is stale by more than 2 minutes.

### Workers (worker-1, worker-2, worker-3)

- On startup: registers in the `workers` table.
- Background thread: sends heartbeat to DB every 20 seconds.
- Main loop: consumes from `scheduler.tasks`, runs the shell command from `command_payload`, publishes result to `scheduler.results`.
- On failure: republishes to `scheduler.retry_delay` with exponential backoff. After `TASK_MAX_RETRIES` failures, routes to `scheduler.tasks.dlq`.
- Each worker exposes its own Prometheus metrics endpoint (ports 9101/9102/9103) so Grafana can show per-replica utilization.

### RabbitMQ

All queues are durable with persistent messages — tasks survive broker and worker restarts.

| Queue | Purpose |
|-------|---------|
| `scheduler.delay` | Wakeup messages with TTL. Dead-letters into `scheduler.due`. |
| `scheduler.due` | Ready-to-dispatch tasks. |
| `scheduler.tasks` | Full payloads consumed by workers. |
| `scheduler.retry_delay` | Failed tasks awaiting backoff TTL. |
| `scheduler.results` | Worker results consumed by coordinator. |
| `scheduler.tasks.dlq` | Poison messages after max retries. |

### Monitoring stack

- **Prometheus** scrapes API gateway, coordinator, all 3 workers, and rabbitmq-exporter every 15s.
- **Grafana** has two pre-provisioned datasources (Prometheus and Loki) and the Distributed Scheduler v2 dashboard auto-loads on startup.
- **Promtail** uses Docker socket discovery to collect logs from every container automatically and ships them to Loki with `service`, `container`, and `stream` labels.
- **Alertmanager** receives fired alerts from Prometheus. Configure Slack/email in `monitoring/alertmanager/alertmanager.yml`.

---

## Task lifecycle (full example)

1. Client sends `POST /api/v1/tasks` with JWT.
2. API inserts task into PostgreSQL (`status=scheduled`, `next_execution_time=T`).
3. API publishes wakeup to `scheduler.delay` with TTL = T - now.
4. At time T, RabbitMQ dead-letters the wakeup into `scheduler.due`.
5. Coordinator consumes it, locks task row, inserts `task_executions`, publishes to `scheduler.tasks`.
6. One worker picks up the message, checks idempotency, runs the command.
7. Worker publishes result to `scheduler.results`.
8. Coordinator updates `task_executions` and `tasks`. If recurring, computes next run and reschedules.
9. Client calls `GET /api/v1/tasks/:id/executions` to see the result.

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
│   └── init.sql          Schema, indexes
├── monitoring/
│   ├── prometheus.yml    Scrape config (all 3 workers listed)
│   ├── prometheus/
│   │   └── alerts.yml    6 alert rules
│   ├── grafana/
│   │   ├── provisioning/ Datasources + dashboard provider
│   │   └── dashboards/   scheduler-overview.json (v2)
│   ├── loki/             loki-config.yml
│   ├── promtail/         promtail-config.yml
│   └── alertmanager/     alertmanager.yml
├── docs/                 Architecture, demo guide, API spec
├── docker-compose.yml
└── .env.example
```
