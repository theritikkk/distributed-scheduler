# Architecture and Design Decisions

This document explains the core design choices in the distributed scheduler — why things are built the way they are, what trade-offs were made, and how the pieces fit together. Written for engineers and interviewers reviewing the project.

---

## System Overview

The scheduler is split into independent services, all running on a single EC2 instance via Docker Compose:

| Service | Language | Role |
|---------|----------|------|
| `api-gateway` | Node.js / TypeScript | Auth + task CRUD REST API |
| `coordinator` | Python | Finds due tasks, dispatches to workers, consumes results |
| `worker-1`, `worker-2`, `worker-3` | Python | Execute commands and report status |
| `postgres` | PostgreSQL 15 | Source of truth for tasks, executions, workers |
| `rabbitmq` | RabbitMQ 3 | Message broker between coordinator and workers |
| `prometheus` | Prometheus | Scrapes metrics from all services |
| `grafana` | Grafana | Dashboards + Loki log queries |
| `loki` | Grafana Loki | Log aggregation (7-day retention) |
| `promtail` | Grafana Promtail | Collects container logs via Docker socket |
| `alertmanager` | Prometheus Alertmanager | Alert routing |
| `rabbitmq-exporter` | kbudde/rabbitmq-exporter | Exposes RabbitMQ queue metrics to Prometheus |

---

## Scheduling Design: TTL + Dead-letter + Reconciliation

### Why not just poll the database every few seconds?

Polling works but burns database capacity continuously and adds baseline latency tied to the poll interval. At scale it becomes a bottleneck.

### Primary path: TTL-based scheduling

When a task is created or rescheduled, the API publishes a small wakeup message to `scheduler.delay` with a **per-message TTL** equal to the milliseconds until `next_execution_time`. When the TTL expires, RabbitMQ automatically dead-letters the message into `scheduler.due`.

The coordinator consumes `scheduler.due`, locks the task row (`SELECT FOR UPDATE`), inserts a `task_executions` record, and publishes the full work payload to `scheduler.tasks`.

```
Task created
     │
     ▼
scheduler.delay  (TTL = time until next_execution_time)
     │
     │  TTL expires → RabbitMQ dead-letters
     ▼
scheduler.due
     │
     ▼
Coordinator → creates task_execution → publishes to scheduler.tasks
     │
     ▼
Worker executes → publishes to scheduler.results
     │
     ▼
Coordinator updates DB, reschedules if recurring
```

### Safety net: slow reconciliation

Brokers restart, TTL edge cases happen. A background poller in the coordinator (default every 120s, `SCHEDULER_RECONCILE_INTERVAL_SEC`) scans PostgreSQL for any `scheduled` tasks already due and nudges them to `scheduler.due`. This is intentionally infrequent — it is a recovery mechanism, not the primary path.

### Trade-offs

| | TTL-based | Pure polling |
|---|---|---|
| DB load | Low — no hot loop | High — queries every few seconds |
| Complexity | Higher — queue topology required | Lower |
| Recovery | Needs reconciliation fallback | Built-in (next poll catches it) |
| Latency | Near-zero (TTL expires exactly on time) | Tied to poll interval |

---

## Why RabbitMQ?

Using a queue between coordinator and workers instead of direct calls gives:

- **Reliability** — durable queues and persistent messages mean tasks survive worker crashes and broker restarts.
- **Decoupling** — coordinator does not know or care which worker runs a task.
- **Scalability** — multiple workers consume in parallel from the same queue; adding a worker requires no code changes.
- **Back-pressure** — the queue absorbs bursts while workers catch up at their own pace.

RabbitMQ specifically was chosen for its mature AMQP support, dead-letter exchange (DLX) mechanism (used for TTL scheduling and DLQ routing), and excellent management UI for debugging.

### Queue topology

| Queue | Purpose |
|-------|---------|
| `scheduler.delay` | Wakeup messages with per-message TTL. Dead-letters into `scheduler.due`. |
| `scheduler.due` | Tasks whose TTL has expired and are ready to dispatch. |
| `scheduler.tasks` | Full task payloads consumed by workers. |
| `scheduler.retry_delay` | Failed tasks waiting for backoff TTL before retry. |
| `scheduler.results` | Worker results consumed by coordinator to update DB. |
| `scheduler.tasks.dlq` | Poison messages after `TASK_MAX_RETRIES` exhausted. |

---

## Retry Chain and DLQ

On failure, the worker republishes the same payload (same `executionId`) to `scheduler.retry_delay` with exponential backoff (base 5s, max 5min). When that TTL expires, RabbitMQ dead-letters it back into `scheduler.tasks` for another attempt.

After `TASK_MAX_RETRIES` (default 3) failures, the message is routed to `scheduler.tasks.dlq` via `scheduler.dlx` and a final `failed` result is published to `scheduler.results`. This prevents infinite retry loops and isolates poison messages for inspection.

```
scheduler.tasks
      │
      ▼ failure
scheduler.retry_delay  (TTL = exponential backoff)
      │
      │ TTL expires
      ▼
scheduler.tasks  (retry attempt)
      │
      │ retries exhausted
      ▼
scheduler.tasks.dlq  +  failed result → coordinator → DB
```

---

## Idempotency

At-least-once delivery means the same message can be delivered more than once (after broker restart, nack, etc.). The system handles this at two points:

**Worker**: before executing, checks `task_executions.completed_at` for that `executionId`. If already set, it acks without re-running.

**Result consumer**: updates `task_executions` only where `completed_at IS NULL`, so duplicate result messages do not double-apply state or double-advance recurring schedules.

---

## Worker Design: Named Replicas

The three workers (`worker-1`, `worker-2`, `worker-3`) are declared as separate named services in `docker-compose.yml` rather than using `deploy.replicas`. This was a deliberate choice for observability:

- Each worker has a unique `WORKER_ID` environment variable and a unique host port (`9101`, `9102`, `9103`).
- Prometheus can scrape each worker independently at its own target, giving per-worker metrics in Grafana.
- With `deploy.replicas`, all replicas share a single DNS name and Prometheus would only reach one of them round-robin.

Each worker registers itself in the `workers` table on startup and sends a heartbeat every 20 seconds. The coordinator marks workers `offline` if their heartbeat is stale by more than 2 minutes.

---

## Observability Design

### Metrics

Prometheus scrapes four jobs:

- `api-gateway:3000/metrics` — HTTP request rates, latency
- `coordinator:9090/metrics` — dispatch rates, reconciliation nudges, results applied, workers marked offline
- `worker-1:9100`, `worker-2:9100`, `worker-3:9100` — per-worker task starts, completions, acks, duration histogram, DLQ count
- `rabbitmq-exporter:9419` — queue depths and message rates per queue

The worker metrics use a `worker_id` label so Grafana can break down utilization per replica.

### Logs

Promtail uses Docker socket discovery to collect logs from every container automatically — no per-service config. Each log line is tagged with `service`, `container`, and `stream` labels and shipped to Loki. Grafana's Explore tab supports LogQL queries for debugging and correlating metrics with logs.

### Alerts

Six alert rules in `monitoring/prometheus/alerts.yml` cover: queue backlog, DLQ growth, worker consumer down, ack rate zero, high retry rate, and DLQ growth rate.

---

## Security Model

- PostgreSQL and RabbitMQ AMQP port (5672) are not exposed to the host — `expose:` rather than `ports:`. Only the RabbitMQ management UI (15672) is optionally accessible.
- Loki (3100) is internal only.
- All secrets are loaded from `.env` via `env_file` — nothing is hardcoded in `docker-compose.yml`.
- JWT authentication on all task endpoints.
- Grafana has login enabled by default (`GF_USERS_ALLOW_SIGN_UP: false`).

---

## Command Execution Trade-offs

Workers execute the `command` string from `command_payload` as a shell subprocess. This is flexible for demonstration but has implications:

**Pros**: easy to show end-to-end execution with any shell command, no pre-registration of task types required.

**Cons**: security risk if untrusted payloads are accepted. Production hardening would add an allowlist of permitted commands, sandboxing (e.g. container-per-task), and resource limits.

---

## Failure Scenarios

| Failure | What happens |
|---------|-------------|
| Worker crashes mid-execution | RabbitMQ re-delivers unacked message to another worker. Idempotency check prevents double execution if the first run completed. |
| Broker restart | Durable queues + persistent messages preserve all in-flight tasks. Reconciliation recovers any missed TTL wakeups. |
| Coordinator restart | Slow reconciliation scan on startup catches any tasks that became due while coordinator was down. |
| Poison payload | Retried with exponential backoff, then isolated to DLQ after max retries. Coordinator marks execution as `failed`. |
| Worker heartbeat stops | Coordinator marks worker `offline` after 2 minutes. |

---

## What This Project Demonstrates

- Event-driven distributed orchestration
- Delayed queue scheduling using RabbitMQ per-message TTL and DLX
- Idempotent message processing under at-least-once delivery
- Retry pipelines with exponential backoff and DLQ isolation
- Reconciliation-based self-healing
- Worker lifecycle tracking and heartbeat monitoring
- Per-replica metrics and independent Prometheus scraping
- Full observability stack: metrics, structured logs, alerting
- Production deployment on AWS EC2 with Docker Compose
