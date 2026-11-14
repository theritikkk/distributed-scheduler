# Architecture and Design Decisions

This document explains the core design choices in the distributed scheduler — why things are built the way they are, what trade-offs were made, and how the pieces fit together. Written for engineers and interviewers reviewing the project.

---

## System Overview

The scheduler is split into independent services, all running on a single EC2 instance via Docker Compose:

| Service | Language | Role |
|---------|----------|------|
| `api-gateway` | Node.js / TypeScript | Auth + task CRUD REST API |
| `coordinator` | Python | Finds due tasks, dispatches to workers, consumes results |
| `worker-1` … `worker-10` | Python | Execute commands and report status |
| `postgres` | PostgreSQL 15 | Source of truth for tasks, executions, workers |
| `rabbitmq` | RabbitMQ 3 | Message broker between coordinator and workers |
| `redis` | Redis 7 | Shared rate-limit buckets plus optional Phase 7 read/quota caches (never authoritative) |
| `prometheus` | Prometheus | Scrapes metrics from all services |
| `grafana` | Grafana | Dashboards + Loki log queries |
| `loki` | Grafana Loki | Log aggregation (7-day retention) |
| `promtail` | Grafana Promtail | Collects container logs via Docker socket |
| `alertmanager` | Prometheus Alertmanager | Alert routing |
| `rabbitmq-exporter` | kbudde/rabbitmq-exporter | Exposes RabbitMQ queue metrics to Prometheus |

---

## Scheduling Design: DB Timers + Reconciliation

### Why not just poll the raw task table every few seconds?

Scanning `tasks` for due work is simple but burns database capacity continuously and ties latency to the poll interval. The scheduler instead keeps a compact, indexed timer table and only publishes rows that are actually due.

### Primary path: PostgreSQL scheduled events

When a task is created or rescheduled, the API computes two times: `next_execution_time` (the durable base cron slot or requested one-time time) and `dispatch_at` (the base time plus an optional stable per-task jitter offset). It commits the task state and a small `task.scheduled` outbox row in one PostgreSQL transaction.

The outbox relay converts that event into `scheduled_events` (`kind=wakeup`, `available_at=dispatch_at`) in the same transaction that marks the outbox row published. A dedicated poller selects due timers with `FOR UPDATE SKIP LOCKED`, publishes to `scheduler.due` with confirms and `mandatory=True`, and marks them published. A crash after publish but before commit can duplicate a wakeup; the dispatch path is idempotent.

The coordinator consumes `scheduler.due`, locks the task row (`SELECT FOR UPDATE`), compares `dispatch_at` with the database clock, inserts exactly one `pending` `task_executions` row per `(task_id, scheduled_for)`, and publishes the full work payload to `scheduler.tasks`.

```
Task created / rescheduled
     │
     ▼
outbox (task.scheduled) ── relay ──▶ scheduled_events (kind=wakeup)
                                         │
                                         │ available_at <= NOW()
                                         ▼
                                  scheduled-event poller
                                         │ confirmed publish
                                         ▼
                                   scheduler.due
                                         │
                                         ▼
Coordinator → SELECT ... FOR UPDATE
     │          → ON CONFLICT DO NOTHING (slot + one active execution)
     │          → publish to scheduler.tasks
     ▼
Worker claims execution (lease + attempt) → executes
     │
     ├─ success/final failure → lease-guarded result outbox event
     ├─ retry → DB pending + next_attempt_at + retry outbox event
     └─ crash → reaper requeues same execution after lease expiry
     ▼
Coordinator applies result if lease matches; recurring next slot advances
```

### Safety net: slow reconciliation

Brokers restart, consumers disconnect, and a poller can fall behind. A background poller in the coordinator (default every 120s, `SCHEDULER_RECONCILE_INTERVAL_SEC`) also scans PostgreSQL for `scheduled` tasks already due and nudges them to `scheduler.due`. This is intentionally infrequent — it is a recovery mechanism, not the primary path. The reaper separately recovers pending retries after `PENDING_RETRY_GRACE_SEC`.

### Trade-offs

| | DB-backed timers | Classic per-message TTL | Raw task polling |
|---|---|---|---|
| Source of truth | PostgreSQL, inspectable | Broker queue contents | PostgreSQL |
| Broker HA | Compatible with quorum queues | TTL queues cannot be quorum | Compatible |
| Extra DB work | One indexed poller query per interval | None | Repeated scans of the task table |
| Latency | Bounded by poll interval (default 1s) | Near-zero for the head message | Tied to scan interval |
| Recovery | Re-poll / reconciliation / reaper | Reconciliation (head-of-line TTL can delay) | Next scan |

The classic TTL queues remain available only for the `OUTBOX_ENABLED=false` rollback. `RABBITMQ_QUEUE_TYPE=quorum` is rejected unless both the outbox and DB timers are enabled.

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
| `scheduler.due` | Due wakeups from the DB timer poller (or reconciliation). |
| `scheduler.tasks` | Full task payloads consumed by workers. |
| `scheduler.results` | Worker results consumed by coordinator to update DB. |
| `scheduler.tasks.dlq` | Poison messages after `TASK_MAX_RETRIES` exhausted. |
| `scheduler.delay` | **Rollback only** (`OUTBOX_ENABLED=false`): per-message TTL wakeups. Not declared in quorum mode. |
| `scheduler.retry_delay` | **Rollback only**: per-message TTL retries. Not declared in quorum mode. |

---

## Retry Chain and DLQ

On failure, the worker updates the same execution to `pending`, clears the lease, sets `next_attempt_at = NOW() + backoff`, closes the current attempt row, and inserts a `task.retry` outbox row in the same transaction. The relay materializes it as a `scheduled_events` row (`kind=retry`, `available_at=retryAt`). The timer poller publishes the stored worker message to `scheduler.tasks` when due; no per-message TTL is involved. If a duplicate task message arrives before `next_attempt_at`, the worker acks it because the retry is already durable in PostgreSQL.

After `TASK_MAX_RETRIES` (default 3) failures, the worker commits `task.dead_letter` and `task.result` outbox rows in one transaction before acking. The relay routes the former to `scheduler.tasks.dlq` via `scheduler.dlx` and the latter to `scheduler.results`. The durable attempt count in PostgreSQL is authoritative. This prevents infinite retry loops and isolates poison messages for inspection.

```
scheduler.tasks
      │
      ▼ failure
execution pending + next_attempt_at + task.retry outbox
      │
      ▼ relay
scheduled_events (kind=retry)
      │
      │ poller at available_at
      ▼
scheduler.tasks  (retry attempt)
      │
      │ retries exhausted
      ▼
scheduler.tasks.dlq  +  failed result → coordinator → DB
```

The direct `scheduler.retry_delay` TTL path is used only when `OUTBOX_ENABLED=false`.

---

## Idempotency and Leases

At-least-once delivery means the same message can be delivered more than once (after broker restart, nack, etc.). The system handles this at several layers:

**API idempotency**: `POST /tasks` accepts an optional `Idempotency-Key`. The API canonicalizes the request body (sorted object keys), hashes it with SHA-256, and atomically claims `(user_id, key)` in `idempotency_keys` inside the same transaction as the task insert and stored response. A concurrent replay blocks on the unique key, then returns the stored `201` response without creating another task. A different body with the same key returns `409`. The coordinator deletes expired keys.

**Dispatch**: the coordinator inserts an execution with `ON CONFLICT DO NOTHING`, backed by unique `uq_task_executions_slot` on `(task_id, scheduled_for)` and `uq_task_executions_active` on `task_id` for `pending`/`running`. Duplicate wakeups cannot create a second execution or a second active attempt.

**Worker claim**: the worker atomically changes `pending → running` only when the retry deadline has passed, sets `worker_id`, a fresh `lease_id`, `lease_expires_at`, and increments `attempt`. It inserts a `task_execution_attempts` row in the same transaction. A duplicate delivery finds the execution already running/completed and acks without executing.

**Lease heartbeat and result guard**: a heartbeat thread extends `lease_expires_at` every 20s while the command runs. Results include `executionId`, `leaseId`, `workerId`, and `attempt`; the coordinator applies them only with the matching `lease_id`, `status = 'running'`, and `completed_at IS NULL`. A stale worker cannot overwrite a newer attempt.

**Reaper**: every 15s the coordinator recovers executions whose lease expired or whose pending dispatch was never claimed. It closes the failed attempt, returns the same execution to `pending`, and republishes it. After `max_attempts` it marks the execution and task failed. Pending retries with a future `next_attempt_at` are not reaped. Recurring rescheduling remains guarded by `status <> 'cancelled'`, so a late result cannot undo cancellation.

The execution-history API exposes the attempt rows, and `task_executions.worker_id`/`attempt` identify the current owner. State changes that produce work are committed with an outbox row, so a crash between the database commit and RabbitMQ publish cannot strand a task. The reaper remains the recovery path for expired leases and lost control-plane wakeups.

---

## Transactional Outbox

PostgreSQL is the source of truth. Every state change that has a downstream RabbitMQ event writes an `outbox` row in the same transaction:

| Producer | Event | Destination |
|----------|-------|-------------|
| API create/update | `task.scheduled` | `scheduled_events` (wakeup) → poller → `scheduler.due` |
| API cancel | `task.cancelled` | cancels pending `scheduled_events`; due consumer also skips the task |
| Due consumer | `task.dispatch` | `scheduler.tasks` |
| Coordinator result consumer | `task.scheduled` | `scheduled_events` (wakeup) → poller → `scheduler.due` |
| Reaper | `task.dispatch` | `scheduler.tasks` |
| Worker retry | `task.retry` | `scheduled_events` (retry) → poller → `scheduler.tasks` |
| Worker final result | `task.result` | `scheduler.results` |
| Worker max-retry failure | `task.dead_letter` | `scheduler.dlx` → `scheduler.tasks.dlq` |

The coordinator runs one relay by default. It selects `pending` rows with `FOR UPDATE SKIP LOCKED`, claims each aggregate with `pg_try_advisory_xact_lock`, publishes with publisher confirms and `mandatory=True`, and marks the row published only after confirmation. A crash after publish but before the status commit duplicates the event; consumers are idempotent. Publish failures use full-jitter exponential backoff and eventually move to `failed` for alerting; broker connection failures leave rows pending without burning attempts. Published rows are deleted after seven days by default.

Scheduled and retry events are materialized into `scheduled_events` inside the relay transaction instead of being published as TTL messages. The timer poller uses the same confirm and `mandatory=True` contract. If an early duplicate wakeup arrives, the due consumer leaves the authoritative DB timer in place. Reconciliation remains a direct publisher-confirm operation and is the final safety net.

Set `OUTBOX_ENABLED=false` in all services to roll back to direct publisher-confirm delivery. The table and pending rows are retained; do not delete unpublished rows during a rollback. Also set `SCHEDULED_EVENTS_ENABLED=false`; the timer table is retained and reconciliation/reaper recover affected tasks.

---

## Worker Design: Named Replicas

The ten workers (`worker-1` … `worker-10`) are declared as separate named services in `docker-compose.yml` rather than using `deploy.replicas`. This was a deliberate choice for observability:

- Each worker has a unique `WORKER_ID` environment variable and a unique host port (`9101`–`9110`).
- Prometheus can scrape each worker independently at its own target, giving per-worker metrics in Grafana.
- With `deploy.replicas`, all replicas share a single DNS name and Prometheus would only reach one of them round-robin.

Each worker registers its base ID in the `workers` table on startup and sends a heartbeat every 20 seconds. The coordinator marks workers `offline` if their heartbeat is stale by more than 2 minutes. Worker registration heartbeats are separate from execution leases: a worker can be healthy but still lose a specific execution lease if its command outlives the lease heartbeat.

Each worker process runs up to eight consumer slots (`WORKER_CONCURRENCY`, default 1). A slot owns its own RabbitMQ confirm connection, channel, and `prefetch_count=1`, and uses the slot identity `${WORKER_ID}-${slot}` in claims, results, and metrics. DB access uses a bounded `psycopg2.pool.ThreadedConnectionPool`; command output is capped by `MAX_OUTPUT_BYTES`. A SIGTERM stops consumers through Pika's thread-safe callback API so in-flight callbacks can finish or be redelivered.

---

## Observability Design

### Metrics

Prometheus scrapes four jobs:

- `api-gateway:3000/metrics` — HTTP request rates, latency
- `coordinator:9090/metrics` — dispatch rates, reconciliation nudges, results applied, stale-lease rejections, executions reaped, idempotency keys cleaned, workers marked offline, applied jitter, schedule lag, dispatch delay, recurring drift, slot conflicts, queue depth, backpressure state, and consumer reconnects
- `api-gateway:3000/metrics` — HTTP rates/latency, rate-limit decisions, fallback/circuit state, admission rejections, and DB pool utilization/waiters
- `worker-1:9100` … `worker-10:9100` — per-worker task starts, completions, acks, duration histogram, retry backoff, lease heartbeat failures/losses, DLQ count
- `rabbitmq-exporter:9419` — queue depths and message rates per queue

The worker metrics use a `worker_id` label so Grafana can break down utilization per replica.

### Logs

Promtail uses Docker socket discovery to collect logs from every container automatically — no per-service config. Each log line is tagged with `service`, `container`, and `stream` labels and shipped to Loki. Grafana's Explore tab supports LogQL queries for debugging and correlating metrics with logs.

### Alerts

The 26 alert rules in `monitoring/prometheus/alerts.yml` cover target health, queue backlog, DLQ growth, worker consumer down, ack rate zero, high retry rate, high schedule lag, duplicate slot conflicts, recurring drift, lease reaping, stale-lease rejection, outbox backlog/lag/failures, DB timer lag/failures/backlog, rate-limit fallback/circuit state, admission rejections, DB saturation, coordinator backpressure pauses/stale signals, consumer reconnects, and due-queue backlog.

---

## Security Model

- PostgreSQL and RabbitMQ AMQP port (5672) are not exposed to the host — `expose:` rather than `ports:`. Only the RabbitMQ management UI (15672) is optionally accessible.
- Loki (3100) is internal only.
- All secrets are loaded from `.env` via `env_file` — nothing is hardcoded in `docker-compose.yml`.
- API, coordinator, and worker configuration is validated at startup. Production requires explicit `DATABASE_URL`, `RABBITMQ_URL`, and a non-default `JWT_SECRET` of at least 32 characters.
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
| Worker crashes mid-execution | Lease stops heartbeating. After expiry, the reaper returns the same execution to `pending` with a new attempt; no second execution is created. The stale result, if it later arrives, is rejected by lease mismatch. |
| Broker restart | Durable queues and persistent messages preserve in-flight tasks. Future wakeups/retries are in PostgreSQL `scheduled_events`, so the timer poller and outbox relay redrain them; in quorum mode a single broker loss is tolerated. |
| Coordinator restart | The outbox relay, timer poller, result consumer, and reaper resume from PostgreSQL. Pending timers remain scheduled; nothing depends on coordinator memory. |
| Poison payload | Retried with exponential backoff, then isolated to DLQ after max retries. Coordinator marks execution as `failed`. |
| Worker heartbeat stops | Coordinator marks worker `offline` after 2 minutes. Any execution lease is recovered independently by the reaper. |
| Stale worker result | Result rejected because its `lease_id` no longer matches the active attempt. |

---

## What This Project Demonstrates

- Event-driven distributed orchestration
- Hybrid delay scheduling: PostgreSQL `scheduled_events` timers plus RabbitMQ quorum queues
- Idempotent message processing under at-least-once delivery
- Retry pipelines with exponential backoff and DLQ isolation
- Transactional outbox, leases, and execution-slot uniqueness
- Reconciliation-based self-healing
- Worker lifecycle tracking and heartbeat monitoring
- Per-replica metrics and independent Prometheus scraping
- Full observability stack: metrics, structured logs, alerting
- Production deployment on AWS EC2 with Docker Compose
