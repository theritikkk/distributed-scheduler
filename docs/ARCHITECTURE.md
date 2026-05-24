# Architecture and Trade-offs

This document explains the core design choices in the distributed scheduler, especially:

- why RabbitMQ is used
- why the coordinator polls the database
- what trade-offs come with these decisions

It is written for interviewers/recruiters and for engineers reviewing the project.

---

## System Overview

The scheduler is split into independent services:

- `api-gateway` (Node.js/TypeScript): auth + task CRUD APIs
- `coordinator` (Python): finds due tasks, publishes them to workers, consumes execution results
- `worker` (Python): executes commands and reports status
- `postgres`: stores users, tasks, executions, worker heartbeats
- `rabbitmq`: message broker between coordinator and workers

High-level data flow:

1. Client creates a task via API.
2. Task is stored in PostgreSQL with a `next_execution_time`.
3. Coordinator periodically polls DB for due tasks.
4. Coordinator publishes due tasks to RabbitMQ (`scheduler.tasks`).
5. Workers consume tasks, execute them, and publish results to `scheduler.results`.
6. Coordinator consumes results and updates `task_executions` + `tasks`.

---

## Why RabbitMQ?

### Why queue over direct execution?

Using a queue instead of calling workers directly gives:

- **Decoupling**: API/coordinator do not need to know which worker will run the task.
- **Reliability**: durable queues keep tasks safe if workers crash or restart.
- **Scalability**: multiple workers can consume in parallel from the same queue.
- **Back-pressure handling**: queue absorbs bursts while workers catch up.
- **Operational flexibility**: workers can be added/removed without changing API logic.

### Why RabbitMQ specifically?

- Mature, widely used AMQP broker.
- Supports durable queues and persistent messages.
- Good tooling and UI for demo/debugging (`:15672` management UI).
- Easy Docker Compose integration for local and VM deployments.

---

## Why delay queues (with lightweight reconciliation)?

### Primary path: TTL + dead-letter scheduling

When a task is created or rescheduled, the API publishes a small wakeup message to `scheduler.delay` with a **per-message TTL** equal to time-until-`next_execution_time`. When TTL expires, RabbitMQ **dead-letters** the message into `scheduler.due`.

The coordinator consumes `scheduler.due`, locks the task row, creates a `task_executions` row, and publishes the full work payload to `scheduler.tasks`.

### Why this replaces fast DB polling

- **Lower steady-state DB load**: no hot loop querying every few seconds.
- **Natural alignment with “run at time T”**: TTL expresses delay directly.
- **Still DB-backed**: PostgreSQL remains the source of truth for schedules and history.

### Why keep a slow reconciliation poller?

Brokers restart, TTL edge cases, and operational surprises happen. A **slow** reconciliation scan (default every 120 seconds) finds any `scheduled` tasks that are already due and nudges them to `scheduler.due`. This is intentionally infrequent compared to the old 5-second poller.

### Trade-offs

**Pros**
- Less aggressive polling
- Clear separation between “wake up” (`scheduler.due`) and “execute work” (`scheduler.tasks`)

**Cons**
- TTL has practical upper bounds; reconciliation covers long horizons and recovery
- Requires correct queue topology declarations across services

### Why not pure DB polling only?

Polling is easy to implement, but it burns database capacity continuously and adds baseline latency tied to poll interval. Delay-first scheduling reduces both, at the cost of more moving parts in RabbitMQ.

---

## Key Trade-offs

### 1) Queue-based execution trade-offs

**Pros**
- Resilient to worker failures
- Horizontal scaling is straightforward
- Better separation of responsibilities

**Cons**
- At-least-once delivery means tasks should be idempotent
- Additional infrastructure (RabbitMQ) to deploy/monitor
- More eventual consistency (status updates are asynchronous)

### 2) Delay + reconciliation trade-offs

**Pros**
- Lower baseline DB load than frequent polling
- Wakeups are explicit broker-managed timers

**Cons**
- More queue topology to declare consistently
- Requires reconciliation for correctness under failure

### 3) Command execution trade-offs

Current worker executes command strings from payload for demo velocity.

**Pros**
- Very flexible for demonstrations
- Easy to show end-to-end execution quickly

**Cons**
- Security risk in production if untrusted payloads are allowed
- Requires sandboxing/allowlists/resource limits for hardening

---

## Reliability Model

This system follows an **at-least-once** processing style:

- task messages are durable/persistent in RabbitMQ
- retries/re-delivery may happen after failures
- execution history is stored in `task_executions`

To make this production-grade, tasks should be idempotent and/or include deduplication keys.

---

## Failure Handling Approach

- **Worker heartbeat tracking**: coordinator marks workers offline if heartbeat is stale.
- **Execution tracking**: each run gets a `task_executions` row before publication.
- **Result-driven state updates**: coordinator updates task status based on worker result.
- **Recurring tasks**: next run time computed from cron and rescheduled.

---

## Advanced scheduling, retries, DLQ, and idempotency

### Delay queues instead of fast polling

Primary scheduling uses **per-message TTL** on `scheduler.delay` so messages **dead-letter** into `scheduler.due` when `next_execution_time` is reached. The coordinator consumes `scheduler.due`, creates a `task_executions` row under a row lock, and publishes work to `scheduler.tasks`.

A **slow reconciliation loop** (default every 120s, `SCHEDULER_RECONCILE_INTERVAL_SEC`) nudges any still-due `scheduled` tasks to `scheduler.due` to recover from missed TTL wakeups (broker restart, rare races).

### Retry chain and DLQ

On failure, the worker republishes the same payload (same `executionId`) to `scheduler.retry_delay` with exponential backoff; when TTL expires, RabbitMQ dead-letters it back to `scheduler.tasks`. After `maxRetries` (default `TASK_MAX_RETRIES=3`), the worker publishes a poison message to `scheduler.tasks.dlq` (via `scheduler.dlx`) and emits a final **failed** result to `scheduler.results`.

Poison messages are isolated into a DLQ after retries are exhausted, preventing infinite retry loops and allowing operational inspection/replay.

### Idempotency (`execution_id`)

- **Worker**: before running a command, it checks `task_executions.completed_at` for that `executionId`. If already set, it **acks** without re-running (duplicate delivery safe).
- **Result consumer**: updates `task_executions` only where `completed_at IS NULL`, so duplicate result messages do not double-apply state or double-advance recurring schedules.

### Trade-offs

- Delay TTL has practical upper bounds; reconciliation covers edge cases.
- Retry uses wall-clock backoff in the broker; very long backoffs need chunking or a DB-backed retry scheduler at huge scale.

---

## Performance/Scale Notes

Defaults are tuned for demo and moderate load:

- **Primary**: TTL-based wakeups + `scheduler.due` consumer
- **Safety net**: reconciliation scan every 120 seconds (`SCHEDULER_RECONCILE_INTERVAL_SEC`)
- **Retries**: exponential backoff via `scheduler.retry_delay` (`TASK_MAX_RETRIES`)

As load grows:

- increase worker replicas
- tune reconciliation interval and batch size
- add indexes and partitioning for execution history
- move from single VM to managed AWS services (RDS, MQ/ECS/EKS)

---

## Why this architecture is resume-worthy

This project demonstrates:
- event-driven distributed orchestration
- delayed queue scheduling using RabbitMQ TTL + DLX
- idempotent message processing under at-least-once delivery
- retry pipelines with exponential backoff and DLQ isolation
- reconciliation-based self-healing
- worker lifecycle tracking and heartbeat monitoring
- transactional coordination between PostgreSQL and RabbitMQ

---
