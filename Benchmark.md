# Benchmark Results

## Current baseline environment (2026-10-31)

| Resource | Value |
|----------|-------|
| Host | Local macOS development machine |
| API | Node.js 22, `api-gateway` built from source |
| Database | Local PostgreSQL 15 |
| Broker | RabbitMQ 3 in Docker (full-stack run only) |
| Workers | 1 local worker (full-stack run only) |
| Scheduling method | TTL-based dead-letter + reconciliation |

The API-only and full-stack runs below are the Phase 0 baselines. Earlier
reliability results are kept at the bottom and are marked as legacy where they
were not re-run in this environment.

## Phase 1 correctness checks (2026-11-02)

Phase 1 is validated by deterministic unit/integration tests rather than a new
throughput claim:

- 10,000 same-slot task IDs produce offsets covering the full `0..30s` window.
- TypeScript and Python stable-offset implementations return the same values.
- Duplicate due wakeups create one execution per `(task_id, scheduled_for)`.
- A future `dispatch_at` is re-published without creating an execution.
- Recurring next-run calculation advances from `scheduled_for`, not `now()`;
  missed slots are skipped.
- A late result does not undo cancellation.
- Migration validation covers fresh bootstrap, simulated old-schema upgrade, backfill,
  unique slot creation, and rollback.

Manual PostgreSQL 15 migration run:

```bash
# fresh database
DATABASE_URL=postgresql:///scheduler_phase1_test python3 scripts/migrate.py bootstrap

# simulated existing database: baseline, stop before contract, then finish
DATABASE_URL=postgresql:///scheduler_upgrade_test python3 scripts/migrate.py baseline
DATABASE_URL=postgresql:///scheduler_upgrade_test python3 scripts/migrate.py up --to 0004
DATABASE_URL=postgresql:///scheduler_upgrade_test python3 scripts/migrate.py up

# rollback the four Phase 1 migrations in reverse order
for _ in 1 2 3 4; do
  DATABASE_URL=postgresql:///scheduler_upgrade_test python3 scripts/migrate.py rollback
done
```

## Phase 2 correctness checks (2026-11-04)

Phase 2 is also validated by deterministic integration tests rather than a new
throughput claim:

- Concurrent requests with the same `Idempotency-Key` create one task and replay
  the stored `201`; a different body returns `409`.
- A failed task insert rolls back the idempotency key claim, so a retry succeeds.
- A duplicate result with the current lease applies once; an old lease is rejected.
- Expired leases and stale pending dispatches are reaped into a new attempt, while a
  retry with a future `next_attempt_at` is left alone.
- Cancelled tasks are not rescheduled by late results.
- Worker claims reject running, completed, cancelled, and not-yet-due executions.
- Attempt history records every claim, retry, completion, and reaper failure.

Manual PostgreSQL 15 migration runs covered fresh bootstrap, a simulated Phase 1
upgrade through `0008`, `0008` rollback/reapply, and the duplicate-active preflight
failure path. An end-to-end local run also verified a successful task, an idempotent
replay, a `409` body conflict, a two-attempt retry, and attempt history through the
HTTP API.

The Phase 0 API and full-stack numbers remain the latest throughput baselines
for this branch.

---

## API submission baseline (Phase 0)

A local API-only baseline captured before scheduling/concurrency changes:

| Resource | Value |
|----------|-------|
| Date | 2026-10-31 |
| Host | Local macOS development machine |
| API | Node.js 22, `api-gateway` built from source |
| Database | Local PostgreSQL 15 |
| Broker | RabbitMQ not running; wakeup publish was a no-op |
| Rate limiter | Raised to 1,000,000 requests/window to measure request handling |

Command:

```bash
BASE_URL=http://localhost:3100 TOKEN=<jwt> \
  k6 run --summary-trend-stats 'avg,min,med,p(90),p(95),p(99),max' \
  tests/production-test.example.js
```

Results (40s, 200 VUs, create-task requests):

| Metric | Value |
|--------|-------|
| Requests | 29,592 (earlier run: 32,350) |
| Throughput | ~740 req/s (earlier run: ~808 req/s) |
| Failed requests | 0.00% |
| p95 latency | 84.5 ms (earlier run: 58.2 ms) |
| p99 latency | 163.2 ms |
| Max latency | 299.0 ms |

The first Phase 0 run with the legacy 10,000 req/15min in-process limiter produced exactly 9,999 successes and then 429s. That baseline motivated the Phase 4 shared limiter, which is now opt-in and off by default. These numbers are not a distributed-processing benchmark because no workers consumed the generated tasks.

---

## Full-stack baseline (Phase 0)

One API process, one coordinator, one worker, local PostgreSQL, and RabbitMQ in
Docker. Tasks were scheduled 5 seconds ahead so the coordinator and worker had to
dispatch and execute them during the run.

```bash
# API, coordinator, and worker started locally with TEST_DATABASE_URL and
# RABBITMQ_URL pointing at the local services.
BASE_URL=http://localhost:3100 TOKEN=<jwt> VUS=10 DURATION=15s DELAY_MS=5000 \
  k6 run tests/load/full-stack.example.js
```

| Metric | Value |
|--------|-------|
| Submitted tasks | 1,362 |
| Submission throughput | 90.2 req/s (10 VUs for 15s) |
| API p95 / max latency | 20.6 ms / 45.8 ms |
| Failed submissions | 0.00% |
| Max observed `scheduler.tasks` depth | 97 |
| Max observed PostgreSQL connections | 14 |
| Coordinator due dispatches | 1,362 |
| Worker starts / completions | 1,362 / 1,362 |
| Worker utilization | ~68.5 tasks/s (1,362 in 19.9s) |
| Dispatch lag avg / p95 / max | 1.06 s / 2.94 s / 4.61 s |
| Failed / retried tasks | 0 / 0 |

Queue depth was sampled every second from the coordinator's RabbitMQ management
poller. Dispatch lag is `task_executions.started_at - tasks.next_execution_time`;
it includes time waiting in `scheduler.tasks` behind the single worker, so it is a
pipeline latency, not coordinator-only latency. This is a smoke baseline, not a
capacity limit; the single worker was deliberately the bottleneck.

---

## Phase 6 HA lab (2026-11-06)

The `deploy/ha-lab/` stack: three Spot `m7i-flex.large` nodes across
`ap-south-1a/b/c`, Patroni PostgreSQL 16.6 with a synchronous standby, a 3-node
RabbitMQ cluster with quorum queues, Redis master/replica + 3 Sentinels, and
per-node HAProxy. Load was submitted through node1's HAProxy `:8080`, which
rotates across three API replicas; three workers per node (9 total,
`WORKER_CONCURRENCY=2`) consumed the tasks.

```bash
# Same k6 script, pointed at the HA lab API. Tasks execute 5s after submission.
docker run --rm -i \
  -e BASE_URL=http://<node1-public-ip>:8080 \
  -e TOKEN=<jwt> -e VUS=20 -e DURATION=60s -e DELAY_MS=5000 \
  grafana/k6 run - < tests/load/full-stack.example.js
```

| Metric | Warm-up | Peak |
|--------|---------|------|
| VUs / duration | 5 / 15s | 20 / 60s |
| Submitted tasks | 454 | 7,586 |
| Submission throughput | 30.0 req/s | 126.1 req/s |
| API p95 / max latency | 60.9 ms / 79.0 ms | 69.6 ms / 672.1 ms |
| Failed submissions | 0.00% | 0.00% |
| Tasks completed | 454 | 7,586 |

Combined result across warm-up and peak: **8,040 tasks completed exactly once**,
8,040 executions, 0 retries, 0 duplicate `(task_id, scheduled_for)` slots, and
0 pending/failed outbox rows. The workers drained the peak backlog in under two
minutes after submissions stopped.

Notes:

- The API is reached through one node's HAProxy; there is no NLB. This measures
  the application and per-node proxy, not a managed load balancer.
- Rate limiting was disabled/observe for this run (the baseline configuration).
  The Redis drill separately validated 10 allowed / 5×429 at the default
  60/min + burst 10 policy.
- These are lab numbers on shared Spot instances with a single PostgreSQL
  primary; they are not a capacity guarantee. Add PgBouncer and a longer soak
  before sizing production pools.

---

## v1.6.0 local Docker E2E (2026-11-10)

300 one-time tasks, all sharing the same due time, submitted at concurrency 20
against the local Docker Compose stack with 10 worker replicas. Completion was
verified from PostgreSQL `tasks` + `task_executions`, not from the submitter.

| Metric | Value |
|--------|-------|
| Tasks submitted / completed | 300 / **300** |
| Failed submissions | **0** |
| Submission wall time | **1.839 s** |
| Submission throughput | **163.1 req/s** |
| Submit latency p50 / p95 / max | **100.97 / 354.11 / 414.73 ms** |
| Drain after shared due time | **17.411 s** |
| Executions / attempt rows | **300 / 300** |
| Execution duration avg / max (claim → complete) | **3047.28 / 5630.73 ms** |
| Worker distribution | **29–32 tasks per worker** across all 10 |

The AWS HA lab remained destroyed, so this is **local E2E evidence, not an AWS
benchmark**. Full screenshots, commands, and the machine-readable result are in
[`screenshots/deployment/test-6/`](screenshots/deployment/test-6/README.md).

---

## Legacy reliability checks

These were run in an earlier environment (10 Compose workers) and have not been
re-run on the current local baseline:

| Scenario | Result |
|----------|--------|
| Worker crash (`docker stop worker-1`) | Tasks automatically re-routed to worker-2 and worker-3. worker-1 recovered on restart with no lost tasks. |
| Poison command (invalid shell command) | Retried 3× with exponential backoff, then isolated to `scheduler.tasks.dlq`. No infinite loop. |
| Broker restart | Durable queues preserved all in-flight messages. Reconciliation scan recovered missed TTL wakeups on coordinator restart. |
| Duplicate message delivery | Idempotency check via `executionId` prevented double execution. |

---

## Observability metrics captured

All metrics scraped by Prometheus every 15s:

| Metric | Description |
|--------|-------------|
| `scheduler_worker_consumer_up` | Per-worker liveness (0 or 1) |
| `scheduler_worker_tasks_started_total` | Tasks picked up from queue |
| `scheduler_worker_tasks_completed_total` | Tasks completed successfully |
| `scheduler_worker_tasks_acked_total` | Tasks acked back to broker |
| `scheduler_worker_task_duration_seconds_bucket` | Execution latency histogram (p95 via Grafana) |
| `scheduler_coordinator_due_dispatched_total{coordinator_id=…}` | Tasks dispatched by coordinator |
| `scheduler_coordinator_results_applied_total{coordinator_id=…,status=…}` | Results applied to DB |
| `scheduler_coordinator_results_skipped_idempotent_total{coordinator_id=…}` | Duplicate results skipped |
| `scheduler_coordinator_reconciliation_nudged_total{coordinator_id=…}` | Tasks recovered by reconciliation |
| `scheduler_coordinator_workers_marked_offline_total{coordinator_id=…}` | Workers marked offline via heartbeat |
| `scheduler_rabbitmq_queue_messages{coordinator_id=…,queue=…}` | Per-queue depth (all tracked queues) |

---

## Full-stack load-test command

For a larger Compose run, use the same script with more VUs and a longer duration:

```bash
BASE_URL=http://localhost:3000 TOKEN=<jwt> VUS=200 DURATION=40s DELAY_MS=60000 \
  k6 run tests/load/full-stack.example.js
```

Target resume bullet (after running a multi-worker Compose environment):
> "Load-tested task submission across 10 worker replicas, monitoring throughput, queue depth, and p95 latency via Prometheus and Grafana."
