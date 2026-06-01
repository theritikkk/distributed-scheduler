# Benchmark Results

Environment running these results:

| Resource | Value |
|----------|-------|
| Instance | AWS EC2, Ubuntu 22.04 |
| Workers | 3 replicas (worker-1, worker-2, worker-3) |
| PostgreSQL | 15, single instance |
| RabbitMQ | 3, durable queues |
| Scheduling method | TTL-based dead-letter + slow reconciliation |

---

## Task Processing

| Metric | Value |
|--------|-------|
| Tasks submitted | 20+ scheduled tasks |
| Task types | One-time and recurring (cron `* * * * *`) |
| Workers active | 3 |
| Queue depth monitored via | Prometheus + Grafana |
| p95 latency tracked via | `scheduler_worker_task_duration_seconds_bucket` histogram |

---

## Reliability Tested

| Scenario | Result |
|----------|--------|
| Worker crash (`docker stop worker-1`) | Tasks automatically re-routed to worker-2 and worker-3. worker-1 recovered on restart with no lost tasks. |
| Poison command (invalid shell command) | Retried 3× with exponential backoff, then isolated to `scheduler.tasks.dlq`. No infinite loop. |
| Broker restart | Durable queues preserved all in-flight messages. Reconciliation scan recovered missed TTL wakeups on coordinator restart. |
| Duplicate message delivery | Idempotency check via `executionId` prevented double execution. |

---

## Observability Metrics Captured

All metrics scraped by Prometheus every 15s:

| Metric | Description |
|--------|-------------|
| `scheduler_worker_consumer_up` | Per-worker liveness (0 or 1) |
| `scheduler_worker_tasks_started_total` | Tasks picked up from queue |
| `scheduler_worker_tasks_completed_total` | Tasks completed successfully |
| `scheduler_worker_tasks_acked_total` | Tasks acked back to broker |
| `scheduler_worker_task_duration_seconds_bucket` | Execution latency histogram (p95 via Grafana) |
| `scheduler_coordinator_due_dispatched_total` | Tasks dispatched by coordinator |
| `scheduler_coordinator_results_applied_total` | Results applied to DB |
| `scheduler_coordinator_reconciliation_nudged_total` | Tasks recovered by reconciliation |
| `scheduler_coordinator_workers_marked_offline_total` | Workers marked offline via heartbeat |
| `scheduler_rabbitmq_queue_messages` | Per-queue depth (all 6 queues) |
| `rabbitmq_connections` | Active AMQP connections |

---

## Suggested Load Test (future)

To generate a stronger benchmark number for your resume, run this load test and capture the results:

```bash
# Install k6
sudo apt install k6

# Submit 1000 tasks as fast as possible
k6 run tests/production-test.example.js
```

Target resume bullet:
> "Load-tested with 1,000+ tasks across 3 worker replicas, monitored throughput, queue depth, and p95 latency via Prometheus and Grafana."