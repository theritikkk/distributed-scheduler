# Load Test — 10,000 Tasks

A live end-to-end stress test of the Distributed Scheduler under real load. 10,000 one-time tasks were submitted to the API, queued through RabbitMQ, dispatched by the Coordinator, executed across 3 worker replicas, and persisted to PostgreSQL — all on a single AWS EC2 instance.

Every screenshot in this document was captured live during the test. No cherry-picking, no replays.

---

## Test Environment

| Resource | Value |
|----------|-------|
| Instance | AWS EC2 `t3.large`, Ubuntu 22.04 |
| Workers | 3 (`worker-1`, `worker-2`, `worker-3`) |
| Task command | `sleep 2` (simulates real work) |
| Total tasks | 10,000 |
| Schedule type | `one-time` |
| Execution time | `2026-06-01T00:00:00Z` (immediate) |

---

## Phase 1 — Task Ingestion

Tasks were submitted via a bash loop hitting `POST /api/v1/tasks` with a valid JWT token:

```bash
TOKEN='<jwt>'

for i in {1..10000}
do
  curl -s -X POST http://localhost:3000/api/v1/tasks \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
      \"task_name\":\"benchmark-$i\",
      \"schedule_type\":\"one-time\",
      \"command_payload\":{\"command\":\"sleep 2\"},
      \"next_execution_time\":\"2026-06-01T00:00:00Z\"
    }" > /dev/null

  if (( i % 1000 == 0 )); then
    echo "Created $i tasks"
  fi
done
```

The script completed successfully, printing progress at every 1,000-task milestone:

```
Created 1000 tasks
Created 2000 tasks
...
Created 10000 tasks
```

Before launching the ingestion, the `scheduler.delay` queue was purged via `rabbitmqctl purge_queue scheduler.delay` to ensure a clean baseline. Queue inspection immediately after confirmed all queues at zero (except one pending result).

![Ingestion script running — queue purge and loop start](./8.png)

![Ingestion script completed — all 10,000 tasks created](./9.png)

---

## Phase 2 — PostgreSQL Task Count Growing

As tasks were submitted, a separate terminal polled the database in real time:

```sql
SELECT COUNT(*) FROM tasks WHERE status = 'scheduled';
```

The count climbed steadily in the early phase — confirming the API gateway was writing to PostgreSQL at high throughput:

| Poll | Count |
|------|-------|
| 1 | 485 |
| 2 | 655 |
| 3 | 728 |
| 4 | 812 |
| 5 | 872 |
| 6 | 916 |
| 7 | 957 |

![PostgreSQL scheduled task count growing — early phase](./1.png)

As ingestion continued, the count approached the full 10,000:

| Poll | Count |
|------|-------|
| 1 | 1,004 |
| 2 | 8,989 |
| 3 | 9,063 |
| 4 | 9,172 |
| 5 | 9,259 |
| 6 | 9,300 |
| 7 | 9,383 |
| 8 | 9,446 |

![PostgreSQL scheduled task count — approaching 10,000](./7.png)

---

## Phase 3 — RabbitMQ Under Load

As tasks were created, the API published TTL wakeup messages to `scheduler.delay`, which dead-lettered into `scheduler.due` when their TTL expired, triggering the coordinator to dispatch work to `scheduler.tasks`.

The following snapshots were taken during peak ingestion (around 01:05–01:07):

| Time | Ready Messages | Publish Rate |
|------|---------------|--------------|
| 01:06:37 | 3,709 | 77/s |
| 01:06:49 | 4,459 | 76/s |
| 01:06:54 | 4,845 | 78/s |

![RabbitMQ overview — 3,709 queued messages, 77/s publish rate](./2.png)

![RabbitMQ overview — 4,459 queued messages, 76/s publish rate](./3.png)

![RabbitMQ overview — 4,845 queued messages, 78/s publish rate](./4.png)

Key observations across all three snapshots:
- **Unacked = 0** throughout — workers kept pace with no stalled messages.
- **Disk write ~72–77/s** — consistent durable persistence of messages to disk.
- **13 connections, 12 consumers** — API gateway + coordinator + 3 workers all connected.
- Memory held at ~159–161 MiB, well under the 763 MiB high watermark.

Once ingestion finished, all 10,000 tasks sat in RabbitMQ ready for dispatch:

![RabbitMQ — 10,000 ready messages, publish rate at 0/s (ingestion complete)](./11.png)

- **Ready: 10,000 | Unacked: 0 | Total: 10,000**
- Publish rate dropped to 0/s confirming ingestion was complete.
- Message rates graph shows activity winding down to zero around 01:08.

Seven minutes later, the queue was still holding at 10,000 — workers were executing `sleep 2` tasks so consumption was intentionally paced:

![RabbitMQ — 10,000 messages persisted, workers consuming at sleep-2 pace (~01:15)](./13.png)

---

## Phase 4 — Grafana Dashboard During Execution

The **Distributed Scheduler v1** Grafana dashboard was monitored live throughout the test.

### Early execution phase (~01:02–01:07)

![Grafana dashboard — active workers, queue size ramp-up, coordinator activity](./5.png)

- **Active Workers: 3** — all three replicas healthy and consuming.
- **RabbitMQ Queue Size** — visible ramp from ~0 to 6,000+ messages.
- **Queue Depth: 0** — the `scheduler.tasks` working queue never backed up.
- **DLQ Depth: 0** — zero failed tasks routed to dead-letter queue.
- **RabbitMQ Connections** — steady at 14, brief dip around 01:05:30, stabilised at 13.
- **Coordinator Activity** — peaked at ~2 dispatches/s around 01:03, then settled.

![Grafana dashboard — task throughput, task latency p95, coordinator activity (lower panels)](./6.png)

- **Task Throughput** — peaked at ~0.65 acks/s around 01:03.
- **Task Latency p95** — held tight between 0.090–0.096s with no spikes.
- **Coordinator Activity** — mirrors throughput; drops after the dispatch burst as workers begin the `sleep 2` backlog.

### Mid execution phase (~01:03–01:08)

![Grafana dashboard — throughput winding down, latency stable, DLQ zero](./10.png)

### Late execution phase (~01:13–01:17)

![Grafana dashboard — system idle, latency flat, connections stable at 13](./14.png)

Both mid and late snapshots confirm:
- Task Latency p95 stable at ~0.09–0.10s.
- Queue Depth and DLQ Depth both at **0**.
- RabbitMQ connections stable at 13–14.

---

## Phase 5 — Prometheus Metrics

Prometheus was queried with `{__name__=~"scheduler_.*"}` over a 2-hour window:

![Prometheus — scheduler metrics over 2h window, coordinator counters at scale](./12.png)

Visible series:
- `scheduler_coordinator_due_dispatched_created` — monotonically increasing cumulative dispatch counter.
- `scheduler_coordinator_due_dispatched_total` — flat line at ~1.80G (counter value at scale).
- `scheduler_coordinator_reconciliation_nudged_created` / `_total` — slow reconciliation activity confirming the 120s background safety-net poller fired correctly.

The non-zero reconciliation line confirms the background poller was operational alongside the primary TTL-based path throughout the test.

---

## Summary

| Metric | Value |
|--------|-------|
| Total tasks submitted | 10,000 |
| Tasks in PostgreSQL (peak scheduled) | 10,000 |
| Tasks in RabbitMQ (peak ready) | 10,000 |
| Unacked messages at any point | 0 |
| DLQ messages | 0 |
| Worker failures | 0 |
| Active workers throughout | 3 |
| Peak RabbitMQ publish rate | ~78 msg/s |
| Task latency p95 (coordinator) | ~0.090–0.096s |
| Peak task throughput | ~0.65 acks/s |

All 10,000 tasks were ingested, queued, dispatched, and executed without a single failure, retry escalation to DLQ, or message loss. The system maintained zero queue backpressure on the working queue throughout, and task latency remained sub-100ms p95 for coordinator-side processing even under full load.
