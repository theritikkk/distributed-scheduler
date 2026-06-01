# Demo Guide

Step-by-step walkthrough for demonstrating the distributed scheduler end-to-end.

---

## Prerequisites

- Stack running (`docker compose ps` shows all containers Up)
- `jq` installed for readable JSON output
- Replace `YOUR_EC2_IP` with your actual EC2 public IP (`13.232.85.244` in the current deployment)

---

## Scheduling flow

```
API Gateway
    │  publishes TTL wakeup
    ▼
scheduler.delay  ──TTL expires──▶  scheduler.due
                                         │
                                    Coordinator
                                         │  dispatches
                                         ▼
                                   scheduler.tasks
                                         │
                                    worker-1/2/3
                                         │  publishes result
                                         ▼
                                   scheduler.results
                                         │
                                    Coordinator
                                         │  updates DB
                                         ▼
                                    PostgreSQL
```

## Retry / failure flow

```
scheduler.tasks
      │  worker failure
      ▼
scheduler.retry_delay  ──TTL expires──▶  scheduler.tasks  (retry)
                                                │  max retries exhausted
                                                ▼
                                       scheduler.tasks.dlq
```

---

## Step 0 — Watch live logs

Open two terminal tabs before starting:

```bash
docker compose logs -f coordinator
```

```bash
docker compose logs -f worker-1 worker-2 worker-3
```

---

## Step 1 — Register a user

```bash
curl -s -X POST http://YOUR_EC2_IP:3000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"demo@example.com","password":"Passw0rd!"}' | jq
```

---

## Step 2 — Login and capture token

```bash
TOKEN=$(curl -s -X POST http://YOUR_EC2_IP:3000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"demo@example.com","password":"Passw0rd!"}' | jq -r '.token')

echo $TOKEN
```

---

## Step 3 — Create a one-time task

Set `next_execution_time` to 1–2 minutes from now in UTC ISO 8601.

```bash
TASK=$(curl -s -X POST http://YOUR_EC2_IP:3000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "Demo hello",
    "command_payload": {"command": "echo hello-from-worker"},
    "schedule_type": "one-time",
    "next_execution_time": "2026-06-01T10:00:00Z"
  }')

echo $TASK | jq
TASK_ID=$(echo $TASK | jq -r '.id')
echo "TASK_ID=$TASK_ID"
```

Watch the coordinator log — you should see the task dispatched when TTL expires.

---

## Step 4 — Check execution history

After the scheduled time:

```bash
curl -s "http://YOUR_EC2_IP:3000/api/v1/tasks/$TASK_ID/executions" \
  -H "Authorization: Bearer $TOKEN" | jq
```

Expected: one execution record, `status: completed`, `output` contains `hello-from-worker`.

---

## Step 5 — Create a recurring task

```bash
RECUR=$(curl -s -X POST http://YOUR_EC2_IP:3000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "Recurring ping",
    "command_payload": {"command": "echo recurring-ping"},
    "schedule_type": "recurring",
    "cron_expression": "* * * * *",
    "next_execution_time": "2026-06-01T10:01:00Z"
  }')

RECUR_ID=$(echo $RECUR | jq -r '.id')
echo "RECUR_ID=$RECUR_ID"
```

Wait 2–3 minutes, then:

```bash
curl -s "http://YOUR_EC2_IP:3000/api/v1/tasks/$RECUR_ID/executions" \
  -H "Authorization: Bearer $TOKEN" | jq
```

Expected: multiple execution rows, each with an updated `next_execution_time`.

---

## Step 6 — Show observability

**RabbitMQ UI** — `http://YOUR_EC2_IP:15672`
- Show queue depths changing as tasks are dispatched and consumed.
- Show the `scheduler.tasks.dlq` queue (should be empty in normal operation).

**Grafana** — `http://YOUR_EC2_IP:3001`
- Open Dashboards → Distributed Scheduler v2.
- Show: Active Workers (3), Queue Depth, Worker Utilization per replica.
- Go to Explore → Loki → run: `{service="worker"} |= "completed"`

**Prometheus** — `http://YOUR_EC2_IP:9091`
- Go to Status → Targets — all 3 workers should show UP with individual scrape timestamps.

---

## 3-minute demo talk track

1. "The API is JWT-protected. I'll register and create a scheduled task."
2. "The task is stored in PostgreSQL. The API publishes a TTL wakeup to RabbitMQ — when the TTL expires, the coordinator is triggered."
3. "Here in the coordinator log you can see it picked up the task, created an execution record, and dispatched it to `scheduler.tasks`."
4. "A worker picked it up, ran the command, and published the result back. The coordinator updated the execution history."
5. "For recurring tasks, the coordinator computes the next run time from the cron expression and reschedules automatically."
6. "All three workers are independently scraped by Prometheus — you can see per-worker utilization in Grafana."
7. "Logs from every container flow into Loki and are queryable in Grafana alongside the metrics."

---

## Common issues

| Symptom | Fix |
|---------|-----|
| 401 Unauthorized | Token expired — re-run login |
| Task not executing | Check `docker compose logs coordinator` — is it consuming `scheduler.due`? |
| No execution records | Verify `next_execution_time` is in the future in UTC |
| Worker not in Prometheus | Check `docker compose logs worker-1` — is metrics port binding? |
| RabbitMQ connection refused | Check `docker compose logs rabbitmq` — wait for healthy |

---

## Failure scenarios to demonstrate

**Worker crash during execution**
```bash
docker stop worker-1
# task re-delivered to worker-2 or worker-3
docker start worker-1
```

**Broker restart**
```bash
docker restart distributed-scheduler-rabbitmq-1
# durable queues preserve messages; reconciliation recovers missed wakeups
```

**Poison task (invalid command)**
```bash
curl -s -X POST http://YOUR_EC2_IP:3000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "Poison task",
    "command_payload": {"command": "this_command_does_not_exist"},
    "schedule_type": "one-time",
    "next_execution_time": "2026-06-01T10:05:00Z"
  }' | jq
# After 3 retries, check scheduler.tasks.dlq in RabbitMQ UI
```