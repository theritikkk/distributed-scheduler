# Demo Guide (Step-by-Step)

## Prerequisites

- Docker + Docker Compose installed
- Project `.env` configured (`DATABASE_URL`, `RABBITMQ_URL`, `JWT_SECRET`, etc.)
- `jq` installed for cleaner JSON output (optional but recommended)

---

## Scheduling Flow

```text
           API
            │
            ▼
    scheduler.delay (TTL based)
            |
            │ TTL expires
            |
            ▼
       scheduler.due
            │
            ▼
        Coordinator
            │
            ▼
    scheduler.tasks
            │
            ▼
         Workers
            │
            ▼
    scheduler.results
            │
            ▼
Coordinator updates PostgreSQL

```

---

## Failure / Retry Flow

```text

  scheduler.tasks
        │
        ▼
  Worker failure
        │
        ▼
scheduler.retry_delay
        |
        │ TTL expires
        |
        ▼
  scheduler.tasks
        |
        │ retries exhausted
        |
        ▼
scheduler.tasks.dlq

```

---

## 0) Start the system

From project root:

```bash
docker compose up -d --build
docker compose ps
```

Expected:

- `api-gateway` running on `http://localhost:3000`
- `rabbitmq` running with UI on `http://localhost:15672`
- `coordinator` and `worker` services healthy

---

## 1) Watch service logs (live)

Open separate terminals:

```bash
docker compose logs -f coordinator
```

```bash
docker compose logs -f worker
```

- The API schedules a delayed wakeup message in RabbitMQ using per-message TTL.
- When the TTL expires, RabbitMQ dead-letters the wakeup into the due queue.
- The coordinator consumes due events, creates execution records transactionally, and dispatches work to distributed workers.

---

## 2) Register a user

```bash
curl -s -X POST http://localhost:3000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"demo@example.com","password":"Passw0rd!"}' | jq
```

If user already exists, continue with login.

---

## 3) Login and capture token

```bash
TOKEN=$(curl -s -X POST http://localhost:3000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"demo@example.com","password":"Passw0rd!"}' | jq -r '.token')

echo "$TOKEN"
```

Expected: non-empty JWT token string.

---

## 4) Create a one-time task (quick demo)

Use a `next_execution_time` 1–2 minutes in the future.

```bash
TASK_RESPONSE=$(curl -s -X POST http://localhost:3000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "One-time hello",
    "command_payload": {"command":"echo hello-from-worker"},
    "schedule_type": "one-time",
    "next_execution_time": "2026-05-10T14:30:00Z"
  }')

echo "$TASK_RESPONSE" | jq
TASK_ID=$(echo "$TASK_RESPONSE" | jq -r '.id')
echo "TASK_ID=$TASK_ID"
```

Expected:

- API returns task JSON with status `scheduled`
- Coordinator log later shows task published
- Worker log shows command execution

---

## 5) Verify execution history

Wait until after the scheduled time, then:

```bash
curl -s "http://localhost:3000/api/v1/tasks/$TASK_ID/executions" \
  -H "Authorization: Bearer $TOKEN" | jq
```

Expected:

- one execution record
- `status: completed` (or failed with error details if command fails)
- `output` includes `hello-from-worker`

---

## 6) Create a recurring task

```bash
RECUR_RESPONSE=$(curl -s -X POST http://localhost:3000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "Recurring ping",
    "command_payload": {"command":"echo recurring-ping"},
    "schedule_type": "recurring",
    "cron_expression": "* * * * *",
    "next_execution_time": "2026-05-10T14:31:00Z"
  }')

echo "$RECUR_RESPONSE" | jq
RECUR_TASK_ID=$(echo "$RECUR_RESPONSE" | jq -r '.id')
```

After 2–3 minutes:

```bash
curl -s "http://localhost:3000/api/v1/tasks/$RECUR_TASK_ID/executions" \
  -H "Authorization: Bearer $TOKEN" | jq
```

Expected:

- multiple execution rows
- task gets re-scheduled with updated `next_execution_time`

---

## 7) Show RabbitMQ and observability (for credibility)

- RabbitMQ UI: `http://localhost:15672`  
  Credentials (default): `scheduler / scheduler_secret`
- Grafana: `http://localhost:3001`  
  Credentials: `admin / admin`

Use these in your demo narrative:

- queue depth changes as tasks are published/consumed
- worker throughput visible through logs/metrics

---

## 8) Suggested 3-minute demo script (talk track)

1. "I’ll register/login and create a scheduled task."
2. "Task is persisted in Postgres with a next execution time."
3. "Coordinator polls due tasks and publishes them to RabbitMQ."
4. "Workers consume tasks and execute commands."
5. "Results are written back and visible in execution history."
6. "Recurring tasks are re-scheduled via cron expression."

This narrative demonstrates distributed systems reasoning, not just API CRUD.

---

## Consistency Guarantees

The system follows an at-least-once processing model.

To safely handle duplicate delivery:

- workers check whether an execution was already completed before running
- result consumers only apply updates where completed_at IS NULL
- PostgreSQL row locks (FOR UPDATE) prevent duplicate dispatch races

This ensures execution state transitions remain idempotent and eventually consistent under retries and redelivery.

---

## Screenshots / GIF:

Add your captures here:

- `docs/assets/rabbitmq-ui.png`
- `docs/assets/grafana-dashboard.png`
- `docs/assets/api-task-response.png`
- `docs/assets/demo-flow.gif` or `docs/assets/demo-video-link.md`

Then reference them in this file:

```md
![RabbitMQ UI](./assets/rabbitmq-ui.png)
![Grafana Dashboard](./assets/grafana-dashboard.png)
![API Response](./assets/api-task-response.png)
```

---

## Common issues and quick fixes

- **401 Unauthorized**: token missing/expired; re-run login.
- **Task not executing**: check coordinator and worker logs.
- **No DB connection**: verify `.env` `DATABASE_URL`.
- **RabbitMQ issues**: verify `RABBITMQ_URL` and container health.
- **Time mismatch**: ensure `next_execution_time` is truly in the future in UTC.

---

## Example Failure Scenarios

### Worker crash during execution
- RabbitMQ re-delivers unacked task
- idempotency prevents duplicate finalization

### Broker restart
- durable queues + persistent messages preserve tasks
- reconciliation scan recovers missed wakeups

### Poison task payload
- retries applied with exponential backoff
- task eventually isolated into DLQ

---

## Resume-ready demo statement

"Deployed and demonstrated a distributed task scheduler where tasks are created through a JWT-protected API, queued through RabbitMQ, executed across worker nodes, and tracked via execution history with recurring cron-based rescheduling."

