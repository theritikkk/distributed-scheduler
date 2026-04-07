# Distributed Task Scheduler

A fault-tolerant, distributed **cron-as-a-service** that schedules one-time and recurring tasks and executes them across multiple worker nodes. Built with a microservices architecture, message queues, and production-oriented patterns.

## Architecture

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                     API Gateway (Node.js)               │
                    │  REST CRUD • JWT Auth • Rate Limit • Validation        │
                    └───────────────────────────┬───────────────────────────┘
                                                │
                    ┌───────────────────────────▼───────────────────────────┐
                    │                  PostgreSQL                            │
                    │  users • tasks • task_executions • workers             │
                    └───────┬───────────────────────────────────┬───────────┘
                            │                                   │
          ┌─────────────────▼──────────────┐    ┌──────────────▼─────────────┐
          │  Coordinator (Python)          │    │  RabbitMQ                  │
          │  • Poll DB for due tasks        │───▶│  scheduler.tasks           │
          │  • Publish to task queue        │    │  scheduler.results          │
          │  • Consume results → update DB  │◀───│  (persistent, durable)     │
          │  • Worker heartbeat / health    │    └──────────────┬──────────────┘
          └────────────────────────────────┘                 │
                                                              │ consume
                    ┌─────────────────────────────────────────▼───────────────┐
                    │  Worker 1 (Python) │  Worker 2 (Python) │  Worker N     │
                    │  • Register + heartbeat in DB                          │
                    │  • Execute task (e.g. shell command from payload)      │
                    │  • Publish result to scheduler.results                │
                    └───────────────────────────────────────────────────────┘
```

### Components

| Component | Tech | Role |
|-----------|------|------|
| **API Gateway** | Node.js, Express, TypeScript | REST API for task CRUD, JWT auth, rate limiting, input validation. Publishes new/updated tasks to RabbitMQ. |
| **Coordinator** | Python | Polls DB for `next_execution_time <= NOW()`, publishes to `scheduler.tasks`; consumes `scheduler.results` and updates `task_executions` and tasks (and next run for recurring). Runs worker heartbeat checker (mark stale workers offline). |
| **Workers** | Python | Register in DB, send heartbeats, consume from `scheduler.tasks`, execute task (e.g. `command` in payload), publish result to `scheduler.results`. |
| **Message Queue** | RabbitMQ | Decouples API/coordinator from workers; ensures tasks are not lost if workers fail (persistent queues). |
| **Database** | PostgreSQL | Users, tasks, task_executions, workers. Indexed for time-based and user-scoped queries. |
| **Monitoring** | Prometheus, Grafana | Metrics from API gateway; optional dashboards. |

### Data flow

1. **Create task**: Client → API (JWT) → INSERT task, publish `{ taskId }` to queue (optional; coordinator also polls).
2. **Execution**: Coordinator polls DB for due tasks → creates `task_execution` row → publishes full task payload to `scheduler.tasks` → worker consumes → runs command → publishes result to `scheduler.results` → coordinator consumes → updates `task_executions` and `tasks` (and `next_execution_time` for recurring).

### Database schema (summary)

- **users** – id, email, password_hash
- **tasks** – id, user_id, task_name, command_payload (JSONB), schedule_type (one-time | recurring), cron_expression, next_execution_time, status
- **task_executions** – id, task_id, worker_id, started_at, completed_at, status, output, error_message
- **workers** – id, worker_id, worker_address, status, last_heartbeat

Indexes on `tasks(next_execution_time)`, `tasks(user_id, status)`, `task_executions(task_id)`, `workers(last_heartbeat)`.

---

For a **full walkthrough** of the system (what each part does, how they connect, and the tech stack), see **[docs/PROJECT_OVERVIEW.md](docs/PROJECT_OVERVIEW.md)**.

## Quick start

### Prerequisites

- Docker and Docker Compose
- Node.js 18+ (for local API dev)
- Python 3.10+ (for local coordinator/worker dev)

### Run with Docker Compose

```bash
git clone <repo>
cd distributed-scheduler
docker compose up -d
```

- **API**: http://localhost:3000  
- **RabbitMQ management**: http://localhost:15672 (scheduler / scheduler_secret)  
- **Grafana**: http://localhost:3001 (admin / admin)  
- **PostgreSQL**: localhost:5432, user `scheduler`, db `distributed_scheduler`, password `scheduler_secret`

### Create a user and a task

```bash
# Register
curl -s -X POST http://localhost:3000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com","password":"passw0rd123"}' | jq

# Login and get token
TOKEN=$(curl -s -X POST http://localhost:3000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com","password":"passw0rd123"}' | jq -r '.token')

# Create one-time task (run once at given time; use ISO8601)
curl -s -X POST http://localhost:3000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "Say hello",
    "command_payload": {"command": "echo hello"},
    "schedule_type": "one-time",
    "next_execution_time": "2025-03-08T12:00:00Z"
  }' | jq

# Create recurring task (cron: every minute for demo)
curl -s -X POST http://localhost:3000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "Recurring ping",
    "command_payload": {"command": "echo ping"},
    "schedule_type": "recurring",
    "cron_expression": "* * * * *",
    "next_execution_time": "2025-03-07T12:00:00Z"
  }' | jq

# List tasks
curl -s http://localhost:3000/api/v1/tasks -H "Authorization: Bearer $TOKEN" | jq

# Get execution history for a task
curl -s "http://localhost:3000/api/v1/tasks/<TASK_ID>/executions" -H "Authorization: Bearer $TOKEN" | jq
```

Use `next_execution_time` in the near future (e.g. 1–2 minutes from now) to see the coordinator pick the task and a worker run it.

---

## API summary

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/v1/auth/register | Register user |
| POST | /api/v1/auth/login | Login (returns JWT) |
| GET | /api/v1/tasks | List tasks (paginated) |
| POST | /api/v1/tasks | Create task |
| GET | /api/v1/tasks/:id | Get task |
| PUT | /api/v1/tasks/:id | Update task |
| DELETE | /api/v1/tasks/:id | Delete task |
| GET | /api/v1/tasks/:id/executions | Execution history (paginated) |

Full API specification: [docs/openapi.yaml](docs/openapi.yaml) (OpenAPI 3.0). You can import it into Swagger UI or Postman.

---

## Local development

### API Gateway

```bash
cd api-gateway
npm install
cp .env.example .env   # if you add one
npm run dev
```

Set `DATABASE_URL` and `RABBITMQ_URL` to your local Postgres and RabbitMQ (e.g. from Docker).

### Coordinator

```bash
cd coordinator
pip install -r requirements.txt
python main.py
```

### Worker

```bash
cd worker
pip install -r requirements.txt
python main.py
```

Ensure PostgreSQL and RabbitMQ are running (e.g. `docker compose up -d postgres rabbitmq`).

---

## Task payload

`command_payload` is JSON. The worker currently interprets a `command` field as a shell command (single string, split on spaces). Example:

```json
{ "command": "echo hello world" }
```

For production you would add validation, sandboxing, timeouts, and possibly a small DSL or allowed commands list.

---

## Monitoring

- **Prometheus**: scrape config in `monitoring/prometheus.yml` (API gateway `/metrics` when available).
- **Grafana**: provisioning in `monitoring/grafana/provisioning`. Add a Prometheus data source and dashboards as needed.

---

## Deployment (outline)

- **AWS**: Run API and Coordinator on ECS or EC2; workers in an Auto Scaling Group; RDS PostgreSQL; Amazon MQ (RabbitMQ) or SQS + a thin adapter; ALB in front of API; CloudWatch for logs/metrics; S3 for execution logs if required.
- **Kubernetes**: Use the same Docker images; deploy API, Coordinator, and Worker as Deployments; use Secrets for DB and RabbitMQ URLs and JWT secret; optional HPA for workers.
- **CI/CD**: GitHub Actions can build images, run tests, and deploy to your environment (see [Artifacts](#artifacts) below).

---

## Artifacts

- **Repository**: This repo with clear structure and this README.
- **API docs**: [docs/openapi.yaml](docs/openapi.yaml) (OpenAPI/Swagger).
- **Docker Compose**: [docker-compose.yml](docker-compose.yml) for local and dev.
- **Architecture**: Described above and in comments in code.
- **Demo**: Use the Quick start and curl examples to show create → list → executions; scale workers and show distribution; stop a worker and show heartbeat/offline behavior.
- **Blog / design doc**: Explain choice of DB + queue (reliability, replay), polling vs pure event-driven, worker registration and heartbeats, and trade-offs (e.g. at-least-once delivery, idempotent execution handling).

---

## License

MIT.
