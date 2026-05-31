# Distributed Task Scheduler

A fault-tolerant, distributed cron-as-a-service that schedules one-time and recurring tasks and executes them across multiple worker nodes. Built with a microservices architecture, message queues, and production-oriented reliability patterns.

[![CI](https://github.com/theritikkk/distributed-scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/theritikkk/distributed-scheduler/actions/workflows/ci.yml)

---

## Table of Contents

- [Architecture](#architecture)
- [Components](#components)
- [Data Flow](#data-flow)
- [Database Schema](#database-schema)
- [Quick Start](#quick-start)
- [Environment Variables](#environment-variables)
- [API Reference](#api-reference)
- [Local Development](#local-development)
- [Monitoring Stack](#monitoring-stack)
- [Deployment on AWS](#deployment-on-aws)
- [Project Structure](#project-structure)
- [License](#license)

---

## Architecture

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                  API Gateway (Node.js/TS)               │
                    │  REST CRUD · JWT Auth · Rate Limit · Input Validation   │
                    └───────────────────────────┬─────────────────────────────┘
                                                │ INSERT task + publish wakeup
                    ┌───────────────────────────▼───────────────────────────┐
                    │                      PostgreSQL                       │
                    │    users · tasks · task_executions · workers          │
                    └───────┬───────────────────────────────────┬───────────┘
                            │                                   │
          ┌─────────────────▼────────────────┐    ┌────────────▼────────────────┐
          │       Coordinator (Python)       │    │          RabbitMQ           │
          │  · TTL wakeups → scheduler.due   │───▶│  scheduler.delay            │
          │  · Dispatch work to workers      │    │  scheduler.due              │
          │  · Consume results → update DB   │◀───│  scheduler.tasks            │
          │  · Slow reconciliation (120s)    │    │  scheduler.retry_delay      │
          │  · Worker heartbeat checker      │    │  scheduler.results          │
          └──────────────────────────────────┘    │  scheduler.tasks.dlq (DLQ)  │
                                                  └───────────┬-────────────────┘
                                                              │ consume
                    ┌──────────────────────────────────────────▼─────────────--┐
                    │           worker-1 │ worker-2 │ worker-3 (Python)        │
                    │  · Register + heartbeat in DB                            │
                    │  · Idempotent execution by executionId                   │
                    │  · Exponential backoff retry via scheduler.retry_delay   │
                    │  · Publish result to scheduler.results                   │
                    └──────────────────────────────────────────────────────────┘
```

### Key Design Decisions

**TTL-based scheduling over hot polling** — when a task is created, the API publishes a wakeup message to `scheduler.delay` with a TTL equal to the time until `next_execution_time`. When TTL expires, RabbitMQ dead-letters it into `scheduler.due`, triggering the coordinator. This keeps steady-state DB load minimal.

**Slow reconciliation as a safety net** — a background poller (default every 120s) scans PostgreSQL for any `scheduled` tasks already due, catching anything missed due to broker restarts or TTL edge cases.

**Idempotent execution** — workers track `executionId` in the database before executing. If the same message is redelivered, the worker skips and re-publishes the existing result, achieving exactly-once semantics at the application level.

**Exponential backoff with DLQ** — failed tasks retry via `scheduler.retry_delay` with exponential backoff (base 5s, max 5min). After `TASK_MAX_RETRIES` attempts the message routes to `scheduler.tasks.dlq` for inspection.

---

## Components

| Component | Tech | Role |
|-----------|------|------|
| **API Gateway** | Node.js, Express, TypeScript | REST API for task CRUD and auth. JWT authentication, rate limiting (10,000 req/15min), input validation. Exposes `/metrics` for Prometheus. |
| **Coordinator** | Python | Consumes `scheduler.due`, creates `task_executions`, dispatches to `scheduler.tasks`. Consumes `scheduler.results` and updates the DB. Runs slow reconciliation and worker heartbeat checking in background threads. Exposes metrics on `:9090`. |
| **Worker** | Python | 3 named replicas (`worker-1`, `worker-2`, `worker-3`). Each registers in DB, sends heartbeats, executes shell commands, retries with backoff, and exposes independent metrics on its own port. |
| **RabbitMQ** | RabbitMQ 3 | Durable queues and persistent messages. Decouples API/coordinator from workers. |
| **PostgreSQL** | PostgreSQL 15 | Source of truth for users, tasks, executions, and worker registry. |
| **Prometheus** | Prometheus | Scrapes all 3 worker replicas independently, plus API gateway, coordinator, and RabbitMQ every 15s. |
| **Grafana** | Grafana | Pre-provisioned Prometheus and Loki datasources. Ships with the Distributed Scheduler v2 dashboard. |
| **Loki** | Grafana Loki | Log aggregation backend. Receives logs from all containers via Promtail. 7-day retention. |
| **Promtail** | Grafana Promtail | Docker socket discovery — tails every container's stdout/stderr and ships to Loki. |
| **Alertmanager** | Prometheus Alertmanager | Routes fired alerts. Wire up Slack/email in `monitoring/alertmanager/alertmanager.yml`. |

---

## Data Flow

1. **Task creation**: Client → API (JWT) → `INSERT` into `tasks` → publish TTL wakeup to `scheduler.delay`.
2. **TTL expiry**: RabbitMQ dead-letters wakeup into `scheduler.due` when `next_execution_time` is reached.
3. **Dispatch**: Coordinator consumes `scheduler.due` → locks task row → inserts `task_executions` → publishes to `scheduler.tasks`.
4. **Execution**: Worker consumes `scheduler.tasks` → checks idempotency → runs shell command → on success publishes to `scheduler.results` → on failure routes to `scheduler.retry_delay` (or DLQ after max retries).
5. **Result**: Coordinator consumes `scheduler.results` → updates `task_executions` → updates `tasks.status` and `next_execution_time` for recurring tasks.

---

## Database Schema

```sql
users            -- id (UUID), email, password_hash, created_at
tasks            -- id, user_id, task_name, command_payload (JSONB),
                 -- schedule_type (one-time|recurring), cron_expression,
                 -- next_execution_time, status, created_at, updated_at
task_executions  -- id, task_id, worker_id, started_at, completed_at,
                 -- status, output, error_message, created_at
workers          -- id, worker_id, worker_address, status, last_heartbeat
```

Indexes: `tasks(next_execution_time)`, `tasks(user_id, status)`, `task_executions(task_id)`, `workers(last_heartbeat)`.

---

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Node.js 20+ (for local API development only)
- Python 3.10+ (for local coordinator/worker development only)

### 1. Clone and configure

```bash
git clone https://github.com/theritikkk/distributed-scheduler.git
cd distributed-scheduler
cp .env.example .env
# Fill in all passwords and JWT_SECRET before starting
```

### 2. Start everything

```bash
docker compose up -d --build
```

### 3. Verify

```bash
docker compose ps                        # all containers should show Up
curl http://localhost:3000/health        # {"status":"ok","service":"api-gateway"}
```

### Service endpoints

| Service | URL |
|---------|-----|
| API Gateway | `http://localhost:3000` |
| Grafana | `http://localhost:3001` |
| Prometheus | `http://localhost:9091` |
| RabbitMQ Management | `http://localhost:15672` |

### 4. Register, login, and create tasks

```bash
# Register
curl -s -X POST http://localhost:3000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com","password":"passw0rd123"}' | jq

# Login — capture token
TOKEN=$(curl -s -X POST http://localhost:3000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com","password":"passw0rd123"}' | jq -r '.token')

# One-time task
curl -s -X POST http://localhost:3000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "Say hello",
    "command_payload": {"command": "echo hello"},
    "schedule_type": "one-time",
    "next_execution_time": "2026-06-01T10:00:00Z"
  }' | jq

# Recurring task (every minute)
curl -s -X POST http://localhost:3000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "Recurring ping",
    "command_payload": {"command": "echo ping"},
    "schedule_type": "recurring",
    "cron_expression": "* * * * *",
    "next_execution_time": "2026-06-01T10:01:00Z"
  }' | jq

# List tasks
curl -s http://localhost:3000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" | jq

# Execution history
curl -s "http://localhost:3000/api/v1/tasks/<TASK_ID>/executions" \
  -H "Authorization: Bearer $TOKEN" | jq
```

---

## Environment Variables

Copy `.env.example` to `.env`. Change every password and `JWT_SECRET` before deploying.

| Variable | Description |
|----------|-------------|
| `POSTGRES_USER` | PostgreSQL user |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `POSTGRES_DB` | PostgreSQL database name |
| `DATABASE_URL` | Full PostgreSQL connection string |
| `RABBITMQ_DEFAULT_USER` | RabbitMQ user |
| `RABBITMQ_DEFAULT_PASS` | RabbitMQ password |
| `RABBITMQ_URL` | Full RabbitMQ connection string |
| `JWT_SECRET` | Secret for signing JWT tokens (min 32 chars) |
| `GF_SECURITY_ADMIN_USER` | Grafana admin username |
| `GF_SECURITY_ADMIN_PASSWORD` | Grafana admin password |
| `TASK_MAX_RETRIES` | Worker retries before routing to DLQ (default `3`) |
| `SCHEDULER_RECONCILE_INTERVAL_SEC` | Coordinator reconciliation interval in seconds (default `120`) |

---

## API Reference

All task endpoints require `Authorization: Bearer <token>`. Full spec: [docs/openapi.yaml](docs/openapi.yaml).

### Auth

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/auth/register` | Register a new user |
| `POST` | `/api/v1/auth/login` | Login and receive a JWT |

### Tasks

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/tasks` | List tasks (`?page=1&limit=20`) |
| `POST` | `/api/v1/tasks` | Create a task |
| `GET` | `/api/v1/tasks/:id` | Get a single task |
| `PUT` | `/api/v1/tasks/:id` | Update a task |
| `DELETE` | `/api/v1/tasks/:id` | Delete a task |
| `GET` | `/api/v1/tasks/:id/executions` | Execution history |

`command_payload` runs the `command` field as a shell command on the worker:

```json
{ "command": "echo hello world" }
```

`cron_expression` is required for `recurring` tasks (standard 5-field cron) and must be omitted for `one-time`.

---

## Local Development

```bash
# Start just the infrastructure
docker compose up -d postgres rabbitmq

# API Gateway
cd api-gateway && npm install && npm run dev

# Coordinator
cd coordinator && pip install -r requirements.txt && python main.py

# Worker
cd worker && pip install -r requirements.txt && WORKER_ID=worker-dev python main.py
```

---

## Monitoring Stack

The full observability stack starts automatically with `docker compose up`.

```
All container logs (stdout/stderr)
         │
         ▼
   Promtail — Docker socket discovery, labels: service, container, stream
         │
         ▼
   Loki :3100 — 7-day retention
         │
         ▼
   Grafana :3001 ◀──── Prometheus :9091
```

### Prometheus scrape targets

| Job | Target | Metrics |
|-----|--------|---------|
| `api-gateway` | `api-gateway:3000/metrics` | HTTP rates, latency |
| `coordinator` | `coordinator:9090/metrics` | Dispatch, reconciliation, results |
| `worker` | `worker-1:9100`, `worker-2:9100`, `worker-3:9100` | Task completions, acks, duration, DLQ |
| `rabbitmq` | `rabbitmq-exporter:9419` | Queue depths, message rates |

### Alert rules (`monitoring/prometheus/alerts.yml`)

| Alert | Severity | Fires when |
|-------|----------|-----------|
| `TaskQueueBacklogHigh` | critical | `scheduler.tasks` depth > 1000 for 5 min |
| `DLQMessagesPresent` | critical | Any message in DLQ for 1 min |
| `WorkerConsumerDown` | critical | Zero workers connected for 2 min |
| `WorkerAckRateZero` | warning | Workers up but no acks in 5 min |
| `HighTaskRetryRate` | warning | Retry rate > 0.5/s for 10 min |
| `DLQGrowthRate` | warning | New DLQ messages over 5 min |

To wire up Slack alerts, edit `monitoring/alertmanager/alertmanager.yml`:

```yaml
receivers:
  - name: default
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/YOUR/WEBHOOK'
        channel: '#alerts'
```

### Grafana dashboard

The **Distributed Scheduler v2** dashboard loads automatically. Open Grafana → Dashboards → Distributed Scheduler v2.

Panels: Active Workers · Queue Depth · DLQ Depth · RabbitMQ Connections · Worker Success Rate · Worker Utilization per replica · Task Throughput · Task Lifecycle · Coordinator Results · Task Latency p95 · Dispatch Rate · Reconciliation Nudges · All Queue Sizes · Workers Marked Offline.

### Querying logs in Grafana → Explore → Loki

```logql
{service="coordinator"}                          # all coordinator logs
{service="worker"} |= "ERROR"                    # worker errors
{service=~"coordinator|worker"} |= "<task-uuid>" # trace a specific task
{service="api-gateway"} |~ "5[0-9]{2}"          # API 5xx errors
```

---

## Deployment on AWS

The stack runs on a single EC2 instance (`t3.large` recommended, Ubuntu 22.04).

### Security group ports

| Port | Source | Purpose |
|------|--------|---------|
| 22 | Your IP | SSH |
| 80 / 443 | 0.0.0.0/0 | Nginx |
| 3000 | 0.0.0.0/0 | API Gateway |
| 3001 | Your IP | Grafana |
| 9091 | Your IP | Prometheus |
| 15672 | Your IP | RabbitMQ UI |

Do not expose ports 5432, 5672, or 3100 publicly.

### Deploy

```bash
# Install Docker
sudo apt update && sudo apt upgrade -y
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER && newgrp docker

# Clone and configure
git clone https://github.com/theritikkk/distributed-scheduler.git
cd distributed-scheduler
cp .env.example .env
nano .env   # set all passwords and JWT_SECRET

# Start
docker compose up -d --build
docker compose ps   # verify all containers are Up
```

### Nginx reverse proxy

```bash
sudo apt install nginx -y
sudo tee /etc/nginx/sites-available/scheduler > /dev/null << 'EOF'
server {
    listen 80;
    server_name _;

    location /api/ {
        proxy_pass http://localhost:3000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }

    location /health {
        proxy_pass http://localhost:3000;
    }
}
EOF
sudo ln -s /etc/nginx/sites-available/scheduler /etc/nginx/sites-enabled/scheduler
sudo nginx -t && sudo systemctl restart nginx
```

### Updating

```bash
cd distributed-scheduler
git pull
docker compose up -d --build
docker image prune -f
```

### Security checklist

- [ ] All passwords changed from `.env.example` defaults
- [ ] `JWT_SECRET` is at least 32 random characters (`openssl rand -hex 32`)
- [ ] `.env` is in `.gitignore` and never committed
- [ ] Ports 5432, 5672, 3100 not in security group
- [ ] Grafana and Prometheus restricted to your IP in security group
- [ ] `restart: unless-stopped` on all services are already set

---

## Project Structure

```
distributed-scheduler/
├── api-gateway/               # Node.js/TypeScript REST API
├── coordinator/               # Python coordinator service
├── worker/                    # Python worker (runs as worker-1/2/3)
├── database/
│   └── init.sql               # Schema and indexes
├── monitoring/
│   ├── prometheus.yml         # Scrape config
│   ├── prometheus/alerts.yml  # 6 alert rules
│   ├── grafana/
│   │   ├── provisioning/      # Datasources + dashboard provider
│   │   └── dashboards/        # scheduler-overview.json
│   ├── loki/loki-config.yml
│   ├── promtail/promtail-config.yml
│   └── alertmanager/alertmanager.yml
├── docs/                      # Architecture, API spec, demo guide
├── docker-compose.yml
├── Makefile
└── .env.example
```

---

## License

[MIT](LICENSE)