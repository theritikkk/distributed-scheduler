# Distributed Task Scheduler

A fault-tolerant, distributed **cron-as-a-service** that schedules one-time and recurring tasks and executes them across multiple worker nodes. Built with a microservices architecture, message queues, and production-oriented reliability patterns.

[![CI](https://github.com/distributed-scheduler/distributed-scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/distributed-scheduler/distributed-scheduler/actions/workflows/ci.yml)

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
- [Scaling Workers](#scaling-workers)
- [Monitoring Stack](#monitoring-stack)
- [Deployment on AWS](#deployment-on-aws)
- [Project Structure](#project-structure)
- [Documentation](#documentation)
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
                    │                      PostgreSQL                        │
                    │    users · tasks · task_executions · workers          │
                    └───────┬───────────────────────────────────┬───────────┘
                            │                                   │
          ┌─────────────────▼────────────────┐    ┌────────────▼────────────────┐
          │       Coordinator (Python)        │    │          RabbitMQ            │
          │  · TTL wakeups → scheduler.due    │───▶│  scheduler.delay            │
          │  · Dispatch work to workers       │    │  scheduler.due              │
          │  · Consume results → update DB    │◀───│  scheduler.tasks            │
          │  · Slow reconciliation (120s)     │    │  scheduler.retry_delay      │
          │  · Worker heartbeat checker       │    │  scheduler.results          │
          └──────────────────────────────────┘    │  scheduler.tasks.dlq (DLQ)  │
                                                   └────────────┬────────────────┘
                                                                │ consume
                    ┌───────────────────────────────────────────▼─────────────┐
                    │       Worker 1 (Python) │ Worker 2 (Python) │ Worker N   │
                    │  · Register + heartbeat in DB                            │
                    │  · Idempotent execution by executionId                   │
                    │  · Exponential backoff retry via scheduler.retry_delay   │
                    │  · Publish result to scheduler.results                   │
                    └──────────────────────────────────────────────────────────┘
```

### Key Design Decisions

**TTL-based scheduling over hot polling** — when a task is created, the API publishes a small wakeup message to `scheduler.delay` with a per-message TTL equal to the time until `next_execution_time`. When TTL expires, RabbitMQ dead-letters it into `scheduler.due`, triggering the coordinator. This keeps steady-state DB load minimal.

**Slow reconciliation as a safety net** — a background poller (default every 120s) scans PostgreSQL for any `scheduled` tasks already due, catching anything missed due to broker restarts or TTL edge cases.

**Idempotent execution** — workers track `executionId` in the database before executing. If the same message is redelivered, the worker skips and re-publishes the existing result, achieving exactly-once semantics at the application level.

**Exponential backoff with DLQ** — failed tasks retry via `scheduler.retry_delay` with exponential backoff (base 5s, max 5min). After `TASK_MAX_RETRIES` attempts the message routes to `scheduler.tasks.dlq` for inspection.

---

## Components

| Component | Tech | Role |
|-----------|------|------|
| **API Gateway** | Node.js, Express, TypeScript | REST API for task CRUD and auth. JWT authentication, rate limiting (10,000 req/15min), input validation. Publishes schedule wakeups to RabbitMQ on task create/update. Exposes `/metrics` for Prometheus. |
| **Coordinator** | Python | Consumes `scheduler.due`, creates `task_executions`, dispatches to `scheduler.tasks`. Consumes `scheduler.results` and updates the DB. Runs slow reconciliation and worker heartbeat checking in background threads. Exposes metrics on `:9090`. |
| **Worker** | Python | Registers in DB, sends periodic heartbeats. Consumes `scheduler.tasks`, executes shell commands from `command_payload`, retries with backoff, sends results to `scheduler.results`. Exposes metrics on `:9100`. |
| **RabbitMQ** | RabbitMQ 3 (AMQP) | Durable queues and persistent messages. Decouples API/coordinator from workers; tasks are not lost on worker crash. |
| **PostgreSQL** | PostgreSQL 15 | Source of truth for users, tasks, executions, and worker registry. Indexed for time-based and user-scoped queries. |
| **Prometheus** | Prometheus | Scrapes API gateway, coordinator, worker, and RabbitMQ every 15s. Evaluates alert rules every 30s. |
| **Grafana** | Grafana | Pre-provisioned Prometheus and Loki data sources. Ships with a Distributed Scheduler overview dashboard. |
| **Loki** | Grafana Loki | Log aggregation backend. Receives structured logs from all containers via Promtail. 7-day retention. |
| **Promtail** | Grafana Promtail | Log collector. Uses Docker socket discovery to tail every container's stdout/stderr and ship to Loki with `service`, `container`, and `stream` labels. |

---

## Data Flow

1. **Task creation**: Client → API (JWT) → `INSERT` into `tasks` → publish TTL wakeup to `scheduler.delay`.
2. **TTL expiry**: RabbitMQ dead-letters wakeup into `scheduler.due` when `next_execution_time` is reached.
3. **Dispatch**: Coordinator consumes `scheduler.due` → locks task row → inserts `task_executions` → publishes full payload to `scheduler.tasks`.
4. **Execution**: Worker consumes `scheduler.tasks` → checks idempotency → runs shell command → on success publishes result to `scheduler.results` → on failure routes to `scheduler.retry_delay` (or DLQ after max retries).
5. **Result**: Coordinator consumes `scheduler.results` → updates `task_executions` (status, output, error) → updates `tasks.status` and `next_execution_time` for recurring tasks.

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
git clone <repo-url>
cd distributed-scheduler
cp .env.example .env
# Open .env and set strong passwords before starting
```

### 2. Start everything

```bash
docker compose up -d --build
```

This starts all services including Prometheus, Grafana, Loki, and Promtail.

### 3. Verify health

```bash
curl http://localhost:3000/health
# {"status":"ok","service":"api-gateway"}

docker compose ps   # all containers should be Up
```

### Service endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| API Gateway | http://localhost:3000 | — |
| RabbitMQ Management | http://localhost:15672 | `scheduler` / `scheduler_secret` |
| Grafana | http://localhost:3001 | `admin` / `admin` |
| Prometheus | http://localhost:9091 | — |
| PostgreSQL | `localhost:5432` | user `scheduler`, db `distributed_scheduler` |

### 4. Create a user and schedule tasks

```bash
# Register
curl -s -X POST http://localhost:3000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com","password":"passw0rd123"}' | jq

# Login — capture token
TOKEN=$(curl -s -X POST http://localhost:3000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com","password":"passw0rd123"}' | jq -r '.token')

# Create a one-time task (~1 min from now in ISO 8601)
curl -s -X POST http://localhost:3000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "Say hello",
    "command_payload": {"command": "echo hello"},
    "schedule_type": "one-time",
    "next_execution_time": "2026-05-26T12:05:00Z"
  }' | jq

# Create a recurring task (every minute)
curl -s -X POST http://localhost:3000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "Recurring ping",
    "command_payload": {"command": "echo ping"},
    "schedule_type": "recurring",
    "cron_expression": "* * * * *",
    "next_execution_time": "2026-05-26T12:01:00Z"
  }' | jq

# List tasks
curl -s http://localhost:3000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" | jq

# Execution history (replace <TASK_ID>)
curl -s "http://localhost:3000/api/v1/tasks/<TASK_ID>/executions" \
  -H "Authorization: Bearer $TOKEN" | jq
```

### Useful Makefile commands

```bash
make docker-up     # docker compose up -d
make docker-down   # docker compose down
make build         # build all services locally
make test          # run all service tests
```

---

## Environment Variables

Copy `.env.example` to `.env` before starting. **Change all passwords and the JWT secret before any non-local deployment.**

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://scheduler:...@postgres:5432/distributed_scheduler` | PostgreSQL connection string |
| `RABBITMQ_URL` | `amqp://scheduler:...@rabbitmq:5672` | RabbitMQ connection string |
| `JWT_SECRET` | _(must be set)_ | Secret used to sign JWT tokens |
| `POSTGRES_USER` | `scheduler` | PostgreSQL user |
| `POSTGRES_PASSWORD` | _(must be set)_ | PostgreSQL password |
| `POSTGRES_DB` | `distributed_scheduler` | PostgreSQL database name |
| `RABBITMQ_DEFAULT_USER` | `scheduler` | RabbitMQ default user |
| `RABBITMQ_DEFAULT_PASS` | _(must be set)_ | RabbitMQ default password |
| `TASK_MAX_RETRIES` | `3` | Worker retries before routing to DLQ |
| `SCHEDULER_RECONCILE_INTERVAL_SEC` | `120` | Coordinator reconciliation interval (seconds) |
| `PORT` | `3000` | API Gateway port |
| `METRICS_PORT` (coordinator) | `9090` | Coordinator Prometheus metrics port |
| `METRICS_PORT` (worker) | `9100` | Worker Prometheus metrics port |
| `WORKER_ID` | `worker-1` | Unique worker identity per replica |

---

## API Reference

All task endpoints require `Authorization: Bearer <token>`. Full spec: [docs/openapi.yaml](docs/openapi.yaml) (OpenAPI 3.0 — import into Swagger UI or Postman).

### Auth

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/auth/register` | Register a new user |
| `POST` | `/api/v1/auth/login` | Login and receive a JWT |

### Tasks

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/tasks` | List tasks (paginated: `?page=1&limit=20`) |
| `POST` | `/api/v1/tasks` | Create a task |
| `GET` | `/api/v1/tasks/:id` | Get a single task |
| `PUT` | `/api/v1/tasks/:id` | Update a task |
| `DELETE` | `/api/v1/tasks/:id` | Delete a task |
| `GET` | `/api/v1/tasks/:id/executions` | Execution history (paginated) |

### Task payload

`command_payload` is a JSON object. The worker executes the `command` field as a shell command:

```json
{ "command": "echo hello world" }
```

`cron_expression` is required for `recurring` tasks (standard 5-field cron). It must be omitted for `one-time` tasks.

---

## Local Development

Start infrastructure first:

```bash
docker compose up -d postgres rabbitmq
```

### API Gateway

```bash
cd api-gateway
npm install
# Edit DATABASE_URL and RABBITMQ_URL in .env to point at localhost
npm run dev
```

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
WORKER_ID=worker-dev python main.py
```

---

## Scaling Workers

The `docker-compose.yml` sets `deploy.replicas: 10` for the worker service. To change the count at runtime:

```bash
docker compose up -d --scale worker=5
```

Each replica registers itself in the `workers` table using its `WORKER_ID` and sends periodic heartbeats. The coordinator marks workers `offline` automatically when heartbeats stop.

---

## Monitoring Stack

The full observability stack — **Prometheus + Grafana + Loki + Promtail** — is wired together and starts automatically with `docker compose up`.

```
All container stdout/stderr
         │
         ▼
   Promtail :9080
   · Docker socket discovery (/var/run/docker.sock)
   · Refreshes container list every 5s
   · Attaches labels: service, container, stream
   · Parses Docker JSON log format
         │
         ▼
   Loki :3100
   · TSDB v13 storage on filesystem
   · 7-day log retention (168h)
         │
         ▼
   Grafana :3001  ◀──── Prometheus :9091
   · Explore tab → LogQL queries
   · Scheduler Overview dashboard
   · Both datasources pre-provisioned
```

### Prometheus — metrics

Prometheus scrapes four jobs every 15s (`monitoring/prometheus.yml`):

| Job | Target | What it measures |
|-----|--------|-----------------|
| `api-gateway` | `api-gateway:3000/metrics` | HTTP request rates, latency, error counts |
| `coordinator` | `coordinator:9090/metrics` | Task dispatch, result consumption, reconciliation |
| `worker` | `worker:9100/metrics` | Task acks, completions, failures, retries, DLQ sends |
| `rabbitmq` | `rabbitmq-exporter:9419` | Queue depths, message rates per queue |

### Prometheus — alert rules

Alert rules in `monitoring/prometheus/alerts.yml` are evaluated every 30s:

| Alert | Severity | Fires when |
|-------|----------|-----------|
| `TaskQueueBacklogHigh` | critical | `scheduler.tasks` depth > 1000 for 5 min |
| `DLQMessagesPresent` | critical | Any message in `scheduler.tasks.dlq` for 1 min |
| `WorkerConsumerDown` | critical | Zero worker consumers connected for 2 min |
| `WorkerAckRateZero` | warning | Consumers up but no acks in 5 min (possible deadlock) |
| `HighTaskRetryRate` | warning | Retry rate > 0.5/s for 10 min |
| `DLQGrowthRate` | warning | New DLQ messages appearing over 5 min |

AlertManager is pre-configured at `alertmanager:9093`. To wire up Slack or email, edit `monitoring/alertmanager/alertmanager.yml`:

```yaml
receivers:
  - name: default
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'
        channel: '#alerts'
```

### Loki + Promtail — log aggregation

Promtail uses Docker socket-based service discovery to automatically collect logs from **every container** — no per-service config required. Each log line is labelled:

| Label | Source | Example value |
|-------|--------|---------------|
| `service` | Docker Compose service name | `worker`, `coordinator`, `api-gateway` |
| `container` | Container name | `distributed-scheduler-worker-1` |
| `stream` | Log stream | `stdout` or `stderr` |

**Querying logs in Grafana → Explore → Loki:**

```logql
# All logs from the coordinator
{service="coordinator"}

# Worker errors only
{service="worker"} |= "ERROR"

# Track a specific task across coordinator and worker
{service=~"coordinator|worker"} |= "<your-task-uuid>"

# API gateway — all 4xx and 5xx responses
{service="api-gateway"} |~ "4[0-9]{2}|5[0-9]{2}"

# Watch a task execute in real time (Grafana Live tail)
{service="worker"} |= "executing task"

# All DLQ events
{service="worker"} |= "dlq"
```

> **Tip:** In Grafana you can correlate metrics and logs side by side — open the Distributed Scheduler dashboard in Grafana, click a spike on the retry rate graph, then click "Logs" to jump straight to the Loki logs for that time window.

### Grafana dashboard

The pre-provisioned **Distributed Scheduler** dashboard (UID `distributed-scheduler`) includes:

- `scheduler.tasks` queue depth (stat, red threshold at 1000)
- DLQ depth (stat, red at any value > 0)
- Active workers (stat)
- Task ack rate over time (time series)
- Completed vs failed vs retried over time (time series)

Open it at http://localhost:3001 → Dashboards → Distributed Scheduler.

---

## Deployment on AWS

The entire stack runs on a single EC2 instance via Docker Compose. This is the recommended starting point — straightforward to operate, easy to debug, and sufficient for substantial load.

### Recommended EC2 instance

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| Instance type | `t3.medium` (2 vCPU, 4 GB) | `t3.large` (2 vCPU, 8 GB) |
| Storage | 40 GB gp3 | 80 GB gp3 |
| OS | Ubuntu 22.04 LTS | Ubuntu 22.04 LTS |

### Step 1 — Launch EC2 and open security group ports

In your EC2 Security Group, allow inbound:

| Port | Protocol | Source | Purpose |
|------|----------|--------|---------|
| 22 | TCP | Your IP | SSH |
| 80 | TCP | 0.0.0.0/0 | Nginx → API |
| 443 | TCP | 0.0.0.0/0 | Nginx HTTPS |
| 3001 | TCP | Your IP | Grafana (restrict to your IP) |
| 9091 | TCP | Your IP | Prometheus (restrict to your IP) |
| 15672 | TCP | Your IP | RabbitMQ UI (restrict to your IP) |

**Do not expose ports 5432 (PostgreSQL), 5672 (RabbitMQ AMQP), or 3100 (Loki) publicly.**

### Step 2 — Install Docker on the instance

```bash
ssh ubuntu@YOUR_EC2_PUBLIC_IP

sudo apt update && sudo apt upgrade -y
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
exit
# SSH back in so the group change takes effect
ssh ubuntu@YOUR_EC2_PUBLIC_IP
docker compose version   # verify
```

### Step 3 — Clone the repo and configure

```bash
git clone https://github.com/YOUR_USERNAME/distributed-scheduler.git
cd distributed-scheduler
cp .env.example .env
nano .env
```

Set strong values for every secret in `.env`:

```env
POSTGRES_USER=scheduler
POSTGRES_PASSWORD=CHANGE_THIS_STRONG_PASSWORD
POSTGRES_DB=distributed_scheduler

RABBITMQ_DEFAULT_USER=scheduler
RABBITMQ_DEFAULT_PASS=CHANGE_THIS_STRONG_PASSWORD

JWT_SECRET=CHANGE_THIS_TO_A_LONG_RANDOM_STRING

DATABASE_URL=postgresql://scheduler:CHANGE_THIS_STRONG_PASSWORD@postgres:5432/distributed_scheduler
RABBITMQ_URL=amqp://scheduler:CHANGE_THIS_STRONG_PASSWORD@rabbitmq:5672

TASK_MAX_RETRIES=3
SCHEDULER_RECONCILE_INTERVAL_SEC=120
```

### Step 4 — Harden docker-compose.yml for production

Remove the public port binding from PostgreSQL so it is only reachable inside the Docker network:

```yaml
# postgres service — replace ports: with expose:
expose:
  - "5432"

# Do NOT publicly expose RabbitMQ AMQP port either
# Remove or comment out: - "5672:5672"
# Keep management UI if you need it (restrict via security group):
ports:
  - "15672:15672"
```

Add `restart: unless-stopped` to every service so containers recover automatically after an instance reboot:

```yaml
services:
  api-gateway:
    restart: unless-stopped
  coordinator:
    restart: unless-stopped
  worker:
    restart: unless-stopped
  prometheus:
    restart: unless-stopped
  grafana:
    restart: unless-stopped
  loki:
    restart: unless-stopped
  promtail:
    restart: unless-stopped
```

### Step 5 — Build and start

```bash
docker compose up -d --build
docker compose ps        # all services should show Up (healthy)
docker compose logs -f   # watch startup logs
```

### Step 6 — Install Nginx as a reverse proxy

```bash
sudo apt install nginx -y
sudo nano /etc/nginx/sites-available/distributed-scheduler
```

Paste this config (replace `YOUR_DOMAIN_OR_IP`):

```nginx
server {
    listen 80;
    server_name YOUR_DOMAIN_OR_IP;

    # API Gateway
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

    # Grafana (optional — or keep it on :3001 and restrict via security group)
    location /grafana/ {
        proxy_pass http://localhost:3001/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/distributed-scheduler /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl restart nginx
```

### Step 7 — HTTPS with Let's Encrypt (if you have a domain)

```bash
sudo apt install certbot python3-certbot-nginx -y
sudo certbot --nginx -d yourdomain.com
sudo systemctl status certbot.timer   # verify auto-renewal
```

### Step 8 — Configure firewall

```bash
sudo ufw allow OpenSSH
sudo ufw allow 80
sudo ufw allow 443
sudo ufw enable
```

### Step 9 — Verify the full stack

```bash
# API
curl https://yourdomain.com/health

# Grafana — open in browser
# http://YOUR_EC2_IP:3001  (or https://yourdomain.com/grafana/)

# Prometheus — open in browser
# http://YOUR_EC2_IP:9091

# RabbitMQ — open in browser
# http://YOUR_EC2_IP:15672
```

### Step 10 — Updating the deployment

```bash
cd distributed-scheduler
git pull
docker compose up -d --build
docker image prune -f    # clean up old images
```

### AWS production topology (for scaling beyond a single instance)

When you outgrow the single-instance setup, the natural next step on AWS is:

```
Internet
    │
    ▼
Application Load Balancer
    │
    ▼
EC2 Auto Scaling Group
(API Gateway containers)
    │              │
    ▼              ▼
RDS PostgreSQL   Amazon MQ (RabbitMQ)
(Multi-AZ)       (broker failover)
                   │
                   ▼
          EC2 Auto Scaling Group
          (Worker containers)
          Scale on: CloudWatch metric
          → SQS ApproximateNumberOfMessages
          or RabbitMQ queue depth via custom metric
```

Key AWS services used at that stage:

| Service | Role |
|---------|------|
| EC2 Auto Scaling | Scale API and worker pools independently |
| RDS PostgreSQL (Multi-AZ) | Managed DB with automatic failover |
| Amazon MQ (RabbitMQ) | Managed broker with HA pair |
| ALB | Load balance API tier, health check `/health` |
| ECR | Store Docker images built by GitHub Actions |
| CloudWatch | Logs, metrics, and scale-out alarms |
| Secrets Manager | Store `JWT_SECRET`, DB password, RabbitMQ password |

### Security checklist for production

- [ ] All passwords changed from defaults in `.env`
- [ ] PostgreSQL port not publicly exposed
- [ ] RabbitMQ AMQP port (5672) not publicly exposed
- [ ] Loki port (3100) not publicly exposed
- [ ] Grafana protected by auth (already on by default) and ideally restricted by IP
- [ ] `JWT_SECRET` is a long random string (32+ chars)
- [ ] `.env` is in `.gitignore` and never committed
- [ ] HTTPS enabled if using a domain
- [ ] `restart: unless-stopped` on all services
- [ ] Regular `docker image prune` to reclaim disk

---

## Project Structure

```
distributed-scheduler/
├── api-gateway/               # Node.js/TypeScript REST API
│   └── src/
│       ├── routes/            # auth.ts, tasks.ts
│       ├── middleware/        # auth, errorHandler, logger
│       ├── queue.ts           # RabbitMQ publish helpers
│       ├── db.ts              # PostgreSQL pool
│       └── metrics.ts         # Prometheus metrics endpoint
├── coordinator/               # Python coordinator service
│   └── internal/
│       ├── queue/             # due_consumer, result_consumer,
│       │                      # reconciliation, publisher, topology
│       └── registry/          # worker heartbeat checker
├── worker/                    # Python worker service
│   ├── main.py                # Task consumer + execution logic
│   ├── metrics.py             # Prometheus metrics
│   └── rabbit_topology.py     # Queue/exchange declarations
├── database/
│   └── init.sql               # Schema, indexes, initial setup
├── proto/                     # Protobuf definitions (future gRPC)
├── monitoring/
│   ├── prometheus.yml         # Scrape config + alertmanager endpoint
│   ├── prometheus/
│   │   └── alerts.yml         # Alert rules (6 rules)
│   ├── grafana/
│   │   ├── provisioning/
│   │   │   ├── datasources/   # Prometheus + Loki auto-provisioned
│   │   │   └── dashboards/    # Dashboard provider config
│   │   └── dashboards/
│   │       └── scheduler-overview.json  # Pre-built dashboard
│   ├── loki/
│   │   └── loki-config.yml    # TSDB storage, 7-day retention
│   ├── promtail/
│   │   └── promtail-config.yml  # Docker socket discovery, label config
│   └── alertmanager/
│       └── alertmanager.yml   # Receiver config (add Slack/email here)
├── docs/
│   ├── ARCHITECTURE.md        # Design decisions and trade-offs
│   ├── PROJECT_OVERVIEW.md    # Full system walkthrough
│   ├── DEPLOYMENT-DOCS.md     # VM deployment guide
│   ├── DEMO.md                # Demo script and chaos testing
│   └── openapi.yaml           # OpenAPI 3.0 API spec
├── tests/
│   └── production-test.example.js   # k6 load test example
├── scripts/
│   └── monitor-test.sh
├── .github/workflows/
│   ├── ci.yml                 # Build + lint on push/PR
│   └── docker-build.yml       # Docker image build
├── docker-compose.yml
├── Makefile
└── .env.example
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Design decisions: RabbitMQ rationale, TTL scheduling vs polling, idempotency, trade-offs |
| [docs/PROJECT_OVERVIEW.md](docs/PROJECT_OVERVIEW.md) | Full walkthrough — components, tech stack, how everything connects |
| [docs/DEPLOYMENT-DOCS.md](docs/DEPLOYMENT-DOCS.md) | Step-by-step VM deployment guide with Nginx and HTTPS |
| [docs/DEMO.md](docs/DEMO.md) | Demo script and chaos testing walkthrough |
| [docs/openapi.yaml](docs/openapi.yaml) | OpenAPI 3.0 API specification |
| [coordinator/README.md](coordinator/README.md) | Coordinator file-by-file guide |

---

## License

[MIT](LICENSE)