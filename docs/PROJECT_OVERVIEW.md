# Distributed Task Scheduler — Project Overview

This document explains **what this project does**, **how the pieces fit together**, and **the tech stack** in plain terms. The backend services (Coordinator and Worker) are written in **Python**; the API is **Node.js/TypeScript**; data lives in **PostgreSQL** and messages in **RabbitMQ**.

---

## What problem does it solve?

You want to run **scheduled tasks** (like “every hour” or “once at 3pm”) in a **reliable, distributed** way:

- Many **workers** can run tasks so you can scale.
- If a worker dies, tasks are not lost (they stay in a **queue**).
- You can create, list, update, and delete tasks via a **REST API** and see **execution history**.

So it’s like “cron + a queue + an API”: **cron-as-a-service**, but distributed and fault-tolerant.

---

## High-level architecture

```
  You (or your app)
        │
        ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │  API GATEWAY (Node.js / Express / TypeScript)                     │
  │  • REST API: create/read/update/delete tasks, auth, rate limit   │
  │  • Talks to: PostgreSQL + RabbitMQ                               │
  └─────────────────────────────────────────────────────────────────┘
        │                                    │
        ▼                                    ▼
  ┌──────────────┐                   ┌──────────────┐
  │  PostgreSQL  │                   │  RabbitMQ    │
  │  • users     │                   │  • task queue│
  │  • tasks     │                   │  • result    │
  │  • executions│                   │    queue     │
  │  • workers   │                   └──────┬───────┘
  └──────┬───────┘                          │
        │                                  │
        │    ┌─────────────────────────────┘
        │    │
        ▼    ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │  COORDINATOR (Python)                                            │
  │  • Polls DB: “which tasks are due now?”                          │
  │  • Puts due tasks into RabbitMQ task queue                        │
  │  • Reads results from RabbitMQ result queue → updates DB          │
  │  • Marks workers “offline” if they stop sending heartbeats       │
  └─────────────────────────────────────────────────────────────────┘
        │
        │  (workers pull tasks from the same RabbitMQ task queue)
        ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │  WORKERS (Python, can run many copies)                           │
  │  • Register in DB + send heartbeats                              │
  │  • Consume messages from task queue                              │
  │  • Run the task (e.g. shell command from payload)                │
  │  • Send result back to result queue                              │
  └─────────────────────────────────────────────────────────────────┘
```

So:

- **You** talk to the **API** (HTTP).
- **API** stores tasks in **PostgreSQL** and can notify via **RabbitMQ**.
- **Coordinator** moves “due” tasks from DB → **RabbitMQ task queue** and moves **results** from **RabbitMQ result queue** → DB.
- **Workers** take tasks from the task queue, run them, and push results to the result queue.

---

## Tech stack (what runs where)

| Part            | Technology        | Your experience |
|-----------------|-------------------|------------------|
| **API Gateway** | Node.js, Express, TypeScript | You know JS/TS |
| **Coordinator** | Python 3.12       | You know Python  |
| **Worker**      | Python 3.12       | You know Python  |
| **Database**    | PostgreSQL 15     | SQL / Docker     |
| **Message queue** | RabbitMQ        | Docker           |
| **Containers**  | Docker, Docker Compose | You know Docker |

Everything can run locally with **Docker Compose** (one command to start all services).

---

## What each component does (in detail)

### 1. API Gateway (Node.js/TypeScript)

- **REST API** for:
  - **Auth**: register, login (JWT).
  - **Tasks**: create, list, get one, update, delete.
  - **Executions**: list execution history for a task (paginated).
- **Security**: Helmet, CORS, rate limiting, input validation (express-validator).
- **Storage**: Reads/writes **PostgreSQL** (users, tasks). Can publish “new task” to **RabbitMQ** so the system knows to consider it.
- **Metrics**: Exposes `/metrics` for Prometheus.

You already know JS/TS and Docker; this is a standard Express app that uses env vars for `DATABASE_URL` and `RABBITMQ_URL`.

---

### 2. PostgreSQL (database)

- **users** – who can log in (email + hashed password).
- **tasks** – what to run, when (one-time or recurring with cron), current status.
- **task_executions** – each time a task runs: when it started/finished, status, output, error.
- **workers** – which worker processes exist and when they last sent a heartbeat.

The schema is in `database/init.sql`. Indexes are set up for time-based and user-based queries.

---

### 3. RabbitMQ (message queue)

- **scheduler.tasks** – messages describing “run this task now” (task id, execution id, payload, etc.).
- **scheduler.results** – messages describing “this run finished” (task id, execution id, status, output, error).

Queues are **durable**: if RabbitMQ restarts, messages are not lost. This gives **fault tolerance**: if a worker dies, the task message stays in the queue and another worker can run it.

---

### 4. Coordinator (Python)

Three jobs (can be thought of as two loops + one background thread):

1. **Poller (main loop)**  
   Every few seconds it:
   - Selects tasks in DB where `status = 'scheduled'` and `next_execution_time <= NOW()` (with `FOR UPDATE SKIP LOCKED` so only one coordinator processes each row).
   - For each such task: creates a row in **task_executions** (status `running`), sets task to `running`, then **publishes a message to RabbitMQ task queue**.
   - So: “due tasks” move from DB → queue.

2. **Result consumer (background thread)**  
   Listens to the **result queue**:
   - When a worker sends a result, it updates **task_executions** (completed_at, status, output, error) and **tasks** (status completed/failed).
   - For **recurring** tasks, it computes the **next run time** from the cron expression (using `croniter`) and sets `next_execution_time` and `status = 'scheduled'` again.

3. **Heartbeat checker (background thread)**  
   Periodically marks **workers** as `offline` if their `last_heartbeat` is older than 2 minutes.

Libraries: `psycopg2` (PostgreSQL), `pika` (RabbitMQ), `croniter` (next cron time).

---

### 5. Worker (Python)

- On startup: **registers** itself in the **workers** table (or updates last_heartbeat).
- A **background thread** sends a **heartbeat** every 20 seconds (updates `last_heartbeat` in DB).
- **Main loop**: consumes messages from the **task queue**.
  - Each message contains task id, execution id, and a **payload** (JSON). The worker looks for a `"command"` string (e.g. `"echo hello"`).
  - It runs that command in a **subprocess** (with a timeout).
  - It publishes a **result** to the **result queue** (task id, execution id, status, output, error).

So: **queue → run command → result queue**. Multiple worker processes can run in parallel (e.g. 2 replicas in Docker Compose); RabbitMQ distributes messages among them.

---

## Request flow (example: create and run a task)

1. You send **POST /api/v1/tasks** with JWT and body (task name, `command_payload`, schedule, etc.).
2. **API** inserts a row in **tasks** (e.g. `status = 'scheduled'`, `next_execution_time = ...`) and can publish to RabbitMQ.
3. **Coordinator** (poller) sees the due task in the DB, creates **task_executions** row, and publishes to **scheduler.tasks**.
4. A **Worker** consumes that message, runs the command, and publishes to **scheduler.results**.
5. **Coordinator** (result consumer) gets the result, updates **task_executions** and **tasks**. If recurring, it sets the next `next_execution_time`.
6. You call **GET /api/v1/tasks/:id/executions** to see the run in the API.

---

## How to run the whole project (Docker)

From the project root:

```bash
docker compose up -d
```

This starts:

- **postgres** (port 5432)
- **rabbitmq** (5672, management UI 15672)
- **api-gateway** (3000)
- **coordinator** (Python)
- **worker** (2 replicas by default)
- **prometheus** (9091)
- **grafana** (3001)

Then you can register, login, create tasks, and list executions as in the main README.

---

## Project folder structure (what you care about)

- **api-gateway/** – Node/TS app (you know this).
- **coordinator/** – Python: `main.py`, `poller.py`, `queue_consumer.py`, `registry.py`, `requirements.txt`, `Dockerfile`.
- **worker/** – Python: `main.py`, `requirements.txt`, `Dockerfile`.
- **database/init.sql** – PostgreSQL schema.
- **docker-compose.yml** – Defines all services and env vars.
- **docs/openapi.yaml** – REST API spec.
- **docs/PROJECT_OVERVIEW.md** – This file.
- **monitoring/** – Prometheus and Grafana config.

---

## Summary

- **API (Node/TS)** = HTTP interface + auth + DB and optional RabbitMQ.
- **PostgreSQL** = persistent storage for users, tasks, executions, workers.
- **RabbitMQ** = task queue + result queue for reliability and distribution.
- **Coordinator (Python)** = “due tasks” from DB → task queue; results from result queue → DB; marks idle workers offline.
- **Worker (Python)** = task queue → run command → result queue; registers and heartbeats in DB.

You can work entirely with **JS/TS (API), Python (Coordinator + Worker), Docker, and SQL** without touching Go.
