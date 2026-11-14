# Deployment Guide

The current stack is a single-node free-tier demo on `m7i-flex.large`
(Amazon Linux 2023, `ap-south-1`), provisioned with Terraform and managed
over AWS Systems Manager (SSM). PostgreSQL, RabbitMQ, Redis, the coordinator,
and all ten workers run on the same host. This is **not** HA.

A separate three-node, cross-AZ HA lab is in `deploy/ha-lab/` and is documented
in [HA.md](HA.md) and [`deploy/ha-lab/README.md`](../deploy/ha-lab/README.md).
It is where the Phase 6 failover drills ran, and v1.5.0 added PgBouncer, nginx,
Redis Cluster support, PITR design/local drill, and HA metrics. Those additions
were validated locally, not by re-provisioning AWS. v1.6.0 adds default-off
Phase 7 caches, tenant quotas, sharded ingress, tracing, and bounded archival,
also validated locally. See the
[version records](evolution/README.md) and [internals](internals/README.md). It
is also credit-based and not a production deployment.

## Current deployment (implemented)

| Resource | Value |
|----------|-------|
| Provider / region | AWS EC2, `ap-south-1` |
| Instance type | `m7i-flex.large` (8 GiB, free-tier eligible) |
| OS | Amazon Linux 2023 |
| Provisioning | Terraform in `deploy/free-tier/` |
| Access | SSM Session Manager; no SSH ingress |
| Deployment method | Docker Compose, bundle delivered through an encrypted S3 bucket |
| Reverse proxy / HTTPS | None (HTTP-on-port-3000 demo) |

### Provision

```bash
cd deploy/free-tier
terraform init
terraform apply -var="allowed_cidr=$(curl -s https://checkip.amazonaws.com)/32"
```

This creates one EC2 instance with an encrypted 30 GiB gp3 root volume, an
SSM instance profile, an encrypted artifact bucket, and a security group
restricted to the operator CIDR.

### Deploy the current working tree

```bash
cd deploy/free-tier
export INSTANCE_ID="$(terraform output -raw instance_id)"
export BUCKET="$(terraform output -raw artifact_bucket)"
export PUBLIC_IP="$(terraform output -raw public_ip)"
./deploy.sh
```

`deploy.sh` generates a fresh production `.env`, uploads a source bundle,
installs Docker/Compose/buildx, runs `docker compose up -d --build`, waits
for `/health`, and removes the artifact. It does not print or commit secrets.

### Security group

| Port | Source | Purpose |
|------|--------|---------|
| 3000 | Operator CIDR | API Gateway |
| 3001 | Operator CIDR | Grafana |
| 9091 | Operator CIDR | Prometheus |
| 15672 | Operator CIDR | RabbitMQ management UI |

Ports 22, 5432, 5672, and 3100 are not exposed. The instance is reached with
SSM, not SSH. Use a scoped deployment role rather than an admin user for
automation, and rotate setup credentials.

### Teardown

```bash
cd deploy/free-tier
terraform destroy
```

The free plan is credit-based; `m7i-flex.large` is free-tier eligible but
still consumes credits. Check the balance with
`aws freetier get-account-plan-state --region us-east-1`.

---

## What runs on the instance

21 containers, all managed by Docker Compose:

| Container | Ports (host) | Notes |
|-----------|-------------|-------|
| `api-gateway` | 3000 | Direct in the demo; Nginx is optional |
| `coordinator` | — | Internal only |
| `worker-1` … `worker-10` | 9101–9110 | Metrics |
| `postgres` | — (expose only) | Not reachable from host |
| `rabbitmq` | 15672 | Management UI only |
| `redis` | — (expose only) | Rate-limit token buckets and optional Phase 7 caches; not authoritative |
| `rabbitmq-exporter` | — (expose only) | Internal |
| `prometheus` | 9091 | |
| `alertmanager` | 9093 | |
| `grafana` | 3001 | |
| `loki` | — (expose only) | Internal |
| `promtail` | — | Reads Docker socket |

---

## Manual Ubuntu/Nginx setup (alternative)

The remaining sections describe the manual single-instance path. They are kept
for reference; the current demo uses Terraform and does not provision Nginx,
Certbot, or SSH ingress. Use the security-group rules in the current-deployment
section above for the demo.

### Setup (run once)

#### 1. Install Docker

```bash
sudo apt update && sudo apt upgrade -y
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker
docker compose version
```

#### 2. Clone the repo

```bash
git clone https://github.com/theritikkk/distributed-scheduler.git
cd distributed-scheduler
```

#### 3. Configure environment

```bash
cp .env.example .env
nano .env
```

Required values to set:

```env
POSTGRES_PASSWORD=<strong password>
RABBITMQ_DEFAULT_PASS=<strong password>
JWT_SECRET=<run: openssl rand -hex 32>
GF_SECURITY_ADMIN_PASSWORD=<strong password>

# Update passwords inside these too:
DATABASE_URL=postgresql://scheduler:<password>@postgres:5432/distributed_scheduler
RABBITMQ_URL=amqp://scheduler:<password>@rabbitmq:5672

# Phase 1 scheduling defaults (jitter is opt-in for existing tasks)
SCHEDULER_JITTER_ENABLED=false
DEFAULT_RECURRING_JITTER_SEC=0
MAX_JITTER_SEC=300
RETRY_JITTER_MODE=full
RECONCILE_INTERVAL_JITTER_RATIO=0.2

# Phase 2 durability
IDEMPOTENCY_ENFORCE=true
REAPER_ENABLED=true
REAPER_INTERVAL_SEC=15
PENDING_RETRY_GRACE_SEC=300
EXECUTION_LEASE_SEC=60
LEASE_HEARTBEAT_SEC=20

# Phase 3 transactional outbox (set false in every service for rollback)
OUTBOX_ENABLED=true
OUTBOX_POLL_INTERVAL_SEC=1
OUTBOX_BATCH_SIZE=100
OUTBOX_MAX_ATTEMPTS=10
OUTBOX_BACKOFF_BASE_SEC=1
OUTBOX_BACKOFF_MAX_SEC=60
OUTBOX_MAX_PUBLISH_PER_SEC=100
OUTBOX_RETENTION_DAYS=7
OUTBOX_CLEANUP_INTERVAL_SEC=3600

# Phase 6 DB-backed timers (required for quorum queues)
SCHEDULED_EVENTS_ENABLED=true
SCHEDULED_EVENT_POLL_INTERVAL_SEC=1.0
SCHEDULED_EVENT_BATCH_SIZE=100
SCHEDULED_EVENT_MAX_ATTEMPTS=10
SCHEDULED_EVENT_BACKOFF_BASE_SEC=1
SCHEDULED_EVENT_BACKOFF_MAX_SEC=60
SCHEDULED_EVENT_RETENTION_DAYS=7
SCHEDULED_EVENT_CLEANUP_INTERVAL_SEC=3600
SCHEDULED_EVENT_MAX_PUBLISH_PER_SEC=100
# classic for the single-node Compose stack; quorum after provisioning HA
RABBITMQ_QUEUE_TYPE=classic

# Phase 4 layered rate limiting (start off/observe)
RATE_LIMIT_ENABLED=false
RATE_LIMIT_MODE=observe
REDIS_URL=redis://redis:6379/0
GLOBAL_INGRESS_RPS=2000
GLOBAL_INGRESS_BURST=4000
RATE_LIMIT_LOGIN_PER_MIN=5
RATE_LIMIT_LOGIN_BURST=2
RATE_LIMIT_USER_TASKS_PER_MIN=60
RATE_LIMIT_USER_TASKS_BURST=10
ADMISSION_ENABLED=false
DB_POOL_HIGH_WATERMARK=0.80
DB_POOL_MAX_WAITERS=5
QUEUE_BACKLOG_HIGH=5000
QUEUE_BACKLOG_LOW=1000
WORKER_CONCURRENCY=1
MAX_OUTPUT_BYTES=1048576

# Phase 5 coordinator backpressure (observe first)
BACKPRESSURE_MODE=observe
BACKPRESSURE_TASKS_HIGH=5000
BACKPRESSURE_TASKS_LOW=1000
BACKPRESSURE_RESULTS_HIGH=2000
BACKPRESSURE_RESULTS_LOW=500
BACKPRESSURE_POLL_SEC=1.0
DISPATCH_RATE_PER_SEC=200
DISPATCH_BURST=20
DB_POOL_MAX=20
DB_POOL_RESUME_WATERMARK=0.60
DB_STATEMENT_TIMEOUT_MS=5000
DB_LOCK_TIMEOUT_MS=2000
RECONCILE_BATCH=100
RECONCILE_PACE_SEC=0.01

# Phase 7 optional accelerators (all default-off, all fail open to PostgreSQL)
CACHE_ENABLED=false
CACHE_TTL_SEC=15
CACHE_REDIS_TIMEOUT_MS=50
IDEMPOTENCY_CACHE_ENABLED=false
IDEMPOTENCY_CACHE_TTL_SEC=900
TENANT_QUOTAS_ENABLED=false
TENANT_QUOTA_CACHE_TTL_SEC=30
TENANT_DEFAULT_WEIGHT=1
TENANT_DEFAULT_TASKS_PER_MIN=60
TENANT_DEFAULT_BURST=10
GLOBAL_INGRESS_SHARDS=16
OTEL_ENABLED=false
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces
OTEL_SAMPLER_ARG=0.1
ARCHIVE_ENABLED=false
ARCHIVE_RETENTION_DAYS=30
ARCHIVE_BATCH_SIZE=500
ARCHIVE_INTERVAL_SEC=3600
ARCHIVE_MAX_BATCHES=20
ARCHIVE_SLEEP_MS=50
```

Before deploying this version over an existing database, run the staged migration from a
host that can reach PostgreSQL (or a one-off container with the repo mounted):

```bash
# Phase 1 (if not already applied)
make migrate MIGRATE_TARGET=0004   # additive columns + unique slot index
# deploy Phase 1 API/coordinator/worker
make migrate                      # NOT NULL constraints and checks

# Phase 2
make migrate MIGRATE_TARGET=0007  # idempotency, leases, attempts + backfill
# deploy Phase 2 API/coordinator/worker; reaper must be running
make migrate                      # one-active-execution unique index

# Phase 3 (additive; safe over Phase 2)
make migrate                      # outbox table, indexes, retention index
# deploy Phase 3 API/coordinator/worker together; the coordinator starts the relay

# Phase 6 (additive; apply before deploying the Phase 6 images)
make migrate                      # scheduled_events table and indexes
# deploy API/coordinator/worker together with SCHEDULED_EVENTS_ENABLED=true.
# Watch scheduler_scheduled_events_pending/lag and scheduler_outbox_pending_rows.

# Phase 7 (additive; apply before deploying the Phase 7 images)
make migrate                      # 0011 tenant_quotas, 0012 archive tables, 0013 timer headers
# deploy with all Phase 7 flags off, then enable cache -> idempotency cache ->
# tenant quotas (observe first) -> tracing -> archival. Run:
make archive-data ARCHIVE_DRY_RUN=1
# before setting ARCHIVE_ENABLED=true.

# Phase 4/5 have no schema migration. Deploy the new images with rate limiting and
# backpressure disabled/observe, then enable them one replica at a time.
# Redis is a new Compose service; rate limiting uses it when enabled and falls back
# locally if it is unavailable.
```

`0007` fails if a task has more than one pending/running execution; resolve those rows
before applying `0008`. Do not run the Phase 1 contract migration while an old
coordinator that inserts NULL `task_executions.scheduled_for` is still running. Do not
run `0008` before the reaper is deployed.

For a Phase 3 rollback, set `OUTBOX_ENABLED=false` in the API, coordinator, and workers
before rolling back the images. Direct publisher-confirm delivery resumes. Keep the
additive outbox table and do not delete pending rows; `0009_outbox` has an intentional
no-op down migration.

For Phase 6 rollback, set `SCHEDULED_EVENTS_ENABLED=false` and `OUTBOX_ENABLED=false`
together so the direct TTL path resumes, or keep the outbox on and leave timers in DB.
The `scheduled_events` table is intentionally retained; reconciliation and the reaper
recover affected tasks. Do not switch to `RABBITMQ_QUEUE_TYPE=quorum` until the old TTL
queues are drained and deleted; see [HA](HA.md).

For Phase 4/5 rollback, set `RATE_LIMIT_MODE=observe` (or `RATE_LIMIT_ENABLED=false`),
`ADMISSION_ENABLED=false`, `WORKER_CONCURRENCY=1`, and `BACKPRESSURE_MODE=observe`. No
schema rollback is required. See [runbooks](runbooks/) for the incident procedures.

For Phase 7 rollback, use flags rather than schema rollback: `CACHE_ENABLED=false`,
`IDEMPOTENCY_CACHE_ENABLED=false`, `TENANT_QUOTAS_ENABLED=false` (or
`RATE_LIMIT_MODE=observe`), `GLOBAL_INGRESS_SHARDS=1`, `OTEL_ENABLED=false`, and
`ARCHIVE_ENABLED=false`. The additive tables and `scheduled_events.headers` can stay.
Dropping `0012` destroys archived history; export it first. See the
[Phase 7 runbook](runbooks/phase7-scaling.md).

#### 4. Start everything

```bash
docker compose up -d --build
docker compose ps   # all containers should show Up
```

#### 5. Nginx reverse proxy

```bash
sudo apt install nginx -y

sudo tee /etc/nginx/sites-available/scheduler > /dev/null << 'NGINXEOF'
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
NGINXEOF

sudo ln -s /etc/nginx/sites-available/scheduler /etc/nginx/sites-enabled/scheduler
sudo nginx -t && sudo systemctl restart nginx
```

#### 6. Firewall

```bash
sudo ufw allow OpenSSH
sudo ufw allow 80
sudo ufw allow 443
sudo ufw enable
```

#### 7. HTTPS (requires a domain pointed at the EC2 IP)

```bash
sudo apt install certbot python3-certbot-nginx -y
sudo certbot --nginx -d yourdomain.com
sudo systemctl status certbot.timer   # verify auto-renewal
```

> Note: HTTPS requires a real domain with an A record pointing to your EC2 IP. It will not work with a raw IP address. Let's Encrypt failed when run against `yourdomain.com` (placeholder) — replace with your actual domain.

---

## Verify the stack

```bash
# API health
curl http://YOUR_EC2_IP:3000/health
# {"status":"ok","service":"api-gateway"}

# All containers running
docker compose ps
```

Open in browser:

| Service | URL |
|---------|-----|
| Grafana | `http://YOUR_EC2_IP:3001` |
| Prometheus | `http://YOUR_EC2_IP:9091` |
| RabbitMQ UI | `http://YOUR_EC2_IP:15672` |

### Live evidence

There are two separate evidence packages. Keep their labels when linking them:

1. **Local Docker E2E (test-6).** API health and a completed task, authoritative
   PostgreSQL task/outbox rows plus the Phase 7 schema, coordinator and worker
   logs, drained RabbitMQ queues, 14/14 Prometheus targets, Grafana, and the
   300-task burst benchmark (300/300 completed, 0 failures, 163.1 req/s
   submission, 17.4 s drain across ten workers). This is **local evidence, not
   an AWS capture**.
2. **AWS HA lab (test-7).** A fresh three-node `ap-south-1` lab was provisioned
   on 2026-11-12 and exercised with four live drills: PostgreSQL primary loss
   (720/720, 1 s promotion, RPO 0), RabbitMQ queue-leader loss with a 200-message
   backlog (200/200 exactly once), Redis master loss with enforce-mode rate
   limiting (0 5xx, Sentinel promotion in 5.95 s), and API/HAProxy node failure
   through nginx (717/720 accepted, all accepted completed). The lab was
   destroyed after capture.

- [Local E2E evidence and reproduction commands](../screenshots/deployment/test-6/README.md)
- [Local 300-task benchmark result](../screenshots/deployment/test-6/benchmark.png)
- [AWS HA lab evidence and drill results](../screenshots/deployment/test-7/README.md)
- [Final architecture](assets/architecture.png)
- [Phase 7 scaling plane](assets/phase7-scaling.png)

The test-6 images are local evidence only and must not be presented as an AWS
capture. Re-provision `deploy/ha-lab/` to reproduce the test-7 drills.

---

## Updating the deployment

The demo is updated by rebuilding the working tree and letting the deploy script
replace the remote app directory. The script runs `docker compose down` without
`-v`, so PostgreSQL and RabbitMQ volumes are preserved.

```bash
cd deploy/free-tier
./deploy.sh
```

For a manual update on an existing Ubuntu host, use `docker compose up -d --build`
and then `docker image prune -f`.

---

## Useful debugging commands

On the free-tier instance there is no SSH ingress. Use SSM Session Manager:

```bash
aws ssm start-session --target "$INSTANCE_ID" --region ap-south-1
cd /home/ec2-user/app
docker compose ps
docker compose logs -f coordinator
docker compose logs -f worker-1
docker compose stats
docker compose exec -T postgres sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

For the manual Ubuntu path, run the equivalent commands over SSH. Never expose
5432, 5672, or 3100 publicly.

---

## Security checklist

- [x] All passwords changed from `.env.example` defaults
- [x] `JWT_SECRET` generated with `openssl rand -hex 32`
- [x] `.env` in `.gitignore`, never committed
- [x] PostgreSQL not exposed publicly (`expose:` not `ports:`)
- [x] RabbitMQ AMQP port (5672) not exposed publicly
- [x] Loki (3100) not exposed publicly
- [x] Grafana login enabled, sign-up disabled
- [x] `restart: unless-stopped` on all services
- [x] Grafana and Prometheus restricted to the operator CIDR in the security group
- [x] No SSH ingress; instance managed through SSM
- [ ] HTTPS enabled (requires a domain or a TLS terminator)
- [ ] Deployment automation uses a scoped role, not an admin user
- [ ] Setup credentials rotated after provisioning