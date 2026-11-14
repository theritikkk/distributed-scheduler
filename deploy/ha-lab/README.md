# Three-node HA lab

This directory provisions and deploys the self-managed Phase 6 HA lab described in
[docs/HA.md](../../docs/HA.md). It is a **validation lab**, not production:

- Three Spot `m7i-flex.large` instances, one per AZ (`ap-south-1a/b/c` by default).
- etcd + Patroni PostgreSQL 16.6: leader, synchronous standby, async replica.
- RabbitMQ 3-node cluster with four quorum queues, each with three members.
- Redis master/replica with three Sentinels and a sentinel-aware proxy (`:6380`).
- Per-node PgBouncer in transaction-pooling mode (`:6432`).
- Per-node HAProxy for PostgreSQL, RabbitMQ, and the API.
- Optional nginx load balancer on node1 (`:8081`) in front of the per-node HAProxy.
- Application services with three API replicas and nine workers.
- Prometheus + Grafana on node1, scraping the application, RabbitMQ, Patroni, HAProxy,
  PgBouncer, and Redis.

There is **no managed NLB**: the account this was built in does not support creating
load balancers. Use nginx on `http://<node1-ip>:8081`, or reach any node's HAProxy
directly on `:8080`. The nginx endpoint is a lab convenience and node1 is a single point
of failure for it; see [docs/internals/08](../../docs/internals/08-load-balancing.md).

## Prerequisites

- Terraform 1.5+ and AWS CLI v2 with EC2, IAM, S3, and SSM permissions.
- `rsync`, `openssl`, `python3`, `tar`, and `base64`.
- The default VPC with at least three subnets in distinct AZs.
- A Spot vCPU quota large enough for three `m7i-flex.large` instances (6 vCPUs).
- Your public IP for `allowed_cidr` (API/monitoring ports are not world-open).

```bash
export PATH=/usr/local/bin:/opt/homebrew/bin:$PATH
export AWS_REGION=ap-south-1
```

## Provision

```bash
cd deploy/ha-lab
terraform init
terraform apply -var="allowed_cidr=$(curl -s https://checkip.amazonaws.com)/32"
```

This creates the instances, security groups, IAM instance profile with SSM, and an
encrypted S3 artifact bucket. It does **not** install Docker or deploy the application.

## Deploy

From the repository root:

```bash
./deploy/ha-lab/deploy.sh
```

The script:

1. Reads Terraform outputs and bundles the working tree (excluding `.env`).
2. Generates or reuses `deploy/ha-lab/.env`, uploads the bundle to S3, and runs the
   deployment over SSM Run Command.
3. Installs Docker, formats/mounts the data volume, writes the Patroni/HAProxy/Redis/
   RabbitMQ/PgBouncer/nginx templates, and starts the data plane, including PgBouncer
   and the exporters.
4. Forms the RabbitMQ cluster, waits for three nodes, applies migrations through
   HAProxy `:5433`, and starts the app services.
5. Adds any missing quorum-queue members, waits for API health on every node, then
   renders and starts nginx on node1 under the `lb` profile.

**Do not delete `deploy/ha-lab/.env` while the cluster exists.** It contains the Patroni
and RabbitMQ credentials baked into the data volumes. Reusing it makes reruns safe;
regenerating it can lock you out of the existing PostgreSQL data directory and break
the RabbitMQ cookie. The file is git-ignored by convention; keep it out of commits.

Reruns are intended to be idempotent, but prefer `deploy.sh` over ad-hoc `docker compose`
commands on the nodes. If a run fails midway, fix the cause and rerun; do not recreate
the data volumes unless you intend to lose the lab data.

## Access

Terraform outputs the addresses:

```bash
terraform output
terraform output -json api_urls
terraform output -json ssm_commands
```

| Endpoint | Where | Notes |
|----------|-------|-------|
| API (load balanced) | `http://<node1-ip>:8081` | nginx on node1; balances across the three HAProxy `:8080` listeners |
| API (per node) | `http://<node-ip>:8080` | Any node's HAProxy; rotates across API replicas |
| HAProxy stats | `http://<node-ip>:8404/` | Backend health for PostgreSQL/RabbitMQ/API; `/metrics` for Prometheus |
| Patroni | `http://<node-private-ip>:8008/cluster` | Reachable inside the VPC/SSM |
| PostgreSQL primary / replica | `<node-ip>:5433` / `:5434` | HAProxy, Patroni health-checked |
| PgBouncer | `<node-ip>:6432` | Local transaction pooler for app services |
| RabbitMQ AMQP / management | `<node-ip>:5673` / node1 `:15672` | Management only on node1 |
| Redis Sentinel proxy | `<node-ip>:6380` | Follows the elected master |
| Prometheus / Grafana | node1 `:9091` / `:3001` | Grafana admin password in `.env` |

SSM session:

```bash
aws ssm start-session --target <instance-id> --region ap-south-1
```

## Validate

Run the drills in [docs/runbooks/failover.md](../../docs/runbooks/failover.md). The
minimum checks after a deploy:

```bash
# Patroni: one leader, one sync standby, one replica
curl -s http://<node-private-ip>:8008/cluster | jq .

# Quorum queues: every queue must list three members
docker compose -f docker-compose.ha-data.yml exec -T rabbitmq \
  rabbitmqctl list_queues name type leader members

# Redis: master plus two replicas
docker run --rm --network host redis:7-alpine \
  redis-cli -h 127.0.0.1 -p 6380 info replication

# API health through each node
for ip in <node1-ip> <node2-ip> <node3-ip>; do
  curl -fsS "http://$ip:8080/health/ready"; echo
done

# nginx on node1 (load-balanced entry point)
curl -fsS http://<node1-ip>:8081/health

# PgBouncer on each node (transaction pooler)
for ip in <node1-ip> <node2-ip> <node3-ip>; do
  PGPASSWORD='<postgres-password>' psql \
    "host=$ip port=6432 user=scheduler dbname=distributed_scheduler" -c 'SELECT 1'
done

# Prometheus targets: expect all up
curl -fsS http://<node1-ip>:9091/api/v1/targets | jq \
  '[.data.activeTargets[] | select(.health != "up")] | length'
```

The validated v1.4.0 results from 2026-11-06 are in
[docs/HA.md](../../docs/HA.md#validated-lab-results-2026-11-06-v140-baseline) and the
drill log is in [docs/runbooks/failover.md](../../docs/runbooks/failover.md#drill-log).
The v1.5.0 additions (PgBouncer, PITR, nginx, Redis Cluster, quorum partition) were
validated locally; see
[docs/evolution/v1.5.0](../../docs/evolution/v1.5.0-data-plane-hardening.md).

The v1.6.0 re-validation on 2026-11-12 ran four live failover drills against a freshly
provisioned lab and includes screenshots, raw logs, and reproduction notes:
[`screenshots/deployment/test-7/`](../../screenshots/deployment/test-7/README.md).

## Destroy

The lab costs money while it runs (three Spot instances, EBS, and a small S3 bucket).
Stop it when you are done:

```bash
cd deploy/ha-lab
terraform destroy -var="allowed_cidr=$(curl -s https://checkip.amazonaws.com)/32"
```

Destroy removes the instances, volumes, security groups, IAM role, and artifact bucket.
`deploy/ha-lab/.env` is local and is **not** removed; delete it yourself only if you do
not plan to redeploy. Terraform state files in this directory are local too; keep them
out of commits.

## Known limitations

- Spot instances can be reclaimed with two minutes' notice; this is not a production
  availability guarantee.
- nginx runs on node1 only. Losing node1 removes the `:8081` entry point even though the
  per-node HAProxy `:8080` listeners and the data plane remain available. Run nginx on
  every node behind DNS or use a managed load balancer for production.
- PITR is a design plus local drill. `deploy.sh` does not create an S3 backup bucket or
  configure WAL-G/pgBackRest. `terraform destroy` deletes the data volumes.
- HAProxy, Patroni, PgBouncer, and Redis are now scrape targets with HA alert rules, but
  Alertmanager routing has not been tested against a real pager.
- Quorum queues and etcd both require a majority and pause writes without one. The
  quorum partition drill ran locally, not in this AWS lab after v1.5.0.
- Redis Cluster is supported in code and was tested locally; this lab still deploys
  Redis Sentinel, not a managed cluster.
- Admin AWS credentials used to build the lab must be rotated and replaced with a
  scoped deployment role before any shared or long-lived use.
