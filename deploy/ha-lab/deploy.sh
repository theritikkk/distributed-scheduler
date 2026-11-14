#!/usr/bin/env bash
# Deploy the current working tree to the three-node HA lab.
#
# Prerequisites:
#   - Terraform has been applied in deploy/ha-lab.
#   - AWS credentials are available to the shell.
#   - The operator IP in allowed_cidr can reach the API and monitoring ports.
#
# The generated .env is uploaded to the encrypted artifact bucket only for the
# duration of the deploy. Do not run this with `bash -x`.

set -euo pipefail

REGION="${AWS_REGION:-ap-south-1}"
TF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$TF_DIR/../.." && pwd)"
STAGE="$(mktemp -d "${TMPDIR:-/tmp}/scheduler-ha-deploy.XXXXXX")"
KEY=""
BUNDLE="$STAGE/scheduler-ha.tgz"
cleanup() {
  if [ -n "$KEY" ]; then
    aws s3 rm "s3://${BUCKET:-}/$KEY" --region "$REGION" >/dev/null 2>&1 || true
  fi
  rm -rf "$STAGE"
}
trap cleanup EXIT

log() {
  printf '==> %s\n' "$*"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_command aws
require_command terraform
require_command rsync
require_command openssl
require_command python3
require_command tar

log "reading Terraform outputs"
TF_JSON="$(terraform -chdir="$TF_DIR" output -json)"
eval "$(TF_JSON="$TF_JSON" python3 - <<'PY'
import json
import os
import shlex

data = json.loads(os.environ["TF_JSON"])
required = [
    "api_urls",
    "artifact_bucket",
    "node_ids",
    "node_private_ips",
    "node_public_ips",
    "node_availability_zones",
]
missing = [name for name in required if name not in data]
if missing:
    raise SystemExit(f"missing Terraform outputs: {', '.join(missing)}")

for name in ("node_ids", "node_private_ips", "node_public_ips", "node_availability_zones"):
    values = data[name]["value"]
    if sorted(values) != ["node1", "node2", "node3"]:
        raise SystemExit(f"{name} must contain node1, node2, and node3")

for name in ("artifact_bucket",):
    print(f"{name.upper()}={shlex.quote(str(data[name]['value']))}")
for name in ("node_ids", "node_private_ips", "node_public_ips", "node_availability_zones"):
    for node in ("node1", "node2", "node3"):
        key = name.replace("node_", "").replace("availability_zones", "az").upper()
        prefix = {
            "node_ids": "ID",
            "node_private_ips": "IP",
            "node_public_ips": "PUBLIC_IP",
            "node_availability_zones": "AZ",
        }[name]
        print(f"{node.upper()}_{prefix}={shlex.quote(str(data[name]['value'][node]))}")
PY
)"

BUCKET="$ARTIFACT_BUCKET"
ETCD_INITIAL_CLUSTER="node1=http://$NODE1_IP:2380,node2=http://$NODE2_IP:2380,node3=http://$NODE3_IP:2380"
NODE_IDS=("$NODE1_ID" "$NODE2_ID" "$NODE3_ID")
NODE_NAMES=("node1" "node2" "node3")
NODE_IPS=("$NODE1_IP" "$NODE2_IP" "$NODE3_IP")
NODE_PUBLIC_IPS=("$NODE1_PUBLIC_IP" "$NODE2_PUBLIC_IP" "$NODE3_PUBLIC_IP")
API_URLS=(
  "http://$NODE1_PUBLIC_IP:8080"
  "http://$NODE2_PUBLIC_IP:8080"
  "http://$NODE3_PUBLIC_IP:8080"
)

log "staging working tree"
mkdir -p "$STAGE/app"
rsync -a \
  --exclude '.git' \
  --exclude '.terraform' \
  --exclude 'screenshots' \
  --exclude '.pytest_cache' \
  --exclude '.ruff_cache' \
  --exclude '*.tfplan' \
  --exclude '*.tfstate' \
  --exclude '*.tfstate.*' \
  --exclude 'node_modules' \
  --exclude 'dist' \
  --exclude '.env' \
  --exclude '.env.save' \
  --exclude '__pycache__' \
  --exclude '*.pyc' \
  --exclude '.venv' \
  --exclude 'venv' \
  --exclude 'coverage' \
  --exclude '.coverage' \
  --exclude 'htmlcov' \
  --exclude '*.log' \
  --exclude '.DS_Store' \
  --exclude '._*' \
  --exclude 'monitoring/targets' \
  --exclude 'runtime' \
  "$ROOT/" "$STAGE/app/"

ENV_FILE="$TF_DIR/.env"
if [ -f "$ENV_FILE" ]; then
  log "reusing deployment .env from $ENV_FILE"
  install -m 600 "$ENV_FILE" "$STAGE/app/.env"
else
log "generating deployment .env"
PG_PASS="$(openssl rand -hex 24)"
PG_REPL_PASS="$(openssl rand -hex 24)"
PG_SUPER_PASS="$(openssl rand -hex 24)"
RMQ_PASS="$(openssl rand -hex 24)"
RMQ_COOKIE="$(openssl rand -hex 32)"
JWT_SECRET="$(openssl rand -hex 32)"
GF_PASS="$(openssl rand -hex 16)"
cat > "$STAGE/app/.env" <<EOF
POSTGRES_USER=scheduler
POSTGRES_PASSWORD=$PG_PASS
POSTGRES_DB=distributed_scheduler
POSTGRES_REPLICATION_PASSWORD=$PG_REPL_PASS
POSTGRES_SUPERUSER_PASSWORD=$PG_SUPER_PASS
RABBITMQ_DEFAULT_USER=scheduler
RABBITMQ_DEFAULT_PASS=$RMQ_PASS
RABBITMQ_ERLANG_COOKIE=$RMQ_COOKIE
JWT_SECRET=$JWT_SECRET
# Application traffic uses the local PgBouncer (transaction pooling); migrations
# and replication checks use HAProxy :5433 directly.
DATABASE_URL=postgresql://scheduler:$PG_PASS@host.docker.internal:6432/distributed_scheduler
RABBITMQ_URL=amqp://scheduler:$RMQ_PASS@host.docker.internal:5673
REDIS_URL=redis://host.docker.internal:6380/0
GF_SECURITY_ADMIN_USER=admin
GF_SECURITY_ADMIN_PASSWORD=$GF_PASS

APP_ENV=production
NODE_ENV=production
PORT=3000
CORS_ORIGIN=*
TRUST_PROXY=false
LOG_REQUESTS=true

TASK_MAX_RETRIES=3
SCHEDULER_RECONCILE_INTERVAL_SEC=120
RECONCILE_INTERVAL_JITTER_RATIO=0.2
SCHEDULER_JITTER_ENABLED=false
RETRY_JITTER_MODE=full
IDEMPOTENCY_ENFORCE=true

OUTBOX_ENABLED=true
OUTBOX_POLL_INTERVAL_SEC=1
OUTBOX_BATCH_SIZE=100
OUTBOX_MAX_ATTEMPTS=10
OUTBOX_BACKOFF_BASE_SEC=1
OUTBOX_BACKOFF_MAX_SEC=60
OUTBOX_MAX_PUBLISH_PER_SEC=100
OUTBOX_RETENTION_DAYS=7
OUTBOX_CLEANUP_INTERVAL_SEC=3600

SCHEDULED_EVENTS_ENABLED=true
SCHEDULED_EVENT_POLL_INTERVAL_SEC=1.0
SCHEDULED_EVENT_BATCH_SIZE=100
SCHEDULED_EVENT_MAX_ATTEMPTS=10
SCHEDULED_EVENT_BACKOFF_BASE_SEC=1
SCHEDULED_EVENT_BACKOFF_MAX_SEC=60
SCHEDULED_EVENT_RETENTION_DAYS=7
SCHEDULED_EVENT_CLEANUP_INTERVAL_SEC=3600
SCHEDULED_EVENT_MAX_PUBLISH_PER_SEC=100
RABBITMQ_QUEUE_TYPE=quorum

REAPER_ENABLED=true
REAPER_INTERVAL_SEC=15
PENDING_RETRY_GRACE_SEC=300
IDEMPOTENCY_CLEANUP_INTERVAL_SEC=3600
EXECUTION_LEASE_SEC=60
LEASE_HEARTBEAT_SEC=20

DB_POOL_MAX=15
DB_POOL_MAX_WAITERS=10
DB_POOL_HIGH_WATERMARK=0.80
DB_POOL_RESUME_WATERMARK=0.60
DISPATCH_RATE_PER_SEC=200
DISPATCH_BURST=20
RECONCILE_BATCH=100
RECONCILE_PACE_SEC=0.01

RATE_LIMIT_ENABLED=false
RATE_LIMIT_MODE=observe
RATE_LIMIT_LOGIN_PER_MIN=5
RATE_LIMIT_LOGIN_BURST=2
RATE_LIMIT_USER_TASKS_PER_MIN=60
RATE_LIMIT_USER_TASKS_BURST=10
ADMISSION_ENABLED=false
QUEUE_BACKLOG_HIGH=5000
QUEUE_BACKLOG_LOW=1000

BACKPRESSURE_MODE=observe
BACKPRESSURE_TASKS_HIGH=5000
BACKPRESSURE_TASKS_LOW=1000
BACKPRESSURE_RESULTS_HIGH=2000
BACKPRESSURE_RESULTS_LOW=500
BACKPRESSURE_DUE_HIGH=2000
BACKPRESSURE_POLL_SEC=5
BACKPRESSURE_STALE_PAUSE_SEC=15

WORKER_CONCURRENCY=2
MAX_OUTPUT_BYTES=1048576
EOF
chmod 600 "$STAGE/app/.env"
install -m 600 "$STAGE/app/.env" "$ENV_FILE"
fi

log "generating Prometheus file_sd targets"
mkdir -p "$STAGE/app/monitoring/targets"
python3 - "$STAGE/app/monitoring/targets" "$NODE1_IP" "$NODE2_IP" "$NODE3_IP" <<'PY'
import json
import pathlib
import sys

out = pathlib.Path(sys.argv[1])
ips = {"node1": sys.argv[2], "node2": sys.argv[3], "node3": sys.argv[4]}

def write(name, groups):
    (out / name).write_text(json.dumps(groups, indent=2) + "\n", encoding="utf-8")

write(
    "api.json",
    [
        {"targets": [f"{ip}:3000"], "labels": {"node": node, "topology": "ha"}}
        for node, ip in ips.items()
    ],
)
write(
    "coordinator.json",
    [
        {"targets": [f"{ip}:9090"], "labels": {"node": node, "topology": "ha"}}
        for node, ip in ips.items()
    ],
)
workers = []
for node, ip in ips.items():
    for index, port in enumerate((9101, 9102, 9103), start=1):
        workers.append({
            "targets": [f"{ip}:{port}"],
            "labels": {
                "node": node,
                "worker": f"{node}-worker-{index}",
                "topology": "ha",
            },
        })
write("worker.json", workers)
write(
    "rabbitmq.json",
    [
        {
            "targets": [f"{ips['node1']}:9419"],
            "labels": {"node": "node1", "topology": "ha"},
        }
    ],
)
write(
    "patroni.json",
    [
        {"targets": [f"{ip}:8008"], "labels": {"node": node, "topology": "ha"}}
        for node, ip in ips.items()
    ],
)
write(
    "haproxy.json",
    [
        {"targets": [f"{ip}:8404"], "labels": {"node": node, "topology": "ha"}}
        for node, ip in ips.items()
    ],
)
write(
    "pgbouncer.json",
    [
        {"targets": [f"{ip}:9127"], "labels": {"node": node, "topology": "ha"}}
        for node, ip in ips.items()
    ],
)
write(
    "redis.json",
    [
        {"targets": [f"{ip}:9121"], "labels": {"node": node, "topology": "ha"}}
        for node, ip in ips.items()
    ],
)
PY

log "creating bundle"
TAR_FLAGS=()
if tar --version 2>/dev/null | grep -qi bsdtar; then
  TAR_FLAGS=(--no-mac-metadata --no-xattrs)
fi
COPYFILE_DISABLE=1 tar "${TAR_FLAGS[@]}" -C "$STAGE/app" -czf "$BUNDLE" .

KEY="ha-deploy/$(date -u +%Y%m%dT%H%M%SZ)-$$.tgz"
log "uploading bundle to s3://$BUCKET/$KEY"
aws s3 cp "$BUNDLE" "s3://$BUCKET/$KEY" --region "$REGION" --sse AES256 >/dev/null
URL="$(aws s3 presign "s3://$BUCKET/$KEY" --region "$REGION" --expires-in 7200)"

log "waiting for EC2 status checks"
aws ec2 wait instance-status-ok --instance-ids "${NODE_IDS[@]}" --region "$REGION"

wait_for_ssm() {
  local instance_id="$1"
  local ping=""
  for _ in $(seq 1 120); do
    ping="$(aws ssm describe-instance-information \
      --region "$REGION" \
      --filters "Key=InstanceIds,Values=$instance_id" \
      --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null || true)"
    if [ "$ping" = "Online" ]; then
      return 0
    fi
    sleep 5
  done
  echo "SSM agent did not come online for $instance_id" >&2
  return 1
}

for index in 0 1 2; do
  log "waiting for SSM on ${NODE_NAMES[$index]} (${NODE_IDS[$index]})"
  wait_for_ssm "${NODE_IDS[$index]}"
done

send_ssm() {
  local instance_id="$1"
  local script="$2"
  local comment="$3"
  local b64
  local json_file
  b64="$(printf '%s' "$script" | base64 | tr -d '\n')"
  json_file="$(mktemp "$STAGE/ssm.XXXXXX")"
  python3 - "$b64" "$instance_id" "$comment" > "$json_file" <<'PY'
import json
import sys

b64, instance_id, comment = sys.argv[1], sys.argv[2], sys.argv[3]
command = f"echo '{b64}' | base64 -d | bash"
print(json.dumps({
    "DocumentName": "AWS-RunShellScript",
    "InstanceIds": [instance_id],
    "TimeoutSeconds": 7200,
    "Parameters": {"commands": [command]},
    "Comment": comment,
}))
PY
  aws ssm send-command \
    --region "$REGION" \
    --cli-input-json "file://$json_file" \
    --query 'Command.CommandId' --output text
}

wait_ssm_command() {
  local instance_id="$1"
  local command_id="$2"
  local status=""
  for _ in $(seq 1 900); do
    status="$(aws ssm get-command-invocation \
      --region "$REGION" \
      --command-id "$command_id" \
      --instance-id "$instance_id" \
      --query 'Status' --output text 2>/dev/null || echo Pending)"
    case "$status" in
      Success)
        aws ssm get-command-invocation \
          --region "$REGION" \
          --command-id "$command_id" \
          --instance-id "$instance_id" \
          --query 'StandardOutputContent' --output text | tail -30
        return 0
        ;;
      Failed|Cancelled|TimedOut)
        echo "SSM command $command_id on $instance_id ended with status $status" >&2
        aws ssm get-command-invocation \
          --region "$REGION" \
          --command-id "$command_id" \
          --instance-id "$instance_id" \
          --query 'StandardErrorContent' --output text >&2 || true
        aws ssm get-command-invocation \
          --region "$REGION" \
          --command-id "$command_id" \
          --instance-id "$instance_id" \
          --query 'StandardOutputContent' --output text >&2 || true
        return 1
        ;;
    esac
    sleep 5
  done
  echo "timed out waiting for SSM command $command_id on $instance_id" >&2
  return 1
}

prepare_script() {
  local node="$1"
  local ip="$2"
  cat <<EOF
#!/bin/bash
set -euo pipefail

for _ in {1..120}; do
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    break
  fi
  sleep 5
done
command -v docker
docker compose version
docker buildx version

curl -fsSL '$URL' -o /tmp/scheduler-ha.tgz
if [ -d /home/ec2-user/app ]; then
  cd /home/ec2-user/app
  docker compose -f docker-compose.ha-app.yml --profile monitoring down --remove-orphans >/dev/null 2>&1 || true
  docker compose -f docker-compose.ha-data.yml down --remove-orphans >/dev/null 2>&1 || true
fi
rm -rf /home/ec2-user/app
mkdir -p /home/ec2-user/app
tar -xzf /tmp/scheduler-ha.tgz -C /home/ec2-user/app
rm -f /tmp/scheduler-ha.tgz
cd /home/ec2-user/app
chmod 600 .env

for key in NODE_NAME NODE_IP NODE1_IP NODE2_IP NODE3_IP ETCD_INITIAL_CLUSTER; do
  sed -i "/^\${key}=/d" .env
done
cat >> .env <<ENV
NODE_NAME=$node
NODE_IP=$ip
NODE1_IP=$NODE1_IP
NODE2_IP=$NODE2_IP
NODE3_IP=$NODE3_IP
ETCD_INITIAL_CLUSTER=$ETCD_INITIAL_CLUSTER
ENV
chmod 600 .env

sed -i '/^# scheduler-ha-lab/,/^# end scheduler-ha-lab/d' /etc/hosts 2>/dev/null || true
cat >> /etc/hosts <<HOSTS
# scheduler-ha-lab
$NODE1_IP node1
$NODE2_IP node2
$NODE3_IP node3
# end scheduler-ha-lab
HOSTS

source .env
mkdir -p runtime
render() {
  sed \
    -e "s|__NODE_NAME__|\$NODE_NAME|g" \
    -e "s|__NODE_IP__|\$NODE_IP|g" \
    -e "s|__NODE1_IP__|\$NODE1_IP|g" \
    -e "s|__NODE2_IP__|\$NODE2_IP|g" \
    -e "s|__NODE3_IP__|\$NODE3_IP|g" \
    -e "s|__POSTGRES_DB__|\$POSTGRES_DB|g" \
    -e "s|__SCHEDULER_PASSWORD__|\$POSTGRES_PASSWORD|g" \
    -e "s|__REPLICATION_PASSWORD__|\$POSTGRES_REPLICATION_PASSWORD|g" \
    -e "s|__SUPERUSER_PASSWORD__|\$POSTGRES_SUPERUSER_PASSWORD|g" \
    "\$1" > "\$2"
}
render deploy/ha-lab/ha/patroni.yml.tmpl runtime/patroni.yml
render deploy/ha-lab/ha/patroni-post-bootstrap.sh.tmpl runtime/patroni-post-bootstrap.sh
render deploy/ha-lab/ha/haproxy.cfg.tmpl runtime/haproxy.cfg
render deploy/ha-lab/ha/nginx.conf.tmpl runtime/nginx.conf
render deploy/ha-lab/ha/redis-sentinel.conf.tmpl runtime/redis-sentinel.conf
render deploy/ha-lab/ha/rabbitmq.conf.tmpl runtime/rabbitmq.conf
render deploy/ha-lab/ha/pgbouncer.ini.tmpl runtime/pgbouncer.ini
render deploy/ha-lab/ha/pgbouncer-userlist.txt.tmpl runtime/pgbouncer-userlist.txt
chmod 755 runtime/patroni-post-bootstrap.sh
chown -R ec2-user:ec2-user /home/ec2-user/app
# edoburu/pgbouncer runs as uid/gid 70 and must be able to read the auth file.
chown 70:70 runtime/pgbouncer.ini runtime/pgbouncer-userlist.txt
chmod 600 runtime/pgbouncer-userlist.txt

docker compose -f docker-compose.ha-data.yml up -d
docker compose -f docker-compose.ha-data.yml ps
EOF
}

log "preparing all nodes (download, render, start data services)"
PREPARE_COMMANDS=()
for index in 0 1 2; do
  node="${NODE_NAMES[$index]}"
  id="${NODE_IDS[$index]}"
  ip="${NODE_IPS[$index]}"
  script="$(prepare_script "$node" "$ip")"
  cmd="$(send_ssm "$id" "$script" "scheduler HA prepare $node")"
  echo "$node: $cmd"
  PREPARE_COMMANDS+=("$id:$cmd")
done
for entry in "${PREPARE_COMMANDS[@]}"; do
  id="${entry%%:*}"
  cmd="${entry##*:}"
  wait_ssm_command "$id" "$cmd"
done

wait_rabbit_ready() {
  local id="$1"
  local script
  script="$(cat <<'EOF'
#!/bin/bash
set -euo pipefail
cd /home/ec2-user/app
for _ in {1..120}; do
  if docker compose -f docker-compose.ha-data.yml exec -T rabbitmq rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
    exit 0
  fi
  sleep 5
done
docker compose -f docker-compose.ha-data.yml ps || true
docker compose -f docker-compose.ha-data.yml logs --tail=80 rabbitmq || true
exit 1
EOF
)"
  local cmd
  cmd="$(send_ssm "$id" "$script" "scheduler HA wait rabbitmq")"
  wait_ssm_command "$id" "$cmd"
}

log "waiting for RabbitMQ on node1"
wait_rabbit_ready "$NODE1_ID"

rabbitmq_node_joined() {
  local node="$1"
  local script cmd out
  script="$(cat <<EOF
#!/bin/bash
set -euo pipefail
cd /home/ec2-user/app
status="\$(docker compose -f docker-compose.ha-data.yml exec -T rabbitmq rabbitmqctl cluster_status --formatter json 2>/dev/null || true)"
printf '%s' "\$status" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("joined" if "rabbit@$node" in d.get("running_nodes", []) else "stale")'
EOF
)"
  cmd="$(send_ssm "$NODE1_ID" "$script" "scheduler HA check rabbitmq $node")"
  out="$(wait_ssm_command "$NODE1_ID" "$cmd" || true)"
  printf '%s' "$out" | grep -q '^joined$'
}

rabbitmq_step() {
  local id="$1" node="$2" step="$3" script="$4"
  local cmd
  cmd="$(send_ssm "$id" "$script" "scheduler HA rabbitmq $step $node")"
  echo "$node $step: $cmd"
  wait_ssm_command "$id" "$cmd" || true
}

log "joining RabbitMQ nodes to the cluster"
for index in 1 2; do
  node="${NODE_NAMES[$index]}"
  id="${NODE_IDS[$index]}"
  if rabbitmq_node_joined "$node"; then
    echo "$node already in RabbitMQ cluster"
    continue
  fi
  # rabbitmqctl reset/join_cluster can terminate the SSM shell when the Erlang
  # node stops. Run each state transition as its own command so that a dead
  # shell cannot skip start_app.
  rabbitmq_step "$id" "$node" stop "$(cat <<'EOF'
#!/bin/bash
set -euo pipefail
cd /home/ec2-user/app
docker compose -f docker-compose.ha-data.yml exec -T rabbitmq rabbitmqctl stop_app || true
EOF
)"
  rabbitmq_step "$id" "$node" reset "$(cat <<'EOF'
#!/bin/bash
set -euo pipefail
cd /home/ec2-user/app
docker compose -f docker-compose.ha-data.yml exec -T rabbitmq rabbitmqctl reset || true
EOF
)"
  rabbitmq_step "$id" "$node" join "$(cat <<'EOF'
#!/bin/bash
set -euo pipefail
cd /home/ec2-user/app
docker compose -f docker-compose.ha-data.yml exec -T rabbitmq rabbitmqctl join_cluster rabbit@node1 || true
EOF
)"
  rabbitmq_step "$id" "$node" start "$(cat <<'EOF'
#!/bin/bash
set -euo pipefail
cd /home/ec2-user/app
docker compose -f docker-compose.ha-data.yml exec -T rabbitmq rabbitmqctl start_app
for _ in {1..60}; do
  if docker compose -f docker-compose.ha-data.yml exec -T rabbitmq rabbitmqctl status >/dev/null 2>&1; then
    exit 0
  fi
  sleep 2
done
exit 1
EOF
)"
done

log "waiting for three-node RabbitMQ cluster"
CLUSTER_SCRIPT="$(cat <<'EOF'
#!/bin/bash
set -euo pipefail
cd /home/ec2-user/app
for _ in {1..60}; do
  status="$(docker compose -f docker-compose.ha-data.yml exec -T rabbitmq rabbitmqctl cluster_status --formatter json 2>/dev/null || true)"
  if printf '%s' "$status" | python3 -c 'import json,sys; d=json.load(sys.stdin); r=set(d.get("running_nodes", [])); sys.exit(0 if {"rabbit@node1","rabbit@node2","rabbit@node3"} <= r else 1)' 2>/dev/null; then
    printf '%s\n' "$status"
    exit 0
  fi
  sleep 5
done
docker compose -f docker-compose.ha-data.yml exec -T rabbitmq rabbitmqctl cluster_status || true
exit 1
EOF
)"
CLUSTER_CMD="$(send_ssm "$NODE1_ID" "$CLUSTER_SCRIPT" "scheduler HA verify rabbitmq cluster")"
wait_ssm_command "$NODE1_ID" "$CLUSTER_CMD"

log "waiting for Patroni primary + replicas and enabling synchronous mode"
PATRONI_IMAGE="ongres/patroni:v4.0.4-pg16.6-build-6.39"
BOOTSTRAP_SCRIPT="$(cat <<EOF
#!/bin/bash
set -euo pipefail
cd /home/ec2-user/app
source .env
primary=0
replicas=0
leader=""
for _ in \$(seq 1 120); do
  primary=0
  replicas=0
  leader=""
  for ip in "$NODE1_IP" "$NODE2_IP" "$NODE3_IP"; do
    code=\$(curl -s -o /dev/null -w '%{http_code}' "http://\$ip:8008/primary" || true)
    if [ "\$code" = "200" ]; then primary=\$((primary + 1)); leader="\$ip"; fi
    code=\$(curl -s -o /dev/null -w '%{http_code}' "http://\$ip:8008/replica" || true)
    if [ "\$code" = "200" ]; then replicas=\$((replicas + 1)); fi
  done
  if [ "\$primary" -eq 1 ] && [ "\$replicas" -ge 2 ]; then
    echo "leader=\$leader replicas=\$replicas"
    break
  fi
  sleep 5
done
if [ "\$primary" -ne 1 ] || [ "\$replicas" -lt 2 ]; then
  echo "Patroni cluster did not reach one primary and two replicas" >&2
  exit 1
fi
curl -fsS -X PATCH -H 'Content-Type: application/json' -d '{"synchronous_mode":true}' "http://\$leader:8008/config" >/dev/null
sync_names=""
for _ in \$(seq 1 60); do
  sync_names=\$(docker run --rm --network host -e PGPASSWORD="\$POSTGRES_PASSWORD" \
    --entrypoint /usr/lib/postgresql/16.6/bin/psql "$PATRONI_IMAGE" \
    -h 127.0.0.1 -p 5433 -U scheduler -d distributed_scheduler \
    -tAc "show synchronous_standby_names" 2>/dev/null || true)
  if [ -n "\$sync_names" ] && [ "\$sync_names" != "''" ]; then
    echo "synchronous_standby_names=\$sync_names"
    break
  fi
  sleep 2
done
if [ -z "\$sync_names" ] || [ "\$sync_names" = "''" ]; then
  echo "synchronous standby did not attach" >&2
  exit 1
fi
EOF
)"
BOOTSTRAP_CMD="$(send_ssm "$NODE1_ID" "$BOOTSTRAP_SCRIPT" "scheduler HA bootstrap postgres")"
wait_ssm_command "$NODE1_ID" "$BOOTSTRAP_CMD"

log "applying database bootstrap and migrations"
MIGRATE_SCRIPT="$(cat <<'EOF'
#!/bin/bash
set -euo pipefail
cd /home/ec2-user/app
source .env
docker run --rm --network host \
  -v /home/ec2-user/app:/app \
  -w /app \
  -e DATABASE_URL="postgresql://scheduler:${POSTGRES_PASSWORD}@127.0.0.1:5433/distributed_scheduler" \
  python:3.12-slim \
  sh -c 'pip install --no-cache-dir psycopg2-binary >/dev/null && python scripts/migrate.py bootstrap'
EOF
)"
MIGRATE_CMD="$(send_ssm "$NODE1_ID" "$MIGRATE_SCRIPT" "scheduler HA migrate")"
wait_ssm_command "$NODE1_ID" "$MIGRATE_CMD"

log "starting RabbitMQ exporter on node1"
EXPORTER_SCRIPT="$(cat <<'EOF'
#!/bin/bash
set -euo pipefail
cd /home/ec2-user/app
docker compose -f docker-compose.ha-data.yml --profile monitoring up -d rabbitmq-exporter
EOF
)"
EXPORTER_CMD="$(send_ssm "$NODE1_ID" "$EXPORTER_SCRIPT" "scheduler HA start exporter")"
wait_ssm_command "$NODE1_ID" "$EXPORTER_CMD"

wait_pgbouncer_ready() {
  local id="$1"
  local script
  script="$(cat <<'EOF'
#!/bin/bash
set -euo pipefail
cd /home/ec2-user/app
for _ in {1..120}; do
  if docker compose -f docker-compose.ha-data.yml exec -T pgbouncer \
      pg_isready -h 127.0.0.1 -p 6432 -U scheduler -d distributed_scheduler >/dev/null 2>&1; then
    exit 0
  fi
  sleep 5
done
docker compose -f docker-compose.ha-data.yml ps || true
docker compose -f docker-compose.ha-data.yml logs --tail=80 pgbouncer || true
exit 1
EOF
)"
  local cmd
  cmd="$(send_ssm "$id" "$script" "scheduler HA wait pgbouncer")"
  wait_ssm_command "$id" "$cmd"
}

log "waiting for PgBouncer on all nodes"
for index in 0 1 2; do
  wait_pgbouncer_ready "${NODE_IDS[$index]}"
done

log "building and starting application services on all nodes"
APP_COMMANDS=()
for index in 0 1 2; do
  node="${NODE_NAMES[$index]}"
  id="${NODE_IDS[$index]}"
  if [ "$node" = "node1" ]; then
    profile="--profile monitoring"
  else
    profile=""
  fi
  script="$(cat <<EOF
#!/bin/bash
set -euo pipefail
cd /home/ec2-user/app
docker compose -f docker-compose.ha-app.yml $profile up -d --build
docker compose -f docker-compose.ha-app.yml $profile ps
EOF
)"
  cmd="$(send_ssm "$id" "$script" "scheduler HA start app $node")"
  echo "$node: $cmd"
  APP_COMMANDS+=("$id:$cmd")
done
for entry in "${APP_COMMANDS[@]}"; do
  id="${entry%%:*}"
  cmd="${entry##*:}"
  wait_ssm_command "$id" "$cmd"
done

# Quorum queue membership is fixed when a queue is first declared. If the
# queues were originally created before all three brokers joined (for example
# during a partial redeploy), later declarations do not add replicas. Add any
# missing members explicitly so a single broker loss cannot lose messages.
log "ensuring quorum queue membership across all brokers"
QUEUE_MEMBERS_SCRIPT="$(cat <<'EOF'
#!/bin/bash
set -euo pipefail
cd /home/ec2-user/app
for _ in {1..60}; do
  if docker compose -f docker-compose.ha-data.yml exec -T rabbitmq rabbitmqctl list_queues name 2>/dev/null | grep -qx scheduler.tasks; then
    break
  fi
  sleep 2
done
for queue in scheduler.tasks scheduler.due scheduler.results scheduler.tasks.dlq; do
  for node in rabbit@node1 rabbit@node2 rabbit@node3; do
    docker compose -f docker-compose.ha-data.yml exec -T rabbitmq \
      rabbitmq-queues add_member --vhost / "$queue" "$node" >/dev/null 2>&1 || true
  done
done
docker compose -f docker-compose.ha-data.yml exec -T rabbitmq \
  rabbitmqctl list_queues name type leader members 2>/dev/null || true
EOF
)"
QUEUE_MEMBERS_CMD="$(send_ssm "$NODE1_ID" "$QUEUE_MEMBERS_SCRIPT" "scheduler HA ensure quorum members")"
wait_ssm_command "$NODE1_ID" "$QUEUE_MEMBERS_CMD"

log "waiting for API through the per-node HAProxy"
HEALTHY_API=""
for _ in $(seq 1 90); do
  for url in "${API_URLS[@]}"; do
    if curl -fsS "$url/health" >/tmp/scheduler-ha-health.json 2>/dev/null; then
      HEALTHY_API="$url"
      cat /tmp/scheduler-ha-health.json
      break 2
    fi
  done
  sleep 5
done
if [ -z "$HEALTHY_API" ]; then
  echo "API health check failed on every node" >&2
  exit 1
fi

log "starting the nginx load balancer on node1"
NGINX_SCRIPT="$(cat <<'EOF'
#!/bin/bash
set -euo pipefail
cd /home/ec2-user/app
docker compose -f docker-compose.ha-data.yml --profile lb up -d nginx
for _ in {1..60}; do
  if curl -fsS http://127.0.0.1:8081/health >/dev/null 2>&1; then
    exit 0
  fi
  sleep 2
done
docker compose -f docker-compose.ha-data.yml logs --tail=80 nginx || true
exit 1
EOF
)"
NGINX_CMD="$(send_ssm "$NODE1_ID" "$NGINX_SCRIPT" "scheduler HA start nginx")"
wait_ssm_command "$NODE1_ID" "$NGINX_CMD"

log "deployment complete"
echo "API URLs:"
for url in "${API_URLS[@]}"; do
  echo "  $url"
done
echo "Load balancer: http://$NODE1_PUBLIC_IP:8081 (nginx on node1)"
echo "Grafana:    http://$NODE1_PUBLIC_IP:3001"
echo "Prometheus: http://$NODE1_PUBLIC_IP:9091"
echo "RabbitMQ:   http://$NODE1_PUBLIC_IP:15672"
echo "PgBouncer:  <node-ip>:6432 (local transaction pooler)"
echo "SSM:"
echo "  node1: aws ssm start-session --target $NODE1_ID --region $REGION"
echo "  node2: aws ssm start-session --target $NODE2_ID --region $REGION"
echo "  node3: aws ssm start-session --target $NODE3_ID --region $REGION"
