#!/usr/bin/env bash
# Deploy the current working tree to the single-node free-tier EC2 instance.
#
# Required environment:
#   INSTANCE_ID  EC2 instance id (terraform output instance_id)
#   BUCKET       artifact bucket (terraform output artifact_bucket)
# Optional:
#   AWS_REGION   default ap-south-1
#   PUBLIC_IP    for the final smoke-test URL
#
# The caller must have AWS credentials in the environment. The generated .env is
# uploaded to the encrypted artifact bucket only for the duration of the deploy.

set -euo pipefail

REGION="${AWS_REGION:-ap-south-1}"
: "${INSTANCE_ID:?set INSTANCE_ID from terraform output}"
: "${BUCKET:?set BUCKET from terraform output}"
PUBLIC_IP="${PUBLIC_IP:-}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STAGE="$(mktemp -d "${TMPDIR:-/tmp}/scheduler-deploy.XXXXXX")"
KEY="deploy/$(date -u +%Y%m%dT%H%M%SZ)-$$.tgz"
BUNDLE="$STAGE/scheduler.tgz"
cleanup() {
  rm -rf "$STAGE"
}
trap cleanup EXIT

echo "==> staging working tree"
mkdir -p "$STAGE/app"
rsync -a \
  --exclude '.git' \
  --exclude '.terraform' \
  --exclude 'screenshots' \
  --exclude '.pytest_cache' \
  --exclude '.ruff_cache' \
  --exclude '*.tfplan' \
  --exclude '*.tfstate*' \
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
  "$ROOT/" "$STAGE/app/"


echo "==> generating deployment .env"
PG_PASS="$(openssl rand -hex 24)"
RMQ_PASS="$(openssl rand -hex 24)"
JWT_SECRET="$(openssl rand -hex 32)"
GF_PASS="$(openssl rand -hex 16)"
cat > "$STAGE/app/.env" <<EOF
POSTGRES_USER=scheduler
POSTGRES_PASSWORD=$PG_PASS
POSTGRES_DB=distributed_scheduler
RABBITMQ_DEFAULT_USER=scheduler
RABBITMQ_DEFAULT_PASS=$RMQ_PASS
JWT_SECRET=$JWT_SECRET
DATABASE_URL=postgresql://scheduler:$PG_PASS@postgres:5432/distributed_scheduler
RABBITMQ_URL=amqp://scheduler:$RMQ_PASS@rabbitmq:5672
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
RABBITMQ_QUEUE_TYPE=classic

REAPER_ENABLED=true
REAPER_INTERVAL_SEC=15
PENDING_RETRY_GRACE_SEC=300
IDEMPOTENCY_CLEANUP_INTERVAL_SEC=3600
EXECUTION_LEASE_SEC=60
LEASE_HEARTBEAT_SEC=20

DB_POOL_MAX=10
DB_POOL_MAX_WAITERS=5
DB_POOL_HIGH_WATERMARK=0.80
DB_POOL_RESUME_WATERMARK=0.60
DISPATCH_RATE_PER_SEC=200
DISPATCH_BURST=20
RECONCILE_BATCH=100
RECONCILE_PACE_SEC=0.01

REDIS_URL=redis://redis:6379/0
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

WORKER_CONCURRENCY=1
MAX_OUTPUT_BYTES=1048576
EOF
chmod 600 "$STAGE/app/.env"

echo "==> creating bundle"
TAR_FLAGS=()
if tar --version 2>/dev/null | grep -qi bsdtar; then
  TAR_FLAGS=(--no-mac-metadata --no-xattrs)
fi
COPYFILE_DISABLE=1 tar "${TAR_FLAGS[@]}" -C "$STAGE/app" -czf "$BUNDLE" .

echo "==> uploading bundle to s3://$BUCKET/$KEY"
aws s3 cp "$BUNDLE" "s3://$BUCKET/$KEY" --region "$REGION" --sse AES256
URL="$(aws s3 presign "s3://$BUCKET/$KEY" --region "$REGION" --expires-in 3600)"

echo "==> waiting for EC2 status checks"
aws ec2 wait instance-status-ok --instance-ids "$INSTANCE_ID" --region "$REGION"

echo "==> waiting for SSM agent"
for _ in $(seq 1 120); do
  PING="$(aws ssm describe-instance-information \
    --region "$REGION" \
    --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
    --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null || true)"
  if [ "$PING" = "Online" ]; then
    break
  fi
  sleep 5
done
if [ "${PING:-}" != "Online" ]; then
  echo "SSM agent did not come online" >&2
  exit 1
fi

REMOTE_SCRIPT="$(cat <<EOF
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
curl -fsSL '$URL' -o /tmp/scheduler-app.tgz
if [ -f /home/ec2-user/app/docker-compose.yml ]; then
  cd /home/ec2-user/app && docker compose down --remove-orphans || true
fi
rm -rf /home/ec2-user/app
mkdir -p /home/ec2-user/app
tar -xzf /tmp/scheduler-app.tgz -C /home/ec2-user/app
chmod 600 /home/ec2-user/app/.env
chown -R ec2-user:ec2-user /home/ec2-user/app
cd /home/ec2-user/app
docker compose up -d --build
for _ in {1..90}; do
  if curl -fsS http://localhost:3000/health >/tmp/health.json 2>/dev/null; then
    cat /tmp/health.json
    exit 0
  fi
  sleep 2
done
docker compose ps || true
docker compose logs --tail=80 api-gateway coordinator worker-1 postgres rabbitmq || true
exit 1
EOF
)"
B64="$(printf '%s' "$REMOTE_SCRIPT" | base64 | tr -d '\n')"

python3 - "$B64" "$INSTANCE_ID" > "$STAGE/ssm.json" <<'PY'
import json
import sys

b64, instance_id = sys.argv[1], sys.argv[2]
command = f"echo '{b64}' | base64 -d | bash"
print(json.dumps({
    "DocumentName": "AWS-RunShellScript",
    "InstanceIds": [instance_id],
    "TimeoutSeconds": 7200,
    "Parameters": {"commands": [command]},
    "Comment": "scheduler free-tier deploy",
}))
PY

echo "==> sending SSM deploy command"
COMMAND_ID="$(aws ssm send-command \
  --region "$REGION" \
  --cli-input-json "file://$STAGE/ssm.json" \
  --query 'Command.CommandId' --output text)"
echo "command id: $COMMAND_ID"

for _ in $(seq 1 720); do
  STATUS="$(aws ssm get-command-invocation \
    --region "$REGION" \
    --command-id "$COMMAND_ID" \
    --instance-id "$INSTANCE_ID" \
    --query 'Status' --output text 2>/dev/null || echo Pending)"
  case "$STATUS" in
    Success)
      aws ssm get-command-invocation \
        --region "$REGION" \
        --command-id "$COMMAND_ID" \
        --instance-id "$INSTANCE_ID" \
        --query 'StandardOutputContent' --output text | tail -40
      break
      ;;
    Failed|Cancelled|TimedOut)
      echo "deploy command ended with status $STATUS" >&2
      aws ssm get-command-invocation \
        --region "$REGION" \
        --command-id "$COMMAND_ID" \
        --instance-id "$INSTANCE_ID" \
        --query 'StandardErrorContent' --output text >&2 || true
      exit 1
      ;;
  esac
  sleep 5
done

aws s3 rm "s3://$BUCKET/$KEY" --region "$REGION" >/dev/null

echo "==> deployed"
if [ -n "$PUBLIC_IP" ]; then
  echo "API:        http://$PUBLIC_IP:3000/health"
  echo "Grafana:    http://$PUBLIC_IP:3001"
  echo "Prometheus: http://$PUBLIC_IP:9091"
  echo "RabbitMQ:   http://$PUBLIC_IP:15672"
fi
