#!/usr/bin/env bash
# Run the Redis Cluster integration suite against a disposable 6-node cluster.
#
#   ./tests/redis-cluster/run.sh                 # no failover test
#   REDIS_CLUSTER_FAILOVER=1 ./tests/redis-cluster/run.sh
#   KEEP=1 ./tests/redis-cluster/run.sh          # keep the cluster running
#
# The token-bucket Lua script is exercised through the real ioredis cluster
# client, including MOVED/ASK routing and (optionally) a manual master failover.

set -euo pipefail

export PATH=/usr/local/bin:/opt/homebrew/bin:$PATH

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
COMPOSE=(docker compose -f "$HERE/docker-compose.yml")
NODES="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006"
FAILOVER="${REDIS_CLUSTER_FAILOVER:-0}"

cleanup() {
  if [ "${KEEP:-0}" = "1" ]; then
    echo "keeping cluster running; remove with: ${COMPOSE[*]} down -v"
    return
  fi
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || {
  echo "docker is required" >&2
  exit 1
}
docker info >/dev/null 2>&1 || {
  echo "docker daemon is not running" >&2
  exit 1
}
command -v redis-cli >/dev/null 2>&1 || {
  echo "redis-cli is required on the host" >&2
  exit 1
}

echo "==> starting six Redis instances"
"${COMPOSE[@]}" up -d

echo "==> waiting for all nodes to answer PING"
for port in 7001 7002 7003 7004 7005 7006; do
  for _ in $(seq 1 60); do
    if redis-cli -h 127.0.0.1 -p "$port" ping 2>/dev/null | grep -qx PONG; then
      break
    fi
    sleep 1
  done
  if ! redis-cli -h 127.0.0.1 -p "$port" ping 2>/dev/null | grep -qx PONG; then
    echo "redis on port $port did not become ready" >&2
    "${COMPOSE[@]}" logs --tail=50
    exit 1
  fi
done

cluster_state() {
  redis-cli -h 127.0.0.1 -p 7001 cluster info 2>/dev/null \
    | tr -d '\r' \
    | awk -F: '/^cluster_state:/{print $2}'
}

if [ "$(cluster_state)" != "ok" ]; then
  echo "==> creating 3 masters + 3 replicas"
  "${COMPOSE[@]}" exec -T redis-cluster redis-cli --cluster create \
    127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 \
    127.0.0.1:7004 127.0.0.1:7005 127.0.0.1:7006 \
    --cluster-replicas 1 --cluster-yes
fi

echo "==> waiting for cluster_state:ok and six known nodes"
for _ in $(seq 1 120); do
  info="$(redis-cli -h 127.0.0.1 -p 7001 cluster info 2>/dev/null | tr -d '\r')"
  state="$(printf '%s\n' "$info" | awk -F: '/^cluster_state:/{print $2}')"
  known="$(printf '%s\n' "$info" | awk -F: '/^cluster_known_nodes:/{print $2}')"
  if [ "$state" = "ok" ] && [ "$known" = "6" ]; then
    break
  fi
  sleep 1
done
if [ "$(cluster_state)" != "ok" ]; then
  echo "cluster did not converge" >&2
  redis-cli -h 127.0.0.1 -p 7001 cluster info || true
  exit 1
fi

echo "==> running integration suite"
cd "$ROOT/api-gateway"
REDIS_CLUSTER_TEST=1 \
REDIS_CLUSTER_NODES="$NODES" \
REDIS_CLUSTER_FAILOVER="$FAILOVER" \
  npx vitest run tests/redis-cluster.integration.test.ts
