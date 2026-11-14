#!/usr/bin/env bash
# Three-node RabbitMQ quorum-queue partition drill.
#
#   ./tests/rabbitmq-quorum/run.sh
#   KEEP=1 ./tests/rabbitmq-quorum/run.sh
#
# The drill publishes a numbered backlog, isolates one broker (quorum still
# available), then isolates a second broker (no majority, queue pauses), and
# finally heals the cluster and verifies every message was consumed exactly
# once.
#
# Fresh named volumes used to intermittently fail the first broker boot with
# "/var/lib/rabbitmq/.erlang.cookie: eacces". The harness now creates the three
# volumes up front and seeds the shared Erlang cookie as uid/gid 100:101 before
# any broker starts. That removes the ownership race and keeps the drill
# deterministic.

set -euo pipefail

export PATH=/usr/local/bin:/opt/homebrew/bin:$PATH

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE=(docker compose -f "$HERE/docker-compose.yml")
NETWORK="scheduler-rabbitmq-quorum_default"
NODES=(scheduler-rmq-quorum-1 scheduler-rmq-quorum-2 scheduler-rmq-quorum-3)
IMAGE="rabbitmq:3.13-management-alpine"
COOKIE="scheduler-quorum-drill-cookie"

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
command -v python3 >/dev/null 2>&1 || {
  echo "python3 is required (standard library only)" >&2
  exit 1
}

seed_volume() {
  local index="$1"
  local volume="scheduler-rabbitmq-quorum_rabbitmq_${index}"
  docker volume create "$volume" >/dev/null
  docker run --rm --entrypoint sh -v "$volume:/data" "$IMAGE" -c "
    printf '%s' '$COOKIE' > /data/.erlang.cookie
    chown 100:101 /data/.erlang.cookie /data
    chmod 400 /data/.erlang.cookie
    chmod 1777 /data
  " >/dev/null
}

wait_for_node() {
  local container="$1"
  local timeout="$2"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if docker exec "$container" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
      return 0
    fi
    case "$(docker inspect -f '{{.State.Status}}' "$container" 2>/dev/null || echo missing)" in
      running) ;;
      *) return 1 ;;
    esac
    sleep 2
  done
  return 1
}

ensure_node() {
  local index="$1"
  local service="rabbitmq-${index}"
  local container="${NODES[$((index - 1))]}"

  for attempt in 1 2 3; do
    if wait_for_node "$container" 120; then
      return 0
    fi
    echo "==> $container did not become ready (attempt $attempt); recreating" >&2
    docker logs --tail 40 "$container" >&2 2>&1 || true
    "${COMPOSE[@]}" rm -sf "$service" >/dev/null 2>&1 || true
    seed_volume "$index"
    "${COMPOSE[@]}" up -d "$service" >/dev/null 2>&1 || true
  done
  return 1
}

echo "==> resetting any previous drill cluster"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true

echo "==> pre-seeding the shared Erlang cookie into three fresh volumes"
for index in 1 2 3; do
  seed_volume "$index"
done

echo "==> starting three RabbitMQ nodes"
"${COMPOSE[@]}" up -d

for index in 1 2 3; do
  if ! ensure_node "$index"; then
    echo "node $index did not become ready after retries" >&2
    "${COMPOSE[@]}" logs --tail=80
    exit 1
  fi
done

for index in 1 2; do
  node="${NODES[$index]}"
  echo "==> joining $node to the cluster"
  docker exec "$node" rabbitmqctl stop_app >/dev/null 2>&1 || true
  docker exec "$node" rabbitmqctl reset >/dev/null
  docker exec "$node" rabbitmqctl join_cluster rabbit@rabbitmq-1 >/dev/null
  docker exec "$node" rabbitmqctl start_app >/dev/null
done

echo "==> waiting for a three-node cluster"
for _ in $(seq 1 120); do
  running="$(docker exec "${NODES[0]}" rabbitmqctl cluster_status --formatter json 2>/dev/null \
    | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("running_nodes", [])))' 2>/dev/null || echo 0)"
  if [ "$running" = "3" ]; then
    break
  fi
  sleep 2
done
if [ "$running" != "3" ]; then
  echo "cluster did not reach three running nodes" >&2
  docker exec "${NODES[0]}" rabbitmqctl cluster_status || true
  exit 1
fi

echo "==> running partition drill"
RABBITMQ_NETWORK="$NETWORK" \
RABBITMQ_MANAGEMENT_URLS="http://127.0.0.1:25672,http://127.0.0.1:25673,http://127.0.0.1:25674" \
RABBITMQ_CONTAINERS="${NODES[*]}" \
RABBITMQ_USER=scheduler \
RABBITMQ_PASSWORD=scheduler_secret \
  python3 "$HERE/partition_test.py"
