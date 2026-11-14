#!/usr/bin/env bash
# Validate the HA-lab nginx load-balancer template locally.
#
#   ./tests/nginx-lb/run.sh
#   KEEP=1 ./tests/nginx-lb/run.sh
#
# Three http-echo containers act as the per-node HAProxy API listeners. The
# test renders deploy/ha-lab/ha/nginx.conf.tmpl with their service names, then
# checks round-robin/least-conn distribution, passive failover, and recovery.

set -euo pipefail

export PATH=/usr/local/bin:/opt/homebrew/bin:$PATH

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
COMPOSE=(docker compose -f "$HERE/docker-compose.yml")
TEMPLATE="$ROOT/deploy/ha-lab/ha/nginx.conf.tmpl"
ENDPOINT="http://127.0.0.1:18081"

cleanup() {
  if [ "${KEEP:-0}" = "1" ]; then
    echo "keeping stack running; remove with: ${COMPOSE[*]} down -v"
    echo "rendered config: $HERE/runtime/nginx.conf"
    return
  fi
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$HERE/runtime"
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
command -v curl >/dev/null 2>&1 || {
  echo "curl is required" >&2
  exit 1
}

mkdir -p "$HERE/runtime"
sed \
  -e 's/__NODE1_IP__/backend1/g' \
  -e 's/__NODE2_IP__/backend2/g' \
  -e 's/__NODE3_IP__/backend3/g' \
  "$TEMPLATE" > "$HERE/runtime/nginx.conf"

echo "==> resetting any previous harness"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true

echo "==> starting three API backends and nginx"
"${COMPOSE[@]}" up -d

for _ in $(seq 1 60); do
  if curl -fsS "$ENDPOINT/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! curl -fsS "$ENDPOINT/health" >/dev/null 2>&1; then
  echo "nginx did not become ready" >&2
  "${COMPOSE[@]}" logs --tail=80 nginx >&2 || true
  exit 1
fi

echo "==> validating the rendered nginx configuration"
"${COMPOSE[@]}" exec -T nginx nginx -t

sample_backends() {
  local count="${1:-12}" result=""
  for _ in $(seq 1 "$count"); do
    local body
    body="$(curl -fsS "$ENDPOINT/api/probe")" || {
      echo "request failed" >&2
      return 1
    }
    result="$result$body\n"
  done
  printf '%b' "$result" | sed '/^$/d' | sort | uniq -c
}

echo "==> checking distribution across all three backends"
distribution="$(sample_backends 12)"
echo "$distribution" | sed 's/^/    /'
unique="$(printf '%s\n' "$distribution" | wc -l | tr -d ' ')"
if [ "$unique" -ne 3 ]; then
  echo "expected all three backends to receive traffic, got $unique" >&2
  exit 1
fi

echo "==> stopping backend1 and checking passive failover"
"${COMPOSE[@]}" stop backend1 >/dev/null
distribution="$(sample_backends 12)"
echo "$distribution" | sed 's/^/    /'
if printf '%s\n' "$distribution" | grep -q 'backend-1'; then
  echo "a stopped backend still received traffic" >&2
  exit 1
fi
if ! printf '%s\n' "$distribution" | grep -q 'backend-2'; then
  echo "backend-2 received no traffic after backend1 stopped" >&2
  exit 1
fi
if ! printf '%s\n' "$distribution" | grep -q 'backend-3'; then
  echo "backend-3 received no traffic after backend1 stopped" >&2
  exit 1
fi

echo "==> restarting backend1 and waiting for fail_timeout recovery"
"${COMPOSE[@]}" start backend1 >/dev/null
sleep 11
distribution="$(sample_backends 12)"
echo "$distribution" | sed 's/^/    /'
if ! printf '%s\n' "$distribution" | grep -q 'backend-1'; then
  echo "backend1 did not return to the upstream pool" >&2
  exit 1
fi

echo
echo "nginx load-balancer drill passed: distribution, failover, and recovery."
