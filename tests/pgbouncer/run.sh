#!/usr/bin/env bash
# Run the PgBouncer integration suite against a disposable PostgreSQL + pooler.
#
#   ./tests/pgbouncer/run.sh
#   KEEP=1 ./tests/pgbouncer/run.sh      # keep the stack for inspection
#
# Ports are deliberately outside the local E2E stack: PostgreSQL 55432,
# PgBouncer 56432, exporter 59127.

set -euo pipefail

export PATH=/usr/local/bin:/opt/homebrew/bin:$PATH

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE=(docker compose -f "$HERE/docker-compose.yml")

cleanup() {
  if [ "${KEEP:-0}" = "1" ]; then
    echo "keeping stack running; remove with: ${COMPOSE[*]} down -v"
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
  echo "python3 is required" >&2
  exit 1
}
python3 -c 'import psycopg2' >/dev/null 2>&1 || {
  echo "psycopg2 is required: pip install -r requirements-dev.txt" >&2
  exit 1
}
command -v curl >/dev/null 2>&1 || {
  echo "curl is required" >&2
  exit 1
}

echo "==> resetting any previous harness"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true

echo "==> starting PostgreSQL, PgBouncer, and the exporter"
"${COMPOSE[@]}" up -d

wait_for_postgres() {
  for _ in $(seq 1 60); do
    if "${COMPOSE[@]}" exec -T postgres pg_isready -U postgres -d postgres \
        >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "PostgreSQL did not become ready" >&2
  "${COMPOSE[@]}" logs --tail=80 postgres >&2 || true
  return 1
}

wait_for_pooler() {
  for _ in $(seq 1 60); do
    if "${COMPOSE[@]}" exec -T pgbouncer \
        pg_isready -h 127.0.0.1 -p 6432 -U scheduler -d distributed_scheduler \
        >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "PgBouncer did not become ready" >&2
  "${COMPOSE[@]}" logs --tail=80 pgbouncer >&2 || true
  return 1
}

wait_for_exporter() {
  for _ in $(seq 1 60); do
    if curl -fsS -o /dev/null http://127.0.0.1:59127/metrics 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "PgBouncer exporter did not become ready" >&2
  "${COMPOSE[@]}" logs --tail=80 pgbouncer-exporter >&2 || true
  return 1
}

wait_for_postgres
wait_for_pooler
wait_for_exporter

echo "==> running integration suite"
python3 "$HERE/test_pgbouncer.py"

if [ "${KEEP:-0}" = "1" ]; then
  echo
  echo "PgBouncer is at 127.0.0.1:56432; exporter at http://127.0.0.1:59127/metrics"
fi
