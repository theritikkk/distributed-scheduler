#!/usr/bin/env bash
# Point-in-time recovery drill using pg_basebackup + archived WAL.
#
#   ./tests/pitr/run.sh
#   KEEP=1 ./tests/pitr/run.sh      # keep the primary and backup volume
#
# The drill inserts batches around a base backup, then restores the same base
# backup twice: once to the first batch and once to the second batch. Each
# restore must promote and stop replaying at the requested time. A final
# post-target batch proves the restored copy does not contain later work.
#
# Ports are outside the local E2E stack: primary 55433, restore 55434.

set -euo pipefail

export PATH=/usr/local/bin:/opt/homebrew/bin:$PATH

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE=(docker compose -f "$HERE/docker-compose.yml")
PROJECT="scheduler-pitr-test"
BACKUP_VOLUME="${PROJECT}_pitr_backup"
RESTORE_NAME="scheduler-pitr-restore"
RESTORE_PORT=55434

cleanup() {
  docker rm -f "$RESTORE_NAME" >/dev/null 2>&1 || true
  if [ "${KEEP:-0}" = "1" ]; then
    echo "keeping PITR stack running; remove with: ${COMPOSE[*]} down -v"
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

chmod +x "$HERE/restore-entrypoint.sh"

sql() {
  "${COMPOSE[@]}" exec -T postgres \
    psql -U postgres -d pitr_test -Atq -c "$1" | tr -d '\r'
}

archive_count() {
  sql "SELECT archived_count FROM pg_stat_archiver;"
}

wait_for_primary() {
  for _ in $(seq 1 60); do
    if "${COMPOSE[@]}" exec -T postgres \
        pg_isready -U postgres -d pitr_test >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "primary did not become ready" >&2
  "${COMPOSE[@]}" logs --tail=80 postgres >&2 || true
  return 1
}

wait_for_rows() {
  local expected="$1"
  for _ in $(seq 1 60); do
    local count
    count="$(sql "SELECT count(*) FROM pitr_events;")"
    if [ "$count" = "$expected" ]; then
      return 0
    fi
    sleep 1
  done
  echo "expected $expected baseline rows" >&2
  return 1
}

wait_for_archive() {
  local before="$1"
  for _ in $(seq 1 60); do
    local after
    after="$(archive_count)"
    if [ "${after:-0}" -gt "${before:-0}" ]; then
      return 0
    fi
    sleep 1
  done
  echo "WAL archive did not advance" >&2
  sql "SELECT archived_count, last_archived_wal, last_failed_wal FROM pg_stat_archiver;" >&2 || true
  "${COMPOSE[@]}" logs --tail=80 postgres >&2 || true
  return 1
}

insert_batch() {
  local start="$1" end="$2" note="$3"
  sql "INSERT INTO pitr_events (id, note)
       SELECT value, '$note' FROM generate_series($start, $end) AS value;" >/dev/null
  local before timestamp
  before="$(archive_count)"
  timestamp="$(sql "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC',
                                  'YYYY-MM-DD\"T\"HH24:MI:SS.US') || '+00';")"
  sql "SELECT pg_switch_wal();" >/dev/null
  wait_for_archive "$before"
  printf '%s' "$timestamp"
}

restore_to() {
  local target="$1" expected_rows="$2" expected_max="$3" label="$4"
  echo "==> restoring to $label at $target"

  docker rm -f "$RESTORE_NAME" >/dev/null 2>&1 || true
  "${COMPOSE[@]}" run -d --rm --service-ports --name "$RESTORE_NAME" \
    -e RECOVERY_TARGET="$target" restore >/dev/null

  local promoted=""
  for _ in $(seq 1 120); do
    if docker exec "$RESTORE_NAME" \
        pg_isready -U postgres -d pitr_test >/dev/null 2>&1; then
      promoted="$(docker exec "$RESTORE_NAME" \
        psql -U postgres -d pitr_test -Atq -c \
        "SELECT pg_is_in_recovery();" 2>/dev/null | tr -d '\r' || true)"
      if [ "$promoted" = "f" ]; then
        break
      fi
    fi
    if ! docker inspect "$RESTORE_NAME" >/dev/null 2>&1; then
      echo "restore container exited before promotion" >&2
      docker logs "$RESTORE_NAME" >&2 2>&1 || true
      return 1
    fi
    sleep 1
  done
  if [ "$promoted" != "f" ]; then
    echo "restore did not promote within the timeout" >&2
    docker logs "$RESTORE_NAME" >&2 2>&1 || true
    return 1
  fi

  local result post_target
  result="$(docker exec "$RESTORE_NAME" \
    psql -U postgres -d pitr_test -Atq -c \
    "SELECT count(*)::text || ':' || max(id)::text FROM pitr_events;" | tr -d '\r')"
  post_target="$(docker exec "$RESTORE_NAME" \
    psql -U postgres -d pitr_test -Atq -c \
    "SELECT count(*) FROM pitr_events WHERE id > $expected_max;" | tr -d '\r')"

  if [ "$result" != "${expected_rows}:${expected_max}" ]; then
    echo "$label: expected ${expected_rows}:${expected_max}, got $result" >&2
    docker logs "$RESTORE_NAME" >&2 2>&1 || true
    return 1
  fi
  if [ "$post_target" != "0" ]; then
    echo "$label: found $post_target rows beyond the recovery target" >&2
    return 1
  fi
  echo "    promoted, rows=$expected_rows, max_id=$expected_max, post_target_rows=0"

  docker stop "$RESTORE_NAME" >/dev/null 2>&1 || true
  for _ in $(seq 1 50); do
    if ! docker inspect "$RESTORE_NAME" >/dev/null 2>&1; then
      break
    fi
    sleep 0.2
  done
  docker rm -f "$RESTORE_NAME" >/dev/null 2>&1 || true
}

echo "==> resetting any previous PITR stack"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
docker rm -f "$RESTORE_NAME" >/dev/null 2>&1 || true

echo "==> pre-creating the backup volume with archive/ and base/ directories"
docker volume create "$BACKUP_VOLUME" >/dev/null
docker run --rm -v "$BACKUP_VOLUME:/backup" alpine:3.20 \
  sh -c 'mkdir -p /backup/archive /backup/base && chown -R 70:70 /backup && chmod 700 /backup/archive /backup/base' >/dev/null

echo "==> starting the primary with WAL archiving"
"${COMPOSE[@]}" up -d postgres
wait_for_primary
wait_for_rows 10
echo "    baseline rows=10"

echo "==> taking a base backup while the primary is running"
"${COMPOSE[@]}" exec -T -e PGPASSWORD=postgres_secret postgres \
  pg_basebackup -h 127.0.0.1 -U postgres -D /backup/base \
  -Fp -Xs -P -c fast >/dev/null
"${COMPOSE[@]}" exec -T postgres test -s /backup/base/PG_VERSION

echo "==> inserting batch 1 (rows 11-20)"
target_one="$(insert_batch 11 20 batch-1)"
echo "    target 1 = $target_one"

echo "==> inserting batch 2 (rows 21-30)"
target_two="$(insert_batch 21 30 batch-2)"
echo "    target 2 = $target_two"

echo "==> inserting post-target rows 31-40"
insert_batch 31 40 post-target >/dev/null

echo "==> verifying row counts on the live primary"
live_count="$(sql "SELECT count(*) FROM pitr_events;")"
if [ "$live_count" != "40" ]; then
  echo "expected 40 live rows, got $live_count" >&2
  exit 1
fi
echo "    live rows=40"

restore_to "$target_one" 20 20 "target 1 (batch 1)"
restore_to "$target_two" 30 30 "target 2 (batch 2)"

if [ "${KEEP:-0}" = "1" ]; then
  echo
  echo "primary is on 127.0.0.1:55433; restore container would use 55434"
fi

echo
echo "PITR drill passed: two point-in-time restores promoted with the expected rows."
