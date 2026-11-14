#!/usr/bin/env bash
# Restore a base backup from /backup/base, replay archived WAL from
# /backup/archive, and promote exactly at RECOVERY_TARGET.
#
# The official image entrypoint runs this as root first; after copying and
# configuring PGDATA we hand off to docker-entrypoint.sh, which drops to the
# postgres user.
set -euo pipefail

: "${PGDATA:?PGDATA must be set}"
: "${RECOVERY_TARGET:?RECOVERY_TARGET must be set}"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "restore: copying base backup into $PGDATA"
    mkdir -p "$PGDATA"
    find "$PGDATA" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    cp -a /backup/base/. "$PGDATA"/
fi

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "restore: /backup/base does not contain a PostgreSQL data directory" >&2
    exit 1
fi

chown -R postgres:postgres "$PGDATA"
chmod 700 "$PGDATA"

# Make the recovery settings idempotent if the same volume is reused.
if [ -f "$PGDATA/postgresql.auto.conf" ]; then
    sed -i '/^# pitr-harness:start/,/^# pitr-harness:end/d' \
        "$PGDATA/postgresql.auto.conf"
fi
{
    echo "# pitr-harness:start"
    echo "archive_mode = 'off'"
    echo "restore_command = 'cp /backup/archive/%f %p'"
    echo "recovery_target_time = '$RECOVERY_TARGET'"
    echo "recovery_target_action = 'promote'"
    echo "recovery_target_inclusive = on"
    echo "recovery_target_timeline = 'latest'"
    echo "# pitr-harness:end"
} >> "$PGDATA/postgresql.auto.conf"

rm -f "$PGDATA/standby.signal" "$PGDATA/recovery.signal"
touch "$PGDATA/recovery.signal"
chown postgres:postgres \
    "$PGDATA/postgresql.auto.conf" "$PGDATA/recovery.signal"

echo "restore: replaying WAL to $RECOVERY_TARGET"
exec docker-entrypoint.sh postgres
