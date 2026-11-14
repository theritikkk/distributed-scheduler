#!/usr/bin/env python3
"""PgBouncer integration checks for the scheduler's connection pattern.

The harness runs a disposable PostgreSQL primary and an edoburu/pgbouncer
transaction pooler. It asserts:

* SCRAM-SHA-256 authentication end to end (and rejection of a bad password).
* Transaction pooling reuses a server connection across transactions while two
  concurrent transactions still get two distinct backends.
* ``pg_try_advisory_xact_lock(hashtext(...))`` behaves exactly as the outbox
  relay expects through the pooler.
* ``DISCARD ALL`` (server_reset_query_always=1) prevents SET and temp-table
  state from leaking to the next client.
* The Prometheus exporter exposes the pool metrics used by the HA alerts.

Run through tests/pgbouncer/run.sh; it owns the Docker lifecycle and ports.
"""
from __future__ import annotations

import os
import sys
import time
import urllib.error
import urllib.request
from contextlib import closing

import psycopg2

PG_HOST = os.environ.get("PGBOUNCER_TEST_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("PGBOUNCER_TEST_PG_PORT", "55432"))
POOLER_PORT = int(os.environ.get("PGBOUNCER_TEST_POOLER_PORT", "56432"))
EXPORTER_URL = os.environ.get(
    "PGBOUNCER_TEST_EXPORTER_URL", "http://127.0.0.1:59127/metrics"
)

DB_NAME = "distributed_scheduler"
APP_USER = "scheduler"
APP_PASSWORD = "scheduler_secret"
SUPERUSER = "postgres"
SUPERUSER_PASSWORD = "postgres_secret"


def check(name: str, condition: bool, detail: str = "") -> None:
    if not condition:
        raise AssertionError(f"{name} failed{': ' + detail if detail else ''}")
    print(f"  ok  {name}")


def direct_connect(*, user: str = APP_USER, password: str = APP_PASSWORD,
                   dbname: str = DB_NAME):
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=user,
        password=password,
        dbname=dbname,
        connect_timeout=5,
        application_name="pgbouncer-direct-test",
    )


def pool_connect(*, user: str = APP_USER, password: str = APP_PASSWORD,
                 dbname: str = DB_NAME):
    return psycopg2.connect(
        host=PG_HOST,
        port=POOLER_PORT,
        user=user,
        password=password,
        dbname=dbname,
        connect_timeout=5,
        application_name="pgbouncer-pool-test",
    )


def wait_for_metrics(timeout: float = 30.0) -> str:
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(EXPORTER_URL, timeout=2) as response:
                if response.status == 200:
                    return response.read().decode("utf-8", "replace")
        except (urllib.error.URLError, TimeoutError) as exc:  # pragma: no cover - timing
            last_error = exc
        time.sleep(0.5)
    raise RuntimeError(f"exporter did not become ready: {last_error}")


def test_scram_authentication() -> None:
    print("==> SCRAM authentication")
    with closing(direct_connect()) as conn, conn.cursor() as cur:
        cur.execute("SHOW password_encryption")
        check("direct connection uses SCRAM", cur.fetchone()[0] == "scram-sha-256")

    with closing(direct_connect(user=SUPERUSER, password=SUPERUSER_PASSWORD,
                                dbname="postgres")) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT rolpassword FROM pg_authid WHERE rolname = %s", (APP_USER,)
        )
        stored = cur.fetchone()[0]
        check(
            "role password is stored as a SCRAM verifier",
            bool(stored) and stored.startswith("SCRAM-SHA-256$"),
            str(stored)[:24],
        )

    with closing(pool_connect()) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
        check("PgBouncer accepts the SCRAM password", cur.fetchone()[0] == 1)

    try:
        pool_connect(password="definitely-wrong")
    except psycopg2.OperationalError as exc:
        message = str(exc).lower()
        check(
            "PgBouncer rejects a bad password",
            "password authentication failed" in message
            or "sasl authentication failed" in message,
            str(exc).strip().splitlines()[-1],
        )
    else:  # pragma: no cover - only reached on a regression
        raise AssertionError("PgBouncer accepted a wrong password")


def test_transaction_pooling() -> None:
    print("==> transaction pooling")
    first = pool_connect()
    second = pool_connect()
    try:
        with first.cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")
            pid_one = cur.fetchone()[0]
        with second.cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")
            pid_two = cur.fetchone()[0]
        check(
            "two concurrent transactions use two server connections",
            pid_one != pid_two,
            f"{pid_one} == {pid_two}",
        )

        first.commit()
        third = pool_connect()
        try:
            with third.cursor() as cur:
                cur.execute("SELECT pg_backend_pid()")
                pid_three = cur.fetchone()[0]
            check(
                "a later transaction reuses the released backend",
                pid_three == pid_one,
                f"{pid_three} != {pid_one}",
            )
        finally:
            third.rollback()
            third.close()
        second.rollback()
    finally:
        first.close()
        second.close()


def test_advisory_xact_lock() -> None:
    print("==> transaction-scoped advisory locks (outbox relay pattern)")
    key = "outbox:task:pgbouncer-test"
    holder = pool_connect()
    contender = pool_connect()
    try:
        with holder.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_xact_lock(hashtext(%s))", (key,)
            )
            check("first session acquires the transaction lock", cur.fetchone()[0] is True)

        with contender.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_xact_lock(hashtext(%s))", (key,)
            )
            check(
                "second session cannot acquire the same lock",
                cur.fetchone()[0] is False,
            )

        holder.commit()
        contender.rollback()

        after = pool_connect()
        try:
            with after.cursor() as cur:
                cur.execute(
                    "SELECT pg_try_advisory_xact_lock(hashtext(%s))", (key,)
                )
                check(
                    "commit releases the transaction lock",
                    cur.fetchone()[0] is True,
                )
        finally:
            after.rollback()
            after.close()
    finally:
        holder.close()
        contender.close()


def test_session_reset() -> None:
    print("==> DISCARD ALL session reset")
    leaked = pool_connect()
    try:
        leaked.autocommit = True
        with leaked.cursor() as cur:
            cur.execute("SET application_name = 'leaked-through-pgbouncer'")
            cur.execute("CREATE TEMP TABLE leak_probe (id integer)")
    finally:
        leaked.close()

    fresh = pool_connect()
    try:
        fresh.autocommit = True
        with fresh.cursor() as cur:
            cur.execute("SHOW application_name")
            app_name = cur.fetchone()[0]
            check(
                "SET application_name does not leak",
                app_name != "leaked-through-pgbouncer",
                app_name,
            )
            cur.execute("SELECT to_regclass('pg_temp.leak_probe')")
            check(
                "temporary table is discarded",
                cur.fetchone()[0] is None,
            )
    finally:
        fresh.close()


def test_pooler_admin_view() -> None:
    print("==> pooler configuration and pool view")
    with closing(pool_connect(dbname="pgbouncer")) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SHOW CONFIG")
            config = {row[0]: row[1] for row in cur.fetchall()}
            check("pool_mode is transaction", config["pool_mode"] == "transaction")
            check("DISCARD ALL is always applied",
                  str(config["server_reset_query_always"]) == "1")
            check("server reset query is DISCARD ALL",
                  config["server_reset_query"].upper() == "DISCARD ALL")

            cur.execute("SHOW POOLS")
            columns = [description[0] for description in cur.description]
            pools = [dict(zip(columns, row)) for row in cur.fetchall()]
            pool = next(
                (item for item in pools if item["database"] == DB_NAME), None
            )
            check("SHOW POOLS contains the scheduler database", pool is not None)
            check("the scheduler pool uses transaction mode",
                  pool is not None and pool["pool_mode"] == "transaction")


def test_exporter_metrics() -> None:
    print("==> exporter metrics")
    # Hold one transaction open so the exporter reports an active client and
    # server rather than only idle counters.
    held = pool_connect()
    try:
        with held.cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")

        body = wait_for_metrics()
        lines = [line for line in body.splitlines() if line.startswith("pgbouncer_")]
        names = {line.split("{", 1)[0].split(" ", 1)[0] for line in lines}

        required = {
            "pgbouncer_pools_client_active_connections",
            "pgbouncer_pools_server_active_connections",
            "pgbouncer_pools_client_waiting_connections",
            "pgbouncer_config_max_client_connections",
        }
        missing = sorted(required - names)
        check("required PgBouncer metrics are exported", not missing, ",".join(missing))

        pool_lines = [
            line for line in lines
            if line.startswith("pgbouncer_pools_")
            and f'database="{DB_NAME}"' in line
        ]
        check("pool metrics carry the scheduler database label", bool(pool_lines))
        print("      sample: " + pool_lines[0])
    finally:
        held.rollback()
        held.close()


def main() -> int:
    print(f"PgBouncer harness: postgres={PG_HOST}:{PG_PORT} "
          f"pooler={PG_HOST}:{POOLER_PORT}")
    test_scram_authentication()
    test_transaction_pooling()
    test_advisory_xact_lock()
    test_session_reset()
    test_pooler_admin_view()
    test_exporter_metrics()
    print("\nAll PgBouncer checks passed.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as exc:
        print(f"\nFAIL: {exc}", file=sys.stderr)
        sys.exit(1)
