import os
import subprocess
import sys
from pathlib import Path

import psycopg2
import pytest

ROOT = Path(__file__).resolve().parents[2]
MIGRATE = ROOT / "scripts" / "migrate.py"
TEST_URL = os.environ.get("TEST_DATABASE_URL")


def _run_migrate(migrations: Path, *args: str) -> subprocess.CompletedProcess[str]:
    assert TEST_URL is not None
    return subprocess.run(
        [
            sys.executable,
            str(MIGRATE),
            "--url",
            TEST_URL,
            "--dir",
            str(migrations),
            *args,
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )


def _table_exists(url: str, table: str) -> bool:
    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
            return cur.fetchone()[0] is not None


@pytest.mark.skipif(not TEST_URL, reason="TEST_DATABASE_URL is not set")
def test_migrate_up_status_checksum_and_rollback(tmp_path: Path) -> None:
    assert TEST_URL is not None
    with psycopg2.connect(TEST_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS schema_migrations, migration_probe CASCADE")

    migrations = tmp_path / "migrations"
    migrations.mkdir()

    (migrations / "0001_baseline.sql").write_text("SELECT 1;\n", encoding="utf-8")
    up = _run_migrate(migrations, "up")
    assert up.returncode == 0, up.stderr
    assert "applied 1 migration(s)" in up.stdout

    status = _run_migrate(migrations, "status")
    assert status.returncode == 0, status.stderr
    assert "0001" in status.stdout
    assert "applied" in status.stdout

    forward = migrations / "0002_probe.sql"
    forward.write_text("CREATE TABLE migration_probe (id integer);\n", encoding="utf-8")
    (migrations / "0002_probe.down.sql").write_text(
        "DROP TABLE migration_probe;\n", encoding="utf-8"
    )

    up = _run_migrate(migrations, "up")
    assert up.returncode == 0, up.stderr
    assert _table_exists(TEST_URL, "migration_probe")

    with psycopg2.connect(TEST_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO migration_probe (id) VALUES (42)")

    forward.write_text(
        "CREATE TABLE migration_probe (id integer);\n-- edited after apply\n",
        encoding="utf-8",
    )
    drift = _run_migrate(migrations, "up")
    assert drift.returncode == 1
    assert "checksum mismatch" in drift.stderr

    forward.write_text("CREATE TABLE migration_probe (id integer);\n", encoding="utf-8")
    rollback = _run_migrate(migrations, "rollback")
    assert rollback.returncode == 0, rollback.stderr
    assert not _table_exists(TEST_URL, "migration_probe")

    reapply = _run_migrate(migrations, "up")
    assert reapply.returncode == 0, reapply.stderr
    assert _table_exists(TEST_URL, "migration_probe")
    with psycopg2.connect(TEST_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM migration_probe")
            assert cur.fetchone()[0] == 0

    status = _run_migrate(migrations, "status")
    assert status.returncode == 0, status.stderr
    assert "0002" in status.stdout
    assert "applied" in status.stdout


@pytest.mark.skipif(not TEST_URL, reason="TEST_DATABASE_URL is not set")
def test_up_to_target_stops_before_later_migrations(tmp_path: Path) -> None:
    assert TEST_URL is not None
    with psycopg2.connect(TEST_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS schema_migrations, target_two, target_three CASCADE")

    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "0001_baseline.sql").write_text("SELECT 1;\n", encoding="utf-8")
    (migrations / "0002_target_two.sql").write_text(
        "CREATE TABLE target_two (id integer);\n", encoding="utf-8"
    )
    (migrations / "0003_target_three.sql").write_text(
        "CREATE TABLE target_three (id integer);\n", encoding="utf-8"
    )

    up = _run_migrate(migrations, "up", "--to", "0002")
    assert up.returncode == 0, up.stderr
    assert _table_exists(TEST_URL, "target_two")
    assert not _table_exists(TEST_URL, "target_three")

    up = _run_migrate(migrations, "up", "--to", "0003")
    assert up.returncode == 0, up.stderr
    assert _table_exists(TEST_URL, "target_three")

    invalid = _run_migrate(migrations, "up", "--to", "9999")
    assert invalid.returncode == 1
    assert "unknown migration target" in invalid.stderr

    with psycopg2.connect(TEST_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS schema_migrations, target_two, target_three CASCADE")


@pytest.mark.skipif(not TEST_URL, reason="TEST_DATABASE_URL is not set")
def test_no_transaction_migration_runs_single_statement(tmp_path: Path) -> None:
    assert TEST_URL is not None
    with psycopg2.connect(TEST_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS schema_migrations, no_tx_probe CASCADE")
            cur.execute("CREATE TABLE no_tx_probe (id integer)")

    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "0001_no_tx.sql").write_text(
        "-- migrate:no-transaction\n"
        "CREATE INDEX CONCURRENTLY no_tx_probe_idx ON no_tx_probe (id);\n",
        encoding="utf-8",
    )
    (migrations / "0001_no_tx.down.sql").write_text(
        "-- migrate:no-transaction\n"
        "DROP INDEX CONCURRENTLY IF EXISTS no_tx_probe_idx;\n",
        encoding="utf-8",
    )

    up = _run_migrate(migrations, "up")
    assert up.returncode == 0, up.stderr
    assert _table_exists(TEST_URL, "no_tx_probe_idx")

    rollback = _run_migrate(migrations, "rollback")
    assert rollback.returncode == 0, rollback.stderr
    assert not _table_exists(TEST_URL, "no_tx_probe_idx")

    with psycopg2.connect(TEST_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS no_tx_probe CASCADE")
