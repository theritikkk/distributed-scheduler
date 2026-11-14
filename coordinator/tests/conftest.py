import os
from pathlib import Path

import psycopg2
import pytest

ROOT = Path(__file__).resolve().parents[2]
INIT_SQL = ROOT / "database" / "init.sql"
TEST_URL = os.environ.get("TEST_DATABASE_URL")


@pytest.fixture
def db_conn():
    """A clean PostgreSQL connection for one coordinator integration test."""
    if not TEST_URL:
        pytest.skip("TEST_DATABASE_URL is not set")

    conn = psycopg2.connect(TEST_URL)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.tasks')")
        if cur.fetchone()[0] is None:
            cur.execute(INIT_SQL.read_text(encoding="utf-8"))
        cur.execute("SELECT to_regclass('public.scheduled_events')")
        if cur.fetchone()[0] is None:
            migration = (
                ROOT / "database" / "migrations" / "0010_scheduled_events.sql"
            )
            cur.execute(migration.read_text(encoding="utf-8"))
        cur.execute(
            "TRUNCATE users, workers, outbox, scheduled_events "
            "RESTART IDENTITY CASCADE"
        )

    conn.autocommit = False
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()
