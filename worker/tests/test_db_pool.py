import os
import psycopg2
import pytest

from db_pool import WorkerDbPool


def test_worker_pool_returns_connections_to_the_pool() -> None:
    try:
        pool = WorkerDbPool(
            os.environ["DATABASE_URL"],
            min_connections=1,
            max_connections=1,
            connect_timeout_sec=2,
        )
    except psycopg2.OperationalError as e:
        pytest.skip(f"PostgreSQL not reachable: {e}")
    try:
        first = pool.get_connection()
        with first.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
        first.close()

        second = pool.get_connection()
        with second.cursor() as cur:
            cur.execute("SELECT 2")
            assert cur.fetchone() == (2,)
        second.close()
        second.close()
    finally:
        pool.closeall()
