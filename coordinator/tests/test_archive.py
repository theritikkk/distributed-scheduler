"""Tests for bounded terminal-history archival."""
from __future__ import annotations

import os
from uuid import uuid4

import psycopg2
import pytest

from internal.archive import archive_batch, archive_cycle

TEST_URL = os.environ.get("TEST_DATABASE_URL")


def _seed_task(conn) -> tuple[str, str]:
    user_id = str(uuid4())
    task_id = str(uuid4())
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (id, email, password_hash) VALUES (%s, %s, 'x')",
            (user_id, f"archive-{user_id}@example.com"),
        )
        cur.execute(
            """
            INSERT INTO tasks
                (id, user_id, task_name, command_payload, schedule_type,
                 next_execution_time, dispatch_at)
            VALUES (%s, %s, 'archive-test', '{"command":"echo hi"}'::jsonb,
                    'one-time', NOW() + INTERVAL '1 hour',
                    NOW() + INTERVAL '1 hour')
            """,
            (task_id, user_id),
        )
    return user_id, task_id


def _insert_execution(
    conn,
    task_id: str,
    *,
    status: str,
    age_days: int,
    slot_offset_sec: int | None = None,
) -> str:
    execution_id = str(uuid4())
    if slot_offset_sec is None:
        slot_offset_sec = uuid4().int % 1_000_000
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO task_executions
                (id, task_id, worker_id, scheduled_for, started_at,
                 completed_at, status, output, attempt, created_at)
            VALUES (%s, %s, 'worker-1',
                    NOW() - make_interval(days => %s, secs => %s),
                    NOW() - make_interval(days => %s),
                    CASE WHEN %s IN ('pending', 'running') THEN NULL
                         ELSE NOW() - make_interval(days => %s) END,
                    %s, 'output', 1, NOW() - make_interval(days => %s))
            """,
            (
                execution_id,
                task_id,
                age_days,
                slot_offset_sec,
                age_days,
                status,
                age_days,
                status,
                age_days,
            ),
        )
    return execution_id


def _insert_attempt(conn, execution_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO task_execution_attempts
                (id, execution_id, attempt, worker_id, started_at, finished_at,
                 status)
            VALUES (%s, %s, 1, 'worker-1', NOW() - INTERVAL '10 days',
                    NOW() - INTERVAL '10 days', 'completed')
            """,
            (str(uuid4()), execution_id),
        )


def _insert_outbox(
    conn, task_id: str, *, status: str, age_days: int
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO outbox
                (aggregate_type, aggregate_id, event_type, payload, status,
                 created_at, published_at)
            VALUES ('task', %s, 'task.scheduled', %s, %s,
                    NOW() - make_interval(days => %s),
                    CASE WHEN %s = 'published'
                         THEN NOW() - make_interval(days => %s) END)
            RETURNING id
            """,
            (
                task_id,
                f'{{"taskId":"{task_id}"}}',
                status,
                age_days,
                status,
                age_days,
            ),
        )
        return int(cur.fetchone()[0])


def test_archive_batch_moves_only_terminal_history(db_conn) -> None:
    _, task_id = _seed_task(db_conn)
    old_execution = _insert_execution(
        db_conn, task_id, status="completed", age_days=10
    )
    _insert_attempt(db_conn, old_execution)
    recent_execution = _insert_execution(
        db_conn, task_id, status="failed", age_days=1
    )
    pending_execution = _insert_execution(
        db_conn, task_id, status="pending", age_days=10
    )
    old_published = _insert_outbox(
        db_conn, task_id, status="published", age_days=10
    )
    old_failed = _insert_outbox(db_conn, task_id, status="failed", age_days=10)
    old_pending = _insert_outbox(
        db_conn, task_id, status="pending", age_days=10
    )

    result = archive_batch(db_conn, retention_days=5, batch_size=100)

    assert result.executions == 1
    assert result.attempts == 1
    assert result.outbox == 2

    with db_conn.cursor() as cur:
        cur.execute("SELECT id FROM task_executions ORDER BY id")
        remaining_executions = {str(row[0]) for row in cur.fetchall()}
        cur.execute(
            "SELECT count(*) FROM task_execution_archive WHERE id = ANY(%s::uuid[])",
            ([old_execution],),
        )
        assert cur.fetchone()[0] == 1
        cur.execute(
            "SELECT count(*) FROM task_execution_attempts_archive "
            "WHERE execution_id = %s",
            (old_execution,),
        )
        archived_attempts = cur.fetchone()[0]
        cur.execute("SELECT id, status FROM outbox ORDER BY id")
        remaining_outbox = cur.fetchall()
        cur.execute(
            "SELECT id, status FROM outbox_archive WHERE id = ANY(%s::bigint[])",
            ([old_published, old_failed],),
        )
        archived_outbox = cur.fetchall()

    assert old_execution not in remaining_executions
    assert recent_execution in remaining_executions
    assert pending_execution in remaining_executions
    assert archived_attempts == 1
    assert {row[0] for row in remaining_outbox} == {old_pending}
    assert {int(row[0]) for row in archived_outbox} == {
        old_published,
        old_failed,
    }


def test_archive_dry_run_counts_without_modifying(db_conn) -> None:
    _, task_id = _seed_task(db_conn)
    execution_id = _insert_execution(
        db_conn, task_id, status="completed", age_days=10
    )
    _insert_attempt(db_conn, execution_id)
    outbox_id = _insert_outbox(
        db_conn, task_id, status="published", age_days=10
    )

    result = archive_batch(
        db_conn, retention_days=5, batch_size=100, dry_run=True
    )

    assert result.executions == 1
    assert result.attempts == 1
    assert result.outbox == 1

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM task_executions")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT count(*) FROM outbox WHERE id = %s", (outbox_id,))
        assert cur.fetchone()[0] == 1
        cur.execute(
            "SELECT count(*) FROM task_execution_archive WHERE id = %s",
            (execution_id,),
        )
        assert cur.fetchone()[0] == 0


def test_archive_batch_is_idempotent_when_nothing_is_eligible(db_conn) -> None:
    _, task_id = _seed_task(db_conn)
    _insert_execution(db_conn, task_id, status="completed", age_days=1)

    result = archive_batch(db_conn, retention_days=5, batch_size=100)

    assert result.total == 0


@pytest.mark.skipif(not TEST_URL, reason="TEST_DATABASE_URL is not set")
def test_archive_cycle_commits_batches_and_stops_when_drained() -> None:
    assert TEST_URL is not None

    seed_conn = psycopg2.connect(TEST_URL)
    seed_conn.autocommit = True
    try:
        _, task_id = _seed_task(seed_conn)
        execution_id = _insert_execution(
            seed_conn, task_id, status="timeout", age_days=10
        )
        outbox_id = _insert_outbox(
            seed_conn, task_id, status="published", age_days=10
        )
    finally:
        seed_conn.close()

    def connect():
        return psycopg2.connect(TEST_URL)

    result = archive_cycle(
        connect,
        retention_days=5,
        batch_size=1,
        max_batches=10,
        sleep_ms=0,
    )

    assert result.executions >= 1
    assert result.outbox >= 1

    verify_conn = psycopg2.connect(TEST_URL)
    verify_conn.autocommit = True
    try:
        with verify_conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM task_execution_archive WHERE id = %s",
                (execution_id,),
            )
            assert cur.fetchone()[0] == 1
            cur.execute(
                "SELECT count(*) FROM outbox_archive WHERE id = %s",
                (outbox_id,),
            )
            assert cur.fetchone()[0] == 1
            cur.execute(
                "SELECT count(*) FROM task_executions WHERE id = %s",
                (execution_id,),
            )
            assert cur.fetchone()[0] == 0

        again = archive_cycle(
            connect,
            retention_days=5,
            batch_size=1,
            max_batches=10,
            sleep_ms=0,
        )
        assert again.total == 0

        with verify_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM task_execution_archive WHERE id = %s",
                (execution_id,),
            )
            cur.execute(
                "DELETE FROM outbox_archive WHERE id = %s", (outbox_id,)
            )
            cur.execute("DELETE FROM tasks WHERE id = %s", (task_id,))
    finally:
        verify_conn.close()
