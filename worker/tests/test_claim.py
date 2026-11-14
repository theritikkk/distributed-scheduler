import json
import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import psycopg2
import pytest
from psycopg2.extras import Json

import main as worker_main

TEST_URL = os.environ.get("TEST_DATABASE_URL")


def _seed_pending_execution(conn):
    email = f"worker-{uuid4()}@example.com"
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (email, password_hash) VALUES (%s, %s) RETURNING id",
            (email, "not-a-real-hash"),
        )
        user_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO tasks
                (user_id, task_name, command_payload, schedule_type,
                 next_execution_time, dispatch_at, status)
            VALUES (%s, 'worker claim', %s, 'one-time', %s, %s, 'running')
            RETURNING id
            """,
            (
                user_id,
                Json({"command": "true"}),
                datetime.now(timezone.utc) - timedelta(minutes=1),
                datetime.now(timezone.utc) - timedelta(minutes=1),
            ),
        )
        task_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO task_executions (task_id, scheduled_for, status)
            VALUES (%s, %s, 'pending')
            RETURNING id
            """,
            (task_id, datetime.now(timezone.utc) - timedelta(minutes=1)),
        )
        execution_id = cur.fetchone()[0]
    conn.commit()
    return str(user_id), str(task_id), str(execution_id)


@pytest.mark.skipif(not TEST_URL, reason="TEST_DATABASE_URL is not set")
def test_claim_attempt_and_retry_transition(monkeypatch) -> None:
    assert TEST_URL is not None
    monkeypatch.setattr(worker_main, "DATABASE_URL", TEST_URL)
    conn = psycopg2.connect(TEST_URL)
    conn.autocommit = False
    try:
        user_id, task_id, execution_id = _seed_pending_execution(conn)

        claim = worker_main.claim_execution(execution_id)
        assert claim is not None
        assert claim.attempt == 1
        assert claim.lease_id

        duplicate = worker_main.claim_execution(execution_id)
        assert duplicate is None

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, worker_id, lease_id, started_at, attempt
                FROM task_executions WHERE id = %s
                """,
                (execution_id,),
            )
            status, claimed_worker, lease_id, started_at, attempt = cur.fetchone()
            assert status == "running"
            assert claimed_worker == worker_main.WORKER_ID
            assert str(lease_id) == claim.lease_id
            assert started_at is not None
            assert attempt == 1

            cur.execute(
                """
                SELECT attempt, status FROM task_execution_attempts
                WHERE execution_id = %s ORDER BY attempt
                """,
                (execution_id,),
            )
            assert cur.fetchall() == [(1, "running")]

        schedule = worker_main.schedule_execution_retry(
            execution_id,
            claim.lease_id,
            1,
            1_000,
            "boom",
            retry_message={
                "taskId": task_id,
                "executionId": execution_id,
                "payload": "{}",
            },
        )
        assert schedule is not None
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_type, status, payload FROM outbox
                WHERE aggregate_id = %s ORDER BY id
                """,
                (task_id,),
            )
            event_type, event_status, event_payload = cur.fetchone()
            assert event_type == "task.retry"
            assert event_status == "pending"
            assert event_payload["message"]["retryCount"] == 1
            assert event_payload["retryAt"]
            cur.execute(
                """
                SELECT status, lease_id, next_attempt_at
                FROM task_executions WHERE id = %s
                """,
                (execution_id,),
            )
            status, lease_id, next_attempt_at = cur.fetchone()
            assert status == "pending"
            assert lease_id is None
            assert next_attempt_at is not None

            cur.execute(
                """
                SELECT status, finished_at FROM task_execution_attempts
                WHERE execution_id = %s AND attempt = 1
                """,
                (execution_id,),
            )
            attempt_status, finished_at = cur.fetchone()
            assert attempt_status == "failed"
            assert finished_at is not None

        assert worker_main.claim_execution(execution_id) is None
        delay_ms = worker_main.pending_retry_delay_ms(execution_id)
        assert delay_ms is not None and delay_ms > 0

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE task_executions SET next_attempt_at = NOW() - interval '1 second' WHERE id = %s",
                (execution_id,),
            )
        conn.commit()

        second_claim = worker_main.claim_execution(execution_id)
        assert second_claim is not None
        assert second_claim.attempt == 2

        assert not worker_main.schedule_execution_retry(
            execution_id,
            "00000000-0000-0000-0000-000000000000",
            second_claim.attempt,
            1_000,
            "stale",
        )
        conn.rollback()

        with conn.cursor() as cur:
            cur.execute("DELETE FROM outbox WHERE aggregate_id = %s", (task_id,))
            cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
    finally:
        conn.rollback()
        conn.close()


def _seed_execution_state(conn, *, execution_status: str, task_status: str):
    email = f"worker-state-{uuid4()}@example.com"
    now = datetime.now(timezone.utc) - timedelta(minutes=1)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (email, password_hash) VALUES (%s, %s) RETURNING id",
            (email, "not-a-real-hash"),
        )
        user_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO tasks
                (user_id, task_name, command_payload, schedule_type,
                 next_execution_time, dispatch_at, status)
            VALUES (%s, 'claim state matrix', %s, 'one-time', %s, %s, %s)
            RETURNING id
            """,
            (user_id, Json({"command": "true"}), now, now, task_status),
        )
        task_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO task_executions
                (task_id, scheduled_for, started_at, status, attempt)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                task_id,
                now,
                None if execution_status == "pending" else now,
                execution_status,
                0 if execution_status == "pending" else 1,
            ),
        )
        execution_id = cur.fetchone()[0]
    conn.commit()
    return str(user_id), str(execution_id)


@pytest.mark.skipif(not TEST_URL, reason="TEST_DATABASE_URL is not set")
@pytest.mark.parametrize(
    ("execution_status", "task_status"),
    [
        ("running", "running"),
        ("completed", "running"),
        ("pending", "scheduled"),
        ("pending", "cancelled"),
    ],
)
def test_claim_rejects_nonclaimable_states(
    monkeypatch, execution_status: str, task_status: str
) -> None:
    assert TEST_URL is not None
    monkeypatch.setattr(worker_main, "DATABASE_URL", TEST_URL)
    conn = psycopg2.connect(TEST_URL)
    conn.autocommit = False
    try:
        user_id, execution_id = _seed_execution_state(
            conn,
            execution_status=execution_status,
            task_status=task_status,
        )

        assert worker_main.claim_execution(execution_id) is None

        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM task_execution_attempts WHERE execution_id = %s",
                (execution_id,),
            )
            assert cur.fetchone()[0] == 0
            cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
    finally:
        conn.rollback()
        conn.close()


class _FakeChannel:
    def __init__(self):
        self.published = []
        self.acked = []

    def basic_publish(self, **kwargs):
        self.published.append(kwargs)

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue=False):
        raise AssertionError("unexpected nack")


class _FakeMethod:
    delivery_tag = 9


class _FakeHeartbeat:
    def __init__(self, *_args, **_kwargs):
        pass

    def start(self):
        pass

    def stop(self):
        pass


@pytest.mark.skipif(not TEST_URL, reason="TEST_DATABASE_URL is not set")
def test_completed_worker_result_is_committed_to_outbox(monkeypatch) -> None:
    assert TEST_URL is not None
    monkeypatch.setattr(worker_main, "DATABASE_URL", TEST_URL)
    monkeypatch.setattr(worker_main, "OUTBOX_ENABLED", True)
    monkeypatch.setattr(worker_main, "LeaseHeartbeat", _FakeHeartbeat)
    monkeypatch.setattr(worker_main, "execute_task", lambda _payload: ("ok", None))

    conn = psycopg2.connect(TEST_URL)
    conn.autocommit = False
    try:
        user_id, task_id, execution_id = _seed_pending_execution(conn)
        body = json.dumps(
            {
                "taskId": task_id,
                "executionId": execution_id,
                "payload": "{}",
                "retryCount": 0,
                "maxRetries": 3,
            }
        ).encode()

        channel = _FakeChannel()
        worker_main.handle_task(channel, _FakeMethod(), None, body)

        assert channel.published == []
        assert channel.acked == [9]

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_type, status, payload FROM outbox
                WHERE aggregate_id = %s ORDER BY id
                """,
                (task_id,),
            )
            event_type, event_status, payload = cur.fetchone()
            assert event_type == "task.result"
            assert event_status == "pending"
            assert payload["executionId"] == execution_id
            assert payload["status"] == "completed"

            cur.execute(
                "SELECT status FROM task_executions WHERE id = %s",
                (execution_id,),
            )
            assert cur.fetchone()[0] == "running"

            cur.execute("DELETE FROM outbox WHERE aggregate_id = %s", (task_id,))
            cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
    finally:
        conn.rollback()
        conn.close()
