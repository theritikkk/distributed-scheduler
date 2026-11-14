import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import psycopg2
import pytest
from psycopg2.extras import Json

from internal.queue.due_consumer import build_task_message, dispatch_due_task
from internal.queue.reaper import reap_stale_executions
from internal.queue.result_consumer import apply_result
from internal.scheduling import stable_offset

TEST_URL = os.environ.get("TEST_DATABASE_URL")


def _seed_task(
    conn,
    *,
    schedule_type: str = "one-time",
    cron: str | None = None,
    next_execution_time: datetime | None = None,
    dispatch_at: datetime | None = None,
    status: str = "scheduled",
    jitter_seconds: int = 0,
    jitter_policy: str = "stable",
):
    email = f"test-{uuid4()}@example.com"
    if next_execution_time is None:
        next_execution_time = datetime.now(timezone.utc) - timedelta(minutes=1)
    if dispatch_at is None:
        dispatch_at = next_execution_time

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
                 cron_expression, next_execution_time, dispatch_at,
                 jitter_seconds, jitter_policy, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                user_id,
                "integration task",
                Json({"command": "echo integration"}),
                schedule_type,
                cron,
                next_execution_time,
                dispatch_at,
                jitter_seconds,
                jitter_policy,
                status,
            ),
        )
        return cur.fetchone()[0]


def _seed_execution(
    conn,
    task_id,
    *,
    scheduled_for: datetime | None = None,
    started_at: datetime | None = None,
    status: str = "running",
    attempt: int = 1,
    lease_id: str | None = None,
    lease_expires_at: datetime | None = None,
    next_attempt_at: datetime | None = None,
    created_at: datetime | None = None,
) -> str:
    if scheduled_for is None:
        scheduled_for = datetime.now(timezone.utc) - timedelta(minutes=1)
    if started_at is None:
        started_at = scheduled_for
    if status == "pending":
        started_at = None
    if lease_id is None and status == "running":
        lease_id = str(uuid4())
    if lease_expires_at is None and status == "running":
        lease_expires_at = datetime.now(timezone.utc) + timedelta(minutes=1)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO task_executions
                (task_id, scheduled_for, started_at, status, attempt, lease_id,
                 lease_expires_at, next_attempt_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()))
            RETURNING id
            """,
            (
                task_id,
                scheduled_for,
                started_at,
                status,
                attempt,
                lease_id,
                lease_expires_at,
                next_attempt_at,
                created_at,
            ),
        )
        return str(cur.fetchone()[0])


def _seed_attempt(
    conn,
    execution_id: str,
    *,
    attempt: int = 1,
    worker_id: str = "test-worker",
    status: str = "running",
    finished_at: datetime | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO task_execution_attempts
                (execution_id, attempt, worker_id, finished_at, status)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (execution_id, attempt, worker_id, finished_at, status),
        )


def _execution_count(conn, task_id) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM task_executions WHERE task_id = %s", (task_id,)
        )
        return cur.fetchone()[0]


def _outbox_events(conn, task_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, status, payload FROM outbox
            WHERE aggregate_id = %s ORDER BY id
            """,
            (task_id,),
        )
        return cur.fetchall()


def test_dispatch_due_task_creates_execution_and_message(db_conn) -> None:
    task_id = _seed_task(db_conn)
    db_conn.commit()

    action = dispatch_due_task(db_conn, str(task_id), max_retries=7)

    assert action.kind == "dispatch"
    assert action.execution_id is not None
    assert action.max_retries == 7
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute("SELECT status FROM tasks WHERE id = %s", (task_id,))
        assert cur.fetchone()[0] == "running"
        cur.execute(
            "SELECT status FROM task_executions WHERE id = %s",
            (action.execution_id,),
        )
        assert cur.fetchone()[0] == "pending"

    message = json.loads(build_task_message(action))
    assert message == {
        "taskId": str(task_id),
        "executionId": action.execution_id,
        "taskName": "integration task",
        "payload": '{"command": "echo integration"}',
        "scheduleType": "one-time",
        "scheduledFor": action.scheduled_for.isoformat().replace("+00:00", "Z"),
        "dispatchAt": action.dispatch_at.isoformat().replace("+00:00", "Z"),
        "retryCount": 0,
        "maxRetries": 7,
    }
    assert action.scheduled_for == action.dispatch_at
    assert action.jitter_seconds == 0

    events = _outbox_events(db_conn, task_id)
    assert [(event_type, status) for event_type, status, _ in events] == [
        ("task.dispatch", "pending")
    ]
    assert events[0][2] == message


def test_duplicate_wakeup_does_not_create_second_execution(db_conn) -> None:
    task_id = _seed_task(db_conn)
    db_conn.commit()

    first = dispatch_due_task(db_conn, str(task_id), max_retries=3)
    assert first.kind == "dispatch"
    db_conn.commit()

    second = dispatch_due_task(db_conn, str(task_id), max_retries=3)
    assert second.kind == "skip"
    db_conn.rollback()

    assert _execution_count(db_conn, task_id) == 1


def test_future_wakeup_is_republished_without_execution(db_conn) -> None:
    task_id = _seed_task(
        db_conn,
        next_execution_time=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    db_conn.commit()

    action = dispatch_due_task(db_conn, str(task_id), max_retries=3)

    assert action.kind == "redeliver"
    assert action.next_execution_time is not None
    db_conn.rollback()
    assert _execution_count(db_conn, task_id) == 0


def test_dispatch_uses_database_time_and_base_slot(db_conn) -> None:
    base = datetime.now(timezone.utc) - timedelta(minutes=2)
    dispatch_at = base + timedelta(seconds=30)
    task_id = _seed_task(
        db_conn,
        next_execution_time=base,
        dispatch_at=dispatch_at,
        jitter_seconds=30,
    )
    db_conn.commit()

    action = dispatch_due_task(db_conn, str(task_id), max_retries=3)

    assert action.kind == "dispatch"
    assert action.scheduled_for == base
    assert action.dispatch_at == dispatch_at
    assert action.jitter_seconds == 30
    assert action.schedule_lag_seconds is not None
    assert action.schedule_lag_seconds > 0
    db_conn.commit()

    message = json.loads(build_task_message(action))
    assert message["scheduledFor"] == base.isoformat().replace("+00:00", "Z")
    assert message["dispatchAt"] == dispatch_at.isoformat().replace("+00:00", "Z")


def test_future_dispatch_at_redelivers_without_execution(db_conn) -> None:
    next_execution = datetime.now(timezone.utc) - timedelta(minutes=2)
    task_id = _seed_task(
        db_conn,
        next_execution_time=next_execution,
        dispatch_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        jitter_seconds=30,
    )
    db_conn.commit()

    action = dispatch_due_task(db_conn, str(task_id), max_retries=3)

    assert action.kind == "redeliver"
    assert action.dispatch_at is not None
    assert action.dispatch_at > datetime.now(timezone.utc)
    db_conn.rollback()
    assert _execution_count(db_conn, task_id) == 0


def test_duplicate_slot_is_rejected(db_conn) -> None:
    base = datetime.now(timezone.utc) - timedelta(minutes=2)
    task_id = _seed_task(db_conn, next_execution_time=base)
    db_conn.commit()

    first = dispatch_due_task(db_conn, str(task_id), max_retries=3)
    assert first.kind == "dispatch"
    db_conn.commit()

    # Simulate a duplicate wakeup that finds the task scheduled again while
    # the execution row for the same base slot still exists.
    with db_conn.cursor() as cur:
        cur.execute(
            "UPDATE tasks SET status = 'scheduled' WHERE id = %s", (task_id,)
        )
    db_conn.commit()

    second = dispatch_due_task(db_conn, str(task_id), max_retries=3)
    assert second.kind == "slot_conflict"
    db_conn.rollback()

    assert _execution_count(db_conn, task_id) == 1


@pytest.mark.skipif(not TEST_URL, reason="TEST_DATABASE_URL is not set")
def test_concurrent_coordinators_create_one_execution(db_conn) -> None:
    assert TEST_URL is not None
    task_id = _seed_task(db_conn)
    db_conn.commit()

    barrier = threading.Barrier(2)

    def dispatch_in_thread() -> str:
        conn = psycopg2.connect(TEST_URL)
        conn.autocommit = False
        try:
            barrier.wait(timeout=5)
            action = dispatch_due_task(conn, str(task_id), max_retries=3)
            if action.kind == "dispatch":
                conn.commit()
            else:
                conn.rollback()
            return action.kind
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = [pool.submit(dispatch_in_thread) for _ in range(2)]
        kinds = sorted(future.result(timeout=10) for future in outcomes)

    assert kinds == ["dispatch", "skip"]
    assert _execution_count(db_conn, task_id) == 1
    assert len(_outbox_events(db_conn, task_id)) == 1


def test_apply_result_is_idempotent(db_conn) -> None:
    task_id = _seed_task(db_conn, status="running")
    lease_id = str(uuid4())
    execution_id = _seed_execution(db_conn, task_id, lease_id=lease_id)
    _seed_attempt(db_conn, execution_id, attempt=1)
    db_conn.commit()

    message = {
        "taskId": str(task_id),
        "executionId": str(execution_id),
        "leaseId": lease_id,
        "status": "completed",
        "output": "ok",
        "errorMessage": "",
    }
    first = apply_result(db_conn, message)
    assert first.applied is True
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT status, output FROM task_executions WHERE id = %s",
            (execution_id,),
        )
        assert cur.fetchone() == ("completed", "ok")
        cur.execute("SELECT status FROM tasks WHERE id = %s", (task_id,))
        assert cur.fetchone()[0] == "completed"
        cur.execute(
            "SELECT status, finished_at FROM task_execution_attempts "
            "WHERE execution_id = %s AND attempt = 1",
            (execution_id,),
        )
        attempt_status, finished_at = cur.fetchone()
        assert attempt_status == "completed"
        assert finished_at is not None

    duplicate = apply_result(db_conn, message)
    assert duplicate.applied is False
    assert duplicate.reason == "duplicate"
    db_conn.rollback()

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM task_executions WHERE task_id = %s", (task_id,))
        assert cur.fetchone()[0] == 1


def test_result_with_stale_lease_is_rejected(db_conn) -> None:
    task_id = _seed_task(db_conn, status="running")
    execution_id = _seed_execution(db_conn, task_id, lease_id=str(uuid4()))
    db_conn.commit()

    result = apply_result(
        db_conn,
        {
            "taskId": str(task_id),
            "executionId": str(execution_id),
            "leaseId": str(uuid4()),
            "status": "completed",
            "output": "stale",
        },
    )

    assert result.applied is False
    assert result.reason == "stale_lease"
    db_conn.rollback()

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT status, completed_at FROM task_executions WHERE id = %s",
            (execution_id,),
        )
        assert cur.fetchone() == ("running", None)
        cur.execute("SELECT status FROM tasks WHERE id = %s", (task_id,))
        assert cur.fetchone()[0] == "running"


def test_recurring_result_schedules_next_run_from_scheduled_slot(db_conn) -> None:
    scheduled_for = (datetime.now(timezone.utc) - timedelta(minutes=3)).replace(
        second=0, microsecond=0
    )
    task_id = _seed_task(
        db_conn,
        schedule_type="recurring",
        cron="* * * * *",
        next_execution_time=scheduled_for,
        dispatch_at=scheduled_for,
        status="running",
        jitter_seconds=30,
    )
    execution_id = _seed_execution(db_conn, task_id, scheduled_for=scheduled_for)
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT lease_id FROM task_executions WHERE id = %s",
            (execution_id,),
        )
        lease_id = str(cur.fetchone()[0])
    db_conn.commit()

    result = apply_result(
        db_conn,
        {
            "taskId": str(task_id),
            "executionId": str(execution_id),
            "leaseId": lease_id,
            "status": "completed",
        },
        jitter_enabled=True,
    )

    assert result.applied is True
    assert result.next_run is not None
    assert result.next_run > datetime.now(timezone.utc)
    assert result.next_dispatch_at is not None
    assert result.next_dispatch_at >= result.next_run
    assert result.next_dispatch_at - result.next_run == timedelta(
        seconds=stable_offset(str(task_id), 30)
    )
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT status, next_execution_time, dispatch_at FROM tasks WHERE id = %s",
            (task_id,),
        )
        status, next_execution_time, dispatch_at = cur.fetchone()
    assert status == "scheduled"
    assert next_execution_time == result.next_run
    assert dispatch_at == result.next_dispatch_at

    events = _outbox_events(db_conn, task_id)
    assert len(events) == 1
    event_type, event_status, payload = events[0]
    assert event_type == "task.scheduled"
    assert event_status == "pending"
    assert payload["taskId"] == str(task_id)


def test_cancelled_task_is_not_rescheduled_by_late_result(db_conn) -> None:
    scheduled_for = (datetime.now(timezone.utc) - timedelta(minutes=3)).replace(
        second=0, microsecond=0
    )
    task_id = _seed_task(
        db_conn,
        schedule_type="recurring",
        cron="* * * * *",
        next_execution_time=scheduled_for,
        dispatch_at=scheduled_for,
        status="cancelled",
    )
    execution_id = _seed_execution(db_conn, task_id, scheduled_for=scheduled_for)
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT lease_id FROM task_executions WHERE id = %s",
            (execution_id,),
        )
        lease_id = str(cur.fetchone()[0])
    db_conn.commit()

    result = apply_result(
        db_conn,
        {
            "taskId": str(task_id),
            "executionId": str(execution_id),
            "leaseId": lease_id,
            "status": "completed",
        },
        jitter_enabled=True,
    )

    assert result.applied is True
    assert result.next_run is None
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute("SELECT status FROM tasks WHERE id = %s", (task_id,))
        assert cur.fetchone()[0] == "cancelled"

    assert _outbox_events(db_conn, task_id) == []


def test_reaper_requeues_execution_with_expired_lease(db_conn) -> None:
    task_id = _seed_task(db_conn, status="running")
    lease_id = str(uuid4())
    execution_id = _seed_execution(
        db_conn,
        task_id,
        attempt=1,
        lease_id=lease_id,
        lease_expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
    )
    _seed_attempt(db_conn, execution_id, attempt=1)
    db_conn.commit()

    actions = reap_stale_executions(
        db_conn, max_retries=3, pending_grace_sec=300
    )
    assert [action.kind for action in actions] == ["requeue"]
    assert actions[0].attempt == 1
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, lease_id, lease_expires_at, next_attempt_at
            FROM task_executions WHERE id = %s
            """,
            (execution_id,),
        )
        status, execution_lease_id, lease_expires_at, next_attempt_at = (
            cur.fetchone()
        )
        assert status == "pending"
        assert execution_lease_id is None
        assert lease_expires_at is None
        assert next_attempt_at is not None

        cur.execute(
            "SELECT status, finished_at FROM task_execution_attempts "
            "WHERE execution_id = %s AND attempt = 1",
            (execution_id,),
        )
        attempt_status, finished_at = cur.fetchone()
        assert attempt_status == "failed"
        assert finished_at is not None

        cur.execute("SELECT status FROM tasks WHERE id = %s", (task_id,))
        assert cur.fetchone()[0] == "running"

    events = _outbox_events(db_conn, task_id)
    assert [(event_type, status) for event_type, status, _ in events] == [
        ("task.dispatch", "pending")
    ]
    assert events[0][2]["taskId"] == str(task_id)


def test_reaper_fails_execution_after_max_attempts(db_conn) -> None:
    task_id = _seed_task(db_conn, status="running")
    execution_id = _seed_execution(
        db_conn,
        task_id,
        attempt=4,
        lease_id=str(uuid4()),
        lease_expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
    )
    _seed_attempt(db_conn, execution_id, attempt=4)
    db_conn.commit()

    actions = reap_stale_executions(
        db_conn, max_retries=3, pending_grace_sec=300
    )
    assert [action.kind for action in actions] == ["failed"]
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT status, completed_at FROM task_executions WHERE id = %s",
            (execution_id,),
        )
        status, completed_at = cur.fetchone()
        assert status == "failed"
        assert completed_at is not None
        cur.execute("SELECT status FROM tasks WHERE id = %s", (task_id,))
        assert cur.fetchone()[0] == "failed"

    assert _outbox_events(db_conn, task_id) == []


def test_reaper_recovers_stale_pending_dispatch(db_conn) -> None:
    task_id = _seed_task(db_conn, status="running")
    execution_id = _seed_execution(
        db_conn,
        task_id,
        status="pending",
        attempt=0,
        created_at=datetime.now(timezone.utc) - timedelta(minutes=10),
    )
    db_conn.commit()

    actions = reap_stale_executions(
        db_conn, max_retries=3, pending_grace_sec=300
    )
    assert [action.kind for action in actions] == ["requeue"]
    assert actions[0].attempt == 0
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT status, next_attempt_at FROM task_executions WHERE id = %s",
            (execution_id,),
        )
        status, next_attempt_at = cur.fetchone()
        assert status == "pending"
        assert next_attempt_at is not None


def test_reaper_ignores_pending_retry_before_deadline(db_conn) -> None:
    task_id = _seed_task(db_conn, status="running")
    _seed_execution(
        db_conn,
        task_id,
        status="pending",
        attempt=1,
        created_at=datetime.now(timezone.utc) - timedelta(minutes=10),
        next_attempt_at=datetime.now(timezone.utc) + timedelta(minutes=10),
    )
    db_conn.commit()

    actions = reap_stale_executions(
        db_conn, max_retries=3, pending_grace_sec=300
    )

    assert actions == []
    db_conn.rollback()


def test_reaper_cancels_execution_for_cancelled_task(db_conn) -> None:
    task_id = _seed_task(db_conn, status="cancelled")
    execution_id = _seed_execution(
        db_conn,
        task_id,
        attempt=1,
        lease_id=str(uuid4()),
        lease_expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
    )
    db_conn.commit()

    actions = reap_stale_executions(
        db_conn, max_retries=3, pending_grace_sec=300
    )
    assert [action.kind for action in actions] == ["cancelled"]
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT status, completed_at FROM task_executions WHERE id = %s",
            (execution_id,),
        )
        status, completed_at = cur.fetchone()
        assert status == "cancelled"
        assert completed_at is not None
        cur.execute("SELECT status FROM tasks WHERE id = %s", (task_id,))
        assert cur.fetchone()[0] == "cancelled"

    assert _outbox_events(db_conn, task_id) == []
