import json
import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pika
import psycopg2
import pytest
from psycopg2.extras import Json

from internal.queue.outbox_relay import (
    TokenBucket,
    build_publish_plan,
    cleanup_published,
    relay_batch,
)
from internal.queue.topology import (
    DLX_EXCHANGE,
    DUE_QUEUE,
    RESULT_QUEUE,
    RETRY_DELAY_QUEUE,
    TASK_DLQ,
    TASK_QUEUE,
)


class FakeChannel:
    def __init__(self, error: Exception | None = None):
        self.error = error
        self.published = []

    def basic_publish(self, **kwargs):
        if self.error is not None:
            raise self.error
        self.published.append(kwargs)
        return True


def _insert_event(
    conn,
    *,
    aggregate_id,
    event_type,
    payload,
    status="pending",
    published_at=None,
    created_at=None,
    headers=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO outbox
                (aggregate_type, aggregate_id, event_type, payload, headers,
                 status, published_at, created_at)
            VALUES ('task', %s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()))
            RETURNING id
            """,
            (
                str(aggregate_id),
                event_type,
                Json(payload),
                Json(headers or {}),
                status,
                published_at,
                created_at,
            ),
        )
        return cur.fetchone()[0]


def test_build_publish_plan_routes_all_event_types() -> None:
    task_id = str(uuid4())
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    scheduled = build_publish_plan(
        "task.scheduled", {"taskId": task_id, "dispatchAt": now.isoformat()}
    )
    assert scheduled.exchange == ""
    assert scheduled.routing_key == DUE_QUEUE
    assert json.loads(scheduled.body) == {"taskId": task_id}

    dispatch = build_publish_plan("task.dispatch", {"taskId": task_id})
    assert dispatch.routing_key == TASK_QUEUE
    assert json.loads(dispatch.body) == {"taskId": task_id}

    result = build_publish_plan("task.result", {"taskId": task_id, "status": "completed"})
    assert result.routing_key == RESULT_QUEUE

    retry_at = (now + timedelta(seconds=10)).isoformat().replace("+00:00", "Z")
    retry = build_publish_plan(
        "task.retry",
        {"message": {"taskId": task_id, "retryCount": 1}, "retryAt": retry_at},
        now=now,
    )
    assert retry.routing_key == RETRY_DELAY_QUEUE
    assert retry.expiration_ms == 10_000
    assert json.loads(retry.body)["retryCount"] == 1

    dead_letter = build_publish_plan(
        "task.dead_letter", {"taskId": task_id, "reason": "max_retries_exceeded"}
    )
    assert dead_letter.exchange == DLX_EXCHANGE
    assert dead_letter.routing_key == TASK_DLQ

    with pytest.raises(ValueError, match="unknown"):
        build_publish_plan("task.unknown", {"taskId": task_id})
    with pytest.raises(ValueError, match="missing taskId"):
        build_publish_plan("task.scheduled", {})
    with pytest.raises(ValueError, match="missing message"):
        build_publish_plan("task.retry", {"retryAt": retry_at})


def test_relay_publishes_confirmed_events_and_marks_them(db_conn) -> None:
    aggregate_id = uuid4()
    task_id = str(aggregate_id)
    event_ids = [
        _insert_event(
            db_conn,
            aggregate_id=aggregate_id,
            event_type="task.scheduled",
            payload={"taskId": task_id},
        ),
        _insert_event(
            db_conn,
            aggregate_id=aggregate_id,
            event_type="task.dispatch",
            payload={"taskId": task_id},
        ),
        _insert_event(
            db_conn,
            aggregate_id=aggregate_id,
            event_type="task.result",
            payload={"taskId": task_id, "status": "completed"},
        ),
    ]
    db_conn.commit()
    channel = FakeChannel()

    result = relay_batch(
        db_conn,
        channel,
        batch_size=10,
        max_attempts=5,
        backoff_base_sec=1,
        backoff_max_sec=60,
    )
    db_conn.commit()

    assert result.published == 3
    assert result.failed == 0
    assert result.skipped == 0
    assert len(channel.published) == 3
    assert [call["routing_key"] for call in channel.published] == [
        DUE_QUEUE,
        TASK_QUEUE,
        RESULT_QUEUE,
    ]
    assert all(call["mandatory"] is True for call in channel.published)

    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, status, published_at, attempts FROM outbox
            WHERE id = ANY(%s) ORDER BY id
            """,
            (event_ids,),
        )
        rows = cur.fetchall()
    assert [row[1] for row in rows] == ["published", "published", "published"]
    assert all(row[2] is not None for row in rows)
    assert [row[3] for row in rows] == [0, 0, 0]


def test_relay_failure_uses_backoff_then_becomes_terminal(db_conn) -> None:
    aggregate_id = uuid4()
    event_id = _insert_event(
        db_conn,
        aggregate_id=aggregate_id,
        event_type="task.dispatch",
        payload={"taskId": str(aggregate_id)},
    )
    db_conn.commit()

    result = relay_batch(
        db_conn,
        FakeChannel(RuntimeError("broker rejected event")),
        batch_size=10,
        max_attempts=2,
        backoff_base_sec=60,
        backoff_max_sec=60,
    )
    db_conn.commit()

    assert result.published == 0
    assert result.failed == 1
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT status, attempts, last_error, available_at > NOW() "
            "FROM outbox WHERE id = %s",
            (event_id,),
        )
        status, attempts, last_error, delayed = cur.fetchone()
    assert status == "pending"
    assert attempts == 1
    assert "broker rejected" in last_error
    assert delayed is True

    with db_conn.cursor() as cur:
        cur.execute(
            "UPDATE outbox SET available_at = NOW() - interval '1 second' WHERE id = %s",
            (event_id,),
        )
    db_conn.commit()

    result = relay_batch(
        db_conn,
        FakeChannel(RuntimeError("still failing")),
        batch_size=10,
        max_attempts=2,
        backoff_base_sec=1,
        backoff_max_sec=1,
    )
    db_conn.commit()

    assert result.failed == 1
    with db_conn.cursor() as cur:
        cur.execute("SELECT status, attempts FROM outbox WHERE id = %s", (event_id,))
        assert cur.fetchone() == ("failed", 2)


def test_relay_connection_failure_leaves_event_pending(db_conn) -> None:
    aggregate_id = uuid4()
    event_id = _insert_event(
        db_conn,
        aggregate_id=aggregate_id,
        event_type="task.dispatch",
        payload={"taskId": str(aggregate_id)},
    )
    db_conn.commit()

    with pytest.raises(pika.exceptions.StreamLostError):
        relay_batch(
            db_conn,
            FakeChannel(pika.exceptions.StreamLostError("connection lost")),
            batch_size=10,
            max_attempts=5,
            backoff_base_sec=1,
            backoff_max_sec=60,
        )
    db_conn.rollback()

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT status, attempts FROM outbox WHERE id = %s",
            (event_id,),
        )
        assert cur.fetchone() == ("pending", 0)


def test_relay_skips_an_aggregate_locked_by_another_relay(db_conn) -> None:
    aggregate_id = uuid4()
    _insert_event(
        db_conn,
        aggregate_id=aggregate_id,
        event_type="task.dispatch",
        payload={"taskId": str(aggregate_id)},
    )
    db_conn.commit()

    test_url = os.environ.get("TEST_DATABASE_URL")
    assert test_url is not None
    other = psycopg2.connect(test_url)
    other.autocommit = False
    try:
        with other.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"task:{aggregate_id}",),
            )

        result = relay_batch(
            db_conn,
            FakeChannel(),
            batch_size=10,
            max_attempts=5,
            backoff_base_sec=1,
            backoff_max_sec=60,
        )
        db_conn.commit()
    finally:
        other.rollback()
        other.close()

    assert result.published == 0
    assert result.skipped == 1


def test_cleanup_deletes_only_old_published_rows(db_conn) -> None:
    aggregate_id = uuid4()
    old = datetime.now(timezone.utc) - timedelta(days=2)
    _insert_event(
        db_conn,
        aggregate_id=aggregate_id,
        event_type="task.dispatch",
        payload={"taskId": str(aggregate_id)},
        status="published",
        published_at=old,
        created_at=old,
    )
    pending_id = _insert_event(
        db_conn,
        aggregate_id=aggregate_id,
        event_type="task.dispatch",
        payload={"taskId": str(aggregate_id)},
        created_at=old,
    )
    db_conn.commit()

    deleted = cleanup_published(db_conn, retention_days=1)
    db_conn.commit()

    assert deleted == 1
    with db_conn.cursor() as cur:
        cur.execute("SELECT id FROM outbox ORDER BY id")
        assert [row[0] for row in cur.fetchall()] == [pending_id]


def test_relay_materializes_scheduled_and_retry_events_without_broker_publish(
    db_conn,
) -> None:
    aggregate_id = uuid4()
    task_id = str(aggregate_id)
    dispatch_at = datetime.now(timezone.utc) + timedelta(seconds=30)
    retry_at = datetime.now(timezone.utc) + timedelta(seconds=10)
    _insert_event(
        db_conn,
        aggregate_id=aggregate_id,
        event_type="task.scheduled",
        payload={
            "taskId": task_id,
            "dispatchAt": dispatch_at.isoformat(),
            "nextExecutionTime": dispatch_at.isoformat(),
        },
    )
    _insert_event(
        db_conn,
        aggregate_id=aggregate_id,
        event_type="task.retry",
        payload={
            "message": {
                "taskId": task_id,
                "executionId": str(uuid4()),
                "retryCount": 1,
            },
            "retryAt": retry_at.isoformat(),
        },
    )
    db_conn.commit()

    channel = FakeChannel()
    result = relay_batch(
        db_conn,
        channel,
        batch_size=10,
        max_attempts=5,
        backoff_base_sec=1,
        backoff_max_sec=60,
        scheduled_events_enabled=True,
    )
    db_conn.commit()

    assert result.materialized == 2
    assert result.published == 0
    assert channel.published == []

    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT kind, destination, status, payload, available_at
            FROM scheduled_events ORDER BY id
            """
        )
        rows = cur.fetchall()
    assert [(row[0], row[1], row[2]) for row in rows] == [
        ("wakeup", "scheduler.due", "pending"),
        ("retry", "scheduler.tasks", "pending"),
    ]
    assert rows[0][3] == {"taskId": task_id}
    assert rows[1][3]["retryCount"] == 1
    assert rows[0][4] == dispatch_at
    assert rows[1][4] == retry_at

    with db_conn.cursor() as cur:
        cur.execute("SELECT status FROM outbox ORDER BY id")
        assert [row[0] for row in cur.fetchall()] == ["published", "published"]


def test_relay_preserves_trace_headers_for_broker_and_timers(db_conn) -> None:
    aggregate_id = uuid4()
    task_id = str(aggregate_id)
    traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    dispatch_at = datetime.now(timezone.utc) + timedelta(seconds=30)

    _insert_event(
        db_conn,
        aggregate_id=aggregate_id,
        event_type="task.dispatch",
        payload={"taskId": task_id},
        headers={"traceparent": traceparent},
    )
    _insert_event(
        db_conn,
        aggregate_id=aggregate_id,
        event_type="task.scheduled",
        payload={
            "taskId": task_id,
            "dispatchAt": dispatch_at.isoformat(),
            "nextExecutionTime": dispatch_at.isoformat(),
        },
        headers={"traceparent": traceparent},
    )
    db_conn.commit()

    channel = FakeChannel()
    result = relay_batch(
        db_conn,
        channel,
        batch_size=10,
        max_attempts=5,
        backoff_base_sec=1,
        backoff_max_sec=60,
        scheduled_events_enabled=True,
    )
    db_conn.commit()

    assert result.published == 1
    assert result.materialized == 1
    assert len(channel.published) == 1
    assert (
        channel.published[0]["properties"].headers.get("traceparent")
        == traceparent
    )

    with db_conn.cursor() as cur:
        cur.execute("SELECT headers FROM scheduled_events")
        assert cur.fetchone()[0].get("traceparent") == traceparent


def test_relay_dedupes_repeated_schedule_timer_semantically(db_conn) -> None:
    aggregate_id = uuid4()
    task_id = str(aggregate_id)
    dispatch_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    for _ in range(2):
        _insert_event(
            db_conn,
            aggregate_id=aggregate_id,
            event_type="task.scheduled",
            payload={"taskId": task_id, "dispatchAt": dispatch_at.isoformat()},
        )
    db_conn.commit()

    result = relay_batch(
        db_conn,
        FakeChannel(),
        batch_size=10,
        max_attempts=5,
        backoff_base_sec=1,
        backoff_max_sec=60,
        scheduled_events_enabled=True,
    )
    db_conn.commit()

    assert result.materialized == 1
    assert result.skipped == 1
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM scheduled_events")
        assert cur.fetchone()[0] == 1


def test_relay_cancellation_marks_pending_timers_cancelled(db_conn) -> None:
    aggregate_id = uuid4()
    task_id = str(aggregate_id)
    _insert_event(
        db_conn,
        aggregate_id=aggregate_id,
        event_type="task.scheduled",
        payload={
            "taskId": task_id,
            "dispatchAt": (
                datetime.now(timezone.utc) + timedelta(minutes=5)
            ).isoformat(),
        },
    )
    _insert_event(
        db_conn,
        aggregate_id=aggregate_id,
        event_type="task.cancelled",
        payload={"taskId": task_id},
    )
    db_conn.commit()

    result = relay_batch(
        db_conn,
        FakeChannel(),
        batch_size=10,
        max_attempts=5,
        backoff_base_sec=1,
        backoff_max_sec=60,
        scheduled_events_enabled=True,
    )
    db_conn.commit()

    assert result.materialized == 1
    assert result.cancelled == 1
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT status, cancelled_at FROM scheduled_events"
        )
        status, cancelled_at = cur.fetchone()
    assert status == "cancelled"
    assert cancelled_at is not None


def test_token_bucket_paces_publishes() -> None:
    clock = [0.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += seconds

    bucket = TokenBucket(
        2,
        clock=lambda: clock[0],
        sleep=sleep,
    )

    bucket.acquire()
    bucket.acquire()
    bucket.acquire()

    assert len(sleeps) == 1
    assert sleeps[0] == pytest.approx(0.5)
