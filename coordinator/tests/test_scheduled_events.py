import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from internal.queue.scheduled_event_poller import poll_due_events
from internal.scheduled_events import cleanup_terminal_events


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
    destination="scheduler.due",
    payload=None,
    available_at=None,
    status="pending",
    attempts=0,
    headers=None,
):
    aggregate_id = uuid4()
    if payload is None:
        payload = {"taskId": str(aggregate_id)}
    if available_at is None:
        available_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO scheduled_events
                (aggregate_id, kind, destination, dedupe_key, payload,
                 headers, available_at, status, attempts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                str(aggregate_id),
                "wakeup" if destination == "scheduler.due" else "retry",
                destination,
                f"test:{aggregate_id}:{destination}:{available_at.isoformat()}",
                json.dumps(payload),
                json.dumps(headers or {}),
                available_at,
                status,
                attempts,
            ),
        )
        return cur.fetchone()[0]


def test_poller_publishes_only_due_rows(db_conn) -> None:
    due_id = _insert_event(db_conn)
    future_id = _insert_event(
        db_conn,
        available_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    db_conn.commit()

    channel = FakeChannel()
    result = poll_due_events(
        db_conn,
        channel,
        batch_size=10,
        max_attempts=5,
        backoff_base_sec=1,
        backoff_max_sec=60,
    )
    db_conn.commit()

    assert result.published == 1
    assert result.failed == 0
    assert len(channel.published) == 1
    publish = channel.published[0]
    assert publish["routing_key"] == "scheduler.due"
    assert publish["mandatory"] is True
    assert json.loads(publish["body"])["taskId"]

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT status, published_at FROM scheduled_events WHERE id = %s",
            (due_id,),
        )
        assert cur.fetchone()[0] == "published"
        cur.execute(
            "SELECT status FROM scheduled_events WHERE id = %s", (future_id,)
        )
        assert cur.fetchone()[0] == "pending"


def test_poller_forwards_trace_headers(db_conn) -> None:
    traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    _insert_event(db_conn, headers={"traceparent": traceparent})
    db_conn.commit()

    channel = FakeChannel()
    result = poll_due_events(
        db_conn,
        channel,
        batch_size=10,
        max_attempts=5,
        backoff_base_sec=1,
        backoff_max_sec=60,
    )
    db_conn.commit()

    assert result.published == 1
    assert (
        channel.published[0]["properties"].headers.get("traceparent")
        == traceparent
    )


def test_poller_retries_then_fails_terminal(db_conn) -> None:
    event_id = _insert_event(db_conn)
    db_conn.commit()

    result = poll_due_events(
        db_conn,
        FakeChannel(RuntimeError("unroutable")),
        batch_size=10,
        max_attempts=2,
        backoff_base_sec=60,
        backoff_max_sec=60,
    )
    db_conn.commit()

    assert result.failed == 1
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT status, attempts, available_at > NOW(), last_error "
            "FROM scheduled_events WHERE id = %s",
            (event_id,),
        )
        status, attempts, delayed, last_error = cur.fetchone()
    assert status == "pending"
    assert attempts == 1
    assert delayed is True
    assert "unroutable" in last_error

    with db_conn.cursor() as cur:
        cur.execute(
            "UPDATE scheduled_events "
            "SET available_at = NOW() - interval '1 second' WHERE id = %s",
            (event_id,),
        )
    db_conn.commit()

    result = poll_due_events(
        db_conn,
        FakeChannel(RuntimeError("still unroutable")),
        batch_size=10,
        max_attempts=2,
        backoff_base_sec=1,
        backoff_max_sec=1,
    )
    db_conn.commit()

    assert result.failed == 1
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT status, attempts FROM scheduled_events WHERE id = %s",
            (event_id,),
        )
        assert cur.fetchone() == ("failed", 2)


def test_cleanup_removes_only_old_terminal_rows(db_conn) -> None:
    old = datetime.now(timezone.utc) - timedelta(days=2)
    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO scheduled_events
                (aggregate_id, kind, destination, dedupe_key, payload,
                 available_at, status, published_at)
            VALUES (%s, 'wakeup', 'scheduler.due', 'cleanup:old',
                    '{"taskId":"old"}', %s, 'published', %s)
            """,
            (str(uuid4()), old, old),
        )
        cur.execute(
            """
            INSERT INTO scheduled_events
                (aggregate_id, kind, destination, dedupe_key, payload,
                 available_at, status)
            VALUES (%s, 'wakeup', 'scheduler.due', 'cleanup:pending',
                    '{"taskId":"pending"}', %s, 'pending')
            """,
            (str(uuid4()), old),
        )
    db_conn.commit()

    deleted = cleanup_terminal_events(db_conn, retention_days=1)
    db_conn.commit()

    assert deleted == 1
    with db_conn.cursor() as cur:
        cur.execute("SELECT dedupe_key FROM scheduled_events")
        assert [row[0] for row in cur.fetchall()] == ["cleanup:pending"]
