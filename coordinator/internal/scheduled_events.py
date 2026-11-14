"""Durable timer helpers for scheduled wakeups and retries.

The outbox relay materializes ``task.scheduled``, ``task.cancelled``, and
``task.retry`` rows into ``scheduled_events`` in the same transaction that marks
the outbox row published. A poller later publishes due rows to RabbitMQ. This
keeps timers in PostgreSQL and removes per-message TTL from the critical path.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

logger = logging.getLogger(__name__)

WAKEUP_KIND = "wakeup"
RETRY_KIND = "retry"
DUE_DESTINATION = "scheduler.due"
TASK_DESTINATION = "scheduler.tasks"


@dataclass(frozen=True)
class MaterializeResult:
    kind: str
    outcome: str
    event_id: int | None = None
    cancelled: int = 0


def parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _wakeup_values(
    outbox_id: int,
    aggregate_id: str,
    payload: Mapping[str, Any],
) -> tuple[str, datetime, str, dict[str, Any]]:
    task_id = str(payload.get("taskId") or aggregate_id)
    if not task_id:
        raise ValueError("task.scheduled payload is missing taskId")
    available_at = parse_timestamp(
        payload.get("dispatchAt") or payload.get("nextExecutionTime")
    )
    if available_at is None:
        raise ValueError("task.scheduled payload is missing dispatchAt")
    dedupe_key = f"wakeup:{task_id}:{iso_utc(available_at)}"
    return task_id, available_at, dedupe_key, {"taskId": task_id}


def _retry_values(
    outbox_id: int,
    aggregate_id: str,
    payload: Mapping[str, Any],
) -> tuple[str, datetime, str, dict[str, Any]]:
    message = payload.get("message")
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except json.JSONDecodeError as exc:
            raise ValueError("task.retry message is not valid JSON") from exc
    if not isinstance(message, dict):
        raise ValueError("task.retry payload is missing message")

    task_id = str(message.get("taskId") or aggregate_id)
    if not task_id:
        raise ValueError("task.retry message is missing taskId")
    available_at = parse_timestamp(payload.get("retryAt"))
    if available_at is None:
        raise ValueError("task.retry payload is missing retryAt")

    execution_id = str(message.get("executionId") or "")
    retry_count = message.get("retryCount")
    if execution_id:
        dedupe_key = (
            f"retry:{task_id}:{execution_id}:{retry_count}:{iso_utc(available_at)}"
        )
    else:
        dedupe_key = f"retry:{task_id}:outbox:{outbox_id}"
    return task_id, available_at, dedupe_key, message


def materialize_event(
    cur,
    *,
    outbox_id: int,
    aggregate_id: str,
    event_type: str,
    payload: Any,
    headers: Mapping[str, Any] | None = None,
) -> MaterializeResult:
    """Turn one outbox event into a scheduled event (or cancel pending ones).

    The caller owns the transaction. ``ON CONFLICT DO NOTHING`` makes duplicate
    relay deliveries safe. Cancelling marks every pending timer for the task
    cancelled; a wakeup already in RabbitMQ is ignored by the due consumer after
    it observes the task's cancelled status.
    """
    if not isinstance(payload, Mapping):
        raise ValueError("outbox payload must be a JSON object")

    task_id = str(payload.get("taskId") or aggregate_id)
    if not task_id:
        raise ValueError(f"{event_type} payload is missing taskId")

    if event_type == "task.cancelled":
        cur.execute(
            """
            UPDATE scheduled_events
            SET status = 'cancelled', cancelled_at = NOW()
            WHERE aggregate_type = 'task'
              AND aggregate_id = %s
              AND status = 'pending'
            """,
            (task_id,),
        )
        return MaterializeResult(
            kind="cancel", outcome="cancelled", cancelled=max(0, cur.rowcount)
        )

    if event_type == "task.scheduled":
        _, available_at, dedupe_key, message = _wakeup_values(
            outbox_id, task_id, payload
        )
        kind = WAKEUP_KIND
        destination = DUE_DESTINATION
    elif event_type == "task.retry":
        _, available_at, dedupe_key, message = _retry_values(
            outbox_id, task_id, payload
        )
        kind = RETRY_KIND
        destination = TASK_DESTINATION
    else:
        raise ValueError(f"event is not DB-scheduled: {event_type}")

    cur.execute(
        """
        INSERT INTO scheduled_events
            (aggregate_type, aggregate_id, kind, destination, dedupe_key,
             payload, headers, available_at)
        VALUES ('task', %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (dedupe_key) DO NOTHING
        RETURNING id
        """,
        (
            task_id,
            kind,
            destination,
            dedupe_key,
            json.dumps(message),
            json.dumps(dict(headers or {})),
            available_at,
        ),
    )
    row = cur.fetchone()
    if row is None:
        return MaterializeResult(kind=kind, outcome="duplicate")
    return MaterializeResult(kind=kind, outcome="materialized", event_id=int(row[0]))


def cleanup_terminal_events(conn, retention_days: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM scheduled_events
            WHERE status IN ('published', 'cancelled')
              AND COALESCE(published_at, cancelled_at)
                  < NOW() - make_interval(days => %s)
            """,
            (retention_days,),
        )
        return max(0, cur.rowcount)


def refresh_event_metrics(conn) -> tuple[int, int, float]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending'),
                COUNT(*) FILTER (WHERE status = 'failed'),
                COALESCE(
                    EXTRACT(EPOCH FROM (
                        NOW() - MIN(available_at) FILTER (WHERE status = 'pending')
                    )),
                    0
                )
            FROM scheduled_events
            """
        )
        pending, failed, lag = cur.fetchone()
    return int(pending or 0), int(failed or 0), max(0.0, float(lag or 0))
