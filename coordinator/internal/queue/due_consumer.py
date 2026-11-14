"""
Consume scheduler.due: wakeups after TTL from scheduler.delay (or direct publish).
Row-lock task, create one execution per base slot, publish to scheduler.tasks.
"""
import json
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pika  # type: ignore[import-untyped]

from .connection import connect_blocking
from .publisher import publish_schedule_wakeup
from internal.outbox import enqueue_event
from internal.scheduling import as_utc
from internal.tracing import extract_context, inject_headers, start_span
from metrics import (
    COORDINATOR_ID,
    CONSUMER_ERRORS,
    CONSUMER_RECONNECTS,
    DISPATCH_LATENCY,
    DUE_DISPATCHED,
    TASK_DISPATCH_DELAY_SECONDS,
    TASK_JITTER_SECONDS,
    TASK_SCHEDULE_LAG_SECONDS,
    TASK_SLOT_CONFLICTS,
)
from .topology import DUE_QUEUE, TASK_QUEUE, declare_scheduler_topology

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DueAction:
    """Decision made while holding the task row lock."""

    kind: str
    task_id: str
    execution_id: str | None = None
    task_name: str | None = None
    command_payload: Any = None
    schedule_type: str | None = None
    scheduled_for: datetime | None = None
    dispatch_at: datetime | None = None
    next_execution_time: datetime | None = None
    max_retries: int = 0
    jitter_seconds: int = 0
    schedule_lag_seconds: float | None = None
    dispatch_delay_seconds: float | None = None


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = as_utc(value) or value
    return normalized.isoformat().replace("+00:00", "Z")


def _jitter_seconds(base: datetime | None, dispatch_at: datetime | None) -> int:
    if base is None or dispatch_at is None:
        return 0
    return max(0, int((dispatch_at - base).total_seconds()))


def dispatch_due_task(
    conn,
    task_id: str,
    max_retries: int,
    *,
    outbox_enabled: bool = True,
) -> DueAction:
    """Create an execution for one due task, or explain why it should be skipped.

    The caller owns the transaction. A ``dispatch`` result must be committed before
    publishing; a ``skip`` or ``slot_conflict`` result should be rolled back.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT task_name, command_payload, schedule_type, status,
                   next_execution_time, dispatch_at,
                   COALESCE(dispatch_at, next_execution_time) AS effective_dispatch_at,
                   (COALESCE(dispatch_at, next_execution_time) <= NOW()) AS is_due
            FROM tasks WHERE id = %s FOR UPDATE
            """,
            (task_id,),
        )

        row = cur.fetchone()
        if not row:
            return DueAction("skip", task_id)

        (
            task_name,
            command_payload,
            schedule_type,
            status,
            next_exec,
            dispatch_at,
            effective_dispatch_at,
            is_due,
        ) = row

        if status != "scheduled":
            return DueAction("skip", task_id)

        next_exec = as_utc(next_exec)
        dispatch_at = as_utc(dispatch_at)
        effective_dispatch_at = as_utc(effective_dispatch_at)

        # Use the database clock for the due decision. The application clock is
        # only used for deterministic offset math.
        if not is_due:
            return DueAction(
                "redeliver",
                task_id,
                next_execution_time=effective_dispatch_at,
                dispatch_at=effective_dispatch_at,
            )

        # The base schedule is the execution identity. A duplicate wakeup for
        # the same slot conflicts on (task_id, scheduled_for) and is a no-op.
        cur.execute(
            """
            INSERT INTO task_executions (task_id, scheduled_for, status)
            VALUES (%s, %s, 'pending')
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            (task_id, next_exec),
        )
        execution_row = cur.fetchone()
        if not execution_row:
            return DueAction(
                "slot_conflict",
                task_id,
                scheduled_for=next_exec,
                dispatch_at=effective_dispatch_at,
            )
        execution_id = str(execution_row[0])

        cur.execute(
            """
            SELECT EXTRACT(EPOCH FROM (NOW() - %s)),
                   EXTRACT(EPOCH FROM (NOW() - %s))
            """,
            (next_exec, effective_dispatch_at),
        )
        timing = cur.fetchone()
        schedule_lag = float(timing[0]) if timing and timing[0] is not None else None
        dispatch_delay = (
            float(timing[1]) if timing and timing[1] is not None else None
        )

        cur.execute(
            """
            UPDATE tasks SET status = 'running', updated_at = NOW()
            WHERE id = %s AND status = 'scheduled'
            """,
            (task_id,),
        )
        if cur.rowcount == 0:
            cur.execute("DELETE FROM task_executions WHERE id = %s", (execution_id,))
            return DueAction("skip", task_id)

    action = DueAction(
        "dispatch",
        task_id,
        execution_id=execution_id,
        task_name=task_name,
        command_payload=command_payload,
        schedule_type=schedule_type,
        scheduled_for=next_exec,
        dispatch_at=effective_dispatch_at,
        max_retries=max_retries,
        jitter_seconds=_jitter_seconds(next_exec, dispatch_at),
        schedule_lag_seconds=schedule_lag,
        dispatch_delay_seconds=dispatch_delay,
    )

    if outbox_enabled:
        with conn.cursor() as cur:
            enqueue_event(
                cur,
                aggregate_type="task",
                aggregate_id=task_id,
                event_type="task.dispatch",
                payload=json.loads(build_task_message(action)),
            )

    return action


def build_task_message(action: DueAction) -> str:
    """Build the worker-facing message for a dispatched due task."""
    return build_worker_message(
        task_id=action.task_id,
        execution_id=action.execution_id,
        task_name=action.task_name,
        command_payload=action.command_payload,
        schedule_type=action.schedule_type,
        scheduled_for=action.scheduled_for,
        dispatch_at=action.dispatch_at,
        retry_count=0,
        max_retries=action.max_retries,
    )


def build_worker_message(
    *,
    task_id: str,
    execution_id: str | None,
    task_name: str | None,
    command_payload: Any,
    schedule_type: str | None,
    scheduled_for: datetime | None,
    dispatch_at: datetime | None,
    retry_count: int,
    max_retries: int,
) -> str:
    """Build the worker-facing message shared by due dispatch and the reaper."""
    payload_str = (
        json.dumps(command_payload)
        if isinstance(command_payload, dict)
        else (command_payload or "{}")
    )
    if isinstance(payload_str, bytes):
        payload_str = payload_str.decode("utf-8")

    return json.dumps(
        {
            "taskId": task_id,
            "executionId": execution_id,
            "taskName": task_name,
            "payload": payload_str,
            "scheduleType": schedule_type,
            "scheduledFor": _iso(scheduled_for),
            "dispatchAt": _iso(dispatch_at),
            "retryCount": retry_count,
            "maxRetries": max_retries,
        }
    )


def handle_due_body(
    conn,
    channel,
    body: bytes,
    max_retries: int,
    *,
    outbox_enabled: bool = True,
    scheduled_events_enabled: bool = False,
) -> None:
    """Handle one due wakeup and return only after commit and publish."""
    msg = json.loads(body)
    task_id = msg.get("taskId")
    if not task_id:
        return

    conn.autocommit = False
    try:
        action = dispatch_due_task(
            conn,
            str(task_id),
            max_retries,
            outbox_enabled=outbox_enabled,
        )
        if action.kind == "dispatch":
            conn.commit()
        else:
            conn.rollback()
    except Exception:
        conn.rollback()
        raise

    if action.kind == "redeliver":
        if scheduled_events_enabled:
            # The authoritative timer already lives in scheduled_events. This
            # wakeup was an early duplicate (for example from a rescheduled
            # task); do not create a TTL message or a fresh DB timer.
            logger.info(
                "due consumer: task %s is not due yet; DB timer remains authoritative",
                action.task_id,
            )
        else:
            publish_schedule_wakeup(
                channel,
                action.task_id,
                action.dispatch_at or action.next_execution_time,
            )
    elif action.kind == "slot_conflict":
        TASK_SLOT_CONFLICTS.inc()
        logger.info(
            "due consumer: duplicate slot for task %s at %s",
            action.task_id,
            action.scheduled_for,
        )
    elif action.kind == "dispatch":
        if not outbox_enabled:
            channel.basic_publish(
                exchange="",
                routing_key=TASK_QUEUE,
                body=build_task_message(action),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                    headers=inject_headers(),
                ),
                mandatory=True,
            )
        DUE_DISPATCHED.labels(coordinator_id=COORDINATOR_ID).inc()
        if action.schedule_type is not None:
            if action.jitter_seconds is not None:
                TASK_JITTER_SECONDS.labels(
                    schedule_type=action.schedule_type
                ).observe(action.jitter_seconds)
            if action.schedule_lag_seconds is not None:
                TASK_SCHEDULE_LAG_SECONDS.labels(
                    schedule_type=action.schedule_type
                ).observe(max(0.0, action.schedule_lag_seconds))
        if action.dispatch_delay_seconds is not None:
            TASK_DISPATCH_DELAY_SECONDS.observe(
                max(0.0, action.dispatch_delay_seconds)
            )
        logger.info(
            "due consumer: task %s execution %s slot %s",
            action.task_id,
            action.execution_id,
            action.scheduled_for,
        )


def _jitter(seconds: float) -> float:
    return seconds * random.uniform(0.5, 1.0)


def run_due_consumer(
    get_db_conn,
    rabbit_url: str,
    max_retries: int,
    outbox_enabled: bool = True,
    *,
    scheduled_events_enabled: bool = False,
    queue_type: str = "classic",
    backpressure=None,
    dispatch_gate=None,
):
    """Pull due wakeups with explicit ack rules and connection recovery.

    ``basic_get`` gives the coordinator a hard pause boundary: while
    backpressure is active it does not fetch another message, but it keeps
    calling ``process_data_events`` so heartbeats and confirms continue.
    """
    backoff = 1.0
    failed_connections = 0

    while True:
        connection = None
        try:
            connection = connect_blocking(rabbit_url)
            channel = connection.channel()
            declare_scheduler_topology(channel, queue_type)
            channel.confirm_delivery()
            if failed_connections:
                CONSUMER_RECONNECTS.labels(consumer="due").inc()
                failed_connections = 0
            logger.info("consuming %s", DUE_QUEUE)
            backoff = 1.0

            while True:
                if backpressure is not None and not backpressure.allow("due"):
                    connection.process_data_events(time_limit=0.2)
                    continue
                if dispatch_gate is not None and not dispatch_gate.consume():
                    connection.process_data_events(time_limit=0.05)
                    continue

                method, properties, body = channel.basic_get(
                    queue=DUE_QUEUE, auto_ack=False
                )
                if method is None:
                    connection.process_data_events(time_limit=0.2)
                    continue

                started = time.monotonic()
                conn = None
                try:
                    conn = get_db_conn()
                    with start_span(
                        "due.process",
                        context=extract_context(
                            getattr(properties, "headers", None)
                        ),
                        attributes={
                            "messaging.system": "rabbitmq",
                            "messaging.destination.name": DUE_QUEUE,
                        },
                    ):
                        handle_due_body(
                            conn,
                            channel,
                            body,
                            max_retries,
                            outbox_enabled=outbox_enabled,
                            scheduled_events_enabled=scheduled_events_enabled,
                        )
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                    DISPATCH_LATENCY.labels(consumer="due").observe(
                        time.monotonic() - started
                    )
                    backoff = 1.0
                except json.JSONDecodeError:
                    CONSUMER_ERRORS.labels(
                        consumer="due", kind="permanent"
                    ).inc()
                    logger.error("due consumer: invalid JSON, dropping message")
                    channel.basic_nack(
                        delivery_tag=method.delivery_tag, requeue=False
                    )
                except Exception as exc:
                    CONSUMER_ERRORS.labels(
                        consumer="due", kind="transient"
                    ).inc()
                    logger.warning("due consumer transient error: %s", exc)
                    channel.basic_nack(
                        delivery_tag=method.delivery_tag, requeue=True
                    )
                    connection.process_data_events(time_limit=_jitter(backoff))
                    backoff = min(backoff * 2, 30.0)
                finally:
                    if conn is not None:
                        conn.close()

        except KeyboardInterrupt:
            raise
        except Exception as exc:
            failed_connections += 1
            CONSUMER_ERRORS.labels(consumer="due", kind="connection").inc()
            logger.warning("due consumer connection lost: %s", exc)
            time.sleep(_jitter(backoff))
            backoff = min(backoff * 2, 30.0)
        finally:
            if connection is not None and connection.is_open:
                try:
                    connection.close()
                except Exception:
                    pass
