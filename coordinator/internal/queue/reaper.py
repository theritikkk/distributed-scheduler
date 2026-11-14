"""
Execution reaper.

Recovers executions whose worker lease expired or whose queued dispatch was
never claimed. The reaper is the safety net for the at-least-once worker
contract: it either republishes the existing execution for another attempt or
finalizes it after max_attempts.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

import pika  # type: ignore[import-untyped]

from internal.queue.connection import connect_blocking
from internal.queue.due_consumer import build_worker_message
from internal.outbox import enqueue_event
from internal.queue.topology import TASK_QUEUE, declare_scheduler_topology
from metrics import (
    COORDINATOR_ID,
    EXECUTIONS_REAPED,
    IDEMPOTENCY_KEYS_CLEANED,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReapAction:
    """One execution handled by the reaper."""

    kind: str
    task_id: str
    execution_id: str
    task_name: str | None = None
    command_payload: Any = None
    schedule_type: str | None = None
    scheduled_for: datetime | None = None
    attempt: int = 0

    @property
    def should_publish(self) -> bool:
        return self.kind == "requeue"


def reap_stale_executions(
    conn,
    *,
    max_retries: int,
    pending_grace_sec: int,
    limit: int = 100,
    outbox_enabled: bool = True,
) -> list[ReapAction]:
    """Reap up to ``limit`` stale executions inside the caller's transaction.

    The caller must commit after this returns. Task and execution rows are
    locked together with SKIP LOCKED so multiple coordinators can run the
    reaper without processing the same execution twice.
    """
    max_attempts = max_retries + 1
    actions: list[ReapAction] = []

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT e.id, e.task_id, e.status, e.attempt, e.scheduled_for,
                   t.task_name, t.command_payload, t.schedule_type,
                   t.status AS task_status
            FROM task_executions e
            JOIN tasks t ON t.id = e.task_id
            WHERE (
                    e.status = 'pending'
                    AND COALESCE(e.next_attempt_at, e.created_at)
                        < NOW() - make_interval(secs => %s)
                  )
               OR (
                    e.status = 'running'
                    AND (
                         e.lease_expires_at < NOW()
                         OR (
                              e.lease_expires_at IS NULL
                              AND COALESCE(e.started_at, e.created_at)
                                  < NOW() - make_interval(secs => %s)
                            )
                       )
                  )
            ORDER BY e.created_at ASC
            LIMIT %s
            FOR UPDATE OF e, t SKIP LOCKED
            """,
            (pending_grace_sec, pending_grace_sec, limit),
        )
        rows = cur.fetchall()

        for (
            execution_id,
            task_id,
            execution_status,
            attempt,
            scheduled_for,
            task_name,
            command_payload,
            schedule_type,
            task_status,
        ) in rows:
            execution_id = str(execution_id)
            task_id = str(task_id)
            attempt = int(attempt or 0)

            if execution_status == "running":
                reason = "lease_expired"
            else:
                reason = "pending_timeout"

            cur.execute(
                """
                UPDATE task_execution_attempts
                SET finished_at = NOW(), status = 'failed',
                    error_message = COALESCE(error_message, %s)
                WHERE execution_id = %s AND finished_at IS NULL
                """,
                (reason, execution_id),
            )

            if task_status in ("cancelled", "completed", "failed"):
                cur.execute(
                    """
                    UPDATE task_executions
                    SET status = 'cancelled', completed_at = NOW(),
                        lease_id = NULL, lease_expires_at = NULL,
                        next_attempt_at = NULL,
                        error_message = 'task is already terminal'
                    WHERE id = %s
                    """,
                    (execution_id,),
                )
                actions.append(
                    ReapAction(
                        "cancelled",
                        task_id,
                        execution_id,
                        task_name,
                        command_payload,
                        schedule_type,
                        scheduled_for,
                        attempt,
                    )
                )
                continue

            if attempt >= max_attempts:
                cur.execute(
                    """
                    UPDATE task_executions
                    SET status = 'failed', completed_at = NOW(),
                        lease_id = NULL, lease_expires_at = NULL,
                        next_attempt_at = NULL,
                        error_message = %s
                    WHERE id = %s
                    """,
                    (f"max attempts ({max_attempts}) exceeded", execution_id),
                )
                cur.execute(
                    """
                    UPDATE tasks
                    SET status = 'failed', updated_at = NOW()
                    WHERE id = %s
                      AND status NOT IN ('cancelled', 'completed', 'failed')
                    """,
                    (task_id,),
                )
                actions.append(
                    ReapAction(
                        "failed",
                        task_id,
                        execution_id,
                        task_name,
                        command_payload,
                        schedule_type,
                        scheduled_for,
                        attempt,
                    )
                )
                continue

            # Return the existing execution to pending. The worker claim
            # increments attempt and creates a fresh attempt row.
            cur.execute(
                """
                UPDATE task_executions
                SET status = 'pending', lease_id = NULL,
                    lease_expires_at = NULL, next_attempt_at = NOW(),
                    error_message = %s
                WHERE id = %s
                """,
                (reason, execution_id),
            )
            cur.execute(
                """
                UPDATE tasks
                SET status = 'running', updated_at = NOW()
                WHERE id = %s
                  AND status NOT IN ('cancelled', 'completed', 'failed')
                """,
                (task_id,),
            )
            action = ReapAction(
                "requeue",
                task_id,
                execution_id,
                task_name,
                command_payload,
                schedule_type,
                scheduled_for,
                attempt,
            )
            if outbox_enabled:
                enqueue_event(
                    cur,
                    aggregate_type="task",
                    aggregate_id=task_id,
                    event_type="task.dispatch",
                    payload=json.loads(
                        build_worker_message(
                            task_id=action.task_id,
                            execution_id=action.execution_id,
                            task_name=action.task_name,
                            command_payload=action.command_payload,
                            schedule_type=action.schedule_type,
                            scheduled_for=action.scheduled_for,
                            dispatch_at=action.scheduled_for,
                            retry_count=action.attempt,
                            max_retries=max_retries,
                        )
                    ),
                )
            actions.append(action)

    return actions


def _publish_requeue(channel, action: ReapAction, max_retries: int) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=TASK_QUEUE,
        body=build_worker_message(
            task_id=action.task_id,
            execution_id=action.execution_id,
            task_name=action.task_name,
            command_payload=action.command_payload,
            schedule_type=action.schedule_type,
            scheduled_for=action.scheduled_for,
            dispatch_at=action.scheduled_for,
            retry_count=action.attempt,
            max_retries=max_retries,
        ),
        properties=pika.BasicProperties(
            delivery_mode=2, content_type="application/json"
        ),
    )


def run_reaper(
    get_db_conn: Callable[[], Any],
    rabbit_url: str,
    interval_sec: int,
    max_retries: int,
    pending_grace_sec: int,
    cleanup_interval_sec: int = 3_600,
    outbox_enabled: bool = True,
    queue_type: str = "classic",
) -> None:
    """Periodically reap stale executions and clean expired idempotency keys."""
    try:
        connection = connect_blocking(rabbit_url)
        channel = connection.channel()
        declare_scheduler_topology(channel, queue_type)
        channel.confirm_delivery()
    except Exception as exc:
        logger.exception("reaper: RabbitMQ connection failed: %s", exc)
        return

    next_cleanup = 0.0

    while True:
        actions: list[ReapAction] = []
        deleted_keys = 0

        try:
            conn = get_db_conn()
            conn.autocommit = False
        except Exception as exc:
            logger.exception("reaper: database connection failed: %s", exc)
            time.sleep(interval_sec)
            continue

        try:
            actions = reap_stale_executions(
                conn,
                max_retries=max_retries,
                pending_grace_sec=pending_grace_sec,
                outbox_enabled=outbox_enabled,
            )

            now_monotonic = time.monotonic()
            cleanup_due = now_monotonic >= next_cleanup
            cleanup_next = next_cleanup
            if cleanup_due:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM idempotency_keys WHERE expires_at < NOW()"
                    )
                    deleted_keys = max(0, cur.rowcount)
                cleanup_next = now_monotonic + cleanup_interval_sec

            conn.commit()
            if cleanup_due:
                next_cleanup = cleanup_next
        except Exception as exc:
            conn.rollback()
            actions = []
            deleted_keys = 0
            logger.exception("reaper: database work failed: %s", exc)
        finally:
            conn.close()

        for action in actions:
            EXECUTIONS_REAPED.labels(
                coordinator_id=COORDINATOR_ID, action=action.kind
            ).inc()
            if action.should_publish:
                if outbox_enabled:
                    logger.info(
                        "reaper: queued outbox dispatch for execution %s task %s attempt %s",
                        action.execution_id,
                        action.task_id,
                        action.attempt,
                    )
                    continue
                try:
                    _publish_requeue(channel, action, max_retries)
                    logger.info(
                        "reaper: requeued execution %s for task %s attempt %s",
                        action.execution_id,
                        action.task_id,
                        action.attempt,
                    )
                except Exception as exc:
                    logger.exception("reaper: publish failed: %s", exc)

        if deleted_keys:
            IDEMPOTENCY_KEYS_CLEANED.labels(
                coordinator_id=COORDINATOR_ID
            ).inc(deleted_keys)

        time.sleep(interval_sec)
