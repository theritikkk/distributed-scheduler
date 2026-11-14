"""
Consume scheduler.results: apply lease-guarded execution updates and task state.
Recurring tasks: schedule the next base slot (plus stable jitter) via publisher.
"""
import json
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from .connection import connect_blocking
from .publisher import publish_schedule_wakeup_url
from internal.outbox import enqueue_event
from internal.scheduling import (
    as_utc,
    cron_period_seconds,
    next_run_for_task,
)
from internal.tracing import extract_context, start_span
from metrics import (
    COORDINATOR_ID,
    CONSUMER_ERRORS,
    CONSUMER_RECONNECTS,
    DISPATCH_LATENCY,
    RESULTS_APPLIED,
    RESULTS_REJECTED_STALE_LEASE,
    RESULTS_SKIPPED_IDEMPOTENT,
    TASK_NEXT_RUN_DRIFT_SECONDS,
)
from .topology import RESULT_QUEUE, declare_scheduler_topology

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResultApplication:
    """Outcome of applying one worker result."""

    applied: bool
    task_id: str
    execution_id: str
    status: str
    next_run: datetime | None = None
    next_dispatch_at: datetime | None = None
    reason: str = "applied"


def _map_status(status: str) -> str:
    if status == "completed":
        return "completed"
    if status in ("failed", "timeout", "cancelled"):
        return "failed"
    return "scheduled"


def apply_result(
    conn,
    msg: dict,
    *,
    jitter_enabled: bool = False,
    outbox_enabled: bool = True,
) -> ResultApplication:
    """Apply a result inside the caller's transaction.

    Duplicate deliveries are safe: the execution update is conditional on
    ``completed_at IS NULL``. The caller commits when ``applied`` is true and
    rolls back otherwise.
    """
    task_id = str(msg.get("taskId") or "")
    execution_id = str(msg.get("executionId") or "")
    lease_id = str(msg.get("leaseId") or "").strip()
    raw_status = str(msg.get("status", "failed"))
    status = (
        raw_status
        if raw_status in ("completed", "failed", "timeout", "cancelled")
        else "failed"
    )
    output = str(msg.get("output") or "")
    error_message = str(msg.get("errorMessage") or "")

    if not lease_id:
        return ResultApplication(
            False, task_id, execution_id, status, reason="missing_lease"
        )

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE task_executions
            SET completed_at = NOW(), status = %s, output = %s,
                error_message = %s, lease_expires_at = NULL,
                next_attempt_at = NULL
            WHERE id = %s AND task_id = %s AND lease_id = %s
              AND status = 'running' AND completed_at IS NULL
            RETURNING attempt
            """,
            (status, output, error_message, execution_id, task_id, lease_id),
        )
        execution_row = cur.fetchone()
        if not execution_row:
            cur.execute(
                """
                SELECT completed_at, lease_id
                FROM task_executions
                WHERE id = %s AND task_id = %s
                """,
                (execution_id, task_id),
            )
            current = cur.fetchone()
            reason = "stale_lease"
            if current and current[0] is not None:
                reason = "duplicate"
            return ResultApplication(
                False, task_id, execution_id, status, reason=reason
            )

        attempt = int(execution_row[0] or 0)
        attempt_status = (
            status
            if status in ("completed", "failed", "timeout", "cancelled")
            else "failed"
        )
        if attempt > 0:
            cur.execute(
                """
                UPDATE task_execution_attempts
                SET finished_at = NOW(), status = %s, error_message = %s
                WHERE execution_id = %s AND attempt = %s
                  AND finished_at IS NULL
                """,
                (attempt_status, error_message, execution_id, attempt),
            )

        cur.execute(
            """
            SELECT schedule_type, cron_expression, jitter_seconds,
                   jitter_policy, status
            FROM tasks
            WHERE id = %s
            FOR UPDATE
            """,
            (task_id,),
        )
        task_row = cur.fetchone()
        if not task_row:
            return ResultApplication(True, task_id, execution_id, status)

        schedule_type, cron_expr, jitter_seconds, jitter_policy, task_status = task_row
        task_cancelled = task_status == "cancelled"

        if not task_cancelled:
            cur.execute(
                """
                UPDATE tasks
                SET status = %s, updated_at = NOW()
                WHERE id = %s AND status <> 'cancelled'
                """,
                (_map_status(status), task_id),
            )
            task_cancelled = cur.rowcount == 0

        next_run = None
        next_dispatch_at = None

        if schedule_type == "recurring" and cron_expr and not task_cancelled:
            cur.execute(
                """
                SELECT COALESCE(scheduled_for, started_at)
                FROM task_executions
                WHERE id = %s AND task_id = %s
                """,
                (execution_id, task_id),
            )
            execution_row = cur.fetchone()
            scheduled_for = (
                as_utc(execution_row[0]) if execution_row and execution_row[0] else None
            )
            if scheduled_for is None:
                scheduled_for = datetime.now(timezone.utc)

            now = datetime.now(timezone.utc)
            next_run, next_dispatch_at = next_run_for_task(
                task_id,
                cron_expr,
                scheduled_for,
                now,
                int(jitter_seconds or 0),
                str(jitter_policy or "stable"),
                jitter_enabled=jitter_enabled,
            )

            cur.execute(
                """
                UPDATE tasks
                SET next_execution_time = %s,
                    dispatch_at = %s,
                    status = 'scheduled',
                    updated_at = NOW()
                WHERE id = %s AND status <> 'cancelled'
                """,
                (next_run, next_dispatch_at, task_id),
            )
            if cur.rowcount == 0:
                next_run = None
                next_dispatch_at = None
            elif next_run is not None:
                period = cron_period_seconds(cron_expr, scheduled_for)
                drift = max(
                    0.0,
                    (next_run - scheduled_for).total_seconds() - period,
                )
                TASK_NEXT_RUN_DRIFT_SECONDS.labels(
                    schedule_type="recurring"
                ).observe(drift)

                if outbox_enabled:
                    enqueue_event(
                        cur,
                        aggregate_type="task",
                        aggregate_id=task_id,
                        event_type="task.scheduled",
                        payload={
                            "taskId": task_id,
                            "nextExecutionTime": next_run.isoformat().replace(
                                "+00:00", "Z"
                            ),
                            "dispatchAt": next_dispatch_at.isoformat().replace(
                                "+00:00", "Z"
                            ),
                        },
                    )

    return ResultApplication(
        True,
        task_id,
        execution_id,
        status,
        next_run,
        next_dispatch_at,
    )


def handle_result_body(
    get_db_conn,
    channel,
    body: bytes,
    *,
    rabbit_url: str,
    scheduler_jitter_enabled: bool = False,
    outbox_enabled: bool = True,
) -> None:
    """Apply one result and return only after commit (or a safe duplicate)."""
    msg = json.loads(body)
    next_dispatch_at = None
    conn = get_db_conn()
    try:
        conn.autocommit = False
        result = apply_result(
            conn,
            msg,
            jitter_enabled=scheduler_jitter_enabled,
            outbox_enabled=outbox_enabled,
        )
        if not result.applied:
            conn.rollback()
            if result.reason in ("stale_lease", "missing_lease"):
                RESULTS_REJECTED_STALE_LEASE.labels(
                    coordinator_id=COORDINATOR_ID
                ).inc()
            else:
                RESULTS_SKIPPED_IDEMPOTENT.labels(
                    coordinator_id=COORDINATOR_ID
                ).inc()
            return

        conn.commit()
        next_dispatch_at = result.next_dispatch_at or result.next_run
        RESULTS_APPLIED.labels(
            coordinator_id=COORDINATOR_ID, status=result.status
        ).inc()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    if next_dispatch_at is not None and not outbox_enabled:
        try:
            publish_schedule_wakeup_url(
                rabbit_url, result.task_id, next_dispatch_at
            )
        except Exception as e:
            logger.exception("schedule next recurring wakeup: %s", e)


def _jitter(seconds: float) -> float:
    return seconds * random.uniform(0.5, 1.0)


def run_result_consumer(
    get_db_conn,
    rabbit_url: str,
    scheduler_jitter_enabled: bool = False,
    outbox_enabled: bool = True,
    *,
    queue_type: str = "classic",
    backpressure=None,
):
    backoff = 1.0
    failed_connections = 0

    while True:
        connection = None
        try:
            connection = connect_blocking(rabbit_url)
            channel = connection.channel()
            declare_scheduler_topology(channel, queue_type)
            if failed_connections:
                CONSUMER_RECONNECTS.labels(consumer="result").inc()
                failed_connections = 0
            logger.info("consuming results from %s", RESULT_QUEUE)
            backoff = 1.0

            while True:
                if backpressure is not None and not backpressure.allow("result"):
                    connection.process_data_events(time_limit=0.2)
                    continue

                method, properties, body = channel.basic_get(
                    queue=RESULT_QUEUE, auto_ack=False
                )
                if method is None:
                    connection.process_data_events(time_limit=0.2)
                    continue

                started = time.monotonic()
                try:
                    with start_span(
                        "result.process",
                        context=extract_context(
                            getattr(properties, "headers", None)
                        ),
                        attributes={
                            "messaging.system": "rabbitmq",
                            "messaging.destination.name": RESULT_QUEUE,
                        },
                    ):
                        handle_result_body(
                            get_db_conn,
                            channel,
                            body,
                            rabbit_url=rabbit_url,
                            scheduler_jitter_enabled=scheduler_jitter_enabled,
                            outbox_enabled=outbox_enabled,
                        )
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                    DISPATCH_LATENCY.labels(consumer="result").observe(
                        time.monotonic() - started
                    )
                    backoff = 1.0
                except json.JSONDecodeError:
                    CONSUMER_ERRORS.labels(
                        consumer="result", kind="permanent"
                    ).inc()
                    logger.error(
                        "result consumer: invalid JSON, dropping message"
                    )
                    channel.basic_nack(
                        delivery_tag=method.delivery_tag, requeue=False
                    )
                except Exception as exc:
                    CONSUMER_ERRORS.labels(
                        consumer="result", kind="transient"
                    ).inc()
                    logger.warning("result consumer transient error: %s", exc)
                    channel.basic_nack(
                        delivery_tag=method.delivery_tag, requeue=True
                    )
                    connection.process_data_events(time_limit=_jitter(backoff))
                    backoff = min(backoff * 2, 30.0)

        except KeyboardInterrupt:
            raise
        except Exception as exc:
            failed_connections += 1
            CONSUMER_ERRORS.labels(consumer="result", kind="connection").inc()
            logger.warning("result consumer connection lost: %s", exc)
            time.sleep(_jitter(backoff))
            backoff = min(backoff * 2, 30.0)
        finally:
            if connection is not None and connection.is_open:
                try:
                    connection.close()
                except Exception:
                    pass
