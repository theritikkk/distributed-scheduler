"""
Worker: consume scheduler.tasks, claim an execution with a lease, run it, and
publish a lease-guarded result. Failures retry via scheduler.retry_delay ->
tasks; poison messages go to the DLQ after max retries.
"""
import json
import logging
import random
import signal
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pika  # type: ignore[import-untyped]
from prometheus_client import start_http_server

from config import load_config
from db_pool import WorkerDbPool
from outbox import OutboxEvent, enqueue_event, enqueue_events
from metrics import (
    CONSUMER_UP,
    LEASE_HEARTBEAT_FAILURES,
    LEASE_LOST,
    RETRY_BACKOFF,
    TASK_DURATION,
    TASKS_ACKED,
    TASKS_COMPLETED,
    TASKS_DLQ,
    TASKS_FAILED,
    TASKS_IDEMPOTENT_SKIP,
    TASKS_RETRIED,
    TASKS_STARTED,
)
from rabbit_topology import (
    DLX_EXCHANGE,
    RESULT_QUEUE,
    RETRY_DELAY_QUEUE,
    TASK_DLQ,
    TASK_QUEUE,
    declare_scheduler_topology,
)
from tracing import configure_tracing, extract_context, inject_headers, start_span

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s level=%(levelname)s service=worker worker_id=%(worker_id)s %(message)s",
)
logger = logging.getLogger(__name__)

CONFIG = load_config()
WORKER_ID = CONFIG.worker_id
DATABASE_URL = CONFIG.database_url
RABBITMQ_URL = CONFIG.rabbitmq_url
METRICS_PORT = CONFIG.metrics_port
OUTBOX_ENABLED = CONFIG.outbox_enabled
RABBITMQ_QUEUE_TYPE = CONFIG.rabbitmq_queue_type

configure_tracing(
    CONFIG.otel_service_name,
    enabled=CONFIG.otel_enabled,
    endpoint=CONFIG.otel_exporter_endpoint,
    sampler_arg=CONFIG.otel_sampler_arg,
)

DEFAULT_MAX_RETRIES = CONFIG.task_max_retries
_WORKER_LOCAL = threading.local()


def _current_worker_id() -> str:
    return getattr(_WORKER_LOCAL, "worker_id", WORKER_ID)
_BASE_BACKOFF_MS = 5000
_MAX_BACKOFF_MS = 300_000


class WorkerIdFilter(logging.Filter):
    def filter(self, record):
        record.worker_id = WORKER_ID
        return True


_worker_id_filter = WorkerIdFilter()
logger.addFilter(_worker_id_filter)
# Third-party loggers (for example pika) propagate to the same handlers; they
# need the field too or the formatter raises while reporting errors.
for _handler in logging.getLogger().handlers:
    _handler.addFilter(_worker_id_filter)


def _max_retries(msg: dict) -> int:
    try:
        return int(
            msg.get(
                "maxRetries",
                str(DEFAULT_MAX_RETRIES),
            )
        )
    except (TypeError, ValueError):
        return DEFAULT_MAX_RETRIES


def retry_backoff_ms(
    retry_count: int,
    mode: str | None = None,
) -> int:
    """Return a bounded retry delay with configurable jitter.

    ``full`` is the default: spread 0..ceiling to avoid retry storms. ``equal``
    keeps at least half of the exponential ceiling. ``none`` preserves the old
    deterministic behavior for rollback/testing.
    """
    # Cap the exponent before shifting so a malformed/hostile message cannot
    # force construction of an enormous Python integer.
    exponent = min(max(0, retry_count), 20)
    ceiling = min(_MAX_BACKOFF_MS, _BASE_BACKOFF_MS * (2 ** exponent))
    selected = (mode or CONFIG.retry_jitter_mode).lower()

    if selected == "none":
        return ceiling
    if selected == "equal":
        half = ceiling / 2
        return int(half + random.uniform(0, half))
    return int(random.uniform(0, ceiling))


_DB_POOL: WorkerDbPool | None = None
_DB_POOL_LOCK = threading.Lock()


def _db_pool() -> WorkerDbPool:
    global _DB_POOL
    if _DB_POOL is None:
        with _DB_POOL_LOCK:
            if _DB_POOL is None:
                _DB_POOL = WorkerDbPool(
                    DATABASE_URL,
                    min_connections=1,
                    max_connections=max(4, CONFIG.worker_concurrency * 2 + 4),
                    connect_timeout_sec=CONFIG.db_connect_timeout_sec,
                    acquire_timeout_sec=max(1, CONFIG.db_connect_timeout_sec),
                )
    return _DB_POOL


def get_db_conn():
    return _db_pool().get_connection()


def register_worker():
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO workers (worker_id, worker_address, status, last_heartbeat)
                VALUES (%s, %s, 'active', NOW())
                ON CONFLICT (worker_id) DO UPDATE SET status = 'active', last_heartbeat = NOW(), updated_at = NOW()
                """,
                (WORKER_ID, "local"),
            )
        conn.commit()
    finally:
        conn.close()


def run_heartbeat(interval_sec=CONFIG.heartbeat_interval_sec):
    while True:
        time.sleep(interval_sec)
        try:
            conn = get_db_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE workers SET last_heartbeat = NOW(), updated_at = NOW() WHERE worker_id = %s",
                        (WORKER_ID,),
                    )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            logger.warning("heartbeat failed: %s", e)


@dataclass(frozen=True)
class ExecutionClaim:
    execution_id: str
    lease_id: str
    attempt: int


@dataclass(frozen=True)
class RetrySchedule:
    task_id: str
    retry_at: datetime
    event_payload: dict[str, Any]


def claim_execution(
    execution_id: str, worker_id: str | None = None
) -> ExecutionClaim | None:
    """Atomically claim a pending execution and create its attempt row.

    A claim only succeeds when the task is still runnable, the execution is
    pending, and its retry deadline has passed. The database row lock makes
    duplicate messages harmless.
    """
    owner = worker_id or _current_worker_id()
    conn = get_db_conn()
    lease_id = str(uuid.uuid4())
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE task_executions e
                SET status = 'running',
                    worker_id = %s,
                    lease_id = %s,
                    lease_expires_at = NOW() + make_interval(secs => %s),
                    started_at = COALESCE(e.started_at, NOW()),
                    attempt = e.attempt + 1,
                    next_attempt_at = NULL,
                    error_message = NULL
                FROM tasks t
                WHERE e.id = %s
                  AND e.status = 'pending'
                  AND (e.next_attempt_at IS NULL OR e.next_attempt_at <= NOW())
                  AND t.id = e.task_id
                  AND t.status = 'running'
                RETURNING e.attempt
                """,
                (owner, lease_id, CONFIG.execution_lease_sec, execution_id),
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return None

            attempt = int(row[0])
            cur.execute(
                """
                INSERT INTO task_execution_attempts
                    (execution_id, attempt, worker_id, status)
                VALUES (%s, %s, %s, 'running')
                """,
                (execution_id, attempt, owner),
            )
        conn.commit()
        return ExecutionClaim(execution_id, lease_id, attempt)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def pending_retry_delay_ms(execution_id: str) -> int | None:
    """Return remaining retry delay for a pending execution, if any."""
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT e.status, t.status,
                       CASE
                         WHEN e.next_attempt_at IS NULL THEN NULL
                         ELSE GREATEST(
                           0,
                           EXTRACT(EPOCH FROM (e.next_attempt_at - NOW())) * 1000
                         )
                       END
                FROM task_executions e
                JOIN tasks t ON t.id = e.task_id
                WHERE e.id = %s
                """,
                (execution_id,),
            )
            row = cur.fetchone()
            if (
                not row
                or row[0] != "pending"
                or row[1] != "running"
                or row[2] is None
            ):
                return None
            return int(float(row[2]))
    finally:
        conn.close()


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def schedule_execution_retry(
    execution_id: str,
    lease_id: str,
    attempt: int,
    backoff_ms: int,
    error_message: str,
    *,
    retry_message: dict[str, Any] | None = None,
    outbox_enabled: bool | None = None,
) -> RetrySchedule | None:
    """Move a running execution back to pending and durably enqueue its retry.

    The execution update and the outbox row commit together. When the outbox is
    disabled, the caller publishes the returned retry event directly after this
    transaction commits.

    The lease guard ensures a worker whose lease was stolen cannot overwrite the
    new owner's state.
    """
    use_outbox = OUTBOX_ENABLED if outbox_enabled is None else outbox_enabled
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE task_executions
                SET status = 'pending',
                    lease_id = NULL,
                    lease_expires_at = NULL,
                    next_attempt_at = NOW() + make_interval(secs => %s),
                    error_message = %s
                WHERE id = %s AND lease_id = %s AND status = 'running'
                RETURNING task_id, next_attempt_at
                """,
                (
                    max(0, backoff_ms) / 1000.0,
                    error_message,
                    execution_id,
                    lease_id,
                ),
            )
            row = cur.fetchone()
            if row is None:
                conn.rollback()
                return None
            task_id, next_attempt_at = row

            cur.execute(
                """
                UPDATE task_execution_attempts
                SET finished_at = NOW(), status = 'failed',
                    error_message = %s
                WHERE execution_id = %s AND attempt = %s
                  AND finished_at IS NULL
                """,
                (error_message, execution_id, attempt),
            )

            message = dict(retry_message or {})
            message["retryCount"] = attempt
            event_payload = {
                "message": message,
                "retryAt": _iso_utc(next_attempt_at),
            }
            if use_outbox:
                enqueue_event(
                    cur,
                    OutboxEvent(
                        aggregate_id=str(task_id),
                        event_type="task.retry",
                        payload=event_payload,
                    ),
                )
        conn.commit()
        return RetrySchedule(str(task_id), next_attempt_at, event_payload)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


class LeaseHeartbeat:
    """Extend an execution lease while a task command is running."""

    def __init__(
        self,
        execution_id: str,
        lease_id: str,
        worker_id: str | None = None,
    ):
        self.execution_id = execution_id
        self.lease_id = lease_id
        self.worker_id = worker_id or WORKER_ID
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=CONFIG.lease_heartbeat_sec + 5)

    def _run(self) -> None:
        while not self._stop.wait(CONFIG.lease_heartbeat_sec):
            try:
                conn = get_db_conn()
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE task_executions
                            SET lease_expires_at = NOW() + make_interval(secs => %s)
                            WHERE id = %s AND lease_id = %s AND status = 'running'
                            """,
                            (
                                CONFIG.execution_lease_sec,
                                self.execution_id,
                                self.lease_id,
                            ),
                        )
                        updated = cur.rowcount
                    conn.commit()
                finally:
                    conn.close()

                if updated == 0:
                    LEASE_LOST.labels(worker_id=self.worker_id).inc()
                    logger.warning(
                        "lease lost execution %s; stopping heartbeat",
                        self.execution_id,
                    )
                    return
            except Exception as e:
                LEASE_HEARTBEAT_FAILURES.labels(worker_id=self.worker_id).inc()
                logger.warning(
                    "lease heartbeat failed execution %s: %s",
                    self.execution_id,
                    e,
                )


def _truncate_output(value: str, limit: int = CONFIG.max_output_bytes) -> str:
    encoded = value.encode("utf-8", errors="replace")
    if len(encoded) <= limit:
        return value
    truncated = encoded[:limit].decode("utf-8", errors="ignore")
    return truncated + "\n[output truncated by scheduler]"


def execute_task(payload_str: str, timeout_sec=CONFIG.command_timeout_sec):
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError:
        return "", None
    cmd = payload.get("command")
    if not cmd or not isinstance(cmd, str):
        return "", None
    parts = cmd.strip().split()
    if not parts:
        return "", None
    try:
        out = subprocess.run(
            parts,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
        combined = _truncate_output((out.stdout or "") + (out.stderr or ""))
        if out.returncode == 0:
            return combined, None
        return combined, Exception(f"exit code {out.returncode}")
    except subprocess.TimeoutExpired as e:
        combined = _truncate_output((e.stdout or "") + (e.stderr or ""))
        return combined, e
    except Exception as e:
        return "", e


def _result_message(
    task_id,
    execution_id,
    status,
    output,
    err_msg,
    *,
    lease_id: str,
    attempt: int,
    worker_id: str | None = None,
) -> dict[str, Any]:
    return {
        "taskId": task_id,
        "executionId": execution_id,
        "workerId": worker_id or WORKER_ID,
        "leaseId": lease_id,
        "attempt": attempt,
        "status": status,
        "output": output,
        "errorMessage": err_msg,
    }


def _publish_result_message(channel, result: dict[str, Any]) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=RESULT_QUEUE,
        body=json.dumps(result),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            headers=inject_headers(),
        ),
    )


def publish_result(
    channel,
    task_id,
    execution_id,
    status,
    output,
    err_msg,
    *,
    lease_id: str,
    attempt: int,
    worker_id: str | None = None,
):
    _publish_result_message(
        channel,
        _result_message(
            task_id,
            execution_id,
            status,
            output,
            err_msg,
            lease_id=lease_id,
            attempt=attempt,
            worker_id=worker_id,
        ),
    )


def _dlq_message(
    task_id,
    execution_id,
    retry_count,
    max_retries,
    payload_str,
    err_msg,
    lease_id: str,
    attempt: int,
    worker_id: str | None = None,
) -> dict[str, Any]:
    return {
        "reason": "max_retries_exceeded",
        "taskId": task_id,
        "executionId": execution_id,
        "workerId": worker_id or WORKER_ID,
        "leaseId": lease_id,
        "attempt": attempt,
        "retryCount": retry_count,
        "maxRetries": max_retries,
        "payload": payload_str,
        "errorMessage": err_msg,
        "timestamp": time.time(),
    }


def _record_dlq(
    task_id,
    execution_id,
    retry_count,
    err_msg,
    worker_id: str | None = None,
) -> None:
    TASKS_DLQ.labels(worker_id=worker_id or WORKER_ID).inc()
    logger.error(
        "poison message: task %s execution %s sent to DLQ after %s retries error=%s",
        task_id,
        execution_id,
        retry_count,
        err_msg,
    )


def _publish_poison_dlq(
    channel,
    task_id,
    execution_id,
    retry_count,
    max_retries,
    payload_str,
    err_msg,
    lease_id: str,
    attempt: int,
    worker_id: str | None = None,
):
    channel.basic_publish(
        exchange=DLX_EXCHANGE,
        routing_key=TASK_DLQ,
        body=json.dumps(
            _dlq_message(
                task_id,
                execution_id,
                retry_count,
                max_retries,
                payload_str,
                err_msg,
                lease_id=lease_id,
                attempt=attempt,
                worker_id=worker_id,
            )
        ),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            headers=inject_headers(),
        ),
    )
    _record_dlq(
        task_id, execution_id, retry_count, err_msg, worker_id=worker_id
    )


def enqueue_outbox_events(events: list[OutboxEvent]) -> None:
    """Commit final worker events before acking the task message."""
    conn = get_db_conn()
    try:
        enqueue_events(conn, events)
    finally:
        conn.close()


def handle_task(
    channel, method, properties, body, worker_id: str | None = None
):
    """Run one task message inside the trace context propagated by the API."""
    parent_context = extract_context(
        getattr(properties, "headers", None) if properties else None
    )
    with start_span(
        "task.process",
        context=parent_context,
        attributes={
            "messaging.system": "rabbitmq",
            "messaging.destination.name": TASK_QUEUE,
        },
    ):
        _handle_task_body(channel, method, body, worker_id)


def _handle_task_body(channel, method, body, worker_id: str | None = None):
    wid = worker_id or WORKER_ID
    _WORKER_LOCAL.worker_id = wid
    started = time.perf_counter()

    try:
        msg = json.loads(body)
    except json.JSONDecodeError:
        logger.error("invalid JSON on task queue, nacking to DLQ")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    task_id = msg.get("taskId")
    execution_id = msg.get("executionId")
    payload_str = msg.get("payload", "{}")
    retry_count = int(msg.get("retryCount") or 0)
    max_retries = _max_retries(msg)

    if not execution_id:
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    logger.info(
        "processing task %s execution %s retry=%s/%s",
        task_id,
        execution_id,
        retry_count,
        max_retries,
    )
    TASKS_STARTED.labels(worker_id=wid).inc()

    claim = claim_execution(str(execution_id))
    if claim is None:
        delay_ms = pending_retry_delay_ms(str(execution_id))
        if delay_ms is not None:
            if OUTBOX_ENABLED:
                # The retry outbox row committed with next_attempt_at. The
                # coordinator relay owns the timer; republishing here would
                # create a second TTL path or a duplicate retry.
                logger.info(
                    "task %s execution %s is not due yet; durable retry already exists",
                    task_id,
                    execution_id,
                )
            else:
                channel.basic_publish(
                    exchange="",
                    routing_key=RETRY_DELAY_QUEUE,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type="application/json",
                        expiration=str(max(1, delay_ms)),
                        headers=inject_headers(),
                    ),
                )
                logger.info(
                    "task %s execution %s is not due yet; republished in %sms",
                    task_id,
                    execution_id,
                    delay_ms,
                )
        else:
            TASKS_IDEMPOTENT_SKIP.labels(worker_id=wid).inc()
            logger.info(
                "idempotent skip task %s execution %s (not claimable)",
                task_id,
                execution_id,
            )
        channel.basic_ack(delivery_tag=method.delivery_tag)
        TASKS_ACKED.labels(worker_id=wid).inc()
        return

    attempt = claim.attempt
    max_attempts = max_retries + 1
    logger.info(
        "claimed task %s execution %s attempt=%s/%s lease=%s",
        task_id,
        execution_id,
        attempt,
        max_attempts,
        claim.lease_id,
    )

    heartbeat = LeaseHeartbeat(str(execution_id), claim.lease_id)
    heartbeat.worker_id = wid
    heartbeat.start()
    try:
        output, err = execute_task(payload_str)
        # The command is finished; stop extending the lease before writing the
        # outcome. A retry update or result publish is immediate and still
        # guarded by the current lease id.
        heartbeat.stop()
        duration = time.perf_counter() - started
        TASK_DURATION.labels(worker_id=wid).observe(duration)

        if err is None:
            result = _result_message(
                task_id,
                execution_id,
                "completed",
                output,
                "",
                lease_id=claim.lease_id,
                attempt=attempt,
                worker_id=wid,
            )
            if OUTBOX_ENABLED:
                enqueue_outbox_events(
                    [
                        OutboxEvent(
                            aggregate_id=str(task_id),
                            event_type="task.result",
                            payload=result,
                        )
                    ]
                )
            else:
                _publish_result_message(channel, result)
            TASKS_COMPLETED.labels(worker_id=wid).inc()
            logger.info(
                "task %s execution %s completed in %.2fs attempt=%s",
                task_id,
                execution_id,
                duration,
                attempt,
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            TASKS_ACKED.labels(worker_id=wid).inc()
            return

        err_msg = str(err)
        TASKS_FAILED.labels(worker_id=wid).inc()

        if attempt <= max_retries:
            backoff_ms = retry_backoff_ms(attempt - 1)
            RETRY_BACKOFF.labels(worker_id=wid).observe(backoff_ms / 1000.0)

            schedule = schedule_execution_retry(
                str(execution_id),
                claim.lease_id,
                attempt,
                backoff_ms,
                err_msg,
                retry_message=msg,
                outbox_enabled=OUTBOX_ENABLED,
            )
            if not schedule:
                LEASE_LOST.labels(worker_id=wid).inc()
                logger.warning(
                    "task %s execution %s lease lost before retry scheduling",
                    task_id,
                    execution_id,
                )
                channel.basic_ack(delivery_tag=method.delivery_tag)
                TASKS_ACKED.labels(worker_id=wid).inc()
                return

            if not OUTBOX_ENABLED:
                channel.basic_publish(
                    exchange="",
                    routing_key=RETRY_DELAY_QUEUE,
                    body=json.dumps(schedule.event_payload["message"]),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type="application/json",
                        expiration=str(backoff_ms),
                        headers=inject_headers(),
                    ),
                )
            TASKS_RETRIED.labels(worker_id=wid).inc()
            logger.warning(
                "task %s execution %s failed in %.2fs (attempt %s/%s), retry in %sms: %s",
                task_id,
                execution_id,
                duration,
                attempt,
                max_attempts,
                backoff_ms,
                err_msg,
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            TASKS_ACKED.labels(worker_id=wid).inc()
            return

        result = _result_message(
            task_id,
            execution_id,
            "failed",
            output,
            err_msg,
            lease_id=claim.lease_id,
            attempt=attempt,
            worker_id=wid,
        )
        if OUTBOX_ENABLED:
            dlq = _dlq_message(
                task_id,
                execution_id,
                max(0, attempt - 1),
                max_retries,
                payload_str,
                err_msg,
                lease_id=claim.lease_id,
                attempt=attempt,
                worker_id=wid,
            )
            enqueue_outbox_events(
                [
                    OutboxEvent(
                        aggregate_id=str(task_id),
                        event_type="task.dead_letter",
                        payload=dlq,
                    ),
                    OutboxEvent(
                        aggregate_id=str(task_id),
                        event_type="task.result",
                        payload=result,
                    ),
                ]
            )
            _record_dlq(
                task_id,
                execution_id,
                max(0, attempt - 1),
                err_msg,
                worker_id=wid,
            )
        else:
            _publish_poison_dlq(
                channel,
                task_id,
                execution_id,
                max(0, attempt - 1),
                max_retries,
                payload_str,
                err_msg,
                lease_id=claim.lease_id,
                attempt=attempt,
                worker_id=wid,
            )
            _publish_result_message(channel, result)
        logger.error(
            "task %s execution %s failed permanently after %.2fs and %s retries",
            task_id,
            execution_id,
            duration,
            max(0, attempt - 1),
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
        TASKS_ACKED.labels(worker_id=wid).inc()
    finally:
        heartbeat.stop()


_shutdown = threading.Event()
_consumer_connections: dict[str, tuple] = {}
_consumer_lock = threading.Lock()


def _consumer_loop(slot: int) -> None:
    """Run one blocking consumer with its own connection and prefetch of one."""
    worker_id = f"{WORKER_ID}-{slot}"
    backoff_sec = 1.0

    while not _shutdown.is_set():
        connection = None
        try:
            connection = pika.BlockingConnection(
                pika.URLParameters(RABBITMQ_URL)
            )
            channel = connection.channel()
            declare_scheduler_topology(channel, RABBITMQ_QUEUE_TYPE)
            channel.confirm_delivery()
            channel.basic_qos(prefetch_count=1)
            with _consumer_lock:
                _consumer_connections[worker_id] = (connection, channel)
            if _shutdown.is_set():
                break
            CONSUMER_UP.labels(worker_id=worker_id).set(1)
            backoff_sec = 1.0
            logger.info(
                "worker slot %s consuming queue %s", worker_id, TASK_QUEUE
            )
            channel.basic_consume(
                queue=TASK_QUEUE,
                on_message_callback=lambda ch, method, props, body, wid=worker_id: handle_task(
                    ch, method, props, body, wid
                ),
            )
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            if not _shutdown.is_set():
                logger.warning(
                    "worker slot %s connection lost: %s; reconnecting in %.1fs",
                    worker_id,
                    e,
                    backoff_sec,
                )
        except Exception as e:
            if not _shutdown.is_set():
                logger.exception(
                    "worker slot %s consumer failed: %s", worker_id, e
                )
        finally:
            with _consumer_lock:
                _consumer_connections.pop(worker_id, None)
            CONSUMER_UP.labels(worker_id=worker_id).set(0)
            if connection is not None and connection.is_open:
                try:
                    connection.close()
                except Exception:
                    pass

        if _shutdown.wait(backoff_sec):
            break
        backoff_sec = min(backoff_sec * 2, 30.0)


def _handle_shutdown(_signum, _frame) -> None:
    _shutdown.set()
    with _consumer_lock:
        consumers = list(_consumer_connections.values())
    for connection, channel in consumers:
        try:
            connection.add_callback_threadsafe(channel.stop_consuming)
        except Exception:
            pass


def main():
    start_http_server(METRICS_PORT)
    logger.info("Prometheus metrics on :%s/metrics", METRICS_PORT)

    try:
        register_worker()
    except Exception as e:
        logger.warning("register_worker: %s", e)

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)
    threading.Thread(target=run_heartbeat, daemon=True).start()

    consumers = []
    for slot in range(1, CONFIG.worker_concurrency + 1):
        thread = threading.Thread(
            target=_consumer_loop,
            args=(slot,),
            name=f"consumer-{slot}",
            daemon=True,
        )
        thread.start()
        consumers.append(thread)

    logger.info(
        "worker %s started %s consumer slot(s)",
        WORKER_ID,
        CONFIG.worker_concurrency,
    )
    _shutdown.wait()
    for thread in consumers:
        thread.join(timeout=5)
    logger.info("worker %s stopped", WORKER_ID)


if __name__ == "__main__":
    main()
    sys.exit(0)
