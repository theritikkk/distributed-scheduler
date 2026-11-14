"""
Transactional outbox relay.

Rows are claimed with FOR UPDATE SKIP LOCKED, published with publisher
confirms and mandatory routing, then marked published in the same transaction.
A crash after publish but before commit can duplicate an event; consumers are
idempotent, so that is the safe failure mode.
"""
from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Callable

import pika  # type: ignore[import-untyped]

from internal.queue.connection import connect_blocking
from internal.queue.topology import (
    DLX_EXCHANGE,
    DUE_QUEUE,
    RESULT_QUEUE,
    RETRY_DELAY_QUEUE,
    TASK_DLQ,
    TASK_QUEUE,
    declare_scheduler_topology,
)
from internal.scheduled_events import materialize_event
from internal.tracing import extract_context, start_span
from metrics import (
    OUTBOX_CLEANUP_DELETED,
    OUTBOX_FAILED_TOTAL,
    OUTBOX_LAG_SECONDS,
    OUTBOX_PENDING_ROWS,
    OUTBOX_PUBLISHED_TOTAL,
    OUTBOX_PUBLISH_DURATION_SECONDS,
    OUTBOX_PUBLISH_FAILURES_TOTAL,
    OUTBOX_SKIPPED_TOTAL,
    SCHEDULED_EVENTS_CANCELLED_TOTAL,
    SCHEDULED_EVENTS_MATERIALIZED_TOTAL,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PublishPlan:
    exchange: str
    routing_key: str
    body: str
    expiration_ms: int | None = None
    headers: dict[str, Any] | None = None


@dataclass(frozen=True)
class RelayResult:
    published: int = 0
    failed: int = 0
    skipped: int = 0
    materialized: int = 0
    cancelled: int = 0


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _retry_delay_ms(payload: dict[str, Any]) -> int:
    retry_at = _parse_timestamp(payload.get("retryAt"))
    if retry_at is None:
        return max(0, int(payload.get("delayMs") or 0))
    remaining = (retry_at - datetime.now(timezone.utc)).total_seconds() * 1000
    return max(0, int(remaining))


def build_publish_plan(
    event_type: str,
    payload: Any,
    *,
    now: datetime | None = None,
) -> PublishPlan:
    """Translate one outbox event into a confirmed RabbitMQ publish."""
    if not isinstance(payload, dict):
        raise ValueError("outbox payload must be a JSON object")

    if event_type in ("task.scheduled", "task.cancelled"):
        task_id = payload.get("taskId")
        if not task_id:
            raise ValueError(f"{event_type} payload is missing taskId")
        return PublishPlan(
            exchange="",
            routing_key=DUE_QUEUE,
            body=json.dumps({"taskId": str(task_id)}),
        )

    if event_type == "task.dispatch":
        return PublishPlan(
            exchange="",
            routing_key=TASK_QUEUE,
            body=json.dumps(payload),
        )

    if event_type == "task.result":
        return PublishPlan(
            exchange="",
            routing_key=RESULT_QUEUE,
            body=json.dumps(payload),
        )

    if event_type == "task.retry":
        message = payload.get("message")
        if isinstance(message, str):
            body = message
        elif isinstance(message, dict):
            body = json.dumps(message)
        else:
            raise ValueError("task.retry payload is missing message")
        delay_ms = _retry_delay_ms(payload)
        if now is not None:
            retry_at = _parse_timestamp(payload.get("retryAt"))
            if retry_at is not None:
                delay_ms = max(
                    0,
                    int((retry_at - now).total_seconds() * 1000),
                )
        return PublishPlan(
            exchange="",
            routing_key=RETRY_DELAY_QUEUE,
            body=body,
            expiration_ms=delay_ms,
        )

    if event_type == "task.dead_letter":
        return PublishPlan(
            exchange=DLX_EXCHANGE,
            routing_key=TASK_DLQ,
            body=json.dumps(payload),
        )

    raise ValueError(f"unknown outbox event type: {event_type}")


class TokenBucket:
    """Small local token bucket used to pace relay publishes."""

    def __init__(
        self,
        rate_per_sec: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.rate_per_sec = max(0.001, float(rate_per_sec))
        self.capacity = max(1.0, self.rate_per_sec)
        self.tokens = self.capacity
        self.updated_at = clock()
        self._clock = clock
        self._sleep = sleep

    def acquire(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self.updated_at)
        self.tokens = min(
            self.capacity,
            self.tokens + elapsed * self.rate_per_sec,
        )
        self.updated_at = now
        if self.tokens < 1.0:
            self._sleep((1.0 - self.tokens) / self.rate_per_sec)
            self.tokens = 0.0
            self.updated_at = self._clock()
        else:
            self.tokens -= 1.0


def _is_connection_error(exc: Exception) -> bool:
    return isinstance(
        exc,
        (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.ChannelClosed,
            pika.exceptions.ChannelWrongStateError,
            pika.exceptions.ConnectionWrongStateError,
        ),
    )


def _publish_with_confirm(channel, plan: PublishPlan) -> None:
    properties = pika.BasicProperties(
        delivery_mode=2,
        content_type="application/json",
        headers=plan.headers or {},
    )
    if plan.expiration_ms is not None:
        properties.expiration = str(max(0, plan.expiration_ms))

    confirmed = channel.basic_publish(
        exchange=plan.exchange,
        routing_key=plan.routing_key,
        body=plan.body,
        properties=properties,
        mandatory=True,
    )
    if confirmed is False:
        raise RuntimeError("publisher did not confirm the outbox event")


def _mark_failed(
    cur,
    row_id: int,
    attempts: int,
    error: Exception,
    *,
    max_attempts: int,
    backoff_base_sec: int,
    backoff_max_sec: int,
) -> bool:
    """Record a failed publish. Return True when the row became terminal."""
    next_attempts = attempts + 1
    if next_attempts >= max_attempts:
        cur.execute(
            """
            UPDATE outbox
            SET status = 'failed', attempts = %s, last_error = %s
            WHERE id = %s
            """,
            (next_attempts, str(error)[:2_000], row_id),
        )
        return True

    ceiling = min(
        float(backoff_max_sec),
        float(backoff_base_sec) * (2 ** min(next_attempts - 1, 20)),
    )
    delay = random.uniform(0.0, max(0.0, ceiling))
    cur.execute(
        """
        UPDATE outbox
        SET attempts = %s, available_at = NOW() + make_interval(secs => %s),
            last_error = %s
        WHERE id = %s
        """,
        (next_attempts, delay, str(error)[:2_000], row_id),
    )
    return False


def relay_batch(
    conn,
    channel,
    *,
    batch_size: int,
    max_attempts: int,
    backoff_base_sec: int,
    backoff_max_sec: int,
    bucket: TokenBucket | None = None,
    scheduled_events_enabled: bool = False,
) -> RelayResult:
    """Publish or materialize one batch inside the caller's transaction."""
    published = 0
    failed = 0
    skipped = 0
    materialized = 0
    cancelled = 0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, aggregate_type, aggregate_id, event_type, payload,
                   headers, attempts
            FROM outbox
            WHERE status = 'pending' AND available_at <= NOW()
            ORDER BY id ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """,
            (batch_size,),
        )
        rows = cur.fetchall()

        for (
            row_id,
            aggregate_type,
            aggregate_id,
            event_type,
            payload,
            row_headers,
            attempts,
        ) in rows:
            headers = dict(row_headers or {})
            parent_context = extract_context(headers)
            lock_key = f"{aggregate_type}:{aggregate_id}"
            cur.execute(
                "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
                (lock_key,),
            )
            locked = cur.fetchone()
            if not locked or not locked[0]:
                skipped += 1
                OUTBOX_SKIPPED_TOTAL.inc()
                continue

            try:
                if scheduled_events_enabled and event_type in (
                    "task.scheduled",
                    "task.cancelled",
                    "task.retry",
                ):
                    with start_span(
                        "outbox.materialize",
                        context=parent_context,
                        attributes={
                            "scheduler.outbox.id": int(row_id),
                            "scheduler.outbox.event_type": event_type,
                        },
                    ):
                        result = materialize_event(
                            cur,
                            outbox_id=int(row_id),
                            aggregate_id=str(aggregate_id),
                            event_type=event_type,
                            payload=payload,
                            headers=headers,
                        )
                    cur.execute(
                        """
                        UPDATE outbox
                        SET status = 'published', published_at = NOW(),
                            last_error = NULL
                        WHERE id = %s
                        """,
                        (row_id,),
                    )
                    if result.outcome == "cancelled":
                        cancelled += 1
                        SCHEDULED_EVENTS_CANCELLED_TOTAL.inc(result.cancelled)
                    elif result.outcome == "duplicate":
                        skipped += 1
                        OUTBOX_SKIPPED_TOTAL.inc()
                    else:
                        materialized += 1
                    SCHEDULED_EVENTS_MATERIALIZED_TOTAL.labels(
                        kind=result.kind, outcome=result.outcome
                    ).inc()
                    continue

                plan = replace(
                    build_publish_plan(event_type, payload),
                    headers=headers,
                )
                if bucket is not None:
                    bucket.acquire()
                started = time.perf_counter()
                with start_span(
                    "outbox.publish",
                    context=parent_context,
                    attributes={
                        "scheduler.outbox.id": int(row_id),
                        "scheduler.outbox.event_type": event_type,
                        "messaging.system": "rabbitmq",
                        "messaging.destination.name": plan.routing_key,
                    },
                ):
                    _publish_with_confirm(channel, plan)
                OUTBOX_PUBLISH_DURATION_SECONDS.observe(
                    max(0.0, time.perf_counter() - started)
                )
            except Exception as exc:
                OUTBOX_PUBLISH_FAILURES_TOTAL.inc()
                if _is_connection_error(exc):
                    # Roll back the batch and reconnect. Do not burn attempts
                    # while the broker is unavailable; the event remains pending.
                    logger.warning(
                        "outbox relay: broker connection failed id=%s: %s",
                        row_id,
                        exc,
                    )
                    raise

                failed += 1
                terminal = _mark_failed(
                    cur,
                    int(row_id),
                    int(attempts or 0),
                    exc,
                    max_attempts=max_attempts,
                    backoff_base_sec=backoff_base_sec,
                    backoff_max_sec=backoff_max_sec,
                )
                if terminal:
                    OUTBOX_FAILED_TOTAL.inc()
                logger.warning(
                    "outbox publish failed id=%s type=%s attempt=%s: %s",
                    row_id,
                    event_type,
                    int(attempts or 0) + 1,
                    exc,
                )
                continue

            cur.execute(
                """
                UPDATE outbox
                SET status = 'published', published_at = NOW(),
                    last_error = NULL
                WHERE id = %s
                """,
                (row_id,),
            )
            published += 1
            OUTBOX_PUBLISHED_TOTAL.inc()

    return RelayResult(
        published=published,
        failed=failed,
        skipped=skipped,
        materialized=materialized,
        cancelled=cancelled,
    )


def refresh_outbox_metrics(conn) -> None:
    """Update pending-row and lag gauges from PostgreSQL."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*),
                   COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at))), 0)
            FROM outbox
            WHERE status = 'pending'
            """
        )
        count, lag = cur.fetchone()
    OUTBOX_PENDING_ROWS.set(float(count or 0))
    OUTBOX_LAG_SECONDS.set(max(0.0, float(lag or 0)))


def cleanup_published(conn, retention_days: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM outbox
            WHERE status = 'published'
              AND published_at < NOW() - make_interval(days => %s)
            """,
            (retention_days,),
        )
        deleted = max(0, cur.rowcount)
    if deleted:
        OUTBOX_CLEANUP_DELETED.inc(deleted)
    return deleted


def _connect_with_retry(
    rabbit_url: str,
    poll_interval_sec: int,
    channel_confirm: bool = True,
    queue_type: str = "classic",
):
    while True:
        try:
            connection = connect_blocking(rabbit_url)
            channel = connection.channel()
            declare_scheduler_topology(channel, queue_type)
            if channel_confirm:
                channel.confirm_delivery()
            return connection, channel
        except Exception as exc:
            logger.exception("outbox relay: RabbitMQ connection failed: %s", exc)
            time.sleep(max(1, poll_interval_sec))


def run_outbox_relay(
    get_db_conn: Callable[[], Any],
    rabbit_url: str,
    *,
    poll_interval_sec: int = 1,
    batch_size: int = 100,
    max_attempts: int = 10,
    backoff_base_sec: int = 1,
    backoff_max_sec: int = 60,
    max_publish_per_sec: int = 100,
    retention_days: int = 7,
    cleanup_interval_sec: int = 3_600,
    scheduled_events_enabled: bool = False,
    queue_type: str = "classic",
    backpressure=None,
) -> None:
    """Run the singleton outbox relay until the process stops."""
    connection, channel = _connect_with_retry(
        rabbit_url, poll_interval_sec, queue_type=queue_type
    )
    bucket = TokenBucket(max_publish_per_sec)
    next_cleanup = 0.0

    while True:
        if backpressure is not None and not backpressure.allow("outbox"):
            time.sleep(poll_interval_sec)
            continue
        try:
            conn = get_db_conn()
            conn.autocommit = False
            try:
                result = relay_batch(
                    conn,
                    channel,
                    batch_size=batch_size,
                    max_attempts=max_attempts,
                    backoff_base_sec=backoff_base_sec,
                    backoff_max_sec=backoff_max_sec,
                    bucket=bucket,
                    scheduled_events_enabled=scheduled_events_enabled,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

            if result.published or result.failed or result.materialized:
                logger.info(
                    "outbox relay: published=%s materialized=%s cancelled=%s "
                    "failed=%s skipped=%s",
                    result.published,
                    result.materialized,
                    result.cancelled,
                    result.failed,
                    result.skipped,
                )

            metrics_conn = get_db_conn()
            try:
                refresh_outbox_metrics(metrics_conn)
                if time.monotonic() >= next_cleanup:
                    cleanup_published(metrics_conn, retention_days)
                    metrics_conn.commit()
                    next_cleanup = time.monotonic() + cleanup_interval_sec
            finally:
                metrics_conn.close()
        except Exception as exc:
            logger.exception("outbox relay: cycle failed: %s", exc)
            try:
                connection.close()
            except Exception:
                pass
            connection, channel = _connect_with_retry(
                rabbit_url, poll_interval_sec, queue_type=queue_type
            )

        time.sleep(poll_interval_sec)
