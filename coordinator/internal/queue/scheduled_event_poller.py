"""
Publish due DB-backed scheduled events.

Rows are claimed with ``FOR UPDATE SKIP LOCKED``, published with confirms and
mandatory routing, then marked published in the same transaction. A crash after
publish but before commit can duplicate a wakeup/retry; due/worker consumers
already make those transitions idempotent.
"""
from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Callable

from internal.queue.connection import connect_blocking
from internal.queue.outbox_relay import (
    PublishPlan,
    TokenBucket,
    _is_connection_error,
    _publish_with_confirm,
)
from internal.queue.topology import declare_scheduler_topology
from internal.scheduled_events import cleanup_terminal_events, refresh_event_metrics
from internal.tracing import extract_context, start_span
from metrics import (
    SCHEDULED_EVENTS_CLEANUP_DELETED,
    SCHEDULED_EVENTS_FAILED,
    SCHEDULED_EVENTS_LAG_SECONDS,
    SCHEDULED_EVENTS_PENDING,
    SCHEDULED_EVENTS_PUBLISHED_TOTAL,
    SCHEDULED_EVENTS_PUBLISH_DURATION_SECONDS,
    SCHEDULED_EVENTS_PUBLISH_FAILURES_TOTAL,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PollResult:
    published: int = 0
    failed: int = 0


def _mark_failed(
    cur,
    row_id: int,
    attempts: int,
    error: Exception,
    *,
    max_attempts: int,
    backoff_base_sec: int,
    backoff_max_sec: int,
) -> None:
    next_attempts = attempts + 1
    if next_attempts >= max_attempts:
        cur.execute(
            """
            UPDATE scheduled_events
            SET status = 'failed', attempts = %s, last_error = %s
            WHERE id = %s
            """,
            (next_attempts, str(error)[:2_000], row_id),
        )
        return

    ceiling = min(
        float(backoff_max_sec),
        float(backoff_base_sec) * (2 ** min(next_attempts - 1, 20)),
    )
    delay = random.uniform(0.0, max(0.0, ceiling))
    cur.execute(
        """
        UPDATE scheduled_events
        SET attempts = %s,
            available_at = NOW() + make_interval(secs => %s),
            last_error = %s
        WHERE id = %s
        """,
        (next_attempts, delay, str(error)[:2_000], row_id),
    )


def poll_due_events(
    conn,
    channel,
    *,
    batch_size: int,
    max_attempts: int,
    backoff_base_sec: int,
    backoff_max_sec: int,
    bucket: TokenBucket | None = None,
) -> PollResult:
    """Publish one batch of due timers inside the caller's transaction."""
    published = 0
    failed = 0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, destination, payload, headers, attempts
            FROM scheduled_events
            WHERE status = 'pending' AND available_at <= NOW()
            ORDER BY available_at ASC, id ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """,
            (batch_size,),
        )
        rows = cur.fetchall()

        for row_id, destination, payload, row_headers, attempts in rows:
            headers = dict(row_headers or {})
            plan = PublishPlan(
                exchange="",
                routing_key=str(destination),
                body=json.dumps(payload),
                headers=headers,
            )
            try:
                if bucket is not None:
                    bucket.acquire()
                started = time.perf_counter()
                with start_span(
                    "scheduled_event.publish",
                    context=extract_context(headers),
                    attributes={
                        "scheduler.scheduled_event.id": int(row_id),
                        "messaging.system": "rabbitmq",
                        "messaging.destination.name": str(destination),
                    },
                ):
                    _publish_with_confirm(channel, plan)
                SCHEDULED_EVENTS_PUBLISH_DURATION_SECONDS.observe(
                    max(0.0, time.perf_counter() - started)
                )
            except Exception as exc:
                SCHEDULED_EVENTS_PUBLISH_FAILURES_TOTAL.inc()
                if _is_connection_error(exc):
                    logger.warning(
                        "scheduled event poller: broker connection failed id=%s: %s",
                        row_id,
                        exc,
                    )
                    raise

                failed += 1
                _mark_failed(
                    cur,
                    int(row_id),
                    int(attempts or 0),
                    exc,
                    max_attempts=max_attempts,
                    backoff_base_sec=backoff_base_sec,
                    backoff_max_sec=backoff_max_sec,
                )
                logger.warning(
                    "scheduled event publish failed id=%s destination=%s "
                    "attempt=%s: %s",
                    row_id,
                    destination,
                    int(attempts or 0) + 1,
                    exc,
                )
                continue

            cur.execute(
                """
                UPDATE scheduled_events
                SET status = 'published', published_at = NOW(),
                    last_error = NULL
                WHERE id = %s
                """,
                (row_id,),
            )
            published += 1
            SCHEDULED_EVENTS_PUBLISHED_TOTAL.inc()

    return PollResult(published=published, failed=failed)


def _connect_with_retry(
    rabbit_url: str, poll_interval_sec: float, queue_type: str = "classic"
):
    while True:
        try:
            connection = connect_blocking(rabbit_url)
            channel = connection.channel()
            declare_scheduler_topology(channel, queue_type)
            channel.confirm_delivery()
            return connection, channel
        except Exception as exc:
            logger.exception(
                "scheduled event poller: RabbitMQ connection failed: %s", exc
            )
            time.sleep(max(0.5, poll_interval_sec))


def run_scheduled_event_poller(
    get_db_conn: Callable[[], Any],
    rabbit_url: str,
    *,
    poll_interval_sec: float = 1.0,
    batch_size: int = 100,
    max_attempts: int = 10,
    backoff_base_sec: int = 1,
    backoff_max_sec: int = 60,
    retention_days: int = 7,
    cleanup_interval_sec: int = 3_600,
    max_publish_per_sec: int = 100,
    queue_type: str = "classic",
    backpressure=None,
) -> None:
    """Run the due-timer poller until the process stops."""
    connection, channel = _connect_with_retry(
        rabbit_url, poll_interval_sec, queue_type=queue_type
    )
    bucket = TokenBucket(max_publish_per_sec)
    next_cleanup = 0.0

    while True:
        try:
            if backpressure is not None and not backpressure.allow("scheduled_events"):
                connection.process_data_events(time_limit=0.2)
                continue

            conn = get_db_conn()
            conn.autocommit = False
            try:
                result = poll_due_events(
                    conn,
                    channel,
                    batch_size=batch_size,
                    max_attempts=max_attempts,
                    backoff_base_sec=backoff_base_sec,
                    backoff_max_sec=backoff_max_sec,
                    bucket=bucket,
                )
                pending, failed, lag = refresh_event_metrics(conn)
                SCHEDULED_EVENTS_PENDING.set(pending)
                SCHEDULED_EVENTS_FAILED.set(failed)
                SCHEDULED_EVENTS_LAG_SECONDS.set(lag)
                if time.monotonic() >= next_cleanup:
                    deleted = cleanup_terminal_events(conn, retention_days)
                    if deleted:
                        SCHEDULED_EVENTS_CLEANUP_DELETED.inc(deleted)
                    next_cleanup = time.monotonic() + cleanup_interval_sec
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

            if result.published or result.failed:
                logger.info(
                    "scheduled event poller: published=%s failed=%s",
                    result.published,
                    result.failed,
                )
            if not result.published and not result.failed:
                time.sleep(poll_interval_sec)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            logger.exception("scheduled event poller: cycle failed: %s", exc)
            try:
                connection.close()
            except Exception:
                pass
            connection, channel = _connect_with_retry(
                rabbit_url, poll_interval_sec, queue_type=queue_type
            )
