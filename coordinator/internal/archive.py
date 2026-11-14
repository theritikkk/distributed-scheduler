"""Bounded online archival for terminal scheduler history.

Only terminal rows move. Pending/running executions, pending outbox events, and
pending timers are never touched. Each batch copies and deletes rows in one
transaction, so a crash either leaves the rows live or fully archived.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable

from metrics import (
    ARCHIVE_DURATION_SECONDS,
    ARCHIVE_ERRORS_TOTAL,
    ARCHIVE_OLDEST_ROW_AGE_SECONDS,
    ARCHIVE_ROWS_TOTAL,
    ARCHIVE_RUNS_TOTAL,
)

logger = logging.getLogger(__name__)

TERMINAL_EXECUTION_STATUSES = ("completed", "failed", "timeout", "cancelled")
TERMINAL_OUTBOX_STATUSES = ("published", "failed")


@dataclass
class ArchiveBatchResult:
    executions: int = 0
    attempts: int = 0
    outbox: int = 0

    @property
    def total(self) -> int:
        return self.executions + self.attempts + self.outbox


def _archive_executions(cur, retention_days: int, batch_size: int) -> tuple[int, int]:
    cur.execute(
        """
        SELECT id, task_id, worker_id, scheduled_for, started_at, completed_at,
               status, output, error_message, lease_id, lease_expires_at,
               next_attempt_at, attempt, created_at
        FROM task_executions
        WHERE status IN ('completed', 'failed', 'timeout', 'cancelled')
          AND COALESCE(completed_at, created_at)
              < NOW() - make_interval(days => %s)
        ORDER BY COALESCE(completed_at, created_at) ASC, id ASC
        LIMIT %s
        FOR UPDATE SKIP LOCKED
        """,
        (retention_days, batch_size),
    )
    rows = cur.fetchall()
    if not rows:
        return 0, 0

    ids = [str(row[0]) for row in rows]

    cur.execute(
        """
        INSERT INTO task_execution_attempts_archive
            (id, execution_id, attempt, worker_id, started_at, finished_at,
             status, error_message)
        SELECT id, execution_id, attempt, worker_id, started_at, finished_at,
               status, error_message
        FROM task_execution_attempts
        WHERE execution_id = ANY(%s::uuid[])
        ON CONFLICT (id) DO NOTHING
        """,
        (ids,),
    )
    attempts_archived = max(0, cur.rowcount)

    cur.execute(
        """
        INSERT INTO task_execution_archive
            (id, task_id, worker_id, scheduled_for, started_at, completed_at,
             status, output, error_message, lease_id, lease_expires_at,
             next_attempt_at, attempt, created_at)
        SELECT id, task_id, worker_id, scheduled_for, started_at, completed_at,
               status, output, error_message, lease_id, lease_expires_at,
               next_attempt_at, attempt, created_at
        FROM task_executions
        WHERE id = ANY(%s::uuid[])
        ON CONFLICT (id) DO NOTHING
        """,
        (ids,),
    )

    cur.execute(
        "DELETE FROM task_execution_attempts WHERE execution_id = ANY(%s::uuid[])",
        (ids,),
    )
    cur.execute(
        "DELETE FROM task_executions WHERE id = ANY(%s::uuid[])",
        (ids,),
    )
    return len(rows), attempts_archived


def _archive_outbox(cur, retention_days: int, batch_size: int) -> int:
    cur.execute(
        """
        SELECT id
        FROM outbox
        WHERE (status = 'published'
               AND published_at < NOW() - make_interval(days => %s))
           OR (status = 'failed'
               AND created_at < NOW() - make_interval(days => %s))
        ORDER BY id ASC
        LIMIT %s
        FOR UPDATE SKIP LOCKED
        """,
        (retention_days, retention_days, batch_size),
    )
    rows = cur.fetchall()
    if not rows:
        return 0
    ids = [int(row[0]) for row in rows]

    cur.execute(
        """
        INSERT INTO outbox_archive
            (id, aggregate_type, aggregate_id, event_type, payload, headers,
             status, attempts, available_at, created_at, published_at,
             last_error)
        SELECT id, aggregate_type, aggregate_id, event_type, payload, headers,
               status, attempts, available_at, created_at, published_at,
               last_error
        FROM outbox
        WHERE id = ANY(%s::bigint[])
        ON CONFLICT (id) DO NOTHING
        """,
        (ids,),
    )
    cur.execute(
        "DELETE FROM outbox WHERE id = ANY(%s::bigint[])",
        (ids,),
    )
    return len(rows)


def refresh_archive_metrics(cur) -> None:
    cur.execute(
        """
        SELECT
            COALESCE(MAX(EXTRACT(EPOCH FROM (
                NOW() - COALESCE(completed_at, created_at)
            ))), 0)
        FROM task_executions
        WHERE status IN ('completed', 'failed', 'timeout', 'cancelled')
        """
    )
    execution_age = cur.fetchone()[0]
    cur.execute(
        """
        SELECT
            COALESCE(MAX(EXTRACT(EPOCH FROM (
                NOW() - CASE
                    WHEN status = 'published' THEN published_at
                    ELSE created_at
                END
            ))), 0)
        FROM outbox
        WHERE status IN ('published', 'failed')
        """
    )
    outbox_age = cur.fetchone()[0]
    ARCHIVE_OLDEST_ROW_AGE_SECONDS.labels(table="task_executions").set(
        max(0.0, float(execution_age or 0))
    )
    ARCHIVE_OLDEST_ROW_AGE_SECONDS.labels(table="outbox").set(
        max(0.0, float(outbox_age or 0))
    )


def archive_batch(
    conn,
    *,
    retention_days: int,
    batch_size: int,
    dry_run: bool = False,
) -> ArchiveBatchResult:
    """Copy and delete one bounded batch. The caller owns commit/rollback."""
    with conn.cursor() as cur:
        if dry_run:
            cur.execute(
                """
                WITH eligible_executions AS (
                    SELECT id FROM task_executions
                    WHERE status IN ('completed', 'failed', 'timeout', 'cancelled')
                      AND COALESCE(completed_at, created_at)
                          < NOW() - make_interval(days => %s)
                    LIMIT %s
                ),
                eligible_outbox AS (
                    SELECT id FROM outbox
                    WHERE (status = 'published'
                           AND published_at < NOW() - make_interval(days => %s))
                       OR (status = 'failed'
                           AND created_at < NOW() - make_interval(days => %s))
                    LIMIT %s
                )
                SELECT
                    (SELECT COUNT(*) FROM eligible_executions),
                    (SELECT COUNT(*) FROM task_execution_attempts a
                     JOIN eligible_executions e ON e.id = a.execution_id),
                    (SELECT COUNT(*) FROM eligible_outbox)
                """,
                (retention_days, batch_size, retention_days, retention_days, batch_size),
            )
            executions, attempts, outbox = cur.fetchone()
            return ArchiveBatchResult(
                executions=int(executions or 0),
                attempts=int(attempts or 0),
                outbox=int(outbox or 0),
            )

        executions, attempts = _archive_executions(
            cur, retention_days, batch_size
        )
        outbox = _archive_outbox(cur, retention_days, batch_size)
        refresh_archive_metrics(cur)

    if executions:
        ARCHIVE_ROWS_TOTAL.labels(table="task_executions").inc(executions)
    if attempts:
        ARCHIVE_ROWS_TOTAL.labels(table="task_execution_attempts").inc(attempts)
    if outbox:
        ARCHIVE_ROWS_TOTAL.labels(table="outbox").inc(outbox)

    return ArchiveBatchResult(
        executions=executions,
        attempts=attempts,
        outbox=outbox,
    )


def archive_cycle(
    get_db_conn: Callable[[], Any],
    *,
    retention_days: int,
    batch_size: int,
    max_batches: int,
    sleep_ms: int = 0,
    dry_run: bool = False,
) -> ArchiveBatchResult:
    """Run up to ``max_batches`` bounded batches and commit each one."""
    total = ArchiveBatchResult()
    started = time.monotonic()

    for _ in range(max_batches):
        conn = get_db_conn()
        conn.autocommit = False
        try:
            result = archive_batch(
                conn,
                retention_days=retention_days,
                batch_size=batch_size,
                dry_run=dry_run,
            )
            if dry_run:
                conn.rollback()
            else:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        total.executions += result.executions
        total.attempts += result.attempts
        total.outbox += result.outbox

        # Dry-run reports one batch of eligible rows; repeating the same count
        # for max_batches would be misleading.
        if dry_run:
            break
        if result.total == 0:
            break
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1_000)

    ARCHIVE_DURATION_SECONDS.observe(time.monotonic() - started)
    ARCHIVE_RUNS_TOTAL.labels(status="ok").inc()
    return total


def run_archive_loop(
    get_db_conn: Callable[[], Any],
    *,
    retention_days: int,
    batch_size: int,
    interval_sec: int,
    max_batches: int,
    sleep_ms: int,
) -> None:
    """Background coordinator loop; errors back off without killing the service."""
    while True:
        try:
            result = archive_cycle(
                get_db_conn,
                retention_days=retention_days,
                batch_size=batch_size,
                max_batches=max_batches,
                sleep_ms=sleep_ms,
            )
            if result.total:
                logger.info(
                    "archive cycle: executions=%s attempts=%s outbox=%s",
                    result.executions,
                    result.attempts,
                    result.outbox,
                )
            time.sleep(interval_sec)
        except Exception as exc:
            ARCHIVE_ERRORS_TOTAL.inc()
            ARCHIVE_RUNS_TOTAL.labels(status="error").inc()
            logger.exception("archive cycle failed: %s", exc)
            time.sleep(min(60, max(1, interval_sec)))
