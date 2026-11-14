"""Schedule calculations shared by coordinator paths.

The stable offset is intentionally identical to api-gateway/src/scheduling.ts:
the first four bytes of SHA-256(task_id) modulo (window + 1).
"""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta, timezone

from croniter import croniter  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

MAX_JITTER_SEC = 3600
_MAX_MISSED_SLOT_ADVANCES = 10_000


def as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def stable_offset(task_id: str, window_sec: int) -> int:
    if window_sec <= 0:
        return 0
    digest = hashlib.sha256(task_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % (window_sec + 1)


def compute_dispatch_at(
    base: datetime,
    task_id: str,
    jitter_seconds: int,
    period_seconds: int | None,
    *,
    enabled: bool = True,
) -> datetime:
    base = as_utc(base) or base
    if not enabled or jitter_seconds <= 0:
        return base

    window = jitter_seconds
    if period_seconds is not None and period_seconds > 1:
        window = min(jitter_seconds, period_seconds - 1)
    if window <= 0:
        return base

    return base + timedelta(seconds=stable_offset(task_id, window))


def cron_period_seconds(cron_expression: str, base: datetime) -> int:
    """Return the gap from one cron occurrence to the next after ``base``."""
    try:
        first = next_cron(base, cron_expression)
        second = next_cron(first, cron_expression)
        return max(1, int((second - first).total_seconds()))
    except Exception:
        return 60


def next_cron(base: datetime, cron_expression: str) -> datetime:
    """Return the next cron occurrence after ``base`` in UTC."""
    base = as_utc(base) or base
    try:
        candidate = croniter(cron_expression, base).get_next(datetime)
    except Exception:
        logger.warning("invalid cron expression %r; using a one-minute fallback", cron_expression)
        return base + timedelta(minutes=1)
    return as_utc(candidate) or candidate


def next_future_cron_slot(
    cron_expression: str,
    scheduled_for: datetime,
    now: datetime,
) -> datetime:
    """Advance the base cron sequence until the next slot is in the future.

    Missed slots are skipped by default. This bounds catch-up work after a
    long worker or coordinator outage.
    """
    scheduled_for = as_utc(scheduled_for) or scheduled_for
    now = as_utc(now) or now
    candidate = next_cron(scheduled_for, cron_expression)

    for _ in range(_MAX_MISSED_SLOT_ADVANCES):
        if candidate > now:
            return candidate
        candidate = next_cron(candidate, cron_expression)

    logger.error(
        "cron %r did not advance past now after %s attempts; using now+1m",
        cron_expression,
        _MAX_MISSED_SLOT_ADVANCES,
    )
    return now + timedelta(minutes=1)


def next_run_for_task(
    task_id: str,
    cron_expression: str,
    scheduled_for: datetime,
    now: datetime,
    jitter_seconds: int,
    jitter_policy: str,
    *,
    jitter_enabled: bool,
) -> tuple[datetime, datetime]:
    """Return ``(next_base_slot, next_dispatch_at)`` for a recurring task."""
    base_next = next_future_cron_slot(cron_expression, scheduled_for, now)
    effective_jitter = (
        0
        if jitter_policy == "none" or not jitter_enabled
        else jitter_seconds
    )
    dispatch_at = compute_dispatch_at(
        base_next,
        task_id,
        effective_jitter,
        cron_period_seconds(cron_expression, base_next),
        enabled=jitter_enabled,
    )
    return base_next, dispatch_at
