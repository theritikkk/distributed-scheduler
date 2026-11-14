"""Transactional outbox helpers shared by coordinator write paths."""
from __future__ import annotations

from typing import Any, Mapping

from psycopg2.extras import Json

from internal.tracing import inject_headers


def enqueue_event(
    cur,
    *,
    aggregate_type: str,
    aggregate_id: str,
    event_type: str,
    payload: Mapping[str, Any] | dict[str, Any],
    headers: Mapping[str, Any] | None = None,
) -> None:
    """Insert an outbox event using the caller's transaction/cursor."""
    cur.execute(
        """
        INSERT INTO outbox
            (aggregate_type, aggregate_id, event_type, payload, headers)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            aggregate_type,
            str(aggregate_id),
            event_type,
            Json(payload),
            Json(dict(headers or inject_headers())),
        ),
    )
