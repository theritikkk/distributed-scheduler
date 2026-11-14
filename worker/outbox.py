"""Transactional outbox helpers for worker result and retry events."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from psycopg2.extras import Json

from tracing import inject_headers


@dataclass(frozen=True)
class OutboxEvent:
    aggregate_id: str
    event_type: str
    payload: Mapping[str, Any]
    headers: Mapping[str, Any] | None = None


def enqueue_event(cur, event: OutboxEvent) -> None:
    """Insert one outbox row using the caller's transaction/cursor."""
    cur.execute(
        """
        INSERT INTO outbox
            (aggregate_type, aggregate_id, event_type, payload, headers)
        VALUES ('task', %s, %s, %s, %s)
        """,
        (
            str(event.aggregate_id),
            event.event_type,
            Json(dict(event.payload)),
            Json(dict(event.headers or inject_headers())),
        ),
    )


def enqueue_events(conn, events: list[OutboxEvent]) -> None:
    """Insert a set of events in their own transaction."""
    try:
        with conn.cursor() as cur:
            for event in events:
                enqueue_event(cur, event)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
