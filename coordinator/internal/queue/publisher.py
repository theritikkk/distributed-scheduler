"""
Publish schedule wakeups: per-message TTL on delay queue -> dead-letter to due queue.
"""
import json
import logging
from datetime import datetime, timezone

import pika  # type: ignore[import-untyped]

from .connection import connect_blocking
from .topology import DELAY_QUEUE, DUE_QUEUE, declare_scheduler_topology
from internal.tracing import inject_headers

logger = logging.getLogger( __name__ )

_MAX_DELAY_MS = 2_147_483_647


def _parse_next_run( value ) -> datetime:
    
    if isinstance( value, datetime ):
        # if already 'datetime' object : normalize timezone safely
        dt = value
        if dt.tzinfo is None:
            # checks : does datetime have timezone info?
            return dt.replace( tzinfo = timezone.utc )
            # if not assume UTC
        return dt.astimezone(timezone.utc)
    
    if isinstance( value, str ):
        s = value.replace( "Z", "+00:00" )
        dt = datetime.fromisoformat( s )
        # converts : "2026-05-18T10:30:00Z" into Python datetime object
        if dt.tzinfo is None:
            dt = dt.replace( tzinfo = timezone.utc )
        return dt.astimezone( timezone.utc )
    raise TypeError( "dispatch_at must be datetime or ISO string" )



def delay_ms_until( dispatch_at ) -> int:
    
    # current UTC timestamp
    now = datetime.now( timezone.utc )
    # normalized scheduled dispatch time
    target = _parse_next_run( dispatch_at )

    # gives us milliseconds until execution
    delta_ms = int( ( target - now ).total_seconds() * 1000 )

    return max(0, min(delta_ms, _MAX_DELAY_MS))
    # prevents negative delays and if already due return 0
    # and prevent exceeding RabbitMQ TTL limit


def publish_schedule_wakeup( channel, task_id: str, dispatch_at ) -> None:

    """Publish to delay queue (TTL -> due) or directly to due queue if already due."""

    # wakeup messages are intentionally lightweight
    body = json.dumps( { "taskId": str(task_id) } )
    # tells system how long RabbitMQ can hold messages
    ms = delay_ms_until( dispatch_at )

    if ms <= 0:
        # already due
        channel.basic_publish(
            exchange = "",
            routing_key = DUE_QUEUE,    # skip delay queue
            body = body,
            properties = pika.BasicProperties(
                delivery_mode = 2,
                content_type = "application/json",
                headers = inject_headers(),
            ),
        )
        logger.info( "published immediate wakeup for task %s", task_id )
        return

    channel.basic_publish(
        exchange = "",
        routing_key = DELAY_QUEUE,      # task enters : scheduler.delay
        body = body,
        properties = pika.BasicProperties(
            delivery_mode =2,
            content_type = "application/json",
            expiration = str(ms),
            headers = inject_headers(),
        ),
    )
    logger.info( "published delayed wakeup for task %s in %sms", task_id, ms )



def publish_schedule_wakeup_url( rabbit_url: str, task_id: str, dispatch_at ) -> None:

    """Short-lived connection: declare topology and publish a wakeup (e.g. from result consumer)."""

    conn = connect_blocking( rabbit_url )
    try:
        ch = conn.channel()
        declare_scheduler_topology( ch )
        ch.confirm_delivery()
        publish_schedule_wakeup( ch, task_id, dispatch_at )
    finally:
        conn.close()
