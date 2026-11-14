"""
Slow reconciliation: nudge due scheduled tasks to scheduler.due (missed TTL wakeups).
The poll interval is randomized to avoid synchronized reconciliation across replicas.
"""
import json
import logging
import random
import time

import pika  # type: ignore[import-untyped]

from .connection import connect_blocking
from metrics import (
    COORDINATOR_ID,
    RECONCILIATION_JITTER_SECONDS,
    RECONCILIATION_NUDGED,
    RECONCILIATION_SKIPPED,
)
from .topology import DUE_QUEUE, declare_scheduler_topology

logger = logging.getLogger(__name__)

RECONCILE_INTERVAL_SEC = 120


def run_reconciliation_poller(
    get_db_conn,
    rabbit_url: str,
    interval_sec: int = RECONCILE_INTERVAL_SEC,
    jitter_ratio: float = 0.2,
    *,
    backpressure=None,
    batch_size: int = 100,
    pace_sec: float = 0.01,
    queue_type: str = "classic",
):
    jitter_ratio = max(0.0, min(0.5, jitter_ratio))
    connection = None
    channel = None

    while True:
        try:
            if connection is None or not connection.is_open:
                connection = connect_blocking(rabbit_url)
                channel = connection.channel()
                declare_scheduler_topology(channel, queue_type)
                channel.confirm_delivery()

            sleep_sec = interval_sec * random.uniform(
                1.0 - jitter_ratio, 1.0 + jitter_ratio
            )
            RECONCILIATION_JITTER_SECONDS.set(sleep_sec)
            time.sleep(sleep_sec)

            if backpressure is not None and not backpressure.allow(
                "reconciliation"
            ):
                RECONCILIATION_SKIPPED.labels(reason="backpressure").inc()
                continue

            conn = get_db_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id FROM tasks
                        WHERE status = 'scheduled'
                          AND COALESCE(dispatch_at, next_execution_time) <= NOW()
                        ORDER BY COALESCE(dispatch_at, next_execution_time) ASC
                        LIMIT %s
                        """,
                        (max(1, batch_size),),
                    )
                    ids = [str(r[0]) for r in cur.fetchall()]
            finally:
                conn.close()

            for task_id in ids:
                if backpressure is not None and not backpressure.allow(
                    "reconciliation"
                ):
                    RECONCILIATION_SKIPPED.labels(
                        reason="backpressure_mid_batch"
                    ).inc()
                    break
                channel.basic_publish(
                    exchange="",
                    routing_key=DUE_QUEUE,
                    body=json.dumps({"taskId": task_id}),
                    properties=pika.BasicProperties(
                        delivery_mode=2, content_type="application/json"
                    ),
                )
                RECONCILIATION_NUDGED.labels(
                    coordinator_id=COORDINATOR_ID
                ).inc()
                logger.info(
                    "reconciliation: nudged task %s to due queue", task_id
                )
                if pace_sec > 0:
                    time.sleep(pace_sec)

        except Exception as e:
            logger.exception("reconciliation: %s", e)
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
            connection = None
            channel = None
            time.sleep(random.uniform(0.5, 2.0))
