"""
Slow reconciliation: nudge due scheduled tasks to scheduler.due (missed TTL wakeups).
"""
import json
import logging
import os
import time

import pika  # type: ignore[import-untyped]

from .connection import connect_blocking
from .topology import DUE_QUEUE, declare_scheduler_topology

logger = logging.getLogger(__name__)

RECONCILE_INTERVAL_SEC = int( os.environ.get("SCHEDULER_RECONCILE_INTERVAL_SEC", "120") )


def run_reconciliation_poller( get_db_conn, rabbit_url: str ):
    try:
        connection = connect_blocking( rabbit_url )
    except Exception as e:
        logger.warning( "reconciliation rabbitmq: %s", e )
        return

    channel = connection.channel()
    declare_scheduler_topology( channel )

    while True:
        try:
            time.sleep( RECONCILE_INTERVAL_SEC )
            conn = get_db_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id FROM tasks
                        WHERE status = 'scheduled' AND next_execution_time <= NOW()
                        ORDER BY next_execution_time ASC
                        LIMIT 100
                        """
                    )
                    ids = [ str( r[0] ) for r in cur.fetchall() ]
            finally:
                conn.close()

            for task_id in ids:
                channel.basic_publish(
                    exchange="",
                    routing_key=DUE_QUEUE,
                    body=json.dumps( {"taskId": task_id} ),
                    properties=pika.BasicProperties( delivery_mode=2, content_type="application/json" ),
                )

                logger.info( "reconciliation: nudged task %s to due queue", task_id )

        except Exception as e:
            logger.exception( "reconciliation: %s", e )
