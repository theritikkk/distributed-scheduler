"""
Consume scheduler.due: wakeups after TTL from scheduler.delay (or direct publish).
Row-lock task, create execution, publish to scheduler.tasks.
"""
import json
import logging
import os
from datetime import datetime, timezone

import pika  # type: ignore[import-untyped]

from .connection import connect_blocking
from .publisher import publish_schedule_wakeup
from metrics import DUE_DISPATCHED
from .topology import DUE_QUEUE, TASK_QUEUE, declare_scheduler_topology

logger = logging.getLogger(__name__)

DEFAULT_MAX_RETRIES = 3


def _max_retries() -> int:
    return int( os.environ.get( "TASK_MAX_RETRIES", str(DEFAULT_MAX_RETRIES) ) )


def run_due_consumer( get_db_conn, rabbit_url: str ):
    connection = connect_blocking( rabbit_url )
    # a common connect_blocking for all RabbitMQ to use
    channel = connection.channel()
    declare_scheduler_topology( channel )

    def on_message( ch, method, _properties, body ):
        try:
            msg = json.loads( body )
            task_id = msg.get( "taskId" )
            if not task_id:
                ch.basic_ack( delivery_tag = method.delivery_tag )
                return

            conn = get_db_conn()
            conn.autocommit = False
            execution_id = None
            task_name = None
            command_payload = None
            schedule_type = None
            try:
                with conn.cursor() as cur:
                    
                    cur.execute(
                        """
                        SELECT task_name, command_payload, schedule_type, status, next_execution_time
                        FROM tasks WHERE id = %s FOR UPDATE
                        """,
                        ( str(task_id), ),
                    )

                    row = cur.fetchone()
                    if not row:
                        conn.rollback()
                        ch.basic_ack( delivery_tag = method.delivery_tag )
                        return

                    task_name, command_payload, schedule_type, status, next_exec = row

                    if status != "scheduled":
                        conn.rollback()
                        ch.basic_ack( delivery_tag = method.delivery_tag )
                        return

                    now = datetime.now( timezone.utc )

                    ne = next_exec
                    if ne is not None:
                        if getattr( ne, "tzinfo", None ) is None:
                            ne = ne.replace( tzinfo = timezone.utc )
                        else:
                            ne = ne.astimezone( timezone.utc )

                        if ne > now:
                            conn.rollback()
                            publish_schedule_wakeup( ch, str( task_id ), ne )
                            ch.basic_ack( delivery_tag = method.delivery_tag )
                            return

                    cur.execute(
                        """
                        INSERT INTO task_executions (task_id, started_at, status)
                        VALUES (%s, NOW(), 'running')
                        RETURNING id
                        """,
                        ( str(task_id), ),
                    )

                    er = cur.fetchone()
                    if not er:
                        conn.rollback()
                        ch.basic_ack( delivery_tag = method.delivery_tag )
                        return
                    execution_id = str( er[0] )

                    cur.execute(
                        """
                        UPDATE tasks SET status = 'running', updated_at = NOW()
                        WHERE id = %s AND status = 'scheduled'
                        """,
                        ( str(task_id), ),
                    )
                    if cur.rowcount == 0:
                        cur.execute( "DELETE FROM task_executions WHERE id = %s", (execution_id,) )
                        conn.rollback()
                        ch.basic_ack( delivery_tag = method.delivery_tag )
                        return

                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

            mx = _max_retries()
            payload_str = (
                json.dumps( command_payload )
                if isinstance( command_payload, dict )
                else ( command_payload or "{}" )
            )
            if isinstance( payload_str, bytes ):
                payload_str = payload_str.decode( "utf-8" )

            out = json.dumps(
                {
                    "taskId": str(task_id),
                    "executionId": execution_id,
                    "taskName": task_name,
                    "payload": payload_str,
                    "scheduleType": schedule_type,
                    "retryCount": 0,
                    "maxRetries": mx,
                }
            )

            ch.basic_publish(
                exchange="",
                routing_key=TASK_QUEUE,
                body=out,
                properties=pika.BasicProperties( delivery_mode=2, content_type="application/json" ),
            )

            DUE_DISPATCHED.inc()
            logger.info( "due consumer: task %s execution %s", task_id, execution_id )
            
        except Exception as e:
            logger.exception( "due consumer error: %s", e )

        ch.basic_ack( delivery_tag = method.delivery_tag )

    channel.basic_qos( prefetch_count = 10 )
    channel.basic_consume( queue = DUE_QUEUE, on_message_callback = on_message )
    logger.info( "consuming %s", DUE_QUEUE )
    channel.start_consuming()
