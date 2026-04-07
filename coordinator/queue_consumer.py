"""
Result consumer: read from scheduler.results, update task_executions and tasks,
set next run time for recurring tasks.
"""
import json
import logging
from datetime import datetime, timedelta

import pika  # type: ignore[import-untyped]
from croniter import croniter  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

RESULT_QUEUE = "scheduler.results"


def _map_status( status: str ) -> str:
    if status == "completed":
        return "completed"

    if status in ("failed", "timeout", "cancelled"):
        return "failed"

    return "scheduled"


def _next_cron( now: datetime, cron_expr: str ) -> datetime:
    try:
        return croniter( cron_expr, now ).get_next( datetime )
    except Exception:
        return now + timedelta( minutes = 1 )


def run_result_consumer( get_db_conn, rabbit_url: str ):
    connection = pika.BlockingConnection( pika.URLParameters( rabbit_url ) )
    channel = connection.channel()
    channel.queue_declare( queue = RESULT_QUEUE, durable = True )

    def on_message( ch, method, _properties, body ):
        try:
            msg = json.loads( body )
            task_id = msg.get( "taskId" )
            execution_id = msg.get( "executionId" )
            status = msg.get( "status", "failed" )
            output = msg.get( "output" ) or ""
            error_message = msg.get( "errorMessage" ) or ""

            conn = get_db_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE task_executions
                        SET completed_at = NOW(), status = %s, output = %s, error_message = %s
                        WHERE id = %s
                        """,
                        ( status, output, error_message, execution_id ),
                    )
                    cur.execute(
                        "UPDATE tasks SET status = %s, updated_at = NOW() WHERE id = %s",
                        ( _map_status(status), task_id ),
                    )
                    cur.execute(
                        "SELECT schedule_type, cron_expression FROM tasks WHERE id = %s",
                        ( task_id, ),
                    )
                    
                    row = cur.fetchone()

                    if row:
                        schedule_type, cron_expr = row
                        if schedule_type == "recurring" and cron_expr:
                            next_run = _next_cron(datetime.utcnow(), cron_expr)
                            cur.execute(
                                """
                                UPDATE tasks
                                SET next_execution_time = %s, status = 'scheduled', updated_at = NOW()
                                WHERE id = %s
                                """,
                                (next_run, task_id),
                            )
                conn.commit()

            finally:
                conn.close()

        except Exception as e:
            logger.exception( "handle_result: %s", e )

        ch.basic_ack( delivery_tag = method.delivery_tag )

    channel.basic_consume( queue = RESULT_QUEUE, on_message_callback = on_message )

    logger.info( "consuming results from %s", RESULT_QUEUE )
    
    channel.start_consuming()
