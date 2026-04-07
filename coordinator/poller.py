"""
Poller: find due tasks in DB and publish them to the task queue.
"""
import json
import logging
import os
import time

import pika

logger = logging.getLogger(__name__)

TASK_QUEUE = "scheduler.tasks"


def get_env( key: str, default: str ) -> str:
    return os.environ.get( key, default )


def get_connection( rabbit_url: str ):
    params = pika.URLParameters( rabbit_url )
    return pika.BlockingConnection( params )


def run_poller( get_db_conn, rabbit_url: str, poll_interval_sec = 5 ):
    """
    Every poll_interval_sec, select due tasks (FOR UPDATE SKIP LOCKED),
    create task_execution row, update task to running, publish to scheduler.tasks.
    """
    try:
        connection = get_connection( rabbit_url )

    except Exception as e:
        logger.warning( "poller rabbitmq: %s (will not publish)", e )
        return

    channel = connection.channel()
    channel.queue_declare( queue = TASK_QUEUE, durable = True )

    while True:
        try:
            time.sleep( poll_interval_sec )
            conn = get_db_conn()

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, task_name, command_payload, schedule_type
                        FROM tasks
                        WHERE status = 'scheduled' AND next_execution_time <= NOW()
                        ORDER BY next_execution_time ASC
                        LIMIT 50
                        FOR UPDATE SKIP LOCKED
                        """
                    )
                    rows = cur.fetchall()
                # Build list of (task_id, execution_id, task_name, payload_str, schedule_type) in one transaction
                
                to_publish = []

                for row in rows:
                    
                    task_id, task_name, command_payload, schedule_type = row
                    
                    payload_str = json.dumps( command_payload ) if isinstance( command_payload, dict ) else ( command_payload or "{}" )
                    
                    if isinstance( payload_str, bytes ):
                        payload_str = payload_str.decode( "utf-8" )
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO task_executions (task_id, started_at, status)
                            VALUES (%s, NOW(), 'running')
                            RETURNING id
                            """,
                            ( str( task_id ),),
                        )
                        exec_row = cur.fetchone()
                        
                        if not exec_row:
                            continue
                        
                        execution_id = exec_row[0]

                        cur.execute(
                            "UPDATE tasks SET status = 'running', updated_at = NOW() WHERE id = %s",
                            (str(task_id),),
                        )

                        to_publish.append((str(task_id), str(execution_id), task_name, payload_str, schedule_type))
                
                conn.commit()

                for task_id, execution_id, task_name, payload_str, schedule_type in to_publish:
                
                    body = json.dumps({
                        "taskId": task_id,
                        "executionId": execution_id,
                        "taskName": task_name,
                        "payload": payload_str,
                        "scheduleType": schedule_type,
                    })

                    try:
                        channel.basic_publish(
                            exchange = "",
                            routing_key = TASK_QUEUE,
                            body = body,
                            properties = pika.BasicProperties( delivery_mode = 2 ),
                        )
                    except Exception as e:
                        logger.warning( "publish task: %s", e )
                        with conn.cursor() as cur:
                            cur.execute( "UPDATE tasks SET status = 'scheduled' WHERE id = %s", (task_id,) )
                        conn.commit()
            finally:
                conn.close()
                
        except Exception as e:
            logger.exception( "poll_and_publish: %s", e )
