"""
Worker service (Python).
- Registers in DB and sends heartbeats
- Consumes from scheduler.tasks, runs the command from payload, publishes to scheduler.results
"""
import json
import logging
import os
import subprocess
import sys
import threading
import time

import pika
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TASK_QUEUE = "scheduler.tasks"
RESULT_QUEUE = "scheduler.results"

WORKER_ID = os.environ.get( "WORKER_ID", "worker-1" )
DATABASE_URL = os.environ.get( "DATABASE_URL", "postgresql://scheduler:scheduler_secret@localhost:5432/distributed_scheduler" )
RABBITMQ_URL = os.environ.get( "RABBITMQ_URL", "amqp://scheduler:scheduler_secret@localhost:5672" )


def get_db_conn():
    return psycopg2.connect( DATABASE_URL )


def register_worker():

    conn = get_db_conn()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO workers (worker_id, worker_address, status, last_heartbeat)
                VALUES (%s, %s, 'active', NOW())
                ON CONFLICT (worker_id) DO UPDATE SET status = 'active', last_heartbeat = NOW(), updated_at = NOW()
                """,
                (WORKER_ID, "local"),
            )
        conn.commit()

    finally:
        conn.close()


def run_heartbeat( interval_sec = 20 ):

    while True:
        time.sleep( interval_sec )

        try:
            conn = get_db_conn()

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE workers SET last_heartbeat = NOW(), updated_at = NOW() WHERE worker_id = %s",
                        (WORKER_ID,),
                    )
                conn.commit()

            finally:
                conn.close()

        except Exception as e:
            logger.warning("heartbeat: %s", e)


def execute_task( payload_str: str, timeout_sec = 300 ):
    """Run the 'command' from the JSON payload (e.g. "echo hello") in a subprocess."""
    try:
        payload = json.loads( payload_str )
    except json.JSONDecodeError:
        return "", None
    cmd = payload.get( "command" )

    if not cmd or not isinstance( cmd, str ):
        return "", None
    parts = cmd.strip().split()

    if not parts:
        return "", None
    
    try:
        out = subprocess.run(
            parts,
            capture_output = True,
            text = True,
            timeout = timeout_sec,
        )
        return out.stdout + out.stderr, None if out.returncode == 0 else Exception( f"exit code {out.returncode}" )

    except subprocess.TimeoutExpired as e:
        return (e.stdout or "") + (e.stderr or ""), e

    except Exception as e:
        return "", e


def handle_task( channel, method, properties, body ):
    
    try:
        msg = json.loads( body )

    except json.JSONDecodeError:
        channel.basic_ack( delivery_tag = method.delivery_tag )
        return

    task_id = msg.get( "taskId" )
    execution_id = msg.get( "executionId" )
    payload_str = msg.get( "payload", "{}" )
    output, err = execute_task( payload_str )
    status = "completed" if err is None else "failed"
    err_msg = str(err) if err else ""

    result = {
        "taskId": task_id,
        "executionId": execution_id,
        "workerId": WORKER_ID,
        "status": status,
        "output": output,
        "errorMessage": err_msg,
    }

    channel.basic_publish(
        exchange = "",
        routing_key = RESULT_QUEUE,
        body=json.dumps( result ),
        properties = pika.BasicProperties( delivery_mode = 2 ),
    )
    channel.basic_ack( delivery_tag = method.delivery_tag )


def main():
    try:
        register_worker()
    except Exception as e:
        logger.warning( "register_worker: %s", e )

    t = threading.Thread( target = run_heartbeat, daemon = True )
    t.start()

    connection = pika.BlockingConnection( pika.URLParameters( RABBITMQ_URL ) )
    channel = connection.channel()
    channel.queue_declare( queue = TASK_QUEUE, durable = True )
    channel.queue_declare( queue = RESULT_QUEUE, durable = True )
    channel.basic_consume( queue = TASK_QUEUE, on_message_callback = lambda ch, method, props, body: handle_task(ch, method, props, body) )

    logger.info( "worker %s started", WORKER_ID )
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()


if __name__ == "__main__":
    main()
    sys.exit(0)
