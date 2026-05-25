"""
Worker: consume scheduler.tasks with idempotency (execution_id), exponential retry via
scheduler.retry_delay -> tasks, poison messages to DLQ after max retries.
"""
import json
import logging
import os
import subprocess
import sys
import threading
import time

import pika  # type: ignore[import-untyped]
import psycopg2  # type: ignore[import-untyped]
from prometheus_client import start_http_server

from metrics import (
    CONSUMER_UP,
    TASK_DURATION,
    TASKS_ACKED,
    TASKS_COMPLETED,
    TASKS_DLQ,
    TASKS_FAILED,
    TASKS_IDEMPOTENT_SKIP,
    TASKS_RETRIED,
    TASKS_STARTED,
)
from rabbit_topology import (
    DLX_EXCHANGE,
    RESULT_QUEUE,
    RETRY_DELAY_QUEUE,
    TASK_DLQ,
    TASK_QUEUE,
    declare_scheduler_topology,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s level=%(levelname)s service=worker worker_id=%(worker_id)s %(message)s",
)
logger = logging.getLogger(__name__)

WORKER_ID = os.environ.get("WORKER_ID", "worker-1")
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://scheduler:scheduler_secret@localhost:5432/distributed_scheduler",
)
RABBITMQ_URL = os.environ.get(
    "RABBITMQ_URL",
    "amqp://scheduler:scheduler_secret@localhost:5672",
)
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9100"))

DEFAULT_MAX_RETRIES = 3
_BASE_BACKOFF_MS = 5000
_MAX_BACKOFF_MS = 300_000


class WorkerIdFilter(logging.Filter):
    def filter(self, record):
        record.worker_id = WORKER_ID
        return True


logger.addFilter(WorkerIdFilter())


def _max_retries(msg: dict) -> int:
    try:
        return int(
            msg.get(
                "maxRetries",
                os.environ.get("TASK_MAX_RETRIES", str(DEFAULT_MAX_RETRIES)),
            )
        )
    except (TypeError, ValueError):
        return DEFAULT_MAX_RETRIES


def get_db_conn():
    return psycopg2.connect(DATABASE_URL)


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


def run_heartbeat(interval_sec=20):
    while True:
        time.sleep(interval_sec)
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
            logger.warning("heartbeat failed: %s", e)


def execution_already_finished(execution_id: str) -> bool:
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT completed_at FROM task_executions WHERE id = %s",
                (execution_id,),
            )
            row = cur.fetchone()
            if not row:
                return False
            return row[0] is not None
    finally:
        conn.close()


def execute_task(payload_str: str, timeout_sec=300):
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError:
        return "", None
    cmd = payload.get("command")
    if not cmd or not isinstance(cmd, str):
        return "", None
    parts = cmd.strip().split()
    if not parts:
        return "", None
    try:
        out = subprocess.run(
            parts,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
        combined = (out.stdout or "") + (out.stderr or "")
        if out.returncode == 0:
            return combined, None
        return combined, Exception(f"exit code {out.returncode}")
    except subprocess.TimeoutExpired as e:
        return (e.stdout or "") + (e.stderr or ""), e
    except Exception as e:
        return "", e


def publish_result(channel, task_id, execution_id, status, output, err_msg):
    result = {
        "taskId": task_id,
        "executionId": execution_id,
        "workerId": WORKER_ID,
        "status": status,
        "output": output,
        "errorMessage": err_msg,
    }
    channel.basic_publish(
        exchange="",
        routing_key=RESULT_QUEUE,
        body=json.dumps(result),
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
    )


def _publish_poison_dlq(
    channel,
    task_id,
    execution_id,
    retry_count,
    max_retries,
    payload_str,
    err_msg,
):
    dlq_envelope = {
        "reason": "max_retries_exceeded",
        "taskId": task_id,
        "executionId": execution_id,
        "workerId": WORKER_ID,
        "retryCount": retry_count,
        "maxRetries": max_retries,
        "payload": payload_str,
        "errorMessage": err_msg,
        "timestamp": time.time(),
    }
    channel.basic_publish(
        exchange=DLX_EXCHANGE,
        routing_key=TASK_DLQ,
        body=json.dumps(dlq_envelope),
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
    )
    TASKS_DLQ.labels(worker_id=WORKER_ID).inc()
    logger.error(
        "poison message: task %s execution %s sent to DLQ after %s retries error=%s",
        task_id,
        execution_id,
        retry_count,
        err_msg,
    )


def handle_task(channel, method, _properties, body):
    wid = WORKER_ID
    started = time.perf_counter()

    try:
        msg = json.loads(body)
    except json.JSONDecodeError:
        logger.error("invalid JSON on task queue, nacking to DLQ")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    task_id = msg.get("taskId")
    execution_id = msg.get("executionId")
    payload_str = msg.get("payload", "{}")
    retry_count = int(msg.get("retryCount") or 0)
    max_retries = _max_retries(msg)

    if not execution_id:
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    logger.info(
        "processing task %s execution %s retry=%s/%s",
        task_id,
        execution_id,
        retry_count,
        max_retries,
    )
    TASKS_STARTED.labels(worker_id=wid).inc()

    if execution_already_finished(str(execution_id)):
        TASKS_IDEMPOTENT_SKIP.labels(worker_id=wid).inc()
        logger.info(
            "idempotent skip task %s execution %s (already completed)",
            task_id,
            execution_id,
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
        TASKS_ACKED.labels(worker_id=wid).inc()
        return

    output, err = execute_task(payload_str)
    duration = time.perf_counter() - started
    TASK_DURATION.labels(worker_id=wid).observe(duration)

    if err is None:
        publish_result(channel, task_id, execution_id, "completed", output, "")
        TASKS_COMPLETED.labels(worker_id=wid).inc()
        logger.info(
            "task %s execution %s completed in %.2fs",
            task_id,
            execution_id,
            duration,
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
        TASKS_ACKED.labels(worker_id=wid).inc()
        return

    err_msg = str(err)
    TASKS_FAILED.labels(worker_id=wid).inc()

    if retry_count < max_retries:
        next_retry = retry_count + 1
        backoff_ms = min(_MAX_BACKOFF_MS, _BASE_BACKOFF_MS * (2**retry_count))
        msg["retryCount"] = next_retry
        channel.basic_publish(
            exchange="",
            routing_key=RETRY_DELAY_QUEUE,
            body=json.dumps(msg),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                expiration=str(backoff_ms),
            ),
        )
        TASKS_RETRIED.labels(worker_id=wid).inc()
        logger.warning(
            "task %s execution %s failed in %.2fs (attempt %s/%s), retry in %sms: %s",
            task_id,
            execution_id,
            duration,
            next_retry,
            max_retries,
            backoff_ms,
            err_msg,
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
        TASKS_ACKED.labels(worker_id=wid).inc()
        return

    _publish_poison_dlq(
        channel,
        task_id,
        execution_id,
        retry_count,
        max_retries,
        payload_str,
        err_msg,
    )
    publish_result(channel, task_id, execution_id, "failed", output, err_msg)
    logger.error(
        "task %s execution %s failed permanently after %.2fs and %s retries",
        task_id,
        execution_id,
        duration,
        retry_count,
    )
    channel.basic_ack(delivery_tag=method.delivery_tag)
    TASKS_ACKED.labels(worker_id=wid).inc()


def main():
    start_http_server(METRICS_PORT)
    logger.info("Prometheus metrics on :%s/metrics", METRICS_PORT)

    try:
        register_worker()
    except Exception as e:
        logger.warning("register_worker: %s", e)

    threading.Thread(target=run_heartbeat, daemon=True).start()

    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    channel = connection.channel()
    declare_scheduler_topology(channel)
    channel.basic_qos(prefetch_count=5)
    CONSUMER_UP.labels(worker_id=WORKER_ID).set(1)

    channel.basic_consume(
        queue=TASK_QUEUE,
        on_message_callback=lambda ch, method, props, body: handle_task(
            ch, method, props, body
        ),
    )

    logger.info("worker consuming queue %s", TASK_QUEUE)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        CONSUMER_UP.labels(worker_id=WORKER_ID).set(0)
        connection.close()


if __name__ == "__main__":
    main()
    sys.exit(0)
