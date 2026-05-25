"""
Coordinator service (Python).
- Delayed wakeups, due consumer, result consumer, reconciliation (see internal.queue)
- Worker heartbeat checker (see internal.registry)
- Prometheus metrics on METRICS_PORT (default 9090)
"""
import logging
import os
import sys
import threading

import psycopg2  # type: ignore[import-untyped]
from prometheus_client import start_http_server

from internal.queue import run_due_consumer, run_reconciliation_poller, run_result_consumer
from internal.queue.rabbitmq_metrics import start_rabbitmq_queue_metrics_poller
from internal.registry import Registry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s level=%(levelname)s service=coordinator %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://scheduler:scheduler_secret@localhost:5432/distributed_scheduler",
)
RABBITMQ_URL = os.environ.get(
    "RABBITMQ_URL",
    "amqp://scheduler:scheduler_secret@localhost:5672",
)
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9090"))


def get_db_conn():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


def main():
    start_http_server(METRICS_PORT)
    logger.info("Prometheus metrics listening on :%s/metrics", METRICS_PORT)

    try:
        conn = get_db_conn()
        conn.close()
    except Exception as e:
        logger.fatal("db: %s", e)
        sys.exit(1)

    start_rabbitmq_queue_metrics_poller(RABBITMQ_URL)

    registry = Registry(get_db_conn)
    threading.Thread(target=registry.run_heartbeat_checker, daemon=True).start()

    threading.Thread(
        target=run_result_consumer,
        args=(get_db_conn, RABBITMQ_URL),
        daemon=True,
    ).start()

    threading.Thread(
        target=run_reconciliation_poller,
        args=(get_db_conn, RABBITMQ_URL),
        daemon=True,
    ).start()

    run_due_consumer(get_db_conn, RABBITMQ_URL)


if __name__ == "__main__":
    main()
