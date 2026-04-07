"""
Coordinator service (Python).
- Polls DB for due tasks and publishes to scheduler.tasks
- Consumes scheduler.results and updates DB (executions + next run for recurring)
- Runs heartbeat checker to mark stale workers offline
"""
import logging
import os
import sys
import threading

import psycopg2

from registry import Registry
from poller import run_poller
from queue_consumer import run_result_consumer

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get( "DATABASE_URL", "postgresql://scheduler:scheduler_secret@localhost:5432/distributed_scheduler" )
RABBITMQ_URL = os.environ.get( "RABBITMQ_URL", "amqp://scheduler:scheduler_secret@localhost:5672" )


def get_db_conn():
    return psycopg2.connect( DATABASE_URL )


def main():

    try:
        conn = get_db_conn()
        conn.close()

    except Exception as e:
        logger.fatal( "db: %s", e )
        sys.exit( 1 )

    registry = Registry( get_db_conn )

    # Heartbeat checker in background
    t = threading.Thread( target = registry.run_heartbeat_checker, daemon = True )
    t.start()

    # Result consumer in background
    t2 = threading.Thread(
        target = run_result_consumer,
        args=( get_db_conn, RABBITMQ_URL ),
        daemon = True,
    )
    t2.start()

    # Poller runs in foreground (blocking loop)
    run_poller( get_db_conn, RABBITMQ_URL )


if __name__ == "__main__":
    main()
