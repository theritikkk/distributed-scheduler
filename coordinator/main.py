"""
Coordinator service (Python).
- Delayed wakeups, due consumer, result consumer, reconciliation (see internal.queue)
- Worker heartbeat checker (see internal.registry)
"""
import logging
import os
import sys
import threading

import psycopg2  # type: ignore[import-untyped]

from internal.queue import run_due_consumer, run_reconciliation_poller, run_result_consumer
from internal.registry import Registry

logging.basicConfig( level = logging.INFO, format = "%(levelname)s %(name)s %(message)s" )
logger = logging.getLogger( __name__ )

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://scheduler:scheduler_secret@localhost:5432/distributed_scheduler",
)
RABBITMQ_URL = os.environ.get(
    "RABBITMQ_URL",
    "amqp://scheduler:scheduler_secret@localhost:5672",
)


def get_db_conn():
    conn = psycopg2.connect( DATABASE_URL )
    conn.autocommit = True
    return conn


def main():
    try:
        conn = get_db_conn()
        conn.close()
    except Exception as e:
        logger.fatal( "db: %s", e )
        sys.exit( 1 )

    registry = Registry( get_db_conn )

    threading.Thread( target = registry.run_heartbeat_checker, daemon = True ).start()

    threading.Thread(
        target = run_result_consumer,
        args = ( get_db_conn, RABBITMQ_URL ),
        daemon = True,
    ).start()

    threading.Thread(
        target = run_reconciliation_poller,
        args = ( get_db_conn, RABBITMQ_URL ),
        daemon = True,
    ).start()

    run_due_consumer( get_db_conn, RABBITMQ_URL )


if __name__ == "__main__":
    main()
