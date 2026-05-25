"""Background poll of RabbitMQ management API for queue depth gauges."""
import logging
import os
import threading
import time
from urllib.parse import quote, urlparse

import requests

from metrics import RABBITMQ_QUEUE_MESSAGES
from .topology import DUE_QUEUE, DELAY_QUEUE, RESULT_QUEUE, RETRY_DELAY_QUEUE, TASK_DLQ, TASK_QUEUE

logger = logging.getLogger( __name__ )

QUEUES_TO_TRACK = (
    TASK_QUEUE,
    TASK_DLQ,
    RETRY_DELAY_QUEUE,
    DUE_QUEUE,
    DELAY_QUEUE,
    RESULT_QUEUE,
)

POLL_INTERVAL_SEC = int( os.environ.get( "RABBITMQ_METRICS_POLL_SEC", "15" ) )


def _management_base_url( rabbit_url: str ) -> str:

    parsed = urlparse( rabbit_url )
    host = parsed.hostname or "rabbitmq"
    user = parsed.username or os.environ.get( "RABBITMQ_DEFAULT_USER", "scheduler" )
    password = parsed.password or os.environ.get( "RABBITMQ_DEFAULT_PASS", "scheduler_secret" )
    cred = f"{quote(user, safe='')}:{quote( password, safe='')}"
    return f"http://{cred}@{host}:15672/api/queues/%2F/"


def _poll_once( api_base: str ) -> None:
    
    for queue in QUEUES_TO_TRACK:
        try:
            r = requests.get( f"{api_base}{quote(queue, safe='')}", timeout=5 )
            if r.status_code != 200:
                continue
            data = r.json()
            depth = int( data.get("messages", 0) or 0 )
            RABBITMQ_QUEUE_MESSAGES.labels( queue = queue ).set( depth )
        except Exception as e:
            logger.debug("queue metrics %s: %s", queue, e)


def run_rabbitmq_queue_metrics_poller( rabbit_url: str ) -> None:
    api_base = _management_base_url( rabbit_url )
    logger.info( "RabbitMQ queue metrics poller started (interval=%ss)", POLL_INTERVAL_SEC )

    while True:
        try:
            _poll_once( api_base )
        except Exception as e:
            logger.warning( "rabbitmq metrics poll: %s", e )
        time.sleep( POLL_INTERVAL_SEC )


def start_rabbitmq_queue_metrics_poller( rabbit_url: str ) -> None:
    
    t = threading.Thread(
        target = run_rabbitmq_queue_metrics_poller,
        args = ( rabbit_url, ),
        daemon = True,
        name = "rabbitmq-metrics",
    )

    t.start()
