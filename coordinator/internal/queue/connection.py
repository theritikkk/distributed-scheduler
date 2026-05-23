"""RabbitMQ blocking connection helper."""

import pika  # type: ignore[import-untyped]


def connect_blocking( rabbit_url: str ):
    return pika.BlockingConnection( pika.URLParameters( rabbit_url ) )
