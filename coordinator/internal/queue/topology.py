"""
RabbitMQ topology: due processing, tasks, retries, DLQ, results.

Classic mode keeps the TTL delay queues for the outbox-disabled rollback path.
Quorum mode deliberately does not declare those queues; dormant events and
retries come from PostgreSQL ``scheduled_events`` instead.

Quorum queues support queue-level TTLs and delivery limits, not per-message
TTLs. These values are part of the queue declaration and must match the API
and worker declarations; changing them requires draining and recreating the
queues (RabbitMQ rejects an unequal redeclare).

The task queue deliberately has no message TTL: accepted work must not expire.
Its delivery limit only guards repeated nack/redelivery loops; application
retries are durable rows in PostgreSQL, not broker redeliveries.
"""

# Wakeup: TTL expires -> dead-lettered to DUE_QUEUE
DELAY_QUEUE = "scheduler.delay"
DUE_QUEUE = "scheduler.due"

TASK_QUEUE = "scheduler.tasks"
RESULT_QUEUE = "scheduler.results"

# Worker failures: TTL expires -> dead-lettered back to TASK_QUEUE
RETRY_DELAY_QUEUE = "scheduler.retry_delay"

DLX_EXCHANGE = "scheduler.dlx"
TASK_DLQ = "scheduler.tasks.dlq"

QUEUE_TYPES = {"classic", "quorum"}

# Keep these in sync with api-gateway/src/queue.ts and worker/rabbit_topology.py.
QUORUM_TASK_DELIVERY_LIMIT = 20
QUORUM_DUE_MESSAGE_TTL_MS = 3_600_000
QUORUM_DUE_DELIVERY_LIMIT = 10
QUORUM_RESULT_MESSAGE_TTL_MS = 604_800_000
QUORUM_RESULT_DELIVERY_LIMIT = 20
QUORUM_DLQ_MESSAGE_TTL_MS = 1_209_600_000


def _queue_arguments(
    queue_type: str,
    arguments: dict | None = None,
    quorum_arguments: dict | None = None,
) -> dict:
    if queue_type not in QUEUE_TYPES:
        raise ValueError(f"unsupported queue type: {queue_type}")
    merged = dict(arguments or {})
    if queue_type == "quorum":
        merged["x-queue-type"] = "quorum"
        merged.update(quorum_arguments or {})
    return merged


def declare_scheduler_topology(channel, queue_type: str = "classic") -> None:
    channel.exchange_declare(exchange=DLX_EXCHANGE, exchange_type="direct", durable=True)

    if queue_type == "classic":
        channel.queue_declare(
            queue=DELAY_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": DUE_QUEUE,
            },
        )

    channel.queue_declare(
        queue=DUE_QUEUE,
        durable=True,
        arguments=_queue_arguments(
            queue_type,
            quorum_arguments={
                "x-message-ttl": QUORUM_DUE_MESSAGE_TTL_MS,
                "x-delivery-limit": QUORUM_DUE_DELIVERY_LIMIT,
            },
        ),
    )

    channel.queue_declare(
        queue=TASK_QUEUE,
        durable=True,
        arguments=_queue_arguments(
            queue_type,
            {
                "x-dead-letter-exchange": DLX_EXCHANGE,
                "x-dead-letter-routing-key": TASK_DLQ,
            },
            quorum_arguments={
                "x-delivery-limit": QUORUM_TASK_DELIVERY_LIMIT,
            },
        ),
    )

    channel.queue_declare(
        queue=TASK_DLQ,
        durable=True,
        arguments=_queue_arguments(
            queue_type,
            quorum_arguments={
                "x-message-ttl": QUORUM_DLQ_MESSAGE_TTL_MS,
            },
        ),
    )
    channel.queue_bind(queue=TASK_DLQ, exchange=DLX_EXCHANGE, routing_key=TASK_DLQ)

    if queue_type == "classic":
        channel.queue_declare(
            queue=RETRY_DELAY_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": TASK_QUEUE,
            },
        )

    channel.queue_declare(
        queue=RESULT_QUEUE,
        durable=True,
        arguments=_queue_arguments(
            queue_type,
            quorum_arguments={
                "x-message-ttl": QUORUM_RESULT_MESSAGE_TTL_MS,
                "x-delivery-limit": QUORUM_RESULT_DELIVERY_LIMIT,
            },
        ),
    )
