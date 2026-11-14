"""
Shared RabbitMQ topology (same as coordinator).

Classic mode keeps TTL delay queues for the outbox-disabled rollback path.
Quorum mode relies on PostgreSQL scheduled_events and does not declare them.

Quorum queue TTLs and delivery limits are part of the declaration and must
match the API and coordinator declarations exactly.
"""

DELAY_QUEUE = "scheduler.delay"
DUE_QUEUE = "scheduler.due"
TASK_QUEUE = "scheduler.tasks"
RESULT_QUEUE = "scheduler.results"
RETRY_DELAY_QUEUE = "scheduler.retry_delay"
DLX_EXCHANGE = "scheduler.dlx"
TASK_DLQ = "scheduler.tasks.dlq"

QUEUE_TYPES = {"classic", "quorum"}

# Keep these in sync with api-gateway/src/queue.ts and
# coordinator/internal/queue/topology.py.
QUORUM_TASK_DELIVERY_LIMIT = 20
QUORUM_DUE_MESSAGE_TTL_MS = 3_600_000
QUORUM_DUE_DELIVERY_LIMIT = 10
QUORUM_RESULT_MESSAGE_TTL_MS = 604_800_000
QUORUM_RESULT_DELIVERY_LIMIT = 20
QUORUM_DLQ_MESSAGE_TTL_MS = 1_209_600_000


def _queue_arguments( queue_type, arguments = None, quorum_arguments = None ):
    if queue_type not in QUEUE_TYPES:
        raise ValueError( f"unsupported queue type: {queue_type}" )
    merged = dict( arguments or {} )
    if queue_type == "quorum":
        merged["x-queue-type"] = "quorum"
        merged.update( quorum_arguments or {} )
    return merged


def declare_scheduler_topology( channel, queue_type = "classic" ):
    channel.exchange_declare( exchange = DLX_EXCHANGE, exchange_type = "direct", durable = True )

    if queue_type == "classic":
        channel.queue_declare(
            queue = DELAY_QUEUE,
            durable = True,
            arguments = {
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": DUE_QUEUE,
            },
        )

    channel.queue_declare(
        queue = DUE_QUEUE,
        durable = True,
        arguments = _queue_arguments(
            queue_type,
            quorum_arguments = {
                "x-message-ttl": QUORUM_DUE_MESSAGE_TTL_MS,
                "x-delivery-limit": QUORUM_DUE_DELIVERY_LIMIT,
            },
        ),
    )

    channel.queue_declare(
        queue = TASK_QUEUE,
        durable = True,
        arguments = _queue_arguments(
            queue_type,
            {
                "x-dead-letter-exchange": DLX_EXCHANGE,
                "x-dead-letter-routing-key": TASK_DLQ,
            },
            quorum_arguments = {
                "x-delivery-limit": QUORUM_TASK_DELIVERY_LIMIT,
            },
        ),
    )

    channel.queue_declare(
        queue = TASK_DLQ,
        durable = True,
        arguments = _queue_arguments(
            queue_type,
            quorum_arguments = {
                "x-message-ttl": QUORUM_DLQ_MESSAGE_TTL_MS,
            },
        ),
    )
    channel.queue_bind( queue = TASK_DLQ, exchange = DLX_EXCHANGE, routing_key = TASK_DLQ )

    if queue_type == "classic":
        channel.queue_declare(
            queue = RETRY_DELAY_QUEUE,
            durable = True,
            arguments = {
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": TASK_QUEUE,
            },
        )

    channel.queue_declare(
        queue = RESULT_QUEUE,
        durable = True,
        arguments = _queue_arguments(
            queue_type,
            quorum_arguments = {
                "x-message-ttl": QUORUM_RESULT_MESSAGE_TTL_MS,
                "x-delivery-limit": QUORUM_RESULT_DELIVERY_LIMIT,
            },
        ),
    )
