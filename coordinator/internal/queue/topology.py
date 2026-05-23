"""
RabbitMQ topology: delay wakeups, due processing, tasks, retries, DLQ, results.
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


def declare_scheduler_topology(channel) -> None:
    channel.exchange_declare(exchange=DLX_EXCHANGE, exchange_type="direct", durable=True)

    channel.queue_declare(
        queue=DELAY_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": DUE_QUEUE,
        },
    )

    channel.queue_declare(queue=DUE_QUEUE, durable=True)

    channel.queue_declare(
        queue=TASK_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX_EXCHANGE,
            "x-dead-letter-routing-key": TASK_DLQ,
        },
    )

    channel.queue_declare(queue=TASK_DLQ, durable=True)
    channel.queue_bind(queue=TASK_DLQ, exchange=DLX_EXCHANGE, routing_key=TASK_DLQ)

    channel.queue_declare(
        queue=RETRY_DELAY_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": TASK_QUEUE,
        },
    )

    channel.queue_declare(queue=RESULT_QUEUE, durable=True)
