import os

import pytest

from internal.queue.connection import connect_blocking
from internal.queue.topology import (
    DELAY_QUEUE,
    DUE_QUEUE,
    QUORUM_DLQ_MESSAGE_TTL_MS,
    QUORUM_DUE_DELIVERY_LIMIT,
    QUORUM_DUE_MESSAGE_TTL_MS,
    QUORUM_RESULT_DELIVERY_LIMIT,
    QUORUM_RESULT_MESSAGE_TTL_MS,
    QUORUM_TASK_DELIVERY_LIMIT,
    RESULT_QUEUE,
    RETRY_DELAY_QUEUE,
    TASK_DLQ,
    TASK_QUEUE,
    declare_scheduler_topology,
)

RABBITMQ_URL = os.environ.get("RABBITMQ_URL")


class FakeChannel:
    def __init__(self):
        self.queues = {}

    def exchange_declare(self, **_kwargs):
        return None

    def queue_declare(self, *, queue, durable, arguments=None):
        self.queues[queue] = {"durable": durable, "arguments": arguments or {}}
        return None

    def queue_bind(self, **_kwargs):
        return None


def test_quorum_topology_omits_ttl_queues_and_marks_quorum() -> None:
    channel = FakeChannel()

    declare_scheduler_topology(channel, "quorum")

    assert DELAY_QUEUE not in channel.queues
    assert RETRY_DELAY_QUEUE not in channel.queues
    for queue in (DUE_QUEUE, TASK_QUEUE, TASK_DLQ, RESULT_QUEUE):
        assert channel.queues[queue]["arguments"]["x-queue-type"] == "quorum"

    due = channel.queues[DUE_QUEUE]["arguments"]
    assert due["x-message-ttl"] == QUORUM_DUE_MESSAGE_TTL_MS
    assert due["x-delivery-limit"] == QUORUM_DUE_DELIVERY_LIMIT

    task = channel.queues[TASK_QUEUE]["arguments"]
    assert task["x-delivery-limit"] == QUORUM_TASK_DELIVERY_LIMIT
    assert "x-message-ttl" not in task
    assert task["x-dead-letter-exchange"] == "scheduler.dlx"

    assert (
        channel.queues[TASK_DLQ]["arguments"]["x-message-ttl"]
        == QUORUM_DLQ_MESSAGE_TTL_MS
    )

    result = channel.queues[RESULT_QUEUE]["arguments"]
    assert result["x-message-ttl"] == QUORUM_RESULT_MESSAGE_TTL_MS
    assert result["x-delivery-limit"] == QUORUM_RESULT_DELIVERY_LIMIT


def test_classic_topology_keeps_ttl_delay_queues() -> None:
    channel = FakeChannel()

    declare_scheduler_topology(channel, "classic")

    assert DELAY_QUEUE in channel.queues
    assert RETRY_DELAY_QUEUE in channel.queues
    assert "x-queue-type" not in channel.queues[TASK_QUEUE]["arguments"]
    for queue in (DUE_QUEUE, TASK_QUEUE, TASK_DLQ, RESULT_QUEUE):
        assert "x-message-ttl" not in channel.queues[queue]["arguments"]
        assert "x-delivery-limit" not in channel.queues[queue]["arguments"]


@pytest.mark.skipif(not RABBITMQ_URL, reason="RABBITMQ_URL is not set")
def test_topology_declares_all_queues() -> None:
    assert RABBITMQ_URL is not None
    connection = connect_blocking(RABBITMQ_URL)
    try:
        channel = connection.channel()
        declare_scheduler_topology(channel)
        for queue in (
            DELAY_QUEUE,
            DUE_QUEUE,
            TASK_QUEUE,
            TASK_DLQ,
            RETRY_DELAY_QUEUE,
            RESULT_QUEUE,
        ):
            assert channel.queue_declare(queue=queue, passive=True).method.queue == queue
    finally:
        connection.close()
