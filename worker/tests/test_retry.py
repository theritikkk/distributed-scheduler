import json
from datetime import datetime, timezone

from main import (
    DLX_EXCHANGE,
    RESULT_QUEUE,
    RETRY_DELAY_QUEUE,
    TASK_DLQ,
    ExecutionClaim,
    OutboxEvent,
    RetrySchedule,
    handle_task,
    retry_backoff_ms,
)
import main as worker_main


class FakeChannel:
    def __init__(self):
        self.published = []
        self.acked = []
        self.nacked = []

    def basic_publish(self, exchange="", routing_key="", body=None, properties=None):
        self.published.append(
            {
                "exchange": exchange,
                "routing_key": routing_key,
                "body": body,
                "properties": properties,
            }
        )

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue=False):
        self.nacked.append((delivery_tag, requeue))


class FakeMethod:
    delivery_tag = 1


class FakeHeartbeat:
    def __init__(self, execution_id, lease_id):
        self.execution_id = execution_id
        self.lease_id = lease_id

    def start(self):
        pass

    def stop(self):
        pass


def _message(retry_count: int, max_retries: int = 3) -> bytes:
    return json.dumps(
        {
            "taskId": "task-1",
            "executionId": "execution-1",
            "payload": "{}",
            "retryCount": retry_count,
            "maxRetries": max_retries,
        }
    ).encode()


def _patch_claim(monkeypatch, attempt: int = 1, claimed: bool = True):
    monkeypatch.setattr(worker_main, "LeaseHeartbeat", FakeHeartbeat)
    monkeypatch.setattr(
        worker_main,
        "claim_execution",
        lambda _: (
            ExecutionClaim("execution-1", "lease-1", attempt) if claimed else None
        ),
    )


def _retry_schedule(attempt: int = 1) -> RetrySchedule:
    message = {
        "taskId": "task-1",
        "executionId": "execution-1",
        "payload": "{}",
        "retryCount": attempt,
        "maxRetries": 3,
    }
    return RetrySchedule(
        "task-1",
        datetime.now(timezone.utc),
        {"message": message},
    )


def test_failed_task_is_retried_with_jittered_backoff(monkeypatch) -> None:
    _patch_claim(monkeypatch, attempt=1)
    monkeypatch.setattr(worker_main, "OUTBOX_ENABLED", False)
    monkeypatch.setattr(
        worker_main,
        "execute_task",
        lambda _payload: ("", Exception("boom")),
    )
    monkeypatch.setattr(
        worker_main,
        "schedule_execution_retry",
        lambda *args, **kwargs: _retry_schedule(1),
    )
    monkeypatch.setattr(
        worker_main.random,
        "uniform",
        lambda low, high: low + 0.8 * (high - low),
    )
    channel = FakeChannel()

    handle_task(channel, FakeMethod(), None, _message(retry_count=0))

    assert len(channel.published) == 1
    published = channel.published[0]
    assert published["routing_key"] == RETRY_DELAY_QUEUE
    assert published["properties"].expiration == "4000"
    assert json.loads(published["body"])["retryCount"] == 1
    assert channel.acked == [1]


def test_retry_db_update_failure_does_not_publish_retry(monkeypatch) -> None:
    _patch_claim(monkeypatch, attempt=1)
    monkeypatch.setattr(worker_main, "OUTBOX_ENABLED", False)
    monkeypatch.setattr(
        worker_main,
        "execute_task",
        lambda _payload: ("", Exception("boom")),
    )
    monkeypatch.setattr(
        worker_main,
        "schedule_execution_retry",
        lambda *args, **kwargs: None,
    )
    channel = FakeChannel()

    handle_task(channel, FakeMethod(), None, _message(retry_count=0))

    assert channel.published == []
    assert channel.acked == [1]


def test_retry_backoff_supports_full_equal_and_none(monkeypatch) -> None:
    monkeypatch.setattr(
        worker_main.random,
        "uniform",
        lambda low, high: (low + high) / 2,
    )

    assert retry_backoff_ms(0, "full") == 2500
    assert retry_backoff_ms(0, "equal") == 3750
    assert retry_backoff_ms(0, "none") == 5000
    assert retry_backoff_ms(10, "none") == 300_000
    assert retry_backoff_ms(10**9, "none") == 300_000


def test_failed_task_at_max_retries_goes_to_dlq_and_result(monkeypatch) -> None:
    _patch_claim(monkeypatch, attempt=3)
    monkeypatch.setattr(worker_main, "OUTBOX_ENABLED", False)
    monkeypatch.setattr(
        worker_main,
        "execute_task",
        lambda _payload: ("partial", Exception("boom")),
    )
    channel = FakeChannel()

    handle_task(channel, FakeMethod(), None, _message(retry_count=2, max_retries=2))

    assert len(channel.published) == 2
    dlq, result = channel.published
    assert dlq["exchange"] == DLX_EXCHANGE
    assert dlq["routing_key"] == TASK_DLQ
    assert json.loads(dlq["body"])["reason"] == "max_retries_exceeded"
    assert dlq["body"]
    assert result["routing_key"] == RESULT_QUEUE
    result_body = json.loads(result["body"])
    assert result_body["status"] == "failed"
    assert result_body["leaseId"] == "lease-1"
    assert result_body["attempt"] == 3
    assert channel.acked == [1]


def test_completed_execution_is_skipped_without_running_or_publishing(
    monkeypatch,
) -> None:
    _patch_claim(monkeypatch, claimed=False)
    monkeypatch.setattr(
        worker_main,
        "pending_retry_delay_ms",
        lambda _: None,
    )
    called = False

    def fail_if_called(_payload):
        nonlocal called
        called = True
        return "", None

    monkeypatch.setattr(worker_main, "execute_task", fail_if_called)
    channel = FakeChannel()

    handle_task(channel, FakeMethod(), None, _message(retry_count=0))

    assert called is False
    assert channel.published == []
    assert channel.acked == [1]


def test_not_due_execution_is_republished_with_remaining_delay_for_direct_mode(
    monkeypatch,
) -> None:
    _patch_claim(monkeypatch, claimed=False)
    monkeypatch.setattr(worker_main, "OUTBOX_ENABLED", False)
    monkeypatch.setattr(
        worker_main,
        "pending_retry_delay_ms",
        lambda _: 4500,
    )
    channel = FakeChannel()

    handle_task(channel, FakeMethod(), None, _message(retry_count=1))

    assert len(channel.published) == 1
    published = channel.published[0]
    assert published["routing_key"] == RETRY_DELAY_QUEUE
    assert published["properties"].expiration == "4500"
    assert channel.acked == [1]


def test_not_due_execution_relies_on_durable_retry_with_outbox(monkeypatch) -> None:
    _patch_claim(monkeypatch, claimed=False)
    monkeypatch.setattr(worker_main, "OUTBOX_ENABLED", True)
    monkeypatch.setattr(
        worker_main,
        "pending_retry_delay_ms",
        lambda _: 4500,
    )
    channel = FakeChannel()

    handle_task(channel, FakeMethod(), None, _message(retry_count=1))

    assert channel.published == []
    assert channel.acked == [1]


def test_completed_task_publishes_lease_guarded_result(monkeypatch) -> None:
    _patch_claim(monkeypatch, attempt=1)
    monkeypatch.setattr(worker_main, "OUTBOX_ENABLED", False)
    monkeypatch.setattr(
        worker_main,
        "execute_task",
        lambda _payload: ("ok", None),
    )
    channel = FakeChannel()

    handle_task(channel, FakeMethod(), None, _message(retry_count=0))

    assert len(channel.published) == 1
    result = channel.published[0]
    assert result["routing_key"] == RESULT_QUEUE
    body = json.loads(result["body"])
    assert body["status"] == "completed"
    assert body["leaseId"] == "lease-1"
    assert body["attempt"] == 1
    assert channel.acked == [1]


def test_completed_task_enqueues_result_event_before_ack(monkeypatch) -> None:
    _patch_claim(monkeypatch, attempt=1)
    monkeypatch.setattr(worker_main, "OUTBOX_ENABLED", True)
    monkeypatch.setattr(
        worker_main,
        "execute_task",
        lambda _payload: ("ok", None),
    )
    events: list[OutboxEvent] = []
    monkeypatch.setattr(
        worker_main,
        "enqueue_outbox_events",
        lambda items: events.extend(items),
    )
    channel = FakeChannel()

    handle_task(channel, FakeMethod(), None, _message(retry_count=0))

    assert channel.published == []
    assert len(events) == 1
    assert events[0].event_type == "task.result"
    assert events[0].payload["status"] == "completed"
    assert channel.acked == [1]


def test_final_failure_enqueues_dlq_and_result_together(monkeypatch) -> None:
    _patch_claim(monkeypatch, attempt=3)
    monkeypatch.setattr(worker_main, "OUTBOX_ENABLED", True)
    monkeypatch.setattr(
        worker_main,
        "execute_task",
        lambda _payload: ("partial", Exception("boom")),
    )
    events: list[OutboxEvent] = []
    monkeypatch.setattr(
        worker_main,
        "enqueue_outbox_events",
        lambda items: events.extend(items),
    )
    channel = FakeChannel()

    handle_task(channel, FakeMethod(), None, _message(retry_count=2, max_retries=2))

    assert channel.published == []
    assert [event.event_type for event in events] == [
        "task.dead_letter",
        "task.result",
    ]
    assert events[0].payload["reason"] == "max_retries_exceeded"
    assert events[1].payload["status"] == "failed"
    assert channel.acked == [1]


def test_retry_uses_outbox_without_direct_publish(monkeypatch) -> None:
    _patch_claim(monkeypatch, attempt=1)
    monkeypatch.setattr(worker_main, "OUTBOX_ENABLED", True)
    monkeypatch.setattr(
        worker_main,
        "execute_task",
        lambda _payload: ("", Exception("boom")),
    )
    monkeypatch.setattr(
        worker_main,
        "schedule_execution_retry",
        lambda *args, **kwargs: _retry_schedule(1),
    )
    channel = FakeChannel()

    handle_task(channel, FakeMethod(), None, _message(retry_count=0))

    assert channel.published == []
    assert channel.acked == [1]
