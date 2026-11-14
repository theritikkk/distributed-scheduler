from datetime import datetime, timezone

import pytest

from internal.queue import reaper
from internal.queue.reaper import ReapAction


class StopLoop(Exception):
    pass


class FakeCursor:
    def __init__(self):
        self.rowcount = 2
        self.statements: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, _params=None):
        self.statements.append(sql)


class FakeConnection:
    def __init__(self):
        self.autocommit = True
        self.cursor_obj = FakeCursor()
        self.committed = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        raise AssertionError("unexpected rollback")

    def close(self):
        self.closed = True


class FakeChannel:
    def confirm_delivery(self):
        return None


class FakeRabbitConnection:
    def channel(self):
        return FakeChannel()


def test_reaper_loop_publishes_actions_and_cleans_keys(monkeypatch) -> None:
    conn = FakeConnection()
    action = ReapAction(
        kind="requeue",
        task_id="00000000-0000-0000-0000-000000000001",
        execution_id="00000000-0000-0000-0000-000000000002",
        scheduled_for=datetime.now(timezone.utc),
        attempt=2,
    )
    published: list[ReapAction] = []

    monkeypatch.setattr(
        reaper, "connect_blocking", lambda _url: FakeRabbitConnection()
    )
    monkeypatch.setattr(
        reaper,
        "declare_scheduler_topology",
        lambda _channel, _queue_type="classic": None,
    )
    monkeypatch.setattr(reaper, "_publish_requeue", lambda _ch, a, _max: published.append(a))
    monkeypatch.setattr(
        reaper, "reap_stale_executions", lambda *_args, **_kwargs: [action]
    )
    monkeypatch.setattr(reaper.time, "sleep", lambda _seconds: (_ for _ in ()).throw(StopLoop()))

    with pytest.raises(StopLoop):
        reaper.run_reaper(
            lambda: conn,
            "amqp://test",
            interval_sec=1,
            max_retries=3,
            pending_grace_sec=300,
            cleanup_interval_sec=3_600,
            outbox_enabled=False,
        )

    assert published == [action]
    assert conn.committed is True
    assert conn.closed is True
    assert any("DELETE FROM idempotency_keys" in sql for sql in conn.cursor_obj.statements)


def test_reaper_loop_does_not_direct_publish_when_outbox_is_enabled(monkeypatch) -> None:
    conn = FakeConnection()
    action = ReapAction(
        kind="requeue",
        task_id="00000000-0000-0000-0000-000000000001",
        execution_id="00000000-0000-0000-0000-000000000002",
        scheduled_for=datetime.now(timezone.utc),
        attempt=2,
    )
    published: list[ReapAction] = []

    monkeypatch.setattr(
        reaper, "connect_blocking", lambda _url: FakeRabbitConnection()
    )
    monkeypatch.setattr(
        reaper,
        "declare_scheduler_topology",
        lambda _channel, _queue_type="classic": None,
    )
    monkeypatch.setattr(
        reaper, "_publish_requeue", lambda _ch, a, _max: published.append(a)
    )
    monkeypatch.setattr(
        reaper, "reap_stale_executions", lambda *_args, **_kwargs: [action]
    )
    monkeypatch.setattr(
        reaper.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(StopLoop()),
    )

    with pytest.raises(StopLoop):
        reaper.run_reaper(
            lambda: conn,
            "amqp://test",
            interval_sec=1,
            max_retries=3,
            pending_grace_sec=300,
            cleanup_interval_sec=3_600,
            outbox_enabled=True,
        )

    assert published == []
    assert conn.committed is True
