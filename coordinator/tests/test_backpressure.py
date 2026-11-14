"""Backpressure controller and gate tests (no RabbitMQ required)."""
from __future__ import annotations

import time

from internal.backpressure.controller import BackpressureController
from internal.backpressure.db_gate import DbGate
from internal.backpressure.dispatch_gate import DispatchGate
from internal.backpressure.gate import BackpressureGate


class FakeMonitor:
    def __init__(self, depths, sampled_at=None):
        self.depths = depths
        self.sampled_at = time.monotonic() if sampled_at is None else sampled_at

    def snapshot(self):
        return dict(self.depths), self.sampled_at


class FakeDbGate:
    def __init__(self, utilization=0.1, waiting=0):
        self.utilization = utilization
        self.waiting = waiting

    def snapshot(self):
        return self.utilization, self.waiting


def _controller(now=None):
    return BackpressureController(
        tasks_high=10,
        tasks_low=2,
        results_high=100,
        results_low=10,
        db_high=0.8,
        db_resume=0.6,
        db_max_waiters=2,
        high_consecutive=2,
        low_consecutive=3,
        stale_pause_sec=15,
        now=now or time.monotonic,
    )


def test_controller_pauses_after_consecutive_high_samples():
    c = _controller()
    sample = dict(
        task_depth=11,
        result_depth=0,
        db_utilization=0.1,
        db_waiters=0,
        sampled_at=time.monotonic(),
    )
    assert c.evaluate(**sample).state == "normal"
    decision = c.evaluate(**sample)
    assert decision.state == "paused"
    assert "task_depth" in decision.reasons


def test_controller_hysteresis_requires_consecutive_low_samples():
    c = _controller()
    high = dict(
        task_depth=11,
        result_depth=0,
        db_utilization=0.1,
        db_waiters=0,
        sampled_at=time.monotonic(),
    )
    c.evaluate(**high)
    assert c.evaluate(**high).state == "paused"

    low = dict(
        task_depth=1,
        result_depth=0,
        db_utilization=0.1,
        db_waiters=0,
        sampled_at=time.monotonic(),
    )
    assert c.evaluate(**low).state == "paused"
    assert c.evaluate(**low).state == "paused"
    assert c.evaluate(**low).state == "normal"


def test_controller_reports_unknown_when_monitor_is_stale():
    c = _controller()
    decision = c.evaluate(
        task_depth=0,
        result_depth=0,
        db_utilization=0.1,
        db_waiters=0,
        sampled_at=time.monotonic() - 60,
    )
    assert decision.state == "unknown"
    assert "stale_or_missing_signal" in decision.reasons


def _gate(mode, depths, db_util=0.1):
    return BackpressureGate(
        mode=mode,
        poll_sec=0.0,
        tasks_high=10,
        tasks_low=2,
        results_high=100,
        results_low=10,
        due_high=10,
        db_high=0.8,
        db_resume=0.6,
        db_max_waiters=2,
        high_consecutive=2,
        low_consecutive=3,
        stale_pause_sec=15,
        monitor=FakeMonitor(depths),
        db_gate=FakeDbGate(db_util),
    )


def test_enforce_gate_blocks_due_dispatch_and_lets_results_drain():
    gate = _gate("enforce", {"scheduler.tasks": 11, "scheduler.results": 0})
    # Two samples are required to enter the paused state.
    assert gate.allow("due") is True
    time.sleep(0.06)
    assert gate.allow("due") is False
    # Result processing must continue when only the task queue is deep.
    assert gate.allow("result") is True


def test_observe_gate_never_blocks():
    gate = _gate("observe", {"scheduler.tasks": 11, "scheduler.results": 0})
    assert gate.allow("due") is True
    assert gate.allow("due") is True
    assert gate.allow("result") is True


def test_db_gate_tracks_connections_and_returns_slots():
    gate = DbGate(max_connections=1)

    class Connection:
        closed = 0

        def close(self):
            self.closed += 1

    first = gate.connect(Connection)
    utilization, waiting = gate.snapshot()
    assert utilization == 1.0
    assert waiting == 0

    first.close()
    first.close()
    assert gate.snapshot() == (0.0, 0)


def test_dispatch_gate_paces_tokens():
    now = 0.0

    def clock():
        return now

    gate = DispatchGate(1, 2, now=clock)
    assert gate.consume() is True
    assert gate.consume() is False
    now = 0.5
    assert gate.consume() is True
