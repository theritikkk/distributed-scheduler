"""Shared gate that turns monitor samples into allow/block decisions."""
from __future__ import annotations

import threading
import time

from metrics import DB_POOL_UTILIZATION, DB_POOL_WAITING, DISPATCH_GATE_BLOCKED

from .controller import BackpressureController, BackpressureDecision
from .db_gate import DbGate
from .queue_monitor import QueueDepthMonitor


class BackpressureGate:
    def __init__(
        self,
        *,
        mode: str,
        poll_sec: float,
        tasks_high: int,
        tasks_low: int,
        results_high: int,
        results_low: int,
        due_high: int,
        db_high: float,
        db_resume: float,
        db_max_waiters: int,
        high_consecutive: int,
        low_consecutive: int,
        stale_pause_sec: float,
        monitor: QueueDepthMonitor,
        db_gate: DbGate,
    ) -> None:
        self.mode = mode
        self.poll_sec = max(0.05, poll_sec)
        self.due_high = due_high
        self.monitor = monitor
        self.db_gate = db_gate
        self.controller = BackpressureController(
            tasks_high=tasks_high,
            tasks_low=tasks_low,
            results_high=results_high,
            results_low=results_low,
            db_high=db_high,
            db_resume=db_resume,
            db_max_waiters=db_max_waiters,
            high_consecutive=high_consecutive,
            low_consecutive=low_consecutive,
            stale_pause_sec=stale_pause_sec,
        )
        self._lock = threading.Lock()
        self._last_eval = 0.0
        self._decision = BackpressureDecision("unknown", ("startup",))
        self._depths: dict[str, int] = {}
        self._sampled_at = 0.0

    def decision(self, force: bool = False) -> BackpressureDecision:
        now = time.monotonic()
        with self._lock:
            if not force and now - self._last_eval < self.poll_sec:
                return self._decision
            depths, sampled_at = self.monitor.snapshot()
            db_util, db_waiters = self.db_gate.snapshot()
            DB_POOL_UTILIZATION.set(db_util)
            DB_POOL_WAITING.set(db_waiters)
            self._decision = self.controller.evaluate(
                task_depth=depths.get("scheduler.tasks"),
                result_depth=depths.get("scheduler.results"),
                db_utilization=db_util,
                db_waiters=db_waiters,
                sampled_at=sampled_at,
            )
            self._depths = depths
            self._sampled_at = sampled_at
            self._last_eval = now
            return self._decision

    def allow(self, consumer: str) -> bool:
        decision = self.decision()
        if self.mode == "observe":
            return True
        if decision.state == "normal":
            return True

        if consumer == "result" and decision.state == "paused":
            # Results must drain unless the DB or result queue itself is the
            # bottleneck. A task backlog alone must not stop result processing.
            if not ({"db_pool", "db_waiters", "result_depth"} & set(decision.reasons)):
                return True

        if consumer == "reconciliation":
            due_depth = self._depths.get("scheduler.due", 0)
            if due_depth >= self.due_high:
                DISPATCH_GATE_BLOCKED.labels(reason="due_depth").inc()
                return False

        DISPATCH_GATE_BLOCKED.labels(reason=consumer).inc()
        return False
