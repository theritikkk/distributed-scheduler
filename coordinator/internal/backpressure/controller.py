"""Queue/database backpressure state machine with hysteresis."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from metrics import (
    BACKPRESSURE_PAUSED_SECONDS,
    BACKPRESSURE_STATE,
    BACKPRESSURE_TRANSITIONS,
)


@dataclass(frozen=True)
class BackpressureDecision:
    state: str
    reasons: tuple[str, ...] = ()


class BackpressureController:
    """Compute normal/paused/unknown from queue and DB signals.

    A high condition must be observed for ``high_consecutive`` samples before
    pausing, and a low condition for ``low_consecutive`` samples before resuming.
    This prevents a single noisy sample from causing flapping.
    """

    def __init__(
        self,
        *,
        tasks_high: int,
        tasks_low: int,
        results_high: int,
        results_low: int,
        db_high: float,
        db_resume: float,
        db_max_waiters: int,
        high_consecutive: int,
        low_consecutive: int,
        stale_pause_sec: float,
        now=time.monotonic,
    ) -> None:
        self.tasks_high = tasks_high
        self.tasks_low = tasks_low
        self.results_high = results_high
        self.results_low = results_low
        self.db_high = db_high
        self.db_resume = db_resume
        self.db_max_waiters = db_max_waiters
        self.high_consecutive = max(1, high_consecutive)
        self.low_consecutive = max(1, low_consecutive)
        self.stale_pause_sec = stale_pause_sec
        self._now = now
        self._lock = threading.Lock()
        self._state = "normal"
        self._high_count = 0
        self._low_count = 0
        self._last_evaluated = now()
        self._paused_since: float | None = None
        self._set_metric("normal")

    def evaluate(
        self,
        *,
        task_depth: int | None,
        result_depth: int | None,
        db_utilization: float | None,
        db_waiters: int | None,
        sampled_at: float,
    ) -> BackpressureDecision:
        now = self._now()
        with self._lock:
            stale = (now - sampled_at) > self.stale_pause_sec
            if stale or task_depth is None or db_utilization is None:
                return self._transition("unknown", ("stale_or_missing_signal",))

            reasons: list[str] = []
            if task_depth >= self.tasks_high:
                reasons.append("task_depth")
            if (result_depth or 0) >= self.results_high:
                reasons.append("result_depth")
            if db_utilization >= self.db_high:
                reasons.append("db_pool")
            if (db_waiters or 0) > self.db_max_waiters:
                reasons.append("db_waiters")

            high = bool(reasons)
            low = (
                task_depth <= self.tasks_low
                and (result_depth or 0) <= self.results_low
                and db_utilization <= self.db_resume
                and (db_waiters or 0) == 0
            )

            if self._state in ("normal", "unknown"):
                self._high_count = self._high_count + 1 if high else 0
                if high and self._high_count >= self.high_consecutive:
                    return self._transition("paused", tuple(reasons))
                return self._transition("normal", ())
            else:
                self._low_count = self._low_count + 1 if low else 0
                if low and self._low_count >= self.low_consecutive:
                    return self._transition("normal", ())
                return self._transition("paused", tuple(reasons) or ("hysteresis",))

    def _transition(self, state: str, reasons: tuple[str, ...]) -> BackpressureDecision:
        now = self._now()
        if state != self._state:
            BACKPRESSURE_TRANSITIONS.labels(
                from_state=self._state, to_state=state
            ).inc()
            self._state = state
            self._high_count = 0
            self._low_count = 0
            if state == "paused":
                self._paused_since = now
            elif self._paused_since is not None:
                BACKPRESSURE_PAUSED_SECONDS.inc(now - self._paused_since)
                self._paused_since = None
            self._set_metric(state)
        self._last_evaluated = now
        return BackpressureDecision(state, reasons)

    @staticmethod
    def _set_metric(state: str) -> None:
        for candidate in ("normal", "paused", "unknown"):
            BACKPRESSURE_STATE.labels(state=candidate).set(
                1 if candidate == state else 0
            )

    @property
    def state(self) -> str:
        with self._lock:
            return self._state
