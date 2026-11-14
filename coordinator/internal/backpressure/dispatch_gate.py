"""Local token bucket for coordinator dispatch pacing."""
from __future__ import annotations

import threading
import time


class DispatchGate:
    def __init__(
        self,
        capacity: int,
        refill_per_sec: float,
        now=time.monotonic,
    ) -> None:
        self.capacity = max(1, capacity)
        self.refill_per_sec = max(0.001, refill_per_sec)
        self._tokens = float(self.capacity)
        self._last = now()
        self._now = now
        self._lock = threading.Lock()

    def consume(self, cost: float = 1.0) -> bool:
        with self._lock:
            now = self._now()
            elapsed = max(0.0, now - self._last)
            self._tokens = min(
                self.capacity,
                self._tokens + elapsed * self.refill_per_sec,
            )
            self._last = now
            if self._tokens < cost:
                return False
            self._tokens -= cost
            return True
