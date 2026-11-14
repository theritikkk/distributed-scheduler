"""Bound coordinator database connection creation and expose gate statistics."""
from __future__ import annotations

import threading


class TrackedConnection:
    """Proxy that returns a connection slot when ``close`` is called."""

    _INTERNAL = {"_connection", "_release", "_closed"}

    def __init__(self, connection, release) -> None:
        object.__setattr__(self, "_connection", connection)
        object.__setattr__(self, "_release", release)
        object.__setattr__(self, "_closed", False)

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def __setattr__(self, name, value):
        if name in self._INTERNAL:
            object.__setattr__(self, name, value)
        else:
            setattr(self._connection, name, value)

    def close(self) -> None:
        if self._closed:
            return
        object.__setattr__(self, "_closed", True)
        try:
            self._connection.close()
        finally:
            self._release()


class DbGate:
    """Semaphore-based connection gate.

    This is deliberately a bound rather than a pool: callers still open a
    short-lived session, but the coordinator can never create more than
    ``max_connections`` at once and the waiting count is visible to the
    backpressure controller.
    """

    def __init__(self, max_connections: int, acquire_timeout_sec: float = 5.0):
        self.max_connections = max(1, max_connections)
        self.acquire_timeout_sec = acquire_timeout_sec
        self._semaphore = threading.BoundedSemaphore(self.max_connections)
        self._lock = threading.Lock()
        self._active = 0
        self._waiting = 0

    def connect(self, factory):
        with self._lock:
            self._waiting += 1
        acquired = self._semaphore.acquire(timeout=self.acquire_timeout_sec)
        with self._lock:
            self._waiting -= 1
            if acquired:
                self._active += 1
        if not acquired:
            raise TimeoutError("database connection gate timed out")

        try:
            connection = factory()
        except Exception:
            self._release()
            raise
        return TrackedConnection(connection, self._release)

    def _release(self) -> None:
        with self._lock:
            self._active = max(0, self._active - 1)
        self._semaphore.release()

    def snapshot(self) -> tuple[float, int]:
        with self._lock:
            return self._active / self.max_connections, self._waiting
