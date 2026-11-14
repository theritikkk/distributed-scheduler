"""Bounded, thread-safe PostgreSQL connection pool for the worker process."""
from __future__ import annotations

import threading
import time

from psycopg2 import pool as pg_pool


class PooledConnection:
    """Proxy that returns its connection to the pool on ``close``."""

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
            self._release()
        except Exception:
            pass


class WorkerDbPool:
    """Wrap psycopg2's ThreadedConnectionPool with bounded acquisition.

    ``ThreadedConnectionPool.getconn`` returns immediately or raises when all
    connections are checked out. The semaphore makes callers wait for a bounded
    period instead, which keeps consumer threads from failing under a short
    burst of heartbeat + task-check activity.
    """

    def __init__(
        self,
        dsn: str,
        *,
        min_connections: int = 1,
        max_connections: int = 8,
        connect_timeout_sec: int = 5,
        acquire_timeout_sec: float = 5.0,
    ) -> None:
        self.max_connections = max(1, max_connections)
        self.acquire_timeout_sec = acquire_timeout_sec
        self._semaphore = threading.BoundedSemaphore(self.max_connections)
        self._pool = pg_pool.ThreadedConnectionPool(
            max(0, min_connections),
            self.max_connections,
            dsn,
            connect_timeout=connect_timeout_sec,
        )

    def get_connection(self) -> PooledConnection:
        if not self._semaphore.acquire(timeout=self.acquire_timeout_sec):
            raise TimeoutError("worker database pool acquisition timed out")

        raw = None
        try:
            deadline = time.monotonic() + self.acquire_timeout_sec
            while raw is None:
                try:
                    raw = self._pool.getconn()
                except pg_pool.PoolError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.01)
        except Exception:
            self._semaphore.release()
            raise

        def release() -> None:
            broken = False
            try:
                # Do not return a connection with an open transaction (or a
                # broken socket) to the next consumer thread.
                try:
                    raw.rollback()
                except Exception:
                    broken = True
                try:
                    raw.autocommit = False
                except Exception:
                    broken = True
                self._pool.putconn(raw, close=broken)
            finally:
                self._semaphore.release()

        return PooledConnection(raw, release)

    def closeall(self) -> None:
        self._pool.closeall()
