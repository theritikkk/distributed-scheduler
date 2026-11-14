"""Dedicated RabbitMQ connection that passively samples queue depths."""
from __future__ import annotations

import logging
import random
import threading
import time

from metrics import QUEUE_DEPTH, QUEUE_MONITOR_RECONNECTS

from ..queue.connection import connect_blocking

logger = logging.getLogger(__name__)


def _jitter(seconds: float) -> float:
    return seconds * random.uniform(0.5, 1.0)


class QueueDepthMonitor:
    def __init__(
        self,
        rabbit_url: str,
        queues: list[str],
        interval_sec: float = 1.0,
    ) -> None:
        self._rabbit_url = rabbit_url
        self._queues = queues
        self._interval_sec = max(0.1, interval_sec)
        self._lock = threading.Lock()
        self._snapshot: dict[str, int] = {}
        self._sampled_at = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="queue-depth-monitor", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def snapshot(self) -> tuple[dict[str, int], float]:
        with self._lock:
            return dict(self._snapshot), self._sampled_at

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            connection = None
            try:
                connection = connect_blocking(self._rabbit_url)
                channel = connection.channel()
                backoff = 1.0
                while not self._stop.is_set():
                    depths: dict[str, int] = {}
                    for queue in self._queues:
                        declared = channel.queue_declare(
                            queue=queue, passive=True
                        )
                        depths[queue] = int(declared.method.message_count)
                        QUEUE_DEPTH.labels(queue=queue).set(depths[queue])
                    with self._lock:
                        self._snapshot = depths
                        self._sampled_at = time.monotonic()
                    self._stop.wait(self._interval_sec)
            except Exception as exc:
                if self._stop.is_set():
                    break
                QUEUE_MONITOR_RECONNECTS.inc()
                logger.warning("queue depth monitor: %s", exc)
                self._stop.wait(_jitter(backoff))
                backoff = min(backoff * 2, 30.0)
            finally:
                if connection is not None and connection.is_open:
                    try:
                        connection.close()
                    except Exception:
                        pass
