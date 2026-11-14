"""
Coordinator service (Python).
- Delayed wakeups, due consumer, result consumer, reconciliation (see internal.queue)
- Worker heartbeat checker (see internal.registry)
- Prometheus metrics on METRICS_PORT (default 9090)
"""
import logging
import sys
import threading

import psycopg2  # type: ignore[import-untyped]
from prometheus_client import start_http_server

from internal.backpressure import (
    BackpressureGate,
    DbGate,
    DispatchGate,
    QueueDepthMonitor,
)
from internal.archive import run_archive_loop
from internal.config import load_config
from internal.queue import (
    run_due_consumer,
    run_outbox_relay,
    run_reaper,
    run_reconciliation_poller,
    run_result_consumer,
    run_scheduled_event_poller,
)
from internal.queue.rabbitmq_metrics import start_rabbitmq_queue_metrics_poller
from internal.queue.topology import DUE_QUEUE, RESULT_QUEUE, TASK_QUEUE
from internal.registry import Registry
from internal.tracing import configure_tracing

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s level=%(levelname)s service=coordinator %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

CONFIG = load_config()
DATABASE_URL = CONFIG.database_url
RABBITMQ_URL = CONFIG.rabbitmq_url
METRICS_PORT = CONFIG.metrics_port

configure_tracing(
    CONFIG.otel_service_name,
    enabled=CONFIG.otel_enabled,
    endpoint=CONFIG.otel_exporter_endpoint,
    sampler_arg=CONFIG.otel_sampler_arg,
)


DB_GATE = DbGate(
    CONFIG.db_pool_max,
    acquire_timeout_sec=max(1, CONFIG.db_connect_timeout_sec),
)


def _connect_db():
    options = (
        f"-c statement_timeout={CONFIG.db_statement_timeout_ms} "
        f"-c lock_timeout={CONFIG.db_lock_timeout_ms}"
    )
    return psycopg2.connect(
        DATABASE_URL,
        connect_timeout=CONFIG.db_connect_timeout_sec,
        options=options,
    )


def get_db_conn():
    conn = DB_GATE.connect(_connect_db)
    conn.autocommit = True
    return conn


def main():
    start_http_server(METRICS_PORT)
    logger.info("Prometheus metrics listening on :%s/metrics", METRICS_PORT)

    try:
        conn = get_db_conn()
        conn.close()
    except Exception as e:
        logger.fatal("db: %s", e)
        sys.exit(1)

    start_rabbitmq_queue_metrics_poller(
        RABBITMQ_URL, CONFIG.rabbitmq_metrics_poll_sec
    )

    monitor = QueueDepthMonitor(
        RABBITMQ_URL,
        [TASK_QUEUE, RESULT_QUEUE, DUE_QUEUE],
        interval_sec=CONFIG.backpressure_poll_sec,
    )
    monitor.start()
    backpressure = BackpressureGate(
        mode=CONFIG.backpressure_mode,
        poll_sec=CONFIG.backpressure_poll_sec,
        tasks_high=CONFIG.backpressure_tasks_high,
        tasks_low=CONFIG.backpressure_tasks_low,
        results_high=CONFIG.backpressure_results_high,
        results_low=CONFIG.backpressure_results_low,
        due_high=CONFIG.backpressure_due_high,
        db_high=CONFIG.db_pool_high_watermark,
        db_resume=CONFIG.db_pool_resume_watermark,
        db_max_waiters=CONFIG.db_pool_max_waiters,
        high_consecutive=CONFIG.backpressure_high_consecutive,
        low_consecutive=CONFIG.backpressure_low_consecutive,
        stale_pause_sec=CONFIG.backpressure_stale_pause_sec,
        monitor=monitor,
        db_gate=DB_GATE,
    )
    dispatch_gate = DispatchGate(
        CONFIG.dispatch_burst, CONFIG.dispatch_rate_per_sec
    )

    registry = Registry(get_db_conn)
    threading.Thread(target=registry.run_heartbeat_checker, daemon=True).start()

    threading.Thread(
        target=run_result_consumer,
        kwargs={
            "get_db_conn": get_db_conn,
            "rabbit_url": RABBITMQ_URL,
            "scheduler_jitter_enabled": CONFIG.scheduler_jitter_enabled,
            "outbox_enabled": CONFIG.outbox_enabled,
            "queue_type": CONFIG.rabbitmq_queue_type,
            "backpressure": backpressure,
        },
        daemon=True,
    ).start()

    if CONFIG.outbox_enabled:
        threading.Thread(
            target=run_outbox_relay,
            kwargs={
                "get_db_conn": get_db_conn,
                "rabbit_url": RABBITMQ_URL,
                "poll_interval_sec": CONFIG.outbox_poll_interval_sec,
                "batch_size": CONFIG.outbox_batch_size,
                "max_attempts": CONFIG.outbox_max_attempts,
                "backoff_base_sec": CONFIG.outbox_backoff_base_sec,
                "backoff_max_sec": CONFIG.outbox_backoff_max_sec,
                "max_publish_per_sec": CONFIG.outbox_max_publish_per_sec,
                "retention_days": CONFIG.outbox_retention_days,
                "cleanup_interval_sec": CONFIG.outbox_cleanup_interval_sec,
                "scheduled_events_enabled": CONFIG.scheduled_events_enabled,
                "queue_type": CONFIG.rabbitmq_queue_type,
                "backpressure": backpressure,
            },
            daemon=True,
        ).start()

    if CONFIG.scheduled_events_enabled:
        threading.Thread(
            target=run_scheduled_event_poller,
            kwargs={
                "get_db_conn": get_db_conn,
                "rabbit_url": RABBITMQ_URL,
                "poll_interval_sec": CONFIG.scheduled_event_poll_interval_sec,
                "batch_size": CONFIG.scheduled_event_batch_size,
                "max_attempts": CONFIG.scheduled_event_max_attempts,
                "backoff_base_sec": CONFIG.scheduled_event_backoff_base_sec,
                "backoff_max_sec": CONFIG.scheduled_event_backoff_max_sec,
                "retention_days": CONFIG.scheduled_event_retention_days,
                "cleanup_interval_sec": (
                    CONFIG.scheduled_event_cleanup_interval_sec
                ),
                "max_publish_per_sec": (
                    CONFIG.scheduled_event_max_publish_per_sec
                ),
                "queue_type": CONFIG.rabbitmq_queue_type,
                "backpressure": backpressure,
            },
            daemon=True,
        ).start()

    if CONFIG.reaper_enabled:
        threading.Thread(
            target=run_reaper,
            args=(
                get_db_conn,
                RABBITMQ_URL,
                CONFIG.reaper_interval_sec,
                CONFIG.task_max_retries,
                CONFIG.pending_retry_grace_sec,
                CONFIG.idempotency_cleanup_interval_sec,
                CONFIG.outbox_enabled,
                CONFIG.rabbitmq_queue_type,
            ),
            daemon=True,
        ).start()

    if CONFIG.archive_enabled:
        threading.Thread(
            target=run_archive_loop,
            kwargs={
                "get_db_conn": get_db_conn,
                "retention_days": CONFIG.archive_retention_days,
                "batch_size": CONFIG.archive_batch_size,
                "interval_sec": CONFIG.archive_interval_sec,
                "max_batches": CONFIG.archive_max_batches,
                "sleep_ms": CONFIG.archive_sleep_ms,
            },
            daemon=True,
        ).start()

    threading.Thread(
        target=run_reconciliation_poller,
        kwargs={
            "get_db_conn": get_db_conn,
            "rabbit_url": RABBITMQ_URL,
            "interval_sec": CONFIG.reconcile_interval_sec,
            "jitter_ratio": CONFIG.reconcile_interval_jitter_ratio,
            "backpressure": backpressure,
            "batch_size": CONFIG.reconcile_batch,
            "pace_sec": CONFIG.reconcile_pace_sec,
            "queue_type": CONFIG.rabbitmq_queue_type,
        },
        daemon=True,
    ).start()

    run_due_consumer(
        get_db_conn,
        RABBITMQ_URL,
        CONFIG.task_max_retries,
        CONFIG.outbox_enabled,
        scheduled_events_enabled=CONFIG.scheduled_events_enabled,
        queue_type=CONFIG.rabbitmq_queue_type,
        backpressure=backpressure,
        dispatch_gate=dispatch_gate,
    )


if __name__ == "__main__":
    main()
