"""Prometheus metrics for worker task execution."""
from prometheus_client import Counter, Gauge, Histogram

WORKER_ID_LABEL = []  # set per process via label in inc calls

TASKS_STARTED = Counter(
    "scheduler_worker_tasks_started_total",
    "Tasks picked up from scheduler.tasks",
    ["worker_id"],
)

TASKS_COMPLETED = Counter(
    "scheduler_worker_tasks_completed_total",
    "Tasks completed successfully",
    ["worker_id"],
)

TASKS_FAILED = Counter(
    "scheduler_worker_tasks_failed_total",
    "Tasks failed (will retry or DLQ)",
    ["worker_id"],
)

TASKS_RETRIED = Counter(
    "scheduler_worker_tasks_retried_total",
    "Tasks sent to scheduler.retry_delay",
    ["worker_id"],
)

TASKS_DLQ = Counter(
    "scheduler_worker_tasks_dlq_total",
    "Poison messages sent to scheduler.tasks.dlq after max retries",
    ["worker_id"],
)

TASKS_IDEMPOTENT_SKIP = Counter(
    "scheduler_worker_tasks_idempotent_skip_total",
    "Duplicate deliveries skipped (execution already finished)",
    ["worker_id"],
)

TASKS_ACKED = Counter(
    "scheduler_worker_tasks_acked_total",
    "RabbitMQ messages acked after processing",
    ["worker_id"],
)

TASK_DURATION = Histogram(
    "scheduler_worker_task_duration_seconds",
    "Wall time to process one task message",
    ["worker_id"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)

CONSUMER_UP = Gauge(
    "scheduler_worker_consumer_up",
    "1 while worker is connected and consuming scheduler.tasks",
    ["worker_id"],
)
