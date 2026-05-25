"""Prometheus metrics for the coordinator service."""
from prometheus_client import Counter, Gauge

DUE_DISPATCHED = Counter(
    "scheduler_coordinator_due_dispatched_total",
    "Tasks dispatched from scheduler.due to scheduler.tasks",
)

RESULTS_APPLIED = Counter(
    "scheduler_coordinator_results_applied_total",
    "Worker results applied to the database",
    ["status"],
)

RESULTS_SKIPPED_IDEMPOTENT = Counter(
    "scheduler_coordinator_results_skipped_idempotent_total",
    "Duplicate result messages ignored (execution already completed)",
)

RECONCILIATION_NUDGED = Counter(
    "scheduler_coordinator_reconciliation_nudged_total",
    "Due tasks nudged to scheduler.due by reconciliation poller",
)

WORKERS_MARKED_OFFLINE = Counter(
    "scheduler_coordinator_workers_marked_offline_total",
    "Workers marked offline by heartbeat checker",
)

# Optional gauge if management API poll is enabled (see internal/queue/rabbitmq_metrics.py)
RABBITMQ_QUEUE_MESSAGES = Gauge(
    "scheduler_rabbitmq_queue_messages",
    "Queue depth from RabbitMQ management API (coordinator poll)",
    ["queue"],
)
