"""Prometheus metrics for the coordinator service."""
import os
import socket

from prometheus_client import Counter, Gauge, Histogram

# A stable identity per process. Override with COORDINATOR_ID when running more than
# one coordinator process (for example, a future horizontally scaled deployment).
COORDINATOR_ID = (
    os.environ.get("COORDINATOR_ID", "").strip()
    or f"{socket.gethostname()}:{os.getpid()}"
)

DUE_DISPATCHED = Counter(
    "scheduler_coordinator_due_dispatched_total",
    "Tasks dispatched from scheduler.due to scheduler.tasks",
    ["coordinator_id"],
)

TASK_JITTER_SECONDS = Histogram(
    "scheduler_task_jitter_seconds",
    "Applied stable jitter offset when a task is dispatched",
    ["schedule_type"],
    buckets=(0, 1, 5, 10, 20, 30, 60, 120, 300, 600, 1800, 3600),
)

TASK_SCHEDULE_LAG_SECONDS = Histogram(
    "scheduler_task_schedule_lag_seconds",
    "Seconds between the base scheduled slot and execution creation",
    ["schedule_type"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300, 900),
)

TASK_DISPATCH_DELAY_SECONDS = Histogram(
    "scheduler_task_dispatch_delay_seconds",
    "Seconds between dispatch_at and execution creation",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300),
)

TASK_NEXT_RUN_DRIFT_SECONDS = Histogram(
    "scheduler_task_next_run_drift_seconds",
    "Extra seconds beyond one cron period when advancing a recurring task",
    ["schedule_type"],
    buckets=(0, 1, 5, 10, 30, 60, 120, 300, 900, 3600),
)

TASK_SLOT_CONFLICTS = Counter(
    "scheduler_task_slot_conflicts_total",
    "Duplicate slot insert attempts rejected by (task_id, scheduled_for)",
)

RECONCILIATION_JITTER_SECONDS = Gauge(
    "scheduler_reconciliation_jitter_seconds",
    "Most recent randomized reconciliation sleep interval",
)

RESULTS_APPLIED = Counter(
    "scheduler_coordinator_results_applied_total",
    "Worker results applied to the database",
    ["coordinator_id", "status"],
)

RESULTS_SKIPPED_IDEMPOTENT = Counter(
    "scheduler_coordinator_results_skipped_idempotent_total",
    "Duplicate result messages ignored (execution already completed)",
    ["coordinator_id"],
)

RESULTS_REJECTED_STALE_LEASE = Counter(
    "scheduler_coordinator_results_rejected_stale_lease_total",
    "Results rejected because the execution lease no longer matches",
    ["coordinator_id"],
)

EXECUTIONS_REAPED = Counter(
    "scheduler_coordinator_executions_reaped_total",
    "Expired or stale executions handled by the reaper",
    ["coordinator_id", "action"],
)

IDEMPOTENCY_KEYS_CLEANED = Counter(
    "scheduler_coordinator_idempotency_keys_cleaned_total",
    "Expired API idempotency keys deleted by the coordinator",
    ["coordinator_id"],
)

RECONCILIATION_NUDGED = Counter(
    "scheduler_coordinator_reconciliation_nudged_total",
    "Due tasks nudged to scheduler.due by reconciliation poller",
    ["coordinator_id"],
)

WORKERS_MARKED_OFFLINE = Counter(
    "scheduler_coordinator_workers_marked_offline_total",
    "Workers marked offline by heartbeat checker",
    ["coordinator_id"],
)

# Optional gauge if management API poll is enabled (see internal/queue/rabbitmq_metrics.py)
RABBITMQ_QUEUE_MESSAGES = Gauge(
    "scheduler_rabbitmq_queue_messages",
    "Queue depth from RabbitMQ management API (coordinator poll)",
    ["coordinator_id", "queue"],
)

OUTBOX_PENDING_ROWS = Gauge(
    "scheduler_outbox_pending_rows",
    "Unpublished outbox rows",
)

OUTBOX_LAG_SECONDS = Gauge(
    "scheduler_outbox_lag_seconds",
    "Age in seconds of the oldest unpublished outbox row",
)

OUTBOX_PUBLISHED_TOTAL = Counter(
    "scheduler_outbox_published_total",
    "Outbox events published and confirmed by RabbitMQ",
)

OUTBOX_PUBLISH_FAILURES_TOTAL = Counter(
    "scheduler_outbox_publish_failures_total",
    "Outbox publish attempts that failed",
)

OUTBOX_FAILED_TOTAL = Counter(
    "scheduler_outbox_failed_total",
    "Outbox events moved to terminal failed status",
)

OUTBOX_SKIPPED_TOTAL = Counter(
    "scheduler_outbox_skipped_total",
    "Outbox rows skipped because another relay holds the aggregate lock",
)

OUTBOX_PUBLISH_DURATION_SECONDS = Histogram(
    "scheduler_outbox_publish_duration_seconds",
    "Time spent publishing and confirming one outbox event",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)

OUTBOX_CLEANUP_DELETED = Counter(
    "scheduler_outbox_cleanup_deleted_total",
    "Published outbox rows removed by retention cleanup",
)

# --- Phase 6 DB-backed scheduled events ---
SCHEDULED_EVENTS_PENDING = Gauge(
    "scheduler_scheduled_events_pending",
    "Pending DB-backed wakeup/retry timers",
)

SCHEDULED_EVENTS_FAILED = Gauge(
    "scheduler_scheduled_events_failed",
    "DB-backed timers that exhausted publish attempts",
)

SCHEDULED_EVENTS_LAG_SECONDS = Gauge(
    "scheduler_scheduled_events_lag_seconds",
    "Seconds the oldest due pending timer is overdue",
)

SCHEDULED_EVENTS_MATERIALIZED_TOTAL = Counter(
    "scheduler_scheduled_events_materialized_total",
    "Outbox events converted into DB-backed timers",
    ["kind", "outcome"],
)

SCHEDULED_EVENTS_CANCELLED_TOTAL = Counter(
    "scheduler_scheduled_events_cancelled_total",
    "Pending timers cancelled because their task was cancelled",
)

SCHEDULED_EVENTS_PUBLISHED_TOTAL = Counter(
    "scheduler_scheduled_events_published_total",
    "Due DB-backed timers published and confirmed by RabbitMQ",
)

SCHEDULED_EVENTS_PUBLISH_FAILURES_TOTAL = Counter(
    "scheduler_scheduled_events_publish_failures_total",
    "DB-backed timer publish attempts that failed",
)

SCHEDULED_EVENTS_PUBLISH_DURATION_SECONDS = Histogram(
    "scheduler_scheduled_events_publish_duration_seconds",
    "Time spent publishing and confirming one due timer",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)

SCHEDULED_EVENTS_CLEANUP_DELETED = Counter(
    "scheduler_scheduled_events_cleanup_deleted_total",
    "Terminal scheduled events removed by retention cleanup",
)

# --- Phase 7 terminal history archival ---
ARCHIVE_ROWS_TOTAL = Counter(
    "scheduler_archive_rows_total",
    "Terminal rows copied to archive tables and deleted from live tables",
    ["table"],
)

ARCHIVE_RUNS_TOTAL = Counter(
    "scheduler_archive_runs_total",
    "Archive cycles by outcome",
    ["status"],
)

ARCHIVE_DURATION_SECONDS = Histogram(
    "scheduler_archive_duration_seconds",
    "Time spent in one archive cycle",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)

ARCHIVE_OLDEST_ROW_AGE_SECONDS = Gauge(
    "scheduler_archive_oldest_row_age_seconds",
    "Age of the oldest terminal row still present in a live table",
    ["table"],
)

ARCHIVE_ERRORS_TOTAL = Counter(
    "scheduler_archive_errors_total",
    "Archive cycle failures",
)

# --- Phase 5 backpressure and reconnect ---
BACKPRESSURE_STATE = Gauge(
    "scheduler_backpressure_state",
    "Coordinator backpressure state (one-hot: normal/paused/unknown)",
    ["state"],
)

BACKPRESSURE_TRANSITIONS = Counter(
    "scheduler_backpressure_transitions_total",
    "Backpressure state transitions",
    ["from_state", "to_state"],
)

BACKPRESSURE_PAUSED_SECONDS = Counter(
    "scheduler_backpressure_paused_seconds_total",
    "Seconds the coordinator spent paused by backpressure",
)

DISPATCH_GATE_BLOCKED = Counter(
    "scheduler_dispatch_gate_blocked_total",
    "Due/outbox/reconciliation fetches blocked by a gate",
    ["reason"],
)

QUEUE_DEPTH = Gauge(
    "scheduler_queue_depth",
    "Queue ready-message depth from passive declares",
    ["queue"],
)

QUEUE_MONITOR_RECONNECTS = Counter(
    "scheduler_queue_monitor_reconnects_total",
    "Queue depth monitor connection attempts after failure",
)

DB_POOL_UTILIZATION = Gauge(
    "scheduler_db_pool_utilization",
    "Coordinator DB connection gate utilization (0-1)",
)

DB_POOL_WAITING = Gauge(
    "scheduler_db_pool_waiting",
    "Callers waiting for a coordinator DB connection",
)

CONSUMER_RECONNECTS = Counter(
    "scheduler_consumer_reconnects_total",
    "Consumer connection reconnects",
    ["consumer"],
)

CONSUMER_ERRORS = Counter(
    "scheduler_consumer_errors_total",
    "Consumer errors by kind",
    ["consumer", "kind"],
)

DISPATCH_LATENCY = Histogram(
    "scheduler_dispatch_latency_seconds",
    "Time to apply one consumed message including commit/publish",
    ["consumer"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)

RECONCILIATION_SKIPPED = Counter(
    "scheduler_reconciliation_skipped_total",
    "Reconciliation cycles skipped",
    ["reason"],
)
