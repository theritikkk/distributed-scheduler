"""Validated coordinator configuration.

Development defaults keep local setup easy; production requires explicit URLs and
rejects invalid or out-of-range values at startup.
"""
from __future__ import annotations

import os
import warnings
from dataclasses import dataclass
from typing import Mapping


class ConfigError(ValueError):
    """Raised when an environment variable is missing or invalid."""


@dataclass(frozen=True)
class CoordinatorConfig:
    app_env: str
    database_url: str
    rabbitmq_url: str
    metrics_port: int
    reconcile_interval_sec: int
    reconcile_interval_jitter_ratio: float
    task_max_retries: int
    scheduler_jitter_enabled: bool
    db_connect_timeout_sec: int
    rabbitmq_metrics_poll_sec: int
    reaper_enabled: bool
    reaper_interval_sec: int
    pending_retry_grace_sec: int
    idempotency_cleanup_interval_sec: int
    outbox_enabled: bool
    outbox_poll_interval_sec: int
    outbox_batch_size: int
    outbox_max_attempts: int
    outbox_backoff_base_sec: int
    outbox_backoff_max_sec: int
    outbox_max_publish_per_sec: int
    outbox_retention_days: int
    outbox_cleanup_interval_sec: int
    scheduled_events_enabled: bool
    scheduled_event_poll_interval_sec: float
    scheduled_event_batch_size: int
    scheduled_event_max_attempts: int
    scheduled_event_backoff_base_sec: int
    scheduled_event_backoff_max_sec: int
    scheduled_event_retention_days: int
    scheduled_event_cleanup_interval_sec: int
    scheduled_event_max_publish_per_sec: int
    rabbitmq_queue_type: str
    backpressure_mode: str
    backpressure_tasks_high: int
    backpressure_tasks_low: int
    backpressure_results_high: int
    backpressure_results_low: int
    backpressure_due_high: int
    backpressure_poll_sec: float
    backpressure_high_consecutive: int
    backpressure_low_consecutive: int
    backpressure_stale_pause_sec: float
    dispatch_rate_per_sec: int
    dispatch_burst: int
    db_pool_max: int
    db_pool_max_waiters: int
    db_pool_high_watermark: float
    db_pool_resume_watermark: float
    db_statement_timeout_ms: int
    db_lock_timeout_ms: int
    reconcile_batch: int
    reconcile_pace_sec: float
    otel_enabled: bool
    otel_service_name: str
    otel_exporter_endpoint: str
    otel_sampler_arg: float
    archive_enabled: bool
    archive_retention_days: int
    archive_batch_size: int
    archive_interval_sec: int
    archive_max_batches: int
    archive_sleep_ms: int

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"


def _read(env: Mapping[str, str], key: str) -> str | None:
    value = env.get(key, "").strip()
    return value or None


def _app_env(env: Mapping[str, str]) -> str:
    value = (_read(env, "APP_ENV") or _read(env, "ENVIRONMENT") or "development").lower()
    if value not in {"development", "test", "production"}:
        raise ConfigError("APP_ENV must be development, test, or production")
    return value


def _required_or_dev(
    env: Mapping[str, str],
    key: str,
    default: str,
    is_production: bool,
    warnings_list: list[str],
) -> str:
    value = _read(env, key)
    if value:
        return value
    if is_production:
        raise ConfigError(f"{key} is required when APP_ENV=production")
    warnings_list.append(f"{key} is not set; using a development-only default")
    return default


def _int(
    env: Mapping[str, str],
    key: str,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw = _read(env, key)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{key} must be an integer") from exc
    if not (minimum <= value <= maximum):
        raise ConfigError(f"{key} must be between {minimum} and {maximum}")
    return value


def _bool(env: Mapping[str, str], key: str, default: bool) -> bool:
    raw = _read(env, key)
    if raw is None:
        return default
    value = raw.lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ConfigError(f"{key} must be true or false")


def _choice(
    env: Mapping[str, str],
    key: str,
    default: str,
    allowed: set[str],
) -> str:
    value = (_read(env, key) or default).lower()
    if value not in allowed:
        raise ConfigError(f"{key} must be one of {', '.join(sorted(allowed))}")
    return value


def _float(
    env: Mapping[str, str],
    key: str,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    raw = _read(env, key)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"{key} must be a number") from exc
    if not (minimum <= value <= maximum):
        raise ConfigError(f"{key} must be between {minimum} and {maximum}")
    return value


def load_config(env: Mapping[str, str] | None = None) -> CoordinatorConfig:
    values: Mapping[str, str] = os.environ if env is None else env
    app_env = _app_env(values)
    is_production = app_env == "production"
    warnings_list: list[str] = []

    database_url = _required_or_dev(
        values,
        "DATABASE_URL",
        "postgresql://scheduler:scheduler_secret@localhost:5432/distributed_scheduler",
        is_production,
        warnings_list,
    )
    rabbitmq_url = _required_or_dev(
        values,
        "RABBITMQ_URL",
        "amqp://scheduler:scheduler_secret@localhost:5672",
        is_production,
        warnings_list,
    )

    for warning in warnings_list:
        warnings.warn(f"[config] {warning}", stacklevel=2)

    config = CoordinatorConfig(
        app_env=app_env,
        database_url=database_url,
        rabbitmq_url=rabbitmq_url,
        metrics_port=_int(values, "METRICS_PORT", 9090, 1, 65535),
        reconcile_interval_sec=_int(
            values, "SCHEDULER_RECONCILE_INTERVAL_SEC", 120, 1, 86_400
        ),
        reconcile_interval_jitter_ratio=_float(
            values, "RECONCILE_INTERVAL_JITTER_RATIO", 0.2, 0.0, 0.5
        ),
        task_max_retries=_int(values, "TASK_MAX_RETRIES", 3, 0, 100),
        scheduler_jitter_enabled=_bool(
            values, "SCHEDULER_JITTER_ENABLED", False
        ),
        db_connect_timeout_sec=_int(values, "DB_CONNECT_TIMEOUT_SEC", 5, 1, 60),
        rabbitmq_metrics_poll_sec=_int(
            values, "RABBITMQ_METRICS_POLL_SEC", 15, 1, 3_600
        ),
        reaper_enabled=_bool(values, "REAPER_ENABLED", True),
        reaper_interval_sec=_int(values, "REAPER_INTERVAL_SEC", 15, 1, 3_600),
        pending_retry_grace_sec=_int(
            values, "PENDING_RETRY_GRACE_SEC", 300, 10, 86_400
        ),
        idempotency_cleanup_interval_sec=_int(
            values, "IDEMPOTENCY_CLEANUP_INTERVAL_SEC", 3_600, 60, 86_400
        ),
        outbox_enabled=_bool(values, "OUTBOX_ENABLED", True),
        outbox_poll_interval_sec=_int(
            values, "OUTBOX_POLL_INTERVAL_SEC", 1, 1, 60
        ),
        outbox_batch_size=_int(values, "OUTBOX_BATCH_SIZE", 100, 1, 1_000),
        outbox_max_attempts=_int(
            values, "OUTBOX_MAX_ATTEMPTS", 10, 1, 1_000
        ),
        outbox_backoff_base_sec=_int(
            values, "OUTBOX_BACKOFF_BASE_SEC", 1, 1, 3_600
        ),
        outbox_backoff_max_sec=_int(
            values, "OUTBOX_BACKOFF_MAX_SEC", 60, 1, 86_400
        ),
        outbox_max_publish_per_sec=_int(
            values, "OUTBOX_MAX_PUBLISH_PER_SEC", 100, 1, 100_000
        ),
        outbox_retention_days=_int(
            values, "OUTBOX_RETENTION_DAYS", 7, 1, 3_650
        ),
        outbox_cleanup_interval_sec=_int(
            values, "OUTBOX_CLEANUP_INTERVAL_SEC", 3_600, 60, 86_400
        ),
        scheduled_events_enabled=_bool(
            values, "SCHEDULED_EVENTS_ENABLED", True
        ),
        scheduled_event_poll_interval_sec=_float(
            values, "SCHEDULED_EVENT_POLL_INTERVAL_SEC", 1.0, 0.05, 60.0
        ),
        scheduled_event_batch_size=_int(
            values, "SCHEDULED_EVENT_BATCH_SIZE", 100, 1, 1_000
        ),
        scheduled_event_max_attempts=_int(
            values, "SCHEDULED_EVENT_MAX_ATTEMPTS", 10, 1, 1_000
        ),
        scheduled_event_backoff_base_sec=_int(
            values, "SCHEDULED_EVENT_BACKOFF_BASE_SEC", 1, 1, 3_600
        ),
        scheduled_event_backoff_max_sec=_int(
            values, "SCHEDULED_EVENT_BACKOFF_MAX_SEC", 60, 1, 86_400
        ),
        scheduled_event_retention_days=_int(
            values, "SCHEDULED_EVENT_RETENTION_DAYS", 7, 1, 3_650
        ),
        scheduled_event_cleanup_interval_sec=_int(
            values, "SCHEDULED_EVENT_CLEANUP_INTERVAL_SEC", 3_600, 60, 86_400
        ),
        scheduled_event_max_publish_per_sec=_int(
            values, "SCHEDULED_EVENT_MAX_PUBLISH_PER_SEC", 100, 1, 100_000
        ),
        rabbitmq_queue_type=_choice(
            values, "RABBITMQ_QUEUE_TYPE", "classic", {"classic", "quorum"}
        ),
        backpressure_mode=_choice(
            values, "BACKPRESSURE_MODE", "observe", {"observe", "enforce"}
        ),
        backpressure_tasks_high=_int(
            values, "BACKPRESSURE_TASKS_HIGH", 5_000, 1, 10_000_000
        ),
        backpressure_tasks_low=_int(
            values, "BACKPRESSURE_TASKS_LOW", 1_000, 0, 10_000_000
        ),
        backpressure_results_high=_int(
            values, "BACKPRESSURE_RESULTS_HIGH", 2_000, 1, 10_000_000
        ),
        backpressure_results_low=_int(
            values, "BACKPRESSURE_RESULTS_LOW", 500, 0, 10_000_000
        ),
        backpressure_due_high=_int(
            values, "BACKPRESSURE_DUE_HIGH", 10_000, 1, 10_000_000
        ),
        backpressure_poll_sec=_float(
            values, "BACKPRESSURE_POLL_SEC", 1.0, 0.05, 60.0
        ),
        backpressure_high_consecutive=_int(
            values, "BACKPRESSURE_HIGH_CONSECUTIVE", 2, 1, 100
        ),
        backpressure_low_consecutive=_int(
            values, "BACKPRESSURE_LOW_CONSECUTIVE", 3, 1, 100
        ),
        backpressure_stale_pause_sec=_float(
            values, "BACKPRESSURE_STALE_PAUSE_SEC", 15.0, 1.0, 3_600.0
        ),
        dispatch_rate_per_sec=_int(
            values, "DISPATCH_RATE_PER_SEC", 200, 1, 1_000_000
        ),
        dispatch_burst=_int(values, "DISPATCH_BURST", 20, 1, 1_000_000),
        db_pool_max=_int(values, "DB_POOL_MAX", 20, 1, 1_000),
        db_pool_max_waiters=_int(
            values, "DB_POOL_MAX_WAITERS", 5, 0, 10_000
        ),
        db_pool_high_watermark=_float(
            values, "DB_POOL_HIGH_WATERMARK", 0.80, 0.0, 1.0
        ),
        db_pool_resume_watermark=_float(
            values, "DB_POOL_RESUME_WATERMARK", 0.60, 0.0, 1.0
        ),
        db_statement_timeout_ms=_int(
            values, "DB_STATEMENT_TIMEOUT_MS", 5_000, 0, 600_000
        ),
        db_lock_timeout_ms=_int(
            values, "DB_LOCK_TIMEOUT_MS", 2_000, 0, 600_000
        ),
        reconcile_batch=_int(values, "RECONCILE_BATCH", 100, 1, 10_000),
        reconcile_pace_sec=_float(
            values, "RECONCILE_PACE_SEC", 0.01, 0.0, 10.0
        ),
        otel_enabled=_bool(values, "OTEL_ENABLED", False),
        otel_service_name=_read(values, "OTEL_SERVICE_NAME") or "coordinator",
        otel_exporter_endpoint=(
            _read(values, "OTEL_EXPORTER_OTLP_ENDPOINT")
            or "http://localhost:4318/v1/traces"
        ),
        otel_sampler_arg=_float(values, "OTEL_SAMPLER_ARG", 0.1, 0.0, 1.0),
        archive_enabled=_bool(values, "ARCHIVE_ENABLED", False),
        archive_retention_days=_int(
            values, "ARCHIVE_RETENTION_DAYS", 30, 1, 3_650
        ),
        archive_batch_size=_int(
            values, "ARCHIVE_BATCH_SIZE", 500, 1, 10_000
        ),
        archive_interval_sec=_int(
            values, "ARCHIVE_INTERVAL_SEC", 3_600, 60, 86_400
        ),
        archive_max_batches=_int(
            values, "ARCHIVE_MAX_BATCHES", 20, 1, 1_000
        ),
        archive_sleep_ms=_int(values, "ARCHIVE_SLEEP_MS", 50, 0, 60_000),
    )
    if config.outbox_backoff_max_sec < config.outbox_backoff_base_sec:
        raise ConfigError(
            "OUTBOX_BACKOFF_MAX_SEC must be greater than or equal to "
            "OUTBOX_BACKOFF_BASE_SEC"
        )
    if (
        config.scheduled_event_backoff_max_sec
        < config.scheduled_event_backoff_base_sec
    ):
        raise ConfigError(
            "SCHEDULED_EVENT_BACKOFF_MAX_SEC must be greater than or equal to "
            "SCHEDULED_EVENT_BACKOFF_BASE_SEC"
        )
    if config.rabbitmq_queue_type == "quorum" and not (
        config.outbox_enabled and config.scheduled_events_enabled
    ):
        raise ConfigError(
            "RABBITMQ_QUEUE_TYPE=quorum requires OUTBOX_ENABLED=true and "
            "SCHEDULED_EVENTS_ENABLED=true"
        )
    for low_key, high_key, low, high in (
        (
            "BACKPRESSURE_TASKS_LOW",
            "BACKPRESSURE_TASKS_HIGH",
            config.backpressure_tasks_low,
            config.backpressure_tasks_high,
        ),
        (
            "BACKPRESSURE_RESULTS_LOW",
            "BACKPRESSURE_RESULTS_HIGH",
            config.backpressure_results_low,
            config.backpressure_results_high,
        ),
    ):
        if low >= high:
            raise ConfigError(f"{low_key} must be lower than {high_key}")
    if config.db_pool_resume_watermark >= config.db_pool_high_watermark:
        raise ConfigError(
            "DB_POOL_RESUME_WATERMARK must be lower than "
            "DB_POOL_HIGH_WATERMARK"
        )
    return config
