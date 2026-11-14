"""Validated worker configuration.

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
class WorkerConfig:
    app_env: str
    worker_id: str
    database_url: str
    rabbitmq_url: str
    metrics_port: int
    task_max_retries: int
    retry_jitter_mode: str
    command_timeout_sec: int
    max_output_bytes: int
    worker_concurrency: int
    db_connect_timeout_sec: int
    heartbeat_interval_sec: int
    execution_lease_sec: int
    lease_heartbeat_sec: int
    outbox_enabled: bool
    rabbitmq_queue_type: str
    otel_enabled: bool
    otel_service_name: str
    otel_exporter_endpoint: str
    otel_sampler_arg: float

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


def load_config(env: Mapping[str, str] | None = None) -> WorkerConfig:
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

    worker_id = _read(values, "WORKER_ID")
    if not worker_id:
        if is_production:
            raise ConfigError("WORKER_ID is required when APP_ENV=production")
        worker_id = "worker-1"
    if len(worker_id) > 128:
        raise ConfigError("WORKER_ID must be at most 128 characters")

    execution_lease_sec = _int(
        values, "EXECUTION_LEASE_SEC", 60, 5, 86_400
    )
    lease_heartbeat_sec = _int(
        values, "LEASE_HEARTBEAT_SEC", 20, 1, 86_400
    )
    if lease_heartbeat_sec >= execution_lease_sec:
        raise ConfigError(
            "LEASE_HEARTBEAT_SEC must be less than EXECUTION_LEASE_SEC"
        )

    for warning in warnings_list:
        warnings.warn(f"[config] {warning}", stacklevel=2)

    outbox_enabled = _bool(values, "OUTBOX_ENABLED", True)
    rabbitmq_queue_type = _choice(
        values, "RABBITMQ_QUEUE_TYPE", "classic", {"classic", "quorum"}
    )
    if rabbitmq_queue_type == "quorum" and not outbox_enabled:
        raise ConfigError(
            "RABBITMQ_QUEUE_TYPE=quorum requires OUTBOX_ENABLED=true"
        )

    return WorkerConfig(
        app_env=app_env,
        worker_id=worker_id,
        database_url=database_url,
        rabbitmq_url=rabbitmq_url,
        metrics_port=_int(values, "METRICS_PORT", 9100, 1, 65535),
        task_max_retries=_int(values, "TASK_MAX_RETRIES", 3, 0, 100),
        retry_jitter_mode=_choice(
            values, "RETRY_JITTER_MODE", "full", {"full", "equal", "none"}
        ),
        command_timeout_sec=_int(values, "COMMAND_TIMEOUT_SEC", 300, 1, 86_400),
        max_output_bytes=_int(
            values, "MAX_OUTPUT_BYTES", 1_048_576, 1_024, 104_857_600
        ),
        worker_concurrency=_int(values, "WORKER_CONCURRENCY", 1, 1, 8),
        db_connect_timeout_sec=_int(values, "DB_CONNECT_TIMEOUT_SEC", 5, 1, 60),
        heartbeat_interval_sec=_int(values, "HEARTBEAT_INTERVAL_SEC", 20, 1, 3_600),
        execution_lease_sec=execution_lease_sec,
        lease_heartbeat_sec=lease_heartbeat_sec,
        outbox_enabled=outbox_enabled,
        rabbitmq_queue_type=rabbitmq_queue_type,
        otel_enabled=_bool(values, "OTEL_ENABLED", False),
        otel_service_name=_read(values, "OTEL_SERVICE_NAME") or "worker",
        otel_exporter_endpoint=(
            _read(values, "OTEL_EXPORTER_OTLP_ENDPOINT")
            or "http://localhost:4318/v1/traces"
        ),
        otel_sampler_arg=_float(values, "OTEL_SAMPLER_ARG", 0.1, 0.0, 1.0),
    )
