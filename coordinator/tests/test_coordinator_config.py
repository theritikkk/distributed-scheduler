import pytest

from internal.config import ConfigError, load_config


def _dev_env(**overrides: str) -> dict[str, str]:
    env = {
        "APP_ENV": "development",
        "DATABASE_URL": "postgresql://scheduler:scheduler_secret@localhost:5432/scheduler",
        "RABBITMQ_URL": "amqp://scheduler:scheduler_secret@localhost:5672",
    }
    env.update(overrides)
    return env


def test_loads_development_defaults() -> None:
    config = load_config(_dev_env(METRICS_PORT="9091"))

    assert config.app_env == "development"
    assert config.metrics_port == 9091
    assert config.reconcile_interval_sec == 120
    assert config.reconcile_interval_jitter_ratio == 0.2
    assert config.task_max_retries == 3
    assert config.scheduler_jitter_enabled is False
    assert config.reaper_enabled is True
    assert config.reaper_interval_sec == 15
    assert config.pending_retry_grace_sec == 300
    assert config.idempotency_cleanup_interval_sec == 3_600
    assert config.outbox_enabled is True
    assert config.outbox_poll_interval_sec == 1
    assert config.outbox_batch_size == 100
    assert config.outbox_max_attempts == 10
    assert config.scheduled_events_enabled is True
    assert config.scheduled_event_batch_size == 100
    assert config.rabbitmq_queue_type == "classic"


def test_uses_development_defaults_with_a_warning() -> None:
    with pytest.warns(UserWarning):
        config = load_config({"APP_ENV": "development"})

    assert config.database_url.startswith("postgresql://")
    assert config.rabbitmq_url.startswith("amqp://")


def test_requires_urls_in_production() -> None:
    with pytest.raises(ConfigError, match="DATABASE_URL"):
        load_config({"APP_ENV": "production"})


def test_accepts_valid_production_configuration() -> None:
    config = load_config(
        {
            "APP_ENV": "production",
            "DATABASE_URL": "postgresql://scheduler:secret@db:5432/scheduler",
            "RABBITMQ_URL": "amqp://scheduler:secret@rabbitmq:5672",
            "METRICS_PORT": "9090",
        }
    )

    assert config.is_production is True
    assert config.database_url.endswith("/scheduler")


def test_rejects_invalid_integer_values() -> None:
    with pytest.raises(ConfigError, match="integer"):
        load_config(_dev_env(METRICS_PORT="not-a-port"))

    with pytest.raises(ConfigError, match="between"):
        load_config(_dev_env(METRICS_PORT="70000"))


def test_parses_scheduling_jitter_configuration() -> None:
    config = load_config(
        _dev_env(
            SCHEDULER_JITTER_ENABLED="true",
            RECONCILE_INTERVAL_JITTER_RATIO="0.1",
        )
    )

    assert config.scheduler_jitter_enabled is True
    assert config.reconcile_interval_jitter_ratio == 0.1

    with pytest.raises(ConfigError, match="true or false"):
        load_config(_dev_env(SCHEDULER_JITTER_ENABLED="sometimes"))

    with pytest.raises(ConfigError, match="between"):
        load_config(_dev_env(RECONCILE_INTERVAL_JITTER_RATIO="0.9"))


def test_disables_reaper_for_rollback() -> None:
    config = load_config(_dev_env(REAPER_ENABLED="false"))

    assert config.reaper_enabled is False


def test_disables_outbox_for_direct_confirms_rollback() -> None:
    config = load_config(_dev_env(OUTBOX_ENABLED="false"))

    assert config.outbox_enabled is False


def test_quorum_mode_requires_outbox_and_db_scheduler() -> None:
    config = load_config(_dev_env(RABBITMQ_QUEUE_TYPE="quorum"))
    assert config.rabbitmq_queue_type == "quorum"

    with pytest.raises(ConfigError, match="SCHEDULED_EVENTS_ENABLED"):
        load_config(
            _dev_env(
                RABBITMQ_QUEUE_TYPE="quorum",
                SCHEDULED_EVENTS_ENABLED="false",
            )
        )
    with pytest.raises(ConfigError, match="OUTBOX_ENABLED"):
        load_config(
            _dev_env(
                RABBITMQ_QUEUE_TYPE="quorum",
                OUTBOX_ENABLED="false",
            )
        )


def test_rejects_outbox_backoff_max_below_base() -> None:
    with pytest.raises(ConfigError, match="OUTBOX_BACKOFF_MAX_SEC"):
        load_config(
            _dev_env(
                OUTBOX_BACKOFF_BASE_SEC="30",
                OUTBOX_BACKOFF_MAX_SEC="10",
            )
        )
