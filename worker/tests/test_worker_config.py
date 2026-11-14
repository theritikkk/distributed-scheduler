import pytest

from config import ConfigError, load_config


def _dev_env(**overrides: str) -> dict[str, str]:
    env = {
        "APP_ENV": "development",
        "WORKER_ID": "worker-test",
        "DATABASE_URL": "postgresql://scheduler:scheduler_secret@localhost:5432/scheduler",
        "RABBITMQ_URL": "amqp://scheduler:scheduler_secret@localhost:5672",
    }
    env.update(overrides)
    return env


def test_loads_development_defaults() -> None:
    config = load_config(_dev_env(METRICS_PORT="9101"))

    assert config.worker_id == "worker-test"
    assert config.metrics_port == 9101
    assert config.task_max_retries == 3
    assert config.retry_jitter_mode == "full"
    assert config.command_timeout_sec == 300
    assert config.max_output_bytes == 1_048_576
    assert config.worker_concurrency == 1
    assert config.execution_lease_sec == 60
    assert config.lease_heartbeat_sec == 20
    assert config.outbox_enabled is True
    assert config.rabbitmq_queue_type == "classic"


def test_uses_development_defaults_with_a_warning() -> None:
    with pytest.warns(UserWarning):
        config = load_config({"APP_ENV": "development"})

    assert config.worker_id == "worker-1"
    assert config.database_url.startswith("postgresql://")


def test_requires_urls_in_production() -> None:
    with pytest.raises(ConfigError, match="DATABASE_URL"):
        load_config({"APP_ENV": "production"})


def test_requires_explicit_worker_id_in_production() -> None:
    with pytest.raises(ConfigError, match="WORKER_ID is required"):
        load_config(
            {
                "APP_ENV": "production",
                "DATABASE_URL": "postgresql://scheduler:secret@db:5432/scheduler",
                "RABBITMQ_URL": "amqp://scheduler:secret@rabbitmq:5672",
            }
        )


def test_accepts_valid_production_configuration() -> None:
    config = load_config(
        {
            "APP_ENV": "production",
            "WORKER_ID": "worker-1",
            "DATABASE_URL": "postgresql://scheduler:secret@db:5432/scheduler",
            "RABBITMQ_URL": "amqp://scheduler:secret@rabbitmq:5672",
        }
    )

    assert config.is_production is True
    assert config.worker_id == "worker-1"


def test_rejects_invalid_worker_values() -> None:
    with pytest.raises(ConfigError, match="integer"):
        load_config(_dev_env(WORKER_CONCURRENCY="many"))

    with pytest.raises(ConfigError, match="between"):
        load_config(_dev_env(WORKER_CONCURRENCY="0"))

    with pytest.raises(ConfigError, match="between"):
        load_config(_dev_env(WORKER_CONCURRENCY="9"))

    with pytest.raises(ConfigError, match="between"):
        load_config(_dev_env(MAX_OUTPUT_BYTES="10"))

    with pytest.raises(ConfigError, match="at most 128"):
        load_config(_dev_env(WORKER_ID="x" * 129))

    with pytest.raises(ConfigError, match="RETRY_JITTER_MODE"):
        load_config(_dev_env(RETRY_JITTER_MODE="sometimes"))


def test_rejects_heartbeat_not_shorter_than_lease() -> None:
    with pytest.raises(ConfigError, match="LEASE_HEARTBEAT_SEC"):
        load_config(
            _dev_env(
                EXECUTION_LEASE_SEC="10",
                LEASE_HEARTBEAT_SEC="10",
            )
        )


def test_disables_worker_outbox_for_direct_confirms_rollback() -> None:
    config = load_config(_dev_env(OUTBOX_ENABLED="false"))

    assert config.outbox_enabled is False


def test_rejects_invalid_outbox_boolean() -> None:
    with pytest.raises(ConfigError, match="true or false"):
        load_config(_dev_env(OUTBOX_ENABLED="sometimes"))


def test_quorum_mode_requires_outbox() -> None:
    config = load_config(_dev_env(RABBITMQ_QUEUE_TYPE="quorum"))
    assert config.rabbitmq_queue_type == "quorum"

    with pytest.raises(ConfigError, match="OUTBOX_ENABLED"):
        load_config(
            _dev_env(
                RABBITMQ_QUEUE_TYPE="quorum",
                OUTBOX_ENABLED="false",
            )
        )
