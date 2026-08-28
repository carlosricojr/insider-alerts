import pytest
from pydantic import ValidationError

from insider_alerts.config import Settings
from insider_alerts.execution.autopilot_watchdog import (
    autopilot_runtime_budget,
    validate_stale_threshold,
)
from insider_alerts.research.history_worker import _parser as history_worker_parser


def test_config_defaults(monkeypatch) -> None:
    monkeypatch.delenv("NTFY_BASE_URL", raising=False)
    monkeypatch.delenv("NTFY_TOPIC", raising=False)
    monkeypatch.delenv("NTFY_TOKEN", raising=False)
    monkeypatch.delenv("NTFY_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("NTFY_RETRY_ATTEMPTS", raising=False)
    monkeypatch.delenv("NTFY_RETRY_MIN_SECONDS", raising=False)
    monkeypatch.delenv("NTFY_RETRY_MAX_SECONDS", raising=False)
    monkeypatch.delenv("NOTIFICATION_TRANSPORT_DB", raising=False)
    monkeypatch.delenv("NOTIFICATION_TRANSPORT_POLICY_PATH", raising=False)
    monkeypatch.delenv("SEC_RSS_URL", raising=False)
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    monkeypatch.delenv("SEC_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("SEC_RATE_LIMIT_PER_SECOND", raising=False)
    monkeypatch.delenv("MARKET_CONTEXT_ENABLED", raising=False)
    monkeypatch.delenv("MARKET_DATA_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("MARKET_DATA_RATE_LIMIT_PER_SECOND", raising=False)
    monkeypatch.delenv("MARKET_DATA_RETRY_ATTEMPTS", raising=False)
    monkeypatch.delenv("MARKET_DATA_RETRY_MIN_SECONDS", raising=False)
    monkeypatch.delenv("MARKET_DATA_RETRY_MAX_SECONDS", raising=False)
    monkeypatch.delenv("MARKET_EARNINGS_SHOCK_DROP_THRESHOLD", raising=False)
    monkeypatch.delenv("IB_GATEWAY_HOST", raising=False)
    monkeypatch.delenv("IB_GATEWAY_PORT", raising=False)
    monkeypatch.delenv("INSIDER_IB_CLIENT_ID", raising=False)
    monkeypatch.delenv("DATABASE_PATH", raising=False)

    settings = Settings(_env_file=None)

    assert settings.ntfy_base_url == "https://ntfy.sh"
    assert settings.ntfy_topic == "insider-alerts"
    assert settings.ntfy_token is None
    assert settings.ntfy_timeout_seconds == 10.0
    assert settings.ntfy_retry_attempts == 3
    assert settings.ntfy_retry_min_seconds == 0.5
    assert settings.ntfy_retry_max_seconds == 3.0
    assert settings.notification_transport_db == "data/research/notification_transport.db"
    assert settings.notification_transport_policy_path == (
        "docs/research/contracts/notification-transport-v1.json"
    )
    assert settings.sec_rate_limit_per_second == 5.0
    assert settings.market_context_enabled is False
    assert settings.market_data_timeout_seconds == 10.0
    assert settings.market_data_rate_limit_per_second == 1.0
    assert settings.market_data_retry_attempts == 3
    assert settings.market_data_retry_min_seconds == 0.5
    assert settings.market_data_retry_max_seconds == 3.0
    assert settings.market_earnings_shock_drop_threshold == 0.08
    assert settings.ib_gateway_host == "127.0.0.1"
    assert settings.ib_gateway_port == 4001
    assert settings.insider_ib_client_id == 171
    assert settings.database_path == "data/insider_alerts.db"


def test_config_loads_ib_gateway_from_env_file(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("IB_GATEWAY_HOST", raising=False)
    monkeypatch.delenv("IB_GATEWAY_PORT", raising=False)
    monkeypatch.delenv("INSIDER_IB_CLIENT_ID", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "IB_GATEWAY_HOST=custom-gateway\n"
        "IB_GATEWAY_PORT=4999\n"
        "INSIDER_IB_CLIENT_ID=222\n",
        encoding="utf-8",
    )

    settings = Settings(_env_file=env_file)

    assert settings.ib_gateway_host == "custom-gateway"
    assert settings.ib_gateway_port == 4999
    assert settings.insider_ib_client_id == 222


def test_config_rejects_inverted_market_data_retry_bounds() -> None:
    with pytest.raises(ValidationError, match="cannot exceed"):
        Settings(
            _env_file=None,
            MARKET_DATA_RETRY_MIN_SECONDS=2.0,
            MARKET_DATA_RETRY_MAX_SECONDS=1.0,
        )


def test_config_preserves_notification_compatibility_outside_autopilot() -> None:
    settings = Settings(
        _env_file=None,
        NTFY_TIMEOUT_SECONDS=21,
        NTFY_RETRY_ATTEMPTS=6,
        NTFY_RETRY_MAX_SECONDS=6,
    )
    assert settings.ntfy_timeout_seconds == 21
    assert settings.ntfy_retry_attempts == 6
    assert settings.ntfy_retry_max_seconds == 6
    with pytest.raises(ValidationError, match="cannot exceed"):
        Settings(
            _env_file=None,
            NTFY_RETRY_MIN_SECONDS=4,
            NTFY_RETRY_MAX_SECONDS=3,
        )


def test_history_worker_timeout_remains_valid_outside_watchdog_runtime() -> None:
    timeout = history_worker_parser().parse_args([]).timeout
    assert timeout == 120
    assert Settings(_env_file=None, SEC_TIMEOUT_SECONDS=timeout).sec_timeout_seconds == 120


def test_default_network_windows_fit_five_minute_watchdog_floor() -> None:
    settings = Settings(_env_file=None)
    budget = autopilot_runtime_budget(settings=settings, quant_timeout_seconds=120)

    assert float(budget["maximum_stage_seconds"]) <= 230
    assert budget["required_stale_seconds"] == 300
    validate_stale_threshold(
        settings=settings,
        quant_timeout_seconds=120,
        stale_seconds=300,
    )


def test_long_network_settings_require_a_larger_autopilot_stale_threshold() -> None:
    settings = Settings(_env_file=None, SEC_TIMEOUT_SECONDS=120)
    budget = autopilot_runtime_budget(settings=settings, quant_timeout_seconds=120)

    assert int(budget["required_stale_seconds"]) > 300
    with pytest.raises(ValueError, match="heartbeat stale threshold"):
        validate_stale_threshold(
            settings=settings,
            quant_timeout_seconds=120,
            stale_seconds=300,
        )


def test_config_rejects_inverted_sec_retry_bounds() -> None:
    with pytest.raises(ValidationError, match="cannot exceed"):
        Settings(
            _env_file=None,
            SEC_RETRY_MIN_SECONDS=2,
            SEC_RETRY_MAX_SECONDS=1,
        )
