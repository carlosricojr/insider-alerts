import pytest
from pydantic import ValidationError

from insider_alerts.config import Settings


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


def test_config_bounds_notification_duration_below_minimum_watchdog_staleness() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, NTFY_TIMEOUT_SECONDS=21)
    with pytest.raises(ValidationError):
        Settings(_env_file=None, NTFY_RETRY_ATTEMPTS=6)
    with pytest.raises(ValidationError):
        Settings(_env_file=None, NTFY_RETRY_MAX_SECONDS=6)
    with pytest.raises(ValidationError, match="cannot exceed"):
        Settings(
            _env_file=None,
            NTFY_RETRY_MIN_SECONDS=4,
            NTFY_RETRY_MAX_SECONDS=3,
        )


@pytest.mark.parametrize(
    ("override", "value"),
    [
        ("SEC_TIMEOUT_SECONDS", 21),
        ("SEC_RATE_LIMIT_PER_SECOND", 0.9),
        ("SEC_RETRY_ATTEMPTS", 5),
        ("SEC_RETRY_MAX_SECONDS", 4),
        ("MARKET_DATA_TIMEOUT_SECONDS", 11),
        ("MARKET_DATA_RATE_LIMIT_PER_SECOND", 0.9),
        ("MARKET_DATA_RETRY_ATTEMPTS", 4),
        ("MARKET_DATA_RETRY_MAX_SECONDS", 4),
    ],
)
def test_config_rejects_network_windows_outside_watchdog_budget(
    override: str,
    value: float,
) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{override: value})


def test_maximum_network_windows_fit_below_five_minute_watchdog_floor() -> None:
    settings = Settings(
        _env_file=None,
        SEC_TIMEOUT_SECONDS=20,
        SEC_RATE_LIMIT_PER_SECOND=1,
        SEC_RETRY_ATTEMPTS=4,
        SEC_RETRY_MIN_SECONDS=3,
        SEC_RETRY_MAX_SECONDS=3,
        MARKET_DATA_TIMEOUT_SECONDS=10,
        MARKET_DATA_RATE_LIMIT_PER_SECOND=1,
        MARKET_DATA_RETRY_ATTEMPTS=3,
        MARKET_DATA_RETRY_MIN_SECONDS=3,
        MARKET_DATA_RETRY_MAX_SECONDS=3,
    )
    sec_window = settings.sec_retry_attempts * (
        settings.sec_timeout_seconds + 1 / settings.sec_rate_limit_per_second
    ) + (settings.sec_retry_attempts - 1) * settings.sec_retry_max_seconds
    ib_window = settings.market_data_retry_attempts * (
        20 + 1 / settings.market_data_rate_limit_per_second
    ) + (settings.market_data_retry_attempts - 1) * settings.market_data_retry_max_seconds
    yahoo_window = settings.market_data_retry_attempts * (
        settings.market_data_timeout_seconds
        + 1 / settings.market_data_rate_limit_per_second
    ) + (settings.market_data_retry_attempts - 1) * settings.market_data_retry_max_seconds

    assert sec_window + ib_window + yahoo_window <= 230


def test_config_rejects_inverted_sec_retry_bounds() -> None:
    with pytest.raises(ValidationError, match="cannot exceed"):
        Settings(
            _env_file=None,
            SEC_RETRY_MIN_SECONDS=2,
            SEC_RETRY_MAX_SECONDS=1,
        )
