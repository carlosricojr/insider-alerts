from insider_alerts.config import Settings


def test_config_defaults(monkeypatch) -> None:
    monkeypatch.delenv("NTFY_BASE_URL", raising=False)
    monkeypatch.delenv("NTFY_TOPIC", raising=False)
    monkeypatch.delenv("NTFY_TOKEN", raising=False)
    monkeypatch.delenv("NTFY_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("NTFY_RETRY_ATTEMPTS", raising=False)
    monkeypatch.delenv("NTFY_RETRY_MIN_SECONDS", raising=False)
    monkeypatch.delenv("NTFY_RETRY_MAX_SECONDS", raising=False)
    monkeypatch.delenv("SEC_RSS_URL", raising=False)
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    monkeypatch.delenv("SEC_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("SEC_RATE_LIMIT_PER_SECOND", raising=False)
    monkeypatch.delenv("MARKET_CONTEXT_ENABLED", raising=False)
    monkeypatch.delenv("MARKET_DATA_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("MARKET_EARNINGS_SHOCK_DROP_THRESHOLD", raising=False)
    monkeypatch.delenv("DATABASE_PATH", raising=False)

    settings = Settings(_env_file=None)

    assert settings.ntfy_base_url == "https://ntfy.sh"
    assert settings.ntfy_topic == "insider-alerts"
    assert settings.ntfy_token is None
    assert settings.ntfy_timeout_seconds == 10.0
    assert settings.ntfy_retry_attempts == 3
    assert settings.ntfy_retry_min_seconds == 0.5
    assert settings.ntfy_retry_max_seconds == 3.0
    assert settings.sec_rate_limit_per_second == 5.0
    assert settings.market_context_enabled is False
    assert settings.market_data_timeout_seconds == 10.0
    assert settings.market_earnings_shock_drop_threshold == 0.08
    assert settings.database_path == "data/insider_alerts.db"
