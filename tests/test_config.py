from insider_alerts.config import get_settings


def test_config_defaults(monkeypatch) -> None:
    monkeypatch.delenv("NTFY_BASE_URL", raising=False)
    monkeypatch.delenv("NTFY_TOPIC", raising=False)
    monkeypatch.delenv("NTFY_TOKEN", raising=False)
    monkeypatch.delenv("NTFY_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("NTFY_RETRY_ATTEMPTS", raising=False)
    monkeypatch.delenv("NTFY_RETRY_MIN_SECONDS", raising=False)
    monkeypatch.delenv("NTFY_RETRY_MAX_SECONDS", raising=False)

    get_settings.cache_clear()
    settings = get_settings()

    assert settings.ntfy_base_url == "https://ntfy.sh"
    assert settings.ntfy_topic == "insider-alerts"
    assert settings.ntfy_token is None
    assert settings.ntfy_timeout_seconds == 10.0
    assert settings.ntfy_retry_attempts == 3
    assert settings.ntfy_retry_min_seconds == 0.5
    assert settings.ntfy_retry_max_seconds == 3.0
