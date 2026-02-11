from collections import deque

import pytest
from pytest_httpx import HTTPXMock

from insider_alerts.config import Settings
from insider_alerts.sec.client import SecHttpClient


def test_sec_client_sends_required_headers(httpx_mock: HTTPXMock) -> None:
    settings = Settings(
        SEC_USER_AGENT="insider-alerts/0.2 (contact: sec-access@example.com)",
        SEC_RATE_LIMIT_PER_SECOND=10,
    )
    client = SecHttpClient(settings)

    httpx_mock.add_response(status_code=200, text="ok")
    client.get_text("https://www.sec.gov/test")

    req = httpx_mock.get_requests()[0]
    assert req.headers["User-Agent"] == settings.sec_user_agent
    assert req.headers["Accept-Encoding"] == "gzip, deflate"


def test_sec_client_retries_on_429(httpx_mock: HTTPXMock) -> None:
    settings = Settings(SEC_RETRY_ATTEMPTS=3, SEC_RATE_LIMIT_PER_SECOND=10)
    client = SecHttpClient(settings)

    httpx_mock.add_response(status_code=429, text="slow down")
    httpx_mock.add_response(status_code=200, text="ok")

    body = client.get_text("https://www.sec.gov/test")

    assert body == "ok"
    assert len(httpx_mock.get_requests()) == 2


def test_sec_client_enforces_rate_limit() -> None:
    settings = Settings(SEC_RATE_LIMIT_PER_SECOND=2)
    now_values = deque([0.0, 0.0, 0.1, 0.1])
    slept: list[float] = []

    def fake_now() -> float:
        if now_values:
            return now_values.popleft()
        return 0.1

    def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    client = SecHttpClient(settings, now_fn=fake_now, sleep_fn=fake_sleep)
    client._enforce_rate_limit()
    client._enforce_rate_limit()

    assert slept
    assert slept[0] == pytest.approx(0.5, abs=1e-6)
