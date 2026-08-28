from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pytest_httpx import HTTPXMock

from insider_alerts.config import Settings
from insider_alerts.notify.ntfy import NtfyNotificationError, NtfyNotifier, NtfyTransportEvent


def test_ntfy_send_headers_and_url(httpx_mock: HTTPXMock) -> None:
    settings = Settings(
        NTFY_BASE_URL="https://ntfy.example.com",
        NTFY_TOPIC="alerts",
        NTFY_TOKEN="secret-token",
        NTFY_RETRY_ATTEMPTS=1,
    )
    notifier = NtfyNotifier(settings)

    httpx_mock.add_response(method="POST", url="https://ntfy.example.com/alerts", status_code=200)

    notifier.send(
        title="Alert",
        message="Body",
        tags=["insider", "test"],
        priority=4,
        click="https://example.com",
        icon="https://example.com/icon.png",
        markdown=True,
    )

    request = httpx_mock.get_requests()[0]
    assert str(request.url) == "https://ntfy.example.com/alerts"
    assert request.headers["Title"] == "Alert"
    assert request.headers["Tags"] == "insider,test"
    assert request.headers["Priority"] == "4"
    assert request.headers["Click"] == "https://example.com"
    assert request.headers["Icon"] == "https://example.com/icon.png"
    assert request.headers["Authorization"] == "Bearer secret-token"
    assert request.headers["Markdown"] == "yes"
    assert request.content == b"Body"


def test_ntfy_send_retries_on_transport_error(monkeypatch) -> None:
    settings = Settings(
        NTFY_BASE_URL="https://ntfy.example.com",
        NTFY_TOPIC="alerts",
        NTFY_RETRY_ATTEMPTS=2,
        NTFY_RETRY_MIN_SECONDS=0,
        NTFY_RETRY_MAX_SECONDS=0,
    )
    notifier = NtfyNotifier(settings)

    call_count = {"value": 0}

    def fake_post(
        self: httpx.Client,
        url: str,
        content: bytes,
        headers: dict[str, str],
    ) -> httpx.Response:
        call_count["value"] += 1
        if call_count["value"] == 1:
            raise httpx.ConnectError("boom")
        return httpx.Response(
            status_code=200,
            request=httpx.Request("POST", url, content=content, headers=headers),
        )

    monkeypatch.setattr(httpx.Client, "post", fake_post)

    notifier.send(title="Retry", message="Body")

    assert call_count["value"] == 2


def test_ntfy_observer_records_existing_retries_and_provider_ack(monkeypatch) -> None:
    settings = Settings(
        NTFY_BASE_URL="https://ntfy.example.com",
        NTFY_TOPIC="private-topic",
        NTFY_RETRY_ATTEMPTS=2,
        NTFY_RETRY_MIN_SECONDS=0,
        NTFY_RETRY_MAX_SECONDS=0,
    )
    base = datetime(2026, 8, 28, 8, 0, tzinfo=UTC)
    times = iter(base + timedelta(milliseconds=index) for index in range(4))
    notifier = NtfyNotifier(settings, now_fn=lambda: next(times))
    events = []
    calls = 0

    def fake_post(
        self: httpx.Client,
        url: str,
        content: bytes,
        headers: dict[str, str],
    ) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("contains private-topic and secret-token")
        return httpx.Response(
            status_code=200,
            content=b'{"id":"abc_123","time":1787904000,"topic":"private-topic"}',
            request=httpx.Request("POST", url, content=content, headers=headers),
        )

    monkeypatch.setattr(httpx.Client, "post", fake_post)

    notifier.send(
        title="Secret title",
        message="Secret body",
        observer=events.append,
    )

    assert [event.phase for event in events] == [
        "request_started",
        "transport_failed",
        "request_started",
        "response_received",
    ]
    assert [event.attempt_number for event in events] == [1, 1, 2, 2]
    assert events[1].exception_class == "ConnectError"
    assert events[-1].provider_message_id == "abc_123"
    assert events[-1].provider_message_time == 1787904000
    rendered = repr(events)
    assert "private-topic" not in rendered
    assert "secret-token" not in rendered
    assert "Secret body" not in rendered


def test_ntfy_observer_failure_never_blocks_delivery(httpx_mock: HTTPXMock) -> None:
    settings = Settings(
        NTFY_BASE_URL="https://ntfy.example.com",
        NTFY_TOPIC="alerts",
        NTFY_RETRY_ATTEMPTS=1,
    )
    notifier = NtfyNotifier(settings)
    httpx_mock.add_response(
        method="POST",
        url="https://ntfy.example.com/alerts",
        status_code=200,
        content=b'{"id":"accepted","time":1787904000}',
    )
    errors: list[str] = []

    def broken_observer(event: NtfyTransportEvent) -> None:
        raise OSError("journal locked")

    notifier.send(
        title="Alert",
        message="Body",
        observer=broken_observer,
        observer_error_handler=lambda exc: errors.append(type(exc).__name__),
    )

    assert errors == ["OSError", "OSError"]
    assert len(httpx_mock.get_requests()) == 1


def test_ntfy_http_failure_emits_response_terminal_not_transport_failure(
    httpx_mock: HTTPXMock,
) -> None:
    settings = Settings(
        NTFY_BASE_URL="https://ntfy.example.com",
        NTFY_TOPIC="alerts",
        NTFY_RETRY_ATTEMPTS=1,
    )
    notifier = NtfyNotifier(settings)
    httpx_mock.add_response(
        method="POST", url="https://ntfy.example.com/alerts", status_code=503
    )
    events = []

    with pytest.raises(NtfyNotificationError, match="NTFY notification failed"):
        notifier.send(title="Alert", message="Body", observer=events.append)

    assert [event.phase for event in events] == ["request_started", "response_received"]
    assert events[-1].http_status == 503
