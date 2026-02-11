import httpx
from pytest_httpx import HTTPXMock

from insider_alerts.config import Settings
from insider_alerts.notify.ntfy import NtfyNotifier


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
