from typer.testing import CliRunner

from insider_alerts.cli import app
from insider_alerts.notify.ntfy import NtfyNotificationError


def test_notify_test_success(monkeypatch) -> None:
    runner = CliRunner()

    def fake_send(self, **kwargs) -> None:  # noqa: ANN001, ARG001
        return None

    monkeypatch.setattr("insider_alerts.notify.ntfy.NtfyNotifier.send", fake_send)

    result = runner.invoke(app, ["notify", "test"])

    assert result.exit_code == 0
    assert "Notification sent." in result.stdout


def test_notify_test_failure(monkeypatch) -> None:
    runner = CliRunner()

    def fake_send(self, **kwargs) -> None:  # noqa: ANN001, ARG001
        raise NtfyNotificationError("boom")

    monkeypatch.setattr("insider_alerts.notify.ntfy.NtfyNotifier.send", fake_send)

    result = runner.invoke(app, ["notify", "test"])

    assert result.exit_code == 1
    assert "Notification failed:" in result.stderr
