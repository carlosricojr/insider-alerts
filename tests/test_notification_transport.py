from __future__ import annotations

import json
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pytest_httpx import HTTPXMock

from insider_alerts import cli
from insider_alerts.config import Settings
from insider_alerts.notify.ntfy import NtfyTransportEvent
from insider_alerts.research.notification_transport import (
    NotificationJournalConfig,
    NotificationJournalError,
    NotificationJournalNotActive,
    NotificationTransportJournal,
    activate_notification_journal,
    notification_journal_status,
    notification_transport_id,
)

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "docs" / "research" / "contracts" / "notification-transport-v1.json"
ACTIVATION = datetime(2026, 8, 28, 8, 0, tzinfo=UTC)
PACKET_ID = "0000320193-26-000001|0000320193|4"


def _config(tmp_path: Path, *, timeout_ms: int = 100) -> NotificationJournalConfig:
    research = tmp_path / "data" / "research"
    research.mkdir(parents=True)
    return NotificationJournalConfig(
        database=research / "notification_transport.db",
        research_root=research,
        policy_path=POLICY,
        runtime_git_commit="a" * 40,
        write_timeout_ms=timeout_ms,
    )


def _event(
    phase: str,
    *,
    attempt: int = 1,
    occurred_at: datetime = ACTIVATION,
) -> NtfyTransportEvent:
    common = {
        "attempt_number": attempt,
        "phase": phase,
        "occurred_at_utc": occurred_at,
        "request_body_sha256": "b" * 64,
        "route_sha256": "c" * 64,
    }
    if phase == "response_received":
        return NtfyTransportEvent(
            **common,  # type: ignore[arg-type]
            http_status=200,
            response_body_sha256="d" * 64,
            provider_message_id="abc_123",
            provider_message_time=1787904000,
        )
    if phase == "transport_failed":
        return NtfyTransportEvent(
            **common,  # type: ignore[arg-type]
            exception_class="ConnectError",
        )
    return NtfyTransportEvent(**common)  # type: ignore[arg-type]


def test_activation_is_immutable_and_does_not_send_or_append(tmp_path: Path) -> None:
    config = _config(tmp_path)

    result = activate_notification_journal(config, activated_at_utc=ACTIVATION)

    assert result["activated_at_utc"] == "2026-08-28T08:00:00.000000Z"
    status = notification_journal_status(config)
    assert status["valid"] is True
    assert status["events"] == 0
    with pytest.raises(NotificationJournalError, match="immutable"):
        activate_notification_journal(
            config, activated_at_utc=ACTIVATION + timedelta(microseconds=1)
        )


def test_append_requires_activation_and_ignores_preactivation_event(tmp_path: Path) -> None:
    config = _config(tmp_path)
    journal = NotificationTransportJournal(config)
    transport_id = notification_transport_id(PACKET_ID, "dispatch-1")

    with pytest.raises(NotificationJournalNotActive):
        journal.append(
            packet_id=PACKET_ID,
            transport_id=transport_id,
            event=_event("request_started"),
        )

    activate_notification_journal(config, activated_at_utc=ACTIVATION)
    assert (
        journal.append(
            packet_id=PACKET_ID,
            transport_id=transport_id,
            event=_event(
                "request_started", occurred_at=ACTIVATION - timedelta(microseconds=1)
            ),
        )
        is False
    )
    assert notification_journal_status(config)["events"] == 0


def test_append_only_attempt_is_content_bound_and_semantically_valid(tmp_path: Path) -> None:
    config = _config(tmp_path)
    activate_notification_journal(config, activated_at_utc=ACTIVATION)
    journal = NotificationTransportJournal(config)
    transport_id = notification_transport_id(PACKET_ID, "dispatch-1")

    assert journal.append(
        packet_id=PACKET_ID,
        transport_id=transport_id,
        event=_event("request_started"),
    )
    assert journal.append(
        packet_id=PACKET_ID,
        transport_id=transport_id,
        event=_event("response_received", occurred_at=ACTIVATION + timedelta(milliseconds=2)),
    )
    assert not journal.append(
        packet_id=PACKET_ID,
        transport_id=transport_id,
        event=_event("response_received", occurred_at=ACTIVATION + timedelta(milliseconds=2)),
    )

    status = notification_journal_status(config)
    assert status["valid"] is True
    assert status["events"] == 2
    assert status["phases"]["request_started"] == 1
    assert status["phases"]["response_received"] == 1
    assert status["unmatched_starts"] == 0
    with sqlite3.connect(config.database) as conn:
        records = [
            bytes(row[0]).decode("utf-8")
            for row in conn.execute(
                "SELECT record_json FROM notification_transport_events ORDER BY sequence"
            )
        ]
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute("UPDATE notification_transport_events SET phase='transport_failed'")
    serialized = "".join(records).lower()
    for prohibited in (
        "private-topic",
        "bearer",
        "secret-token",
        "secret body",
        "secret title",
        "https://",
    ):
        assert prohibited not in serialized
    parsed = json.loads(records[-1])
    assert set(parsed) == {
        "attempt_number",
        "contract_version",
        "event_id",
        "exception_class",
        "http_status",
        "occurred_at_utc",
        "packet_id",
        "phase",
        "policy_sha256",
        "provider_message_id",
        "provider_message_time",
        "request_body_sha256",
        "response_body_sha256",
        "route_sha256",
        "runtime_git_commit",
        "schema_version",
        "transport_id",
    }


def test_unmatched_start_is_explicitly_reported_as_crash_shaped_unknown(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    activate_notification_journal(config, activated_at_utc=ACTIVATION)
    journal = NotificationTransportJournal(config)
    assert journal.append(
        packet_id=PACKET_ID,
        transport_id=notification_transport_id(PACKET_ID, "dispatch-1"),
        event=_event("request_started"),
    )

    status = notification_journal_status(config)

    assert status["valid"] is True
    assert status["unmatched_starts"] == 1


def test_terminal_event_requires_a_start_and_only_one_terminal(tmp_path: Path) -> None:
    config = _config(tmp_path)
    activate_notification_journal(config, activated_at_utc=ACTIVATION)
    journal = NotificationTransportJournal(config)
    transport_id = notification_transport_id(PACKET_ID, "dispatch-1")

    with pytest.raises(sqlite3.IntegrityError, match="requires a start"):
        journal.append(
            packet_id=PACKET_ID,
            transport_id=transport_id,
            event=_event("transport_failed"),
        )
    assert journal.append(
        packet_id=PACKET_ID,
        transport_id=transport_id,
        event=_event("request_started"),
    )
    assert journal.append(
        packet_id=PACKET_ID,
        transport_id=transport_id,
        event=_event("transport_failed", occurred_at=ACTIVATION + timedelta(milliseconds=1)),
    )
    with pytest.raises(sqlite3.IntegrityError, match="terminal event"):
        journal.append(
            packet_id=PACKET_ID,
            transport_id=transport_id,
            event=_event(
                "response_received", occurred_at=ACTIVATION + timedelta(milliseconds=2)
            ),
        )


def test_locked_journal_append_obeys_small_latency_bound(tmp_path: Path) -> None:
    config = _config(tmp_path, timeout_ms=30)
    activate_notification_journal(config, activated_at_utc=ACTIVATION)
    journal = NotificationTransportJournal(config)
    lock = sqlite3.connect(config.database)
    lock.execute("BEGIN IMMEDIATE")
    started = time.monotonic()
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            journal.append(
                packet_id=PACKET_ID,
                transport_id=notification_transport_id(PACKET_ID, "dispatch-1"),
                event=_event("request_started"),
            )
    finally:
        elapsed = time.monotonic() - started
        lock.rollback()
        lock.close()
    assert elapsed < 0.5


def test_journal_path_must_remain_under_research_root(tmp_path: Path) -> None:
    config = _config(tmp_path)
    escaped = NotificationJournalConfig(
        database=tmp_path / "escaped.db",
        research_root=config.research_root,
        policy_path=config.policy_path,
        runtime_git_commit=config.runtime_git_commit,
    )

    with pytest.raises(NotificationJournalError, match="escaped"):
        NotificationTransportJournal(escaped)


def test_transport_identity_distinguishes_resends_of_the_same_packet() -> None:
    first = notification_transport_id(PACKET_ID, "dispatch-1")

    assert first == notification_transport_id(PACKET_ID, "dispatch-1")
    assert first != notification_transport_id(PACKET_ID, "dispatch-2")


def test_status_fails_closed_when_an_immutability_trigger_is_missing(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    activate_notification_journal(config, activated_at_utc=ACTIVATION)
    with sqlite3.connect(config.database) as conn:
        conn.execute("DROP TRIGGER notification_transport_events_no_update")

    status = notification_journal_status(config)

    assert status["valid"] is False
    assert status["reason"] == "notification_journal_schema_incomplete"
    assert status["integrity_errors"] == [
        "missing_trigger:notification_transport_events_no_update"
    ]


def test_activation_rejects_a_semantically_weakened_policy(tmp_path: Path) -> None:
    config = _config(tmp_path)
    altered_policy = json.loads(POLICY.read_text(encoding="utf-8"))
    altered_policy["delivery_behavior"]["changes_headers"] = True
    policy_path = tmp_path / "altered-policy.json"
    policy_path.write_text(json.dumps(altered_policy), encoding="utf-8")
    weakened = NotificationJournalConfig(
        database=config.database,
        research_root=config.research_root,
        policy_path=policy_path,
        runtime_git_commit=config.runtime_git_commit,
    )

    with pytest.raises(NotificationJournalError, match="reviewed contract"):
        activate_notification_journal(weakened, activated_at_utc=ACTIVATION)


def test_transport_module_is_order_and_trial_incapable() -> None:
    source = (ROOT / "src/insider_alerts/research/notification_transport.py").read_text(
        encoding="utf-8"
    )

    assert "ib_async" not in source
    assert "insider_alerts.research.trial" not in source
    assert "active evidence" not in source.lower()


def test_review_notification_records_one_complete_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    httpx_mock: HTTPXMock,
) -> None:
    config = _config(tmp_path)
    activate_notification_journal(
        config, activated_at_utc=ACTIVATION - timedelta(days=1)
    )
    monkeypatch.setattr(cli, "_notification_transport_config", lambda settings: config)
    monkeypatch.setattr(cli.secrets, "token_hex", lambda size: "dispatch-nonce")
    settings = Settings(
        NTFY_BASE_URL="https://ntfy.example.com",
        NTFY_TOPIC="private-topic",
        NTFY_RETRY_ATTEMPTS=1,
        NOTIFICATION_TRANSPORT_DB=str(config.database),
    )
    httpx_mock.add_response(
        method="POST",
        url="https://ntfy.example.com/private-topic",
        status_code=200,
        content=b'{"id":"provider_1","time":1787904000}',
    )

    cli._send_review_notification(
        settings,
        {"packet_id": PACKET_ID, "decision": "reject", "analyst": "operator"},
    )

    status = notification_journal_status(config)
    assert status["valid"] is True
    assert status["events"] == 2
    assert status["phases"] == {
        "request_started": 1,
        "response_received": 1,
        "transport_failed": 0,
    }
    assert status["unmatched_starts"] == 0
    with sqlite3.connect(config.database) as conn:
        transport_ids = {
            str(row[0])
            for row in conn.execute("SELECT transport_id FROM notification_transport_events")
        }
    assert transport_ids == {notification_transport_id(PACKET_ID, "dispatch-nonce")}


def test_observer_setup_failure_does_not_block_notification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    httpx_mock: HTTPXMock,
) -> None:
    fake_module = tmp_path / "src" / "insider_alerts" / "cli.py"
    monkeypatch.setattr(cli, "__file__", str(fake_module))

    def fail_config(settings: Settings) -> NotificationJournalConfig:
        raise NotificationJournalError("invalid journal")

    monkeypatch.setattr(cli, "_notification_transport_config", fail_config)
    dummy_database = tmp_path / "active.db"
    dummy_database.touch()
    settings = Settings(
        NTFY_BASE_URL="https://ntfy.example.com",
        NTFY_TOPIC="private-topic",
        NTFY_RETRY_ATTEMPTS=1,
        NOTIFICATION_TRANSPORT_DB=str(dummy_database),
    )
    httpx_mock.add_response(
        method="POST",
        url="https://ntfy.example.com/private-topic",
        status_code=200,
    )

    cli._send_review_notification(
        settings,
        {"packet_id": PACKET_ID, "decision": "reject", "analyst": "operator"},
    )

    assert len(httpx_mock.get_requests()) == 1
    error_log = tmp_path / "logs" / "notification-transport.err.log"
    assert error_log.read_text(encoding="utf-8").endswith(
        "notification transport capture isolated: NotificationJournalError\n"
    )


def test_inactive_journal_does_not_resolve_git_on_notification_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        NOTIFICATION_TRANSPORT_DB=str(tmp_path / "missing.db"),
    )

    def forbidden_git_resolution(repo_root: Path) -> str:
        raise AssertionError("git must not run before journal activation")

    monkeypatch.setattr(cli, "resolve_git_commit", forbidden_git_resolution)
    cli._notification_runtime_git_commit.cache_clear()

    observer, error_handler = cli._notification_transport_observer(
        settings,
        {"packet_id": PACKET_ID},
    )

    assert observer is None
    assert error_handler is None


def test_runtime_git_commit_is_resolved_once_per_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def resolve_once(repo_root: Path) -> str:
        nonlocal calls
        calls += 1
        return "a" * 40

    monkeypatch.setattr(cli, "resolve_git_commit", resolve_once)
    cli._notification_runtime_git_commit.cache_clear()
    repo_root = Path("C:/loaded-source")

    assert cli._notification_runtime_git_commit(repo_root) == "a" * 40
    assert cli._notification_runtime_git_commit(repo_root) == "a" * 40
    assert calls == 1
    cli._notification_runtime_git_commit.cache_clear()
