from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from insider_alerts.backtest.models import DailyBar
from insider_alerts.backtest.signal_study import DeliveredSignal
from insider_alerts.execution.canary import (
    ARM_PHRASE,
    AccountSnapshot,
    BrokerOrder,
    CanaryConfig,
    CanaryRunner,
    CanaryStore,
    CommissionPreview,
    broker_token,
    deterministic_rank,
    eligibility,
    entry_session,
    planned_quantity,
    poll_delay_seconds,
    runtime_source_fingerprint,
    status_report,
)


def _signal(signal_at: datetime) -> DeliveredSignal:
    return DeliveredSignal(
        packet_id="packet-001",
        accession_number="0001-26-000001",
        cik="1",
        symbol="TEST",
        filed_at=signal_at - timedelta(minutes=5),
        signal_at=signal_at,
        score=0.9,
        rationale={},
    )


def _bars(end: date, *, close: float = 10.0, volume: float = 100_000.0) -> list[DailyBar]:
    days: list[date] = []
    cursor = end
    while len(days) < 60:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor -= timedelta(days=1)
    return [
        DailyBar("TEST", day, close, close * 1.02, close * 0.98, close, volume)
        for day in sorted(days)
    ]


def test_runtime_source_fingerprint_detects_source_changes(tmp_path: Path) -> None:
    source = tmp_path / "module.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    before = runtime_source_fingerprint(tmp_path)

    source.write_text("VALUE = 2\n", encoding="utf-8")

    assert runtime_source_fingerprint(tmp_path) != before


def test_status_report_exposes_current_runtime_revision(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger.db"
    store = CanaryStore(str(ledger))
    now = datetime.now(UTC)
    fingerprint = runtime_source_fingerprint()
    store.set_metadata(
        {
            "runtime_started_utc": now.isoformat(),
            "runtime_source_fingerprint": fingerprint,
            "last_cycle_success_utc": now.isoformat(),
        },
        now=now,
    )

    report = status_report(str(ledger))

    assert report["runtime_source_fingerprint"] == fingerprint
    assert report["current_source_fingerprint"] == fingerprint
    assert report["source_revision_current"] is True


class FakeBroker:
    def __init__(self, sessions: list[date], bars: list[DailyBar], commission: float) -> None:
        self.session_values = sessions
        self.bar_values = bars
        self.commission = commission
        self.connected: list[bool] = []
        self.disconnected = 0
        self.submitted: list[tuple[str, int, str]] = []
        self.protected: list[tuple[str, int, float, float, str]] = []
        self.timed_exits: list[tuple[str, int, str]] = []
        self.market_exits: list[tuple[str, int, str]] = []
        self.cancelled_orders: list[int] = []
        self.order_values: list[BrokerOrder] = []
        self.snapshot = AccountSnapshot("ACCOUNT", 493.5, 493.5, 493.5, {}, 0)

    async def connect(self, *, readonly: bool) -> None:
        self.connected.append(readonly)

    def disconnect(self) -> None:
        self.disconnected += 1

    async def sessions(self, *, around: datetime, count: int = 90) -> list[date]:
        return self.session_values

    async def daily_bars(self, symbol: str, *, duration: str = "6 M") -> list[DailyBar]:
        return [
            DailyBar(symbol, bar.trade_date, bar.open, bar.high, bar.low, bar.close, bar.volume)
            for bar in self.bar_values
        ]

    async def account_snapshot(self) -> AccountSnapshot:
        return self.snapshot

    async def preview_entry(self, symbol: str, quantity: int) -> CommissionPreview:
        return CommissionPreview(self.commission, "USD")

    async def submit_market_on_open(self, symbol: str, quantity: int, order_ref: str) -> int:
        self.submitted.append((symbol, quantity, order_ref))
        return 101

    async def submit_protective_oca(
        self,
        symbol: str,
        quantity: int,
        *,
        stop_price: float,
        target_price: float,
        oca_group: str,
        order_ref_prefix: str,
    ) -> tuple[int, int]:
        self.protected.append((symbol, quantity, stop_price, target_price, oca_group))
        return 201, 202

    async def submit_market_on_close(
        self,
        symbol: str,
        quantity: int,
        *,
        oca_group: str,
        order_ref: str,
    ) -> int:
        self.timed_exits.append((symbol, quantity, oca_group))
        return 203

    async def submit_market_exit(
        self,
        symbol: str,
        quantity: int,
        *,
        oca_group: str,
        order_ref: str,
    ) -> int:
        self.market_exits.append((symbol, quantity, oca_group))
        return 204

    async def cancel_order(self, order_id: int) -> None:
        self.cancelled_orders.append(order_id)

    async def orders(self) -> list[BrokerOrder]:
        return self.order_values


def test_entry_session_does_not_chase_after_submission_cutoff() -> None:
    monday = date(2026, 1, 5)
    sessions = [monday, monday + timedelta(days=1)]
    before_open = datetime(2026, 1, 5, 12, 0, tzinfo=UTC)  # 07:00 ET
    assert entry_session(before_open, sessions, now=before_open) == monday
    after_cutoff = datetime(2026, 1, 5, 14, 25, tzinfo=UTC)  # 09:25 ET
    assert entry_session(before_open, sessions, now=after_cutoff) == monday + timedelta(days=1)


def test_polling_accelerates_only_around_weekday_open() -> None:
    config = CanaryConfig(source_db="unused", poll_seconds=15)
    assert poll_delay_seconds(config, datetime(2026, 1, 5, 14, 20, tzinfo=UTC)) == 2
    assert poll_delay_seconds(config, datetime(2026, 1, 5, 15, 0, tzinfo=UTC)) == 15
    assert poll_delay_seconds(config, datetime(2026, 1, 4, 14, 20, tzinfo=UTC)) == 15


def test_eligibility_and_rank_are_frozen_and_deterministic() -> None:
    signal_at = datetime(2026, 1, 5, 12, 0, tzinfo=UTC)
    signal = _signal(signal_at)
    config = CanaryConfig(source_db="unused")
    ok, reason, prior_close, median_dv = eligibility(config, signal, _bars(date(2026, 1, 2)))
    assert ok is True
    assert reason == "eligible_E07_F00"
    assert prior_close == 10.0
    assert median_dv == 1_000_000.0
    assert planned_quantity(config, prior_close) == 20
    assert deterministic_rank(config, signal, date(2026, 1, 5)) == deterministic_rank(
        config, signal, date(2026, 1, 5)
    )


def test_fixed_commission_preview_rejects_live_entry(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)  # 09:18:30 ET
    signal = _signal(now - timedelta(minutes=30))
    monkeypatch.setattr(
        "insider_alerts.execution.canary.load_delivered_signals",
        lambda *args, **kwargs: [signal],
    )
    config = CanaryConfig(
        source_db=str(tmp_path / "source.db"),
        ledger_db=str(tmp_path / "ledger.db"),
        live_requested=True,
        arm_phrase=ARM_PHRASE,
    )
    broker = FakeBroker(
        [date(2026, 1, 5) + timedelta(days=index) for index in range(15)],
        _bars(date(2026, 1, 2)),
        commission=1.0,
    )
    runner = CanaryRunner(config, broker)
    runner.store.activation(now - timedelta(hours=1))
    result = asyncio.run(runner.cycle(now))
    assert result.live_submitted == 0
    assert broker.submitted == []
    assert runner.store.rows()[0]["live_state"] == "preflight_rejected"


def test_invalid_commission_preview_rejected_in_strict_mode(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    signal = _signal(now - timedelta(minutes=30))
    monkeypatch.setattr(
        "insider_alerts.execution.canary.load_delivered_signals",
        lambda *args, **kwargs: [signal],
    )
    config = CanaryConfig(
        source_db=str(tmp_path / "source.db"),
        ledger_db=str(tmp_path / "ledger.db"),
        live_requested=True,
        arm_phrase=ARM_PHRASE,
        invalid_commission_handling="reject",
    )
    broker = FakeBroker(
        [date(2026, 1, 5) + timedelta(days=index) for index in range(15)],
        _bars(date(2026, 1, 2)),
        commission=0.35,
    )

    async def invalid_preview(symbol: str, quantity: int) -> CommissionPreview:
        del symbol, quantity
        return CommissionPreview(float("nan"), "USD", commission_valid=False)

    monkeypatch.setattr(broker, "preview_entry", invalid_preview)
    runner = CanaryRunner(config, broker)
    runner.store.activation(now - timedelta(hours=1))

    result = asyncio.run(runner.cycle(now))
    assert result.live_submitted == 0
    assert broker.submitted == []
    assert runner.store.rows()[0]["live_state"] == "preflight_rejected"


def test_invalid_commission_preview_rejected_by_default(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    signal = _signal(now - timedelta(minutes=30))
    monkeypatch.setattr(
        "insider_alerts.execution.canary.load_delivered_signals",
        lambda *args, **kwargs: [signal],
    )
    config = CanaryConfig(
        source_db=str(tmp_path / "source.db"),
        ledger_db=str(tmp_path / "ledger.db"),
        live_requested=True,
        arm_phrase=ARM_PHRASE,
    )
    broker = FakeBroker(
        [date(2026, 1, 5) + timedelta(days=index) for index in range(15)],
        _bars(date(2026, 1, 2)),
        commission=0.35,
    )

    async def invalid_preview(symbol: str, quantity: int) -> CommissionPreview:
        del symbol, quantity
        return CommissionPreview(
            float("nan"),
            "USD",
            commission_valid=False,
            estimate_source="unavailable",
        )

    monkeypatch.setattr(broker, "preview_entry", invalid_preview)
    runner = CanaryRunner(config, broker)
    runner.store.activation(now - timedelta(hours=1))

    result = asyncio.run(runner.cycle(now))
    assert result.live_submitted == 0
    assert broker.submitted == []
    assert runner.store.rows()[0]["live_state"] == "preflight_rejected"


def test_invalid_commission_handling_requires_known_mode() -> None:
    with pytest.raises(ValueError, match="invalid_commission_handling"):
        CanaryConfig(
            source_db="unused",
            invalid_commission_handling="please_block",
        )


def test_invalid_commission_preview_falls_back_to_cap_when_enabled(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    signal = _signal(now - timedelta(minutes=30))
    monkeypatch.setattr(
        "insider_alerts.execution.canary.load_delivered_signals",
        lambda *args, **kwargs: [signal],
    )
    config = CanaryConfig(
        source_db=str(tmp_path / "source.db"),
        ledger_db=str(tmp_path / "ledger.db"),
        live_requested=True,
        arm_phrase=ARM_PHRASE,
        invalid_commission_handling="fallback_to_cap",
    )
    broker = FakeBroker(
        [date(2026, 1, 5) + timedelta(days=index) for index in range(15)],
        _bars(date(2026, 1, 2)),
        commission=0.35,
    )

    async def invalid_preview(symbol: str, quantity: int) -> CommissionPreview:
        del symbol, quantity
        return CommissionPreview(float("nan"), "", commission_valid=False)

    monkeypatch.setattr(broker, "preview_entry", invalid_preview)
    runner = CanaryRunner(config, broker)
    runner.store.activation(now - timedelta(hours=1))

    result = asyncio.run(runner.cycle(now))
    assert result.live_submitted == 1
    assert broker.submitted == [("TEST", 20, f"IA-E07-{broker_token('packet-001')}-ENTRY")]


def test_invalid_commission_preview_warning_blocks_even_in_fallback_mode(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    signal = _signal(now - timedelta(minutes=30))
    monkeypatch.setattr(
        "insider_alerts.execution.canary.load_delivered_signals",
        lambda *args, **kwargs: [signal],
    )
    config = CanaryConfig(
        source_db=str(tmp_path / "source.db"),
        ledger_db=str(tmp_path / "ledger.db"),
        live_requested=True,
        arm_phrase=ARM_PHRASE,
        invalid_commission_handling="fallback_to_cap",
    )
    broker = FakeBroker(
        [date(2026, 1, 5) + timedelta(days=index) for index in range(15)],
        _bars(date(2026, 1, 2)),
        commission=0.35,
    )

    async def invalid_preview_with_warning(symbol: str, quantity: int) -> CommissionPreview:
        del symbol, quantity
        return CommissionPreview(
            float("nan"),
            "",
            "order would trigger margin warning",
            commission_valid=False,
        )

    monkeypatch.setattr(broker, "preview_entry", invalid_preview_with_warning)
    runner = CanaryRunner(config, broker)
    runner.store.activation(now - timedelta(hours=1))

    result = asyncio.run(runner.cycle(now))
    assert result.live_submitted == 0
    assert broker.submitted == []
    assert runner.store.rows()[0]["live_state"] == "preflight_rejected"


def test_non_finite_account_funds_block_live_entry(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    signal = _signal(now - timedelta(minutes=30))
    monkeypatch.setattr(
        "insider_alerts.execution.canary.load_delivered_signals",
        lambda *args, **kwargs: [signal],
    )
    config = CanaryConfig(
        source_db=str(tmp_path / "source.db"),
        ledger_db=str(tmp_path / "ledger.db"),
        live_requested=True,
        arm_phrase=ARM_PHRASE,
    )
    broker = FakeBroker(
        [date(2026, 1, 5) + timedelta(days=index) for index in range(15)],
        _bars(date(2026, 1, 2)),
        commission=0.35,
    )
    broker.snapshot = AccountSnapshot("ACCOUNT", float("nan"), 493.5, float("nan"), {}, 0)
    runner = CanaryRunner(config, broker)
    runner.store.activation(now - timedelta(hours=1))
    result = asyncio.run(runner.cycle(now))

    assert result.live_submitted == 0
    assert result.live_gate.startswith("insufficient_or_unsettled_cash")
    assert broker.submitted == []
    assert runner.store.rows()[0]["live_state"] == "queued"


def test_tiered_preview_submits_then_fill_gets_oca_protection(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    signal = _signal(now - timedelta(minutes=30))
    monkeypatch.setattr(
        "insider_alerts.execution.canary.load_delivered_signals",
        lambda *args, **kwargs: [signal],
    )
    config = CanaryConfig(
        source_db=str(tmp_path / "source.db"),
        ledger_db=str(tmp_path / "ledger.db"),
        live_requested=True,
        arm_phrase=ARM_PHRASE,
    )
    sessions = [date(2026, 1, 5) + timedelta(days=index) for index in range(15)]
    broker = FakeBroker(sessions, _bars(date(2026, 1, 2)), commission=0.35)
    runner = CanaryRunner(config, broker)
    runner.store.activation(now - timedelta(hours=1))

    first = asyncio.run(runner.cycle(now))
    assert first.live_submitted == 1
    assert broker.submitted[0][0:2] == ("TEST", 20)

    broker.snapshot = AccountSnapshot(
        "ACCOUNT", 493.5, 293.5, 293.5, {"TEST": 20}, 0, {"TEST": 10.0}
    )
    broker.order_values = [
        BrokerOrder(
            101,
            "IA-E07-packet-001-ENTRY",
            "TEST",
            "entry",
            "Filled",
            20,
            0,
            10.0,
            0.35,
        )
    ]
    second = asyncio.run(runner.cycle(now + timedelta(minutes=12)))
    assert second.live_opened == 1
    assert broker.protected == [
        ("TEST", 20, 9.0, 11.0, f"IA-E07-{broker_token('packet-001')}-EXIT")
    ]
    row = runner.store.rows()[0]
    assert row["live_state"] == "open"
    assert row["live_exit_session"] == sessions[9].isoformat()
    assert row["live_entry_commission"] == 0.35


def test_failed_preflight_does_not_consume_live_capacity(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    base_signal = _signal(now - timedelta(minutes=30))
    signals = [
        DeliveredSignal(
            packet_id=packet_id,
            accession_number=f"accession-{index}",
            cik=str(index),
            symbol=symbol,
            filed_at=base_signal.filed_at,
            signal_at=base_signal.signal_at,
            score=base_signal.score,
            rationale={},
        )
        for index, (packet_id, symbol) in enumerate(
            [
                ("bad-packet", "BAD"),
                ("duplicate-packet", "GOOD1"),
                ("good-one-packet", "GOOD1"),
                ("good-two-packet", "GOOD2"),
            ],
            start=1,
        )
    ]
    ranks = {"BAD": "00", "GOOD1": "10", "GOOD2": "20"}
    monkeypatch.setattr(
        "insider_alerts.execution.canary.load_delivered_signals",
        lambda *args, **kwargs: signals,
    )
    monkeypatch.setattr(
        "insider_alerts.execution.canary.deterministic_rank",
        lambda config, signal, session: (
            ranks[signal.symbol] + ("0" if signal.packet_id == "duplicate-packet" else "1")
        ),
    )
    config = CanaryConfig(
        source_db=str(tmp_path / "source.db"),
        ledger_db=str(tmp_path / "ledger.db"),
        live_requested=True,
        arm_phrase=ARM_PHRASE,
    )
    broker = FakeBroker(
        [date(2026, 1, 5) + timedelta(days=index) for index in range(15)],
        _bars(date(2026, 1, 2)),
        commission=0.35,
    )

    async def preview_by_symbol(symbol: str, quantity: int) -> CommissionPreview:
        del quantity
        return CommissionPreview(1.0 if symbol == "BAD" else 0.35, "USD")

    monkeypatch.setattr(broker, "preview_entry", preview_by_symbol)
    runner = CanaryRunner(config, broker)
    runner.store.activation(now - timedelta(hours=1))

    result = asyncio.run(runner.cycle(now))

    assert result.live_submitted == 2
    assert [submission[0] for submission in broker.submitted] == ["GOOD1", "GOOD2"]
    states = {str(row["packet_id"]): str(row["live_state"]) for row in runner.store.rows()}
    assert states == {
        "bad-packet": "preflight_rejected",
        "duplicate-packet": "submitted",
        "good-one-packet": "overlap_suppressed",
        "good-two-packet": "submitted",
    }


def test_unexpected_manual_position_blocks_new_entry(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    signal = _signal(now - timedelta(minutes=30))
    monkeypatch.setattr(
        "insider_alerts.execution.canary.load_delivered_signals",
        lambda *args, **kwargs: [signal],
    )
    config = CanaryConfig(
        source_db="unused",
        ledger_db=str(tmp_path / "ledger.db"),
        live_requested=True,
        arm_phrase=ARM_PHRASE,
    )
    broker = FakeBroker(
        [date(2026, 1, 5) + timedelta(days=index) for index in range(15)],
        _bars(date(2026, 1, 2)),
        commission=0.35,
    )
    broker.snapshot = AccountSnapshot("ACCOUNT", 493.5, 393.5, 393.5, {"MANUAL": 1}, 0)
    runner = CanaryRunner(config, broker)
    runner.store.activation(now - timedelta(hours=1))
    result = asyncio.run(runner.cycle(now))
    assert result.live_gate.startswith("unexpected_broker_position")
    assert broker.submitted == []
