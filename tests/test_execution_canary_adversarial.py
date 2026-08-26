from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import replace
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
    CommissionPreview,
    broker_token,
)
from insider_alerts.execution.errors import ContractQualificationError


def _signal(now: datetime) -> DeliveredSignal:
    return DeliveredSignal(
        packet_id="crash-test-packet",
        accession_number="0001-26-999999",
        cik="1",
        symbol="TEST",
        filed_at=now - timedelta(hours=1),
        signal_at=now - timedelta(minutes=30),
        score=0.99,
        rationale={},
    )


def _bars(end: date) -> list[DailyBar]:
    output: list[DailyBar] = []
    cursor = end
    while len(output) < 70:
        if cursor.weekday() < 5:
            output.append(DailyBar("TEST", cursor, 10, 10.2, 9.8, 10, 100_000))
        cursor -= timedelta(days=1)
    return sorted(output, key=lambda bar: bar.trade_date)


class HostileBroker:
    def __init__(self, sessions: list[date], bars: list[DailyBar]) -> None:
        self.session_values = sessions
        self.bar_values = bars
        self.snapshot = AccountSnapshot("ACCOUNT", 493.5, 493.5, 493.5, {}, 0)
        self.preview = CommissionPreview(0.35, "USD")
        self.order_values: list[BrokerOrder] = []
        self.entry_calls = 0
        self.crash_after_entry_accept = False
        self.protected: list[tuple[str, int, float, float, str]] = []
        self.market_exits: list[tuple[str, int, str]] = []
        self.cancelled_orders: list[int] = []
        self.moc_calls = 0
        self.crash_after_moc_accept = False
        self.daily_bar_errors: dict[str, Exception] = {}

    async def connect(self, *, readonly: bool) -> None:
        return None

    def disconnect(self) -> None:
        return None

    async def sessions(self, *, around: datetime, count: int = 90) -> list[date]:
        return self.session_values

    async def daily_bars(self, symbol: str, *, duration: str = "6 M") -> list[DailyBar]:
        error = self.daily_bar_errors.get(symbol)
        if error is not None:
            raise error
        return self.bar_values

    async def account_snapshot(self) -> AccountSnapshot:
        return self.snapshot

    async def preview_entry(self, symbol: str, quantity: int) -> CommissionPreview:
        return self.preview

    async def submit_market_on_open(self, symbol: str, quantity: int, order_ref: str) -> int:
        self.entry_calls += 1
        order_id = 100 + self.entry_calls
        self.order_values.append(
            BrokerOrder(order_id, order_ref, symbol, "entry", "PreSubmitted", 0, quantity, 0)
        )
        self.snapshot = AccountSnapshot("ACCOUNT", 493.5, 493.5, 493.5, {}, 1)
        if self.crash_after_entry_accept:
            self.crash_after_entry_accept = False
            raise ConnectionError("simulated crash after broker acceptance")
        return order_id

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
        self.moc_calls += 1
        order_id = 202 + self.moc_calls
        self.order_values.append(
            BrokerOrder(order_id, order_ref, symbol, "time", "Submitted", 0, quantity, 0)
        )
        self.snapshot = AccountSnapshot(
            self.snapshot.account,
            self.snapshot.net_liquidation,
            self.snapshot.available_funds,
            self.snapshot.settled_cash,
            self.snapshot.positions,
            self.snapshot.open_order_count + 1,
            self.snapshot.average_costs,
        )
        if self.crash_after_moc_accept:
            self.crash_after_moc_accept = False
            raise ConnectionError("simulated crash after MOC acceptance")
        return order_id

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


def _runner(
    tmp_path: Path,
    monkeypatch: Any,
    now: datetime,
) -> tuple[CanaryRunner, HostileBroker]:
    signal = _signal(now)
    monkeypatch.setattr(
        "insider_alerts.execution.canary.load_delivered_signals",
        lambda *args, **kwargs: [signal],
    )
    sessions = [now.date() + timedelta(days=index) for index in range(20)]
    broker = HostileBroker(sessions, _bars(now.date() - timedelta(days=1)))
    config = CanaryConfig(
        source_db="unused",
        ledger_db=str(tmp_path / "ledger.db"),
        live_requested=True,
        arm_phrase=ARM_PHRASE,
    )
    runner = CanaryRunner(config, broker)
    runner.store.activation(now - timedelta(hours=2))
    return runner, broker


def test_discovery_quarantines_unqualifiable_contract_and_continues(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    bad = replace(_signal(now), packet_id="bad-packet", symbol="BAD")
    good = replace(_signal(now), packet_id="good-packet", symbol="GOOD")
    monkeypatch.setattr(
        "insider_alerts.execution.canary.load_delivered_signals",
        lambda *args, **kwargs: [bad, good],
    )
    broker.daily_bar_errors["BAD"] = ContractQualificationError("unknown contract")

    result = asyncio.run(
        runner._discover(
            now - timedelta(hours=2),
            broker.session_values,
            now,
        )
    )

    assert (result.detected, result.eligible, result.rejected) == (2, 1, 1)
    rows = {str(row["packet_id"]): row for row in runner.store.rows()}
    assert rows["bad-packet"]["eligibility_reason"] == "contract_qualification_failed"
    assert rows["bad-packet"]["shadow_state"] == "rejected"
    assert rows["bad-packet"]["live_state"] == "rejected"
    assert rows["good-packet"]["eligible"] == 1
    with runner.store.connect() as conn:
        event = conn.execute(
            "select detail_json from events where packet_id='bad-packet'"
        ).fetchone()
    assert event is not None
    assert '"qualification_error":"unknown contract"' in str(event["detail_json"])


def test_discovery_retries_transient_daily_bar_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    broker.daily_bar_errors["TEST"] = ConnectionError("temporary disconnect")

    with pytest.raises(ConnectionError, match="temporary disconnect"):
        asyncio.run(
            runner._discover(
                now - timedelta(hours=2),
                broker.session_values,
                now,
            )
        )

    assert runner.store.rows() == []


def test_discovery_rolls_back_candidate_when_evidence_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    broker.daily_bar_errors["TEST"] = ContractQualificationError("unknown contract")
    with runner.store.connect() as conn:
        conn.execute(
            """
            CREATE TRIGGER fail_candidate_event
            BEFORE INSERT ON events
            BEGIN
                SELECT RAISE(ABORT, 'evidence write failed');
            END
            """
        )
        conn.commit()

    with pytest.raises(sqlite3.IntegrityError, match="evidence write failed"):
        asyncio.run(
            runner._discover(
                now - timedelta(hours=2),
                broker.session_values,
                now,
            )
        )

    assert runner.store.rows() == []


def test_crash_after_entry_acceptance_does_not_duplicate_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    broker.crash_after_entry_accept = True
    with pytest.raises(ConnectionError, match="after broker acceptance"):
        asyncio.run(runner.cycle(now))

    asyncio.run(runner.cycle(now + timedelta(seconds=15)))
    assert broker.entry_calls == 1
    row = runner.store.rows()[0]
    assert row["live_state"] == "submitted"
    assert row["parent_order_id"] == 101


def test_restart_recovers_broker_position_when_fill_trade_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    asyncio.run(runner.cycle(now))
    broker.order_values = []
    broker.snapshot = AccountSnapshot(
        "ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 20}, 0, {"TEST": 10.0}
    )

    asyncio.run(runner.cycle(now + timedelta(minutes=12)))
    assert broker.protected
    assert runner.store.rows()[0]["live_state"] == "open"


def test_overdue_tenth_session_exit_uses_immediate_market_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    asyncio.run(runner.cycle(now))
    token = broker_token("crash-test-packet")
    broker.snapshot = AccountSnapshot("ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 20}, 0)
    broker.order_values = [
        BrokerOrder(101, f"IA-E07-{token}-ENTRY", "TEST", "entry", "Filled", 20, 0, 10)
    ]
    asyncio.run(runner.cycle(now + timedelta(minutes=12)))
    runner.store.update(
        "crash-test-packet",
        live_exit_session=(now.date() - timedelta(days=1)).isoformat(),
    )
    broker.order_values = [
        BrokerOrder(201, f"IA-E07-{token}-TARGET", "TEST", "target", "Submitted", 0, 20, 0),
        BrokerOrder(202, f"IA-E07-{token}-STOP", "TEST", "stop", "Submitted", 0, 20, 0),
    ]
    broker.snapshot = AccountSnapshot("ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 20}, 2)

    asyncio.run(runner.cycle(now + timedelta(days=1)))
    assert broker.market_exits == [
        ("TEST", 20, f"IA-E07-{token}-EXIT")
    ]
    assert runner.store.rows()[0]["live_state"] == "closing"


def test_partial_exit_remains_managed_and_flattens_residual_position(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    asyncio.run(runner.cycle(now))
    token = broker_token("crash-test-packet")
    broker.snapshot = AccountSnapshot("ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 20}, 0)
    broker.order_values = [
        BrokerOrder(101, f"IA-E07-{token}-ENTRY", "TEST", "entry", "Filled", 20, 0, 10)
    ]
    asyncio.run(runner.cycle(now + timedelta(minutes=12)))

    broker.snapshot = AccountSnapshot("ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 15}, 2)
    broker.order_values = [
        BrokerOrder(201, f"IA-E07-{token}-TARGET", "TEST", "target", "Submitted", 5, 15, 11),
        BrokerOrder(202, f"IA-E07-{token}-STOP", "TEST", "stop", "Submitted", 0, 20, 0),
    ]
    asyncio.run(runner.cycle(now + timedelta(minutes=13)))

    row = runner.store.rows()[0]
    assert row["live_state"] == "closing"
    assert row["live_exit_at"] is None
    assert broker.market_exits[-1][:2] == ("TEST", 15)


def test_cancelled_timed_exit_after_partial_fill_is_replaced_for_residual(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    asyncio.run(runner.cycle(now))
    token = broker_token("crash-test-packet")
    broker.snapshot = AccountSnapshot(
        "ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 20}, 0, {"TEST": 10.0}
    )
    broker.order_values = [
        BrokerOrder(101, f"IA-E07-{token}-ENTRY", "TEST", "entry", "Filled", 20, 0, 10)
    ]
    asyncio.run(runner.cycle(now + timedelta(minutes=12)))
    runner.store.update("crash-test-packet", live_exit_session=now.date().isoformat())
    protective_orders = [
        BrokerOrder(201, f"IA-E07-{token}-TARGET", "TEST", "target", "Submitted", 0, 20, 0),
        BrokerOrder(202, f"IA-E07-{token}-STOP", "TEST", "stop", "Submitted", 0, 20, 0),
    ]
    broker.order_values = protective_orders
    broker.snapshot = AccountSnapshot(
        "ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 20}, 2, {"TEST": 10.0}
    )
    due = datetime(2026, 1, 5, 20, 31, tzinfo=UTC)
    asyncio.run(runner.cycle(due))
    assert broker.moc_calls == 1

    broker.snapshot = AccountSnapshot(
        "ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 15}, 0, {"TEST": 10.0}
    )
    broker.order_values = [
        BrokerOrder(201, f"IA-E07-{token}-TARGET", "TEST", "target", "Cancelled", 5, 15, 11),
        BrokerOrder(202, f"IA-E07-{token}-STOP", "TEST", "stop", "Cancelled", 0, 20, 0),
        BrokerOrder(203, f"IA-E07-{token}-TIME", "TEST", "time", "Cancelled", 0, 20, 0),
    ]

    asyncio.run(runner.cycle(due + timedelta(minutes=1)))

    row = runner.store.rows()[0]
    assert row["live_state"] == "closing"
    assert row["live_quantity"] == 15
    assert row["timed_exit_order_id"] == 204
    assert broker.moc_calls == 2
    assert broker.protected[-1][1] == 15


def test_incomplete_exchange_schedule_rejects_before_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    broker.session_values = [now.date() + timedelta(days=index) for index in range(5)]

    result = asyncio.run(runner.cycle(now))
    assert result.rejected == 1
    assert broker.entry_calls == 0
    row = runner.store.rows()[0]
    assert row["eligibility_reason"] == "insufficient_exchange_schedule_for_time_exit"


def test_preflight_warning_blocks_even_when_commission_is_low(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    broker.preview = CommissionPreview(0.35, "USD", "security verification required")

    result = asyncio.run(runner.cycle(now))
    assert result.live_submitted == 0
    assert broker.entry_calls == 0
    assert runner.store.rows()[0]["live_state"] == "preflight_rejected"


def test_partial_entry_is_cancelled_before_protection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    asyncio.run(runner.cycle(now))
    token = broker_token("crash-test-packet")
    broker.snapshot = AccountSnapshot(
        "ACCOUNT", 393.5, 393.5, 393.5, {"TEST": 10}, 1, {"TEST": 10.0}
    )
    broker.order_values = [
        BrokerOrder(101, f"IA-E07-{token}-ENTRY", "TEST", "entry", "Submitted", 10, 10, 10)
    ]

    asyncio.run(runner.cycle(now + timedelta(minutes=12)))
    assert broker.cancelled_orders == [101]
    assert broker.protected[0][1] == 10
    assert runner.store.rows()[0]["live_state"] == "open"


def test_position_quantity_mismatch_flattens_instead_of_leaving_excess_unprotected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    asyncio.run(runner.cycle(now))
    token = broker_token("crash-test-packet")
    broker.snapshot = AccountSnapshot(
        "ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 20}, 0, {"TEST": 10.0}
    )
    broker.order_values = [
        BrokerOrder(101, f"IA-E07-{token}-ENTRY", "TEST", "entry", "Filled", 20, 0, 10)
    ]
    asyncio.run(runner.cycle(now + timedelta(minutes=12)))

    broker.snapshot = AccountSnapshot(
        "ACCOUNT", 243.5, 243.5, 243.5, {"TEST": 25}, 2, {"TEST": 10.0}
    )
    broker.order_values = [
        BrokerOrder(201, f"IA-E07-{token}-TARGET", "TEST", "target", "Submitted", 0, 20, 0),
        BrokerOrder(202, f"IA-E07-{token}-STOP", "TEST", "stop", "Submitted", 0, 20, 0),
    ]
    asyncio.run(runner.cycle(now + timedelta(minutes=13)))
    assert broker.market_exits == [("TEST", 25, f"IA-E07-{token}-EXIT")]
    assert runner.store.rows()[0]["live_state"] == "closing"


def test_unrecognized_broker_order_blocks_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    broker.snapshot = AccountSnapshot("ACCOUNT", 493.5, 493.5, 493.5, {}, 1)

    result = asyncio.run(runner.cycle(now))
    assert result.live_gate.startswith("unexpected_non_canary_open_order")
    assert broker.entry_calls == 0


def test_crashed_entry_that_filled_is_adopted_and_protected_same_cycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    broker.crash_after_entry_accept = True
    with pytest.raises(ConnectionError):
        asyncio.run(runner.cycle(now))
    token = broker_token("crash-test-packet")
    broker.order_values = [
        BrokerOrder(101, f"IA-E07-{token}-ENTRY", "TEST", "entry", "Filled", 20, 0, 10)
    ]
    broker.snapshot = AccountSnapshot(
        "ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 20}, 0, {"TEST": 10.0}
    )

    result = asyncio.run(runner.cycle(now + timedelta(seconds=15)))
    assert result.live_opened == 1
    assert broker.entry_calls == 1
    assert broker.protected
    assert runner.store.rows()[0]["live_state"] == "open"


def test_crash_after_moc_acceptance_adopts_exit_without_duplicate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    asyncio.run(runner.cycle(now))
    token = broker_token("crash-test-packet")
    broker.snapshot = AccountSnapshot(
        "ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 20}, 0, {"TEST": 10.0}
    )
    broker.order_values = [
        BrokerOrder(101, f"IA-E07-{token}-ENTRY", "TEST", "entry", "Filled", 20, 0, 10)
    ]
    asyncio.run(runner.cycle(now + timedelta(minutes=12)))
    runner.store.update("crash-test-packet", live_exit_session=now.date().isoformat())
    broker.order_values = [
        BrokerOrder(201, f"IA-E07-{token}-TARGET", "TEST", "target", "Submitted", 0, 20, 0),
        BrokerOrder(202, f"IA-E07-{token}-STOP", "TEST", "stop", "Submitted", 0, 20, 0),
    ]
    broker.snapshot = AccountSnapshot(
        "ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 20}, 2, {"TEST": 10.0}
    )
    broker.crash_after_moc_accept = True
    due = datetime(2026, 1, 5, 20, 31, tzinfo=UTC)  # 15:31 ET
    with pytest.raises(ConnectionError, match="MOC acceptance"):
        asyncio.run(runner.cycle(due))

    asyncio.run(runner.cycle(due + timedelta(seconds=15)))
    assert broker.moc_calls == 1
    row = runner.store.rows()[0]
    assert row["live_state"] == "closing"
    assert row["timed_exit_order_id"] == 203


def test_missing_protective_orders_are_recreated_while_position_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    asyncio.run(runner.cycle(now))
    token = broker_token("crash-test-packet")
    broker.snapshot = AccountSnapshot(
        "ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 20}, 0, {"TEST": 10.0}
    )
    broker.order_values = [
        BrokerOrder(101, f"IA-E07-{token}-ENTRY", "TEST", "entry", "Filled", 20, 0, 10)
    ]
    asyncio.run(runner.cycle(now + timedelta(minutes=12)))
    assert len(broker.protected) == 1

    # Simulate both server-held exit orders disappearing while the shares remain.
    broker.order_values = []
    broker.snapshot = AccountSnapshot(
        "ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 20}, 0, {"TEST": 10.0}
    )
    asyncio.run(runner.cycle(now + timedelta(minutes=13)))

    assert len(broker.protected) == 2
    assert runner.store.rows()[0]["live_state"] == "open"

    # Losing only one leg must also repair the pair without changing position state.
    broker.order_values = [
        BrokerOrder(201, f"IA-E07-{token}-TARGET", "TEST", "target", "Submitted", 0, 20, 0)
    ]
    broker.snapshot = AccountSnapshot(
        "ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 20}, 1, {"TEST": 10.0}
    )
    asyncio.run(runner.cycle(now + timedelta(minutes=14)))
    assert len(broker.protected) == 3
    assert runner.store.rows()[0]["live_state"] == "open"


def test_filled_target_closes_position_and_persists_exit_commission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    asyncio.run(runner.cycle(now))
    token = broker_token("crash-test-packet")
    broker.snapshot = AccountSnapshot(
        "ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 20}, 0, {"TEST": 10.0}
    )
    broker.order_values = [
        BrokerOrder(101, f"IA-E07-{token}-ENTRY", "TEST", "entry", "Filled", 20, 0, 10)
    ]
    asyncio.run(runner.cycle(now + timedelta(minutes=12)))

    broker.snapshot = AccountSnapshot("ACCOUNT", 513.0, 513.0, 513.0, {}, 1)
    broker.order_values = [
        BrokerOrder(
            201,
            f"IA-E07-{token}-TARGET",
            "TEST",
            "target",
            "Filled",
            20,
            0,
            11.0,
            0.42,
        ),
        BrokerOrder(202, f"IA-E07-{token}-STOP", "TEST", "stop", "Cancelled", 0, 20, 0),
    ]
    result = asyncio.run(runner.cycle(now + timedelta(minutes=13)))
    row = runner.store.rows()[0]
    assert result.live_closed == 1
    assert row["live_state"] == "closed"
    assert row["live_exit_reason"] == "target"
    assert row["live_exit_commission"] == 0.42
