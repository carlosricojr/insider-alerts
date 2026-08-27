from __future__ import annotations

import ast
from datetime import date, timedelta
from pathlib import Path

from insider_alerts.backtest.models import DailyBar
from insider_alerts.execution.canary import CanaryConfig
from insider_alerts.strategy.e07 import evaluate_shadow


def _bar(
    day: date,
    *,
    open_price: float = 10.0,
    high: float = 10.5,
    low: float = 9.5,
    close: float = 10.0,
) -> DailyBar:
    return DailyBar("TEST", day, open_price, high, low, close, 100_000.0)


def test_shadow_policy_freezes_collision_gap_target_and_time_exit() -> None:
    config = CanaryConfig(source_db="fixture.db")
    entry_day = date(2026, 1, 5)

    missing = evaluate_shadow(config, [_bar(entry_day + timedelta(days=1))], entry_day)
    assert missing.entry_bar is None

    collision = evaluate_shadow(
        config,
        [_bar(entry_day, high=11.1, low=8.9)],
        entry_day,
    )
    assert collision.exit_price == 9.0
    assert collision.exit_reason == "stop_and_target_same_day_stop_assumed"

    gap = evaluate_shadow(
        config,
        [
            _bar(entry_day),
            _bar(
                entry_day + timedelta(days=1),
                open_price=8.5,
                high=9.0,
                low=8.0,
                close=8.6,
            ),
        ],
        entry_day,
    )
    assert gap.exit_price == 8.5
    assert gap.exit_reason == "stop"

    target = evaluate_shadow(
        config,
        [_bar(entry_day, high=11.5, low=9.5, close=11.2)],
        entry_day,
    )
    assert target.exit_price == 11.0
    assert target.exit_reason == "target"

    quiet = [
        _bar(entry_day + timedelta(days=index), close=10.0 + index / 100) for index in range(10)
    ]
    timed = evaluate_shadow(config, quiet, entry_day)
    assert timed.exit_bar == quiet[-1]
    assert timed.exit_price == quiet[-1].close
    assert timed.exit_reason == "time"


def test_policy_kernel_has_no_execution_network_or_order_import() -> None:
    import insider_alerts.strategy.e07 as e07

    source = Path(e07.__file__).read_text(encoding="utf-8")
    modules = {
        alias.name
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.ImportFrom)
    }

    assert not any(
        module == root or module.startswith(root + ".")
        for module in modules
        for root in ("insider_alerts.execution", "ib_async", "socket", "httpx", "requests")
    )
    assert "order" not in source.lower()
