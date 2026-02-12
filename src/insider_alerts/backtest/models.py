from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(slots=True)
class SignalEvent:
    packet_id: str
    symbol: str
    filed_at: datetime
    score: float
    open_market_buy_shares: float
    open_market_net_shares: float
    has_10b5_1_plan: bool
    has_equity_comp_event: bool
    has_tax_withholding_language: bool
    role_tier: str


@dataclass(slots=True)
class DailyBar:
    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(slots=True)
class TradeResult:
    packet_id: str
    symbol: str
    signal_date: date
    entry_date: date
    exit_date: date
    hold_days: int
    entry_price: float
    exit_price: float
    stop_price: float | None
    take_profit_price: float | None
    exit_reason: str
    gross_return: float
    net_return: float
    benchmark_return: float | None
    alpha_return: float | None
