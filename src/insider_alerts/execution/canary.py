from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import statistics
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any, Literal, Protocol
from zoneinfo import ZoneInfo

from insider_alerts.backtest.models import DailyBar
from insider_alerts.backtest.signal_study import DeliveredSignal, load_delivered_signals

NEW_YORK = ZoneInfo("America/New_York")
ARM_PHRASE = "I_ACCEPT_LIVE_CANARY_RISK"


@dataclass(slots=True, frozen=True)
class CanaryConfig:
    source_db: str
    ledger_db: str = "data/live_canary.db"
    host: str = "127.0.0.1"
    port: int = 4001
    client_id: int = 173
    account: str | None = None
    live_requested: bool = False
    arm_phrase: str = ""
    poll_seconds: int = 60
    slot_budget: float = 200.0
    max_live_slots: int = 2
    max_shadow_slots: int = 20
    cash_reserve: float = 50.0
    market_order_cushion: float = 0.05
    min_price: float = 2.0
    max_price: float = 200.0
    min_median_dollar_volume_20d: float = 500_000.0
    stop_loss_pct: float = 0.10
    take_profit_pct: float = 0.10
    max_sessions: int = 10
    entry_submission_start: time = time(9, 18)
    entry_submission_deadline: time = time(9, 20)
    timed_exit_submission_time: time = time(15, 30)
    moc_submission_deadline: time = time(15, 45)
    max_one_way_commission: float = 0.75
    max_one_way_commission_bps: float = 50.0
    invalid_commission_handling: Literal["fallback_to_cap", "reject"] = "reject"
    lottery_salt: str = "E07-F00-live-canary-v1"

    def __post_init__(self) -> None:
        if self.invalid_commission_handling not in {"fallback_to_cap", "reject"}:
            raise ValueError("invalid_commission_handling must be 'fallback_to_cap' or 'reject'")

    @property
    def live_armed(self) -> bool:
        return self.live_requested and self.arm_phrase == ARM_PHRASE


@dataclass(slots=True, frozen=True)
class AccountSnapshot:
    account: str
    net_liquidation: float
    available_funds: float
    settled_cash: float
    positions: dict[str, float]
    open_order_count: int
    average_costs: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class CommissionPreview:
    commission: float
    currency: str
    warning: str = ""
    commission_valid: bool = True
    commission_error: str = ""
    estimate_source: Literal["exact", "range_upper_bound", "unavailable"] = "exact"
    min_commission: float | None = None
    max_commission: float | None = None


@dataclass(slots=True, frozen=True)
class BrokerOrder:
    order_id: int
    order_ref: str
    symbol: str
    kind: str
    status: str
    filled: float
    remaining: float
    average_fill_price: float
    commission: float | None = None


@dataclass(slots=True, frozen=True)
class CycleResult:
    detected: int = 0
    eligible: int = 0
    rejected: int = 0
    shadow_opened: int = 0
    shadow_closed: int = 0
    live_submitted: int = 0
    live_opened: int = 0
    live_closed: int = 0
    live_gate: str = "not_checked"


class CanaryBroker(Protocol):
    async def connect(self, *, readonly: bool) -> None: ...

    def disconnect(self) -> None: ...

    async def sessions(self, *, around: datetime, count: int = 90) -> list[date]: ...

    async def daily_bars(self, symbol: str, *, duration: str = "6 M") -> list[DailyBar]: ...

    async def account_snapshot(self) -> AccountSnapshot: ...

    async def preview_entry(self, symbol: str, quantity: int) -> CommissionPreview: ...

    async def submit_market_on_open(self, symbol: str, quantity: int, order_ref: str) -> int: ...

    async def submit_protective_oca(
        self,
        symbol: str,
        quantity: int,
        *,
        stop_price: float,
        target_price: float,
        oca_group: str,
        order_ref_prefix: str,
    ) -> tuple[int, int]: ...

    async def submit_market_on_close(
        self,
        symbol: str,
        quantity: int,
        *,
        oca_group: str,
        order_ref: str,
    ) -> int: ...

    async def submit_market_exit(
        self,
        symbol: str,
        quantity: int,
        *,
        oca_group: str,
        order_ref: str,
    ) -> int: ...

    async def cancel_order(self, order_id: int) -> None: ...

    async def orders(self) -> list[BrokerOrder]: ...


def deterministic_rank(config: CanaryConfig, signal: DeliveredSignal, session: date) -> str:
    material = (
        f"{config.lottery_salt}|{session.isoformat()}|{signal.packet_id}|"
        f"{signal.accession_number}|{signal.symbol}"
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def broker_token(packet_id: str) -> str:
    """Return a short, collision-resistant token safe for IBKR orderRef/OCA fields."""

    return hashlib.sha256(packet_id.encode("utf-8")).hexdigest()[:12].upper()


def poll_delay_seconds(config: CanaryConfig, now: datetime) -> int:
    """Poll rapidly around the opening auction so fills receive prompt protection."""

    local = now.astimezone(NEW_YORK)
    if local.weekday() < 5 and time(9, 15) <= local.time() <= time(9, 35):
        return min(config.poll_seconds, 2)
    return config.poll_seconds


def entry_session(signal_at: datetime, sessions: Sequence[date], *, now: datetime) -> date | None:
    """Choose the first executable RTH open without chasing a missed auction."""

    local_signal = signal_at.astimezone(NEW_YORK)
    local_now = now.astimezone(NEW_YORK)
    candidates = [session for session in sorted(set(sessions)) if session >= local_signal.date()]
    for session in candidates:
        if session == local_signal.date() and local_signal.time() >= time(9, 30):
            continue
        if session == local_now.date() and local_now.time() >= time(9, 20):
            continue
        if session < local_now.date():
            continue
        return session
    return None


def completed_bars(bars: Sequence[DailyBar], signal_at: datetime) -> list[DailyBar]:
    local = signal_at.astimezone(NEW_YORK)
    same_day_complete = local.time() >= time(16, 0)
    return [
        bar
        for bar in bars
        if bar.trade_date < local.date() or (same_day_complete and bar.trade_date == local.date())
    ]


def eligibility(
    config: CanaryConfig,
    signal: DeliveredSignal,
    bars: Sequence[DailyBar],
) -> tuple[bool, str, float | None, float | None]:
    completed = completed_bars(bars, signal.signal_at)
    if len(completed) < 20:
        return False, "fewer_than_20_completed_daily_bars", None, None
    prior_close = completed[-1].close
    dollar_volumes = [bar.close * bar.volume for bar in completed[-20:]]
    if not math.isfinite(prior_close) or not config.min_price <= prior_close <= config.max_price:
        return False, "prior_close_outside_price_bounds", prior_close, None
    if any(not math.isfinite(value) or value <= 0 for value in dollar_volumes):
        return False, "invalid_dollar_volume_history", prior_close, None
    median_dollar_volume = statistics.median(dollar_volumes)
    if median_dollar_volume < config.min_median_dollar_volume_20d:
        return False, "median_20d_dollar_volume_below_floor", prior_close, median_dollar_volume
    return True, "eligible_E07_F00", prior_close, median_dollar_volume


def planned_quantity(config: CanaryConfig, reference_price: float) -> int:
    if not math.isfinite(reference_price) or reference_price <= 0:
        return 0
    return max(0, math.floor(config.slot_budget / reference_price))


def round_stock_price(value: float) -> float:
    # This canary excludes sub-$2 shares, for which a cent is a conservative valid increment.
    return round(value + 1e-12, 2)


def _finite_number_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return value if math.isfinite(value) else None


def runtime_source_fingerprint(package_root: Path | None = None) -> str:
    """Hash the Python source that the long-running worker loaded at startup."""

    root = package_root or Path(__file__).resolve().parents[1]
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*.py"), key=lambda item: item.as_posix()):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        content = path.read_bytes()
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


class CanaryStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.ensure_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def ensure_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS candidates (
                    packet_id TEXT PRIMARY KEY,
                    accession_number TEXT NOT NULL,
                    cik TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    signal_at TEXT NOT NULL,
                    score REAL NOT NULL,
                    entry_session TEXT,
                    lottery_rank TEXT,
                    eligible INTEGER NOT NULL,
                    eligibility_reason TEXT NOT NULL,
                    prior_close REAL,
                    median_dollar_volume_20d REAL,
                    planned_quantity INTEGER,
                    shadow_state TEXT NOT NULL,
                    live_state TEXT NOT NULL,
                    parent_order_id INTEGER,
                    stop_order_id INTEGER,
                    target_order_id INTEGER,
                    timed_exit_order_id INTEGER,
                    live_quantity INTEGER,
                    live_entry_price REAL,
                    live_entry_commission REAL,
                    live_entry_at TEXT,
                    live_exit_session TEXT,
                    live_exit_price REAL,
                    live_exit_commission REAL,
                    live_exit_at TEXT,
                    live_exit_reason TEXT,
                    oca_group TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_canary_candidates_entry
                    ON candidates(entry_session, lottery_rank);
                CREATE INDEX IF NOT EXISTS idx_canary_candidates_live
                    ON candidates(live_state, entry_session);
                CREATE TABLE IF NOT EXISTS shadow_trades (
                    packet_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    entry_session TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    stop_price REAL NOT NULL,
                    target_price REAL NOT NULL,
                    exit_session TEXT NOT NULL,
                    exit_price REAL NOT NULL,
                    exit_reason TEXT NOT NULL,
                    gross_return REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    closed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at TEXT NOT NULL,
                    level TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    packet_id TEXT,
                    detail_json TEXT NOT NULL
                );
                """
            )
            columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(candidates)")}
            if "live_entry_commission" not in columns:
                conn.execute("ALTER TABLE candidates ADD COLUMN live_entry_commission REAL")
            if "live_exit_commission" not in columns:
                conn.execute("ALTER TABLE candidates ADD COLUMN live_exit_commission REAL")
            conn.commit()

    def activation(self, now: datetime) -> datetime:
        with self.connect() as conn:
            row = conn.execute("SELECT value FROM metadata WHERE key='activation_utc'").fetchone()
            if row is not None:
                return datetime.fromisoformat(str(row["value"])).astimezone(UTC)
            value = now.astimezone(UTC).isoformat()
            conn.execute(
                "INSERT INTO metadata(key,value,updated_at) VALUES('activation_utc',?,?)",
                (value, value),
            )
            conn.commit()
            return datetime.fromisoformat(value)

    def set_metadata(self, values: dict[str, str], *, now: datetime) -> None:
        timestamp = now.astimezone(UTC).isoformat()
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO metadata(key,value,updated_at) VALUES(?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                [(key, value, timestamp) for key, value in values.items()],
            )
            conn.commit()

    def has_candidate(self, packet_id: str) -> bool:
        with self.connect() as conn:
            return (
                conn.execute("SELECT 1 FROM candidates WHERE packet_id=?", (packet_id,)).fetchone()
                is not None
            )

    def insert_candidate(
        self,
        signal: DeliveredSignal,
        *,
        session: date | None,
        rank: str | None,
        is_eligible: bool,
        reason: str,
        prior_close: float | None,
        median_dollar_volume: float | None,
        quantity: int,
        now: datetime,
    ) -> None:
        stamp = now.astimezone(UTC).isoformat()
        shadow_state = "queued" if is_eligible and session is not None else "rejected"
        live_state = "queued" if is_eligible and session is not None else "rejected"
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO candidates(
                    packet_id, accession_number, cik, symbol, signal_at, score,
                    entry_session, lottery_rank, eligible, eligibility_reason,
                    prior_close, median_dollar_volume_20d, planned_quantity,
                    shadow_state, live_state, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    signal.packet_id,
                    signal.accession_number,
                    signal.cik,
                    signal.symbol,
                    signal.signal_at.astimezone(UTC).isoformat(),
                    signal.score,
                    session.isoformat() if session else None,
                    rank,
                    int(is_eligible),
                    reason,
                    prior_close,
                    median_dollar_volume,
                    quantity,
                    shadow_state,
                    live_state,
                    stamp,
                    stamp,
                ),
            )
            conn.commit()

    def rows(self, where: str = "1=1", params: Sequence[object] = ()) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                f"SELECT * FROM candidates WHERE {where} ORDER BY entry_session, lottery_rank",
                params,
            ).fetchall()

    def update(self, packet_id: str, **values: object) -> None:
        if not values:
            return
        values["updated_at"] = datetime.now(UTC).isoformat()
        columns = ", ".join(f"{key}=?" for key in values)
        with self.connect() as conn:
            conn.execute(
                f"UPDATE candidates SET {columns} WHERE packet_id=?",
                (*values.values(), packet_id),
            )
            conn.commit()

    def event(
        self,
        event_type: str,
        *,
        packet_id: str | None = None,
        level: str = "info",
        **detail: object,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO events(occurred_at,level,event_type,packet_id,detail_json) "
                "VALUES(?,?,?,?,?)",
                (
                    datetime.now(UTC).isoformat(),
                    level,
                    event_type,
                    packet_id,
                    json.dumps(detail, sort_keys=True, separators=(",", ":")),
                ),
            )
            conn.commit()

    def record_shadow_trade(
        self,
        row: sqlite3.Row,
        *,
        quantity: int,
        entry_bar: DailyBar,
        exit_bar: DailyBar,
        exit_price: float,
        exit_reason: str,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        stop = entry_bar.open * 0.90
        target = entry_bar.open * 1.10
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO shadow_trades(
                    packet_id,symbol,quantity,entry_session,entry_price,stop_price,target_price,
                    exit_session,exit_price,exit_reason,gross_return,created_at,closed_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row["packet_id"],
                    row["symbol"],
                    quantity,
                    entry_bar.trade_date.isoformat(),
                    entry_bar.open,
                    stop,
                    target,
                    exit_bar.trade_date.isoformat(),
                    exit_price,
                    exit_reason,
                    exit_price / entry_bar.open - 1.0,
                    now,
                    now,
                ),
            )
            conn.execute(
                "UPDATE candidates SET shadow_state='closed',updated_at=? WHERE packet_id=?",
                (now, row["packet_id"]),
            )
            conn.commit()


class CanaryRunner:
    def __init__(self, config: CanaryConfig, broker: CanaryBroker) -> None:
        self.config = config
        self.broker = broker
        self.store = CanaryStore(config.ledger_db)
        self.runtime_started_at = datetime.now(UTC)
        self.runtime_fingerprint = runtime_source_fingerprint()

    def source_revision_changed(self) -> bool:
        return runtime_source_fingerprint() != self.runtime_fingerprint

    async def cycle(
        self,
        now: datetime | None = None,
        *,
        disconnect_after: bool = True,
    ) -> CycleResult:
        wall_clock_mode = now is None
        now = (now or datetime.now(UTC)).astimezone(UTC)
        self.store.set_metadata(
            {
                "runtime_started_utc": self.runtime_started_at.isoformat(),
                "runtime_source_fingerprint": self.runtime_fingerprint,
                "last_cycle_started_utc": now.isoformat(),
            },
            now=now,
        )
        try:
            activation = self.store.activation(now)
            await self.broker.connect(readonly=not self.config.live_armed)
            sessions = await self.broker.sessions(around=now, count=120)
            result = await self._discover(activation, sessions, now)
            shadow_opened, shadow_closed = await self._settle_shadow(sessions, now)
            result = CycleResult(
                **{
                    **asdict(result),
                    "shadow_opened": shadow_opened,
                    "shadow_closed": shadow_closed,
                }
            )
            if not self.config.live_armed:
                final_result = CycleResult(
                    **{**asdict(result), "live_gate": "shadow_only_not_armed"}
                )
            else:
                final_result = await self._run_live(
                    result,
                    sessions,
                    datetime.now(UTC) if wall_clock_mode else now,
                    enforce_wall_clock=wall_clock_mode,
                )
            success_time = datetime.now(UTC)
            self.store.set_metadata(
                {
                    "last_cycle_success_utc": success_time.isoformat(),
                    "last_cycle_error": "",
                },
                now=success_time,
            )
            return final_result
        except Exception as exc:
            failure_time = datetime.now(UTC)
            self.store.set_metadata(
                {
                    "last_cycle_error_utc": failure_time.isoformat(),
                    "last_cycle_error": f"{type(exc).__name__}: {exc}",
                },
                now=failure_time,
            )
            raise
        finally:
            if disconnect_after:
                self.broker.disconnect()

    async def _discover(
        self,
        activation: datetime,
        sessions: Sequence[date],
        now: datetime,
    ) -> CycleResult:
        signals = load_delivered_signals(
            self.config.source_db,
            start_date=activation.astimezone(NEW_YORK).date(),
        )
        detected = eligible_count = rejected = 0
        for signal in signals:
            if signal.signal_at < activation or self.store.has_candidate(signal.packet_id):
                continue
            detected += 1
            bars = await self.broker.daily_bars(signal.symbol)
            ok, reason, prior_close, median_dv = eligibility(self.config, signal, bars)
            target_session = entry_session(signal.signal_at, sessions, now=now) if ok else None
            quantity = planned_quantity(self.config, prior_close or 0.0) if ok else 0
            if ok and (target_session is None or quantity < 1):
                ok = False
                reason = (
                    "no_future_entry_session" if target_session is None else "zero_whole_shares"
                )
            if ok and target_session is not None:
                scheduled_horizon = [session for session in sessions if session >= target_session]
                if len(scheduled_horizon) < self.config.max_sessions:
                    ok = False
                    reason = "insufficient_exchange_schedule_for_time_exit"
            rank = (
                deterministic_rank(self.config, signal, target_session) if target_session else None
            )
            self.store.insert_candidate(
                signal,
                session=target_session,
                rank=rank,
                is_eligible=ok,
                reason=reason,
                prior_close=prior_close,
                median_dollar_volume=median_dv,
                quantity=quantity,
                now=now,
            )
            self.store.event(
                "candidate_detected",
                packet_id=signal.packet_id,
                symbol=signal.symbol,
                eligible=ok,
                reason=reason,
                entry_session=target_session.isoformat() if target_session else None,
            )
            eligible_count += int(ok)
            rejected += int(not ok)
        return CycleResult(detected=detected, eligible=eligible_count, rejected=rejected)

    async def _settle_shadow(
        self,
        sessions: Sequence[date],
        now: datetime,
    ) -> tuple[int, int]:
        """Materialize the preregistered daily-bar shadow result once bars exist.

        The row remains queued until either a stop/target or the tenth-session close can be
        observed. This avoids using incomplete intraday bars and exactly matches the research
        convention that same-day stop+target collisions are charged to the stop.
        """

        opened = closed = 0
        rows = self.store.rows("shadow_state IN ('queued','open')")
        by_session: dict[str, list[sqlite3.Row]] = {}
        for row in rows:
            by_session.setdefault(str(row["entry_session"]), []).append(row)
        for session_text, candidates in by_session.items():
            session = date.fromisoformat(session_text)
            if session >= now.astimezone(NEW_YORK).date():
                continue
            # The shadow book is the inferential portfolio: deterministic and capacity-limited.
            open_rows = [row for row in candidates if row["shadow_state"] == "open"]
            queued_rows = [row for row in candidates if row["shadow_state"] == "queued"]
            existing_symbols = {str(row["symbol"]) for row in open_rows}
            unique_queued: list[sqlite3.Row] = []
            for row in queued_rows:
                symbol = str(row["symbol"])
                if symbol in existing_symbols:
                    self.store.update(str(row["packet_id"]), shadow_state="overlap_suppressed")
                    continue
                existing_symbols.add(symbol)
                unique_queued.append(row)
            active_count = len(self.store.rows("shadow_state='open'"))
            capacity = max(0, self.config.max_shadow_slots - active_count)
            selected = open_rows + unique_queued[:capacity]
            for row in unique_queued[capacity:]:
                self.store.update(str(row["packet_id"]), shadow_state="capacity_suppressed")
            for row in selected:
                symbol = str(row["symbol"])
                overlapping = self.store.rows(
                    "symbol=? AND shadow_state='open' AND packet_id<>?",
                    (symbol, str(row["packet_id"])),
                )
                if row["shadow_state"] == "queued" and overlapping:
                    self.store.update(str(row["packet_id"]), shadow_state="overlap_suppressed")
                    continue
                bars = await self.broker.daily_bars(symbol)
                post_entry = [bar for bar in bars if bar.trade_date >= session]
                if not post_entry or post_entry[0].trade_date != session:
                    continue
                if row["shadow_state"] == "queued":
                    opened += 1
                entry_bar = post_entry[0]
                stop = entry_bar.open * (1.0 - self.config.stop_loss_pct)
                target = entry_bar.open * (1.0 + self.config.take_profit_pct)
                outcome: tuple[DailyBar, float, str] | None = None
                for index, bar in enumerate(post_entry[: self.config.max_sessions]):
                    stop_hit = bar.low <= stop
                    target_hit = bar.high >= target
                    if stop_hit:
                        outcome = (
                            bar,
                            stop,
                            "stop_and_target_same_day_stop_assumed" if target_hit else "stop",
                        )
                        break
                    if target_hit:
                        outcome = (bar, target, "target")
                        break
                    if index == self.config.max_sessions - 1:
                        outcome = (bar, bar.close, "time")
                if outcome is None:
                    self.store.update(str(row["packet_id"]), shadow_state="open")
                    continue
                exit_bar, exit_price, reason = outcome
                self.store.record_shadow_trade(
                    row,
                    quantity=int(row["planned_quantity"]),
                    entry_bar=entry_bar,
                    exit_bar=exit_bar,
                    exit_price=exit_price,
                    exit_reason=reason,
                )
                closed += 1
        return opened, closed

    async def _run_live(
        self,
        result: CycleResult,
        sessions: Sequence[date],
        now: datetime,
        *,
        enforce_wall_clock: bool,
    ) -> CycleResult:
        account = await self.broker.account_snapshot()
        broker_orders = await self.broker.orders()
        self._adopt_broker_orders(broker_orders)
        gate = self._account_gate(account, broker_orders)
        live_opened, live_closed = await self._reconcile_orders(
            broker_orders, sessions, account, now
        )
        timed_exits = await self._submit_due_time_exits(now)
        submitted = 0
        if gate == "account_ready":
            submitted = await self._submit_due_entries(
                account,
                now,
                enforce_wall_clock=enforce_wall_clock,
            )
        else:
            self.store.event("live_gate_blocked", level="warning", reason=gate)
        return CycleResult(
            **{
                **asdict(result),
                "live_submitted": submitted,
                "live_opened": live_opened,
                "live_closed": live_closed,
                "live_gate": f"{gate};time_exits={timed_exits}",
            }
        )

    def _adopt_broker_orders(self, orders: Sequence[BrokerOrder]) -> None:
        """Bind broker-accepted orders that survived a crash before the SQLite commit."""

        rejected_statuses = {"cancelled", "apicancelled", "inactive"}
        by_ref = {
            order.order_ref: order
            for order in orders
            if order.status.lower() not in rejected_statuses
        }
        for row in self.store.rows("live_state='queued'"):
            token = broker_token(str(row["packet_id"]))
            order = by_ref.get(f"IA-E07-{token}-ENTRY")
            if order is None:
                continue
            self.store.update(
                str(row["packet_id"]),
                live_state="submitted",
                parent_order_id=order.order_id,
                live_quantity=int(row["planned_quantity"]),
            )
            self.store.event(
                "orphan_entry_order_adopted",
                packet_id=str(row["packet_id"]),
                level="warning",
                order_id=order.order_id,
                status=order.status,
            )
        exit_suffixes = ("TIME", "OVERDUE", "QTYFAIL")
        for row in self.store.rows("live_state='open' AND timed_exit_order_id IS NULL"):
            token = broker_token(str(row["packet_id"]))
            order = next(
                (
                    by_ref.get(f"IA-E07-{token}-{suffix}")
                    for suffix in exit_suffixes
                    if by_ref.get(f"IA-E07-{token}-{suffix}") is not None
                ),
                None,
            )
            if order is None:
                continue
            self.store.update(
                str(row["packet_id"]),
                live_state="closing",
                timed_exit_order_id=order.order_id,
            )
            self.store.event(
                "orphan_exit_order_adopted",
                packet_id=str(row["packet_id"]),
                level="warning",
                order_id=order.order_id,
                status=order.status,
            )

    def _account_gate(
        self,
        account: AccountSnapshot,
        broker_orders: Sequence[BrokerOrder],
    ) -> str:
        if self.config.account is not None and account.account != self.config.account:
            return "configured_account_mismatch"
        if not (
            math.isfinite(account.net_liquidation)
            and math.isfinite(account.available_funds)
            and math.isfinite(account.settled_cash)
        ):
            return "insufficient_or_unsettled_cash"
        if account.net_liquidation <= 0 or account.settled_cash < self.config.cash_reserve:
            return "insufficient_or_unsettled_cash"
        known_open_symbols = {
            str(row["symbol"])
            for row in self.store.rows("live_state IN ('submitted','open','closing')")
        }
        unexpected = {
            symbol
            for symbol, quantity in account.positions.items()
            if quantity != 0 and symbol not in known_open_symbols
        }
        if unexpected:
            return "unexpected_broker_position"
        expected_position_symbols = {
            str(row["symbol"]) for row in self.store.rows("live_state IN ('open','closing')")
        }
        if any(account.positions.get(symbol, 0.0) == 0 for symbol in expected_position_symbols):
            return "expected_broker_position_missing"
        active_statuses = {"pendingsubmit", "presubmitted", "submitted", "pendingcancel"}
        visible_canary_open_orders = sum(
            order.status.lower() in active_statuses for order in broker_orders
        )
        if account.open_order_count > visible_canary_open_orders:
            return "unexpected_non_canary_open_order"
        return "account_ready"

    async def _submit_due_entries(
        self,
        account: AccountSnapshot,
        now: datetime,
        *,
        enforce_wall_clock: bool,
    ) -> int:
        local = now.astimezone(NEW_YORK)
        for row in self.store.rows(
            "live_state='queued' AND entry_session<?", (local.date().isoformat(),)
        ):
            self.store.update(str(row["packet_id"]), live_state="entry_window_missed")
            self.store.event(
                "live_entry_window_missed",
                packet_id=str(row["packet_id"]),
                level="warning",
            )
        if not (
            self.config.entry_submission_start
            <= local.time()
            < self.config.entry_submission_deadline
        ):
            return 0
        today = local.date().isoformat()
        active = self.store.rows("live_state IN ('submitted','open','closing')")
        capacity = max(0, self.config.max_live_slots - len(active))
        if capacity == 0:
            return 0
        queued = self.store.rows("live_state='queued' AND entry_session=?", (today,))
        settled_available = (
            min(account.settled_cash, account.available_funds) - self.config.cash_reserve
        )
        if not math.isfinite(settled_available):
            return 0
        submitted = 0
        active_symbols = {str(row["symbol"]) for row in active}
        selected: list[sqlite3.Row] = []
        for row in queued:
            symbol = str(row["symbol"])
            if symbol in active_symbols:
                self.store.update(str(row["packet_id"]), live_state="overlap_suppressed")
                continue
            active_symbols.add(symbol)
            selected.append(row)
        for index, row in enumerate(selected):
            if submitted >= capacity:
                self.store.update(str(row["packet_id"]), live_state="capacity_suppressed")
                continue
            quantity = int(row["planned_quantity"])
            reference = float(row["prior_close"])
            planned_notional = quantity * reference
            if planned_notional * (1.0 + self.config.market_order_cushion) > settled_available:
                self.store.update(str(row["packet_id"]), live_state="cash_suppressed")
                continue
            preview = await self.broker.preview_entry(str(row["symbol"]), quantity)
            if (
                not preview.commission_valid
                and self.config.invalid_commission_handling != "fallback_to_cap"
            ):
                rejection_warning = preview.warning or preview.commission_error
                self.store.update(
                    str(row["packet_id"]),
                    live_state="preflight_rejected",
                )
                self.store.event(
                    "entry_preflight_rejected",
                    packet_id=str(row["packet_id"]),
                    level="warning",
                    commission=_finite_number_or_none(preview.commission),
                    currency=preview.currency,
                    warning=rejection_warning,
                    commission_bps=None,
                    commission_mode="invalid_rejected",
                    commission_estimate_source=preview.estimate_source,
                    min_commission=preview.min_commission,
                    max_commission=preview.max_commission,
                )
                continue
            preview_warning = preview.warning or ""
            warning_is_real = bool(preview_warning and preview_warning != preview.commission_error)
            planned_commission = (
                preview.commission
                if preview.commission_valid
                else self.config.max_one_way_commission
            )
            commission_bps = planned_commission / planned_notional * 10_000.0
            commission_currency = (
                preview.currency.upper() if preview.commission_valid and preview.currency else "USD"
            )
            if (
                commission_currency != "USD"
                or warning_is_real
                or planned_commission > self.config.max_one_way_commission
                or commission_bps > self.config.max_one_way_commission_bps
            ):
                self.store.update(str(row["packet_id"]), live_state="preflight_rejected")
                self.store.event(
                    "entry_preflight_rejected",
                    packet_id=str(row["packet_id"]),
                    level="warning",
                    commission=planned_commission,
                    currency=commission_currency,
                    warning=preview_warning,
                    commission_bps=commission_bps,
                    commission_mode=(
                        "fallback_to_cap"
                        if not preview.commission_valid
                        else preview.estimate_source
                    ),
                    min_commission=preview.min_commission,
                    max_commission=preview.max_commission,
                )
                continue
            token = broker_token(str(row["packet_id"]))
            order_ref = f"IA-E07-{token}-ENTRY"
            if enforce_wall_clock:
                send_time = datetime.now(NEW_YORK).time()
                if not (
                    self.config.entry_submission_start
                    <= send_time
                    < self.config.entry_submission_deadline
                ):
                    for pending in selected[index:]:
                        self.store.update(
                            str(pending["packet_id"]),
                            live_state="entry_window_missed",
                        )
                        self.store.event(
                            "entry_send_boundary_missed",
                            packet_id=str(pending["packet_id"]),
                            level="warning",
                            observed_time=send_time.isoformat(),
                        )
                    break
            order_id = await self.broker.submit_market_on_open(
                str(row["symbol"]), quantity, order_ref
            )
            self.store.update(
                str(row["packet_id"]),
                live_state="submitted",
                parent_order_id=order_id,
                live_quantity=quantity,
            )
            self.store.event(
                "live_entry_submitted",
                packet_id=str(row["packet_id"]),
                order_id=order_id,
                quantity=quantity,
                reference_price=reference,
                commission_preview=planned_commission,
                commission_mode=(
                    "fallback_to_cap" if not preview.commission_valid else preview.estimate_source
                ),
                min_commission=preview.min_commission,
                max_commission=preview.max_commission,
            )
            settled_available -= planned_notional * (1.0 + self.config.market_order_cushion)
            submitted += 1
        return submitted

    async def _reconcile_orders(
        self,
        orders: Sequence[BrokerOrder],
        sessions: Sequence[date],
        account: AccountSnapshot,
        now: datetime,
    ) -> tuple[int, int]:
        by_id = {order.order_id: order for order in orders}
        by_ref = {order.order_ref: order for order in orders}
        opened = closed = 0
        newly_protected: set[str] = set()
        for row in self.store.rows("live_state='submitted'"):
            parent_id = row["parent_order_id"]
            order = by_id.get(int(parent_id)) if parent_id is not None else None
            if order is None:
                symbol = str(row["symbol"])
                recovered_quantity = int(round(account.positions.get(symbol, 0.0)))
                recovered_price = account.average_costs.get(symbol, 0.0)
                if recovered_quantity > 0 and recovered_price > 0:
                    await self._protect_position(
                        row,
                        quantity=recovered_quantity,
                        price=recovered_price,
                        sessions=sessions,
                        recovery="broker_average_cost",
                        commission=None,
                    )
                    newly_protected.add(str(row["packet_id"]))
                    opened += 1
                continue
            if order.filled <= 0 and order.status.lower() in {
                "cancelled",
                "apicancelled",
                "inactive",
            }:
                self.store.update(str(row["packet_id"]), live_state="entry_failed")
                continue
            if order.filled <= 0 or order.average_fill_price <= 0:
                continue
            actual_quantity = int(round(account.positions.get(str(row["symbol"]), 0.0)))
            if actual_quantity <= 0:
                self.store.update(
                    str(row["packet_id"]),
                    live_state="closed_unattributed",
                    live_exit_at=datetime.now(UTC).isoformat(),
                    live_exit_reason="entry_fill_visible_but_broker_flat",
                )
                self.store.event(
                    "entry_fill_broker_flat",
                    packet_id=str(row["packet_id"]),
                    level="critical",
                    filled=order.filled,
                )
                closed += 1
                continue
            if order.remaining > 0:
                await self.broker.cancel_order(order.order_id)
                self.store.event(
                    "partial_entry_cancelled",
                    packet_id=str(row["packet_id"]),
                    level="critical",
                    filled=order.filled,
                    remaining=order.remaining,
                )
            await self._protect_position(
                row,
                quantity=actual_quantity,
                price=order.average_fill_price,
                sessions=sessions,
                recovery="order_fill",
                commission=order.commission,
            )
            newly_protected.add(str(row["packet_id"]))
            opened += 1
        for row in self.store.rows("live_state IN ('open','closing')"):
            token = broker_token(str(row["packet_id"]))
            parent = (
                by_id.get(int(row["parent_order_id"])) if row["parent_order_id"] else None
            ) or by_ref.get(f"IA-E07-{token}-ENTRY")
            if (
                row["live_entry_commission"] is None
                and parent is not None
                and parent.commission is not None
            ):
                self.store.update(str(row["packet_id"]), live_entry_commission=parent.commission)
            if str(row["packet_id"]) in newly_protected:
                continue
            target = (
                by_id.get(int(row["target_order_id"])) if row["target_order_id"] else None
            ) or by_ref.get(f"IA-E07-{token}-TARGET")
            stop = (
                by_id.get(int(row["stop_order_id"])) if row["stop_order_id"] else None
            ) or by_ref.get(f"IA-E07-{token}-STOP")
            timed = (
                by_id.get(int(row["timed_exit_order_id"])) if row["timed_exit_order_id"] else None
            )
            filled_exit = next(
                (order for order in (stop, target, timed) if order and order.filled > 0),
                None,
            )
            if filled_exit is None:
                current_position_quantity = account.positions.get(str(row["symbol"]), 0.0)
                expected_quantity = float(row["live_quantity"] or 0.0)
                if current_position_quantity == 0 and expected_quantity > 0:
                    self.store.update(
                        str(row["packet_id"]),
                        live_state="closed_unattributed",
                        live_exit_at=datetime.now(UTC).isoformat(),
                        live_exit_reason="broker_flat_without_visible_execution",
                    )
                    self.store.event(
                        "broker_position_missing",
                        packet_id=str(row["packet_id"]),
                        level="critical",
                        expected_quantity=expected_quantity,
                    )
                    closed += 1
                elif (
                    current_position_quantity > 0
                    and current_position_quantity != expected_quantity
                    and row["timed_exit_order_id"] is None
                ):
                    token = broker_token(str(row["packet_id"]))
                    order_id = await self.broker.submit_market_exit(
                        str(row["symbol"]),
                        int(round(current_position_quantity)),
                        oca_group=str(row["oca_group"]),
                        order_ref=f"IA-E07-{token}-QTYFAIL",
                    )
                    self.store.update(
                        str(row["packet_id"]),
                        live_state="closing",
                        timed_exit_order_id=order_id,
                    )
                    self.store.event(
                        "position_quantity_mismatch_flattening",
                        packet_id=str(row["packet_id"]),
                        level="critical",
                        expected_quantity=expected_quantity,
                        actual_quantity=current_position_quantity,
                        order_id=order_id,
                    )
                elif current_position_quantity > 0:
                    protective_statuses = {
                        "pendingsubmit",
                        "presubmitted",
                        "submitted",
                        "pendingcancel",
                    }
                    target_present = (
                        target is not None and target.status.lower() in protective_statuses
                    )
                    stop_present = stop is not None and stop.status.lower() in protective_statuses
                    if not target_present or not stop_present:
                        entry_price = float(row["live_entry_price"] or 0.0)
                        if entry_price <= 0:
                            entry_price = account.average_costs.get(str(row["symbol"]), 0.0)
                        if entry_price <= 0:
                            self.store.event(
                                "protective_order_repair_price_missing",
                                packet_id=str(row["packet_id"]),
                                level="critical",
                            )
                            continue
                        target_id, stop_id = await self.broker.submit_protective_oca(
                            str(row["symbol"]),
                            int(round(current_position_quantity)),
                            stop_price=round_stock_price(
                                entry_price * (1.0 - self.config.stop_loss_pct)
                            ),
                            target_price=round_stock_price(
                                entry_price * (1.0 + self.config.take_profit_pct)
                            ),
                            oca_group=str(row["oca_group"]),
                            order_ref_prefix=f"IA-E07-{token}",
                        )
                        self.store.update(
                            str(row["packet_id"]),
                            target_order_id=target_id,
                            stop_order_id=stop_id,
                        )
                        self.store.event(
                            "protective_orders_repaired",
                            packet_id=str(row["packet_id"]),
                            level="critical",
                            target_was_present=target_present,
                            stop_was_present=stop_present,
                            target_order_id=target_id,
                            stop_order_id=stop_id,
                        )
                continue
            reason = (
                "stop"
                if filled_exit.kind == "stop"
                else ("target" if filled_exit.kind == "target" else "time")
            )
            self.store.update(
                str(row["packet_id"]),
                live_state="closed",
                live_exit_price=filled_exit.average_fill_price,
                live_exit_commission=filled_exit.commission,
                live_exit_at=datetime.now(UTC).isoformat(),
                live_exit_reason=reason,
            )
            self.store.event(
                "live_position_closed",
                packet_id=str(row["packet_id"]),
                reason=reason,
                price=filled_exit.average_fill_price,
            )
            closed += 1
        today = now.astimezone(NEW_YORK).date()
        for row in self.store.rows("live_state='submitted'"):
            parent_id = int(row["parent_order_id"]) if row["parent_order_id"] else None
            if (
                date.fromisoformat(str(row["entry_session"])) < today
                and parent_id not in by_id
                and account.positions.get(str(row["symbol"]), 0.0) == 0
            ):
                self.store.update(str(row["packet_id"]), live_state="entry_expired_unknown")
                self.store.event(
                    "submitted_entry_missing_after_session",
                    packet_id=str(row["packet_id"]),
                    level="critical",
                )
        return opened, closed

    async def _protect_position(
        self,
        row: sqlite3.Row,
        *,
        quantity: int,
        price: float,
        sessions: Sequence[date],
        recovery: str,
        commission: float | None,
    ) -> None:
        token = broker_token(str(row["packet_id"]))
        oca_group = f"IA-E07-{token}-EXIT"
        target_id, stop_id = await self.broker.submit_protective_oca(
            str(row["symbol"]),
            quantity,
            stop_price=round_stock_price(price * (1.0 - self.config.stop_loss_pct)),
            target_price=round_stock_price(price * (1.0 + self.config.take_profit_pct)),
            oca_group=oca_group,
            order_ref_prefix=f"IA-E07-{token}",
        )
        entry_day = date.fromisoformat(str(row["entry_session"]))
        future = [session for session in sessions if session >= entry_day]
        exit_session = (
            future[self.config.max_sessions - 1]
            if len(future) >= self.config.max_sessions
            else None
        )
        self.store.update(
            str(row["packet_id"]),
            live_state="open",
            live_quantity=quantity,
            live_entry_price=price,
            live_entry_commission=commission,
            live_entry_at=datetime.now(UTC).isoformat(),
            live_exit_session=exit_session.isoformat() if exit_session else None,
            oca_group=oca_group,
            target_order_id=target_id,
            stop_order_id=stop_id,
        )
        self.store.event(
            "live_position_protected",
            packet_id=str(row["packet_id"]),
            quantity=quantity,
            fill_price=price,
            target_order_id=target_id,
            stop_order_id=stop_id,
            exit_session=exit_session.isoformat() if exit_session else None,
            recovery=recovery,
        )

    async def _submit_due_time_exits(self, now: datetime) -> int:
        local = now.astimezone(NEW_YORK)
        count = 0
        for row in self.store.rows(
            "live_state='open' AND live_exit_session<=? AND timed_exit_order_id IS NULL",
            (local.date().isoformat(),),
        ):
            due_date = date.fromisoformat(str(row["live_exit_session"]))
            is_overdue = due_date < local.date()
            after_moc_cutoff = local.time() >= self.config.moc_submission_deadline
            if not is_overdue and local.time() < self.config.timed_exit_submission_time:
                continue
            token = broker_token(str(row["packet_id"]))
            if is_overdue or after_moc_cutoff:
                order_id = await self.broker.submit_market_exit(
                    str(row["symbol"]),
                    int(row["live_quantity"]),
                    oca_group=str(row["oca_group"]),
                    order_ref=f"IA-E07-{token}-OVERDUE",
                )
                event_type = "overdue_market_exit_submitted"
            else:
                order_id = await self.broker.submit_market_on_close(
                    str(row["symbol"]),
                    int(row["live_quantity"]),
                    oca_group=str(row["oca_group"]),
                    order_ref=f"IA-E07-{token}-TIME",
                )
                event_type = "timed_exit_submitted"
            self.store.update(
                str(row["packet_id"]),
                live_state="closing",
                timed_exit_order_id=order_id,
            )
            self.store.event(
                event_type,
                packet_id=str(row["packet_id"]),
                order_id=order_id,
            )
            count += 1
        return count


def status_report(ledger_db: str) -> dict[str, Any]:
    store = CanaryStore(ledger_db)
    with store.connect() as conn:
        activation = conn.execute(
            "SELECT value FROM metadata WHERE key='activation_utc'"
        ).fetchone()
        metadata = {
            str(row["key"]): str(row["value"])
            for row in conn.execute("SELECT key,value FROM metadata")
        }
        candidate_counts = {
            str(row["live_state"]): int(row["count"])
            for row in conn.execute(
                "SELECT live_state,COUNT(*) AS count FROM candidates GROUP BY live_state"
            )
        }
        shadow_count = int(conn.execute("SELECT COUNT(*) FROM shadow_trades").fetchone()[0])
        recent_events = [
            dict(row)
            for row in conn.execute(
                "SELECT occurred_at,level,event_type,packet_id,detail_json "
                "FROM events ORDER BY id DESC LIMIT 20"
            )
        ]
    current_fingerprint = runtime_source_fingerprint()
    runtime_fingerprint = metadata.get("runtime_source_fingerprint")
    return {
        "activation_utc": str(activation["value"]) if activation else None,
        "runtime_started_utc": metadata.get("runtime_started_utc"),
        "last_cycle_started_utc": metadata.get("last_cycle_started_utc"),
        "last_cycle_success_utc": metadata.get("last_cycle_success_utc"),
        "last_cycle_error_utc": metadata.get("last_cycle_error_utc"),
        "last_cycle_error": metadata.get("last_cycle_error") or None,
        "runtime_source_fingerprint": runtime_fingerprint,
        "current_source_fingerprint": current_fingerprint,
        "source_revision_current": runtime_fingerprint == current_fingerprint,
        "live_states": candidate_counts,
        "closed_shadow_trades": shadow_count,
        "recent_events": recent_events,
    }
