"""Equal-cash E07 exit-width replay over qualified, sealed research inputs."""

from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import ROUND_HALF_UP, Decimal
from typing import Literal

import rfc8785

from e07_volatility_exits.auction import (
    Auction,
    AuctionFill,
    AuctionMiss,
    EntryConvention,
    EntryPath,
    SessionHours,
    valid_bar,
    validate_opening_evidence,
)
from e07_volatility_exits.auction import BarKey as BarKey
from e07_volatility_exits.auction import UnavailableExecution as UnavailableExecution
from e07_volatility_exits.features import (
    BASE_WIDTH,
    FEATURE_CUTOFF,
    MAX_WIDTH,
    MIN_WIDTH,
    ExitWidths,
)
from insider_alerts.backtest.models import DailyBar
from insider_alerts.strategy import e07

Arm = Literal["fixed", "scaled", "matched"]
CollisionPolicy = Literal["stop_first", "target_first"]
CENT = Decimal("0.01")
ZERO = Decimal("0")
RESERVE = Decimal("50")
CUSHION = Decimal("1.05")
COMMISSION_CAP = Decimal("0.75")
COMMISSION_RATE_CAP = Decimal("0.005")
SLOT_COUNT = 2


@dataclass(frozen=True, slots=True)
class ReplayPolicy:
    """Research copy of the frozen E07 sizing, ranking, and exit settings."""

    lottery_salt: str = "E07-F00-live-canary-v1"
    entry_submission_deadline: time = time(9, 20)
    min_price: float = 2.0
    max_price: float = 200.0
    min_median_dollar_volume_20d: float = 500_000.0
    slot_budget: float = 200.0
    stop_loss_pct: float = BASE_WIDTH
    take_profit_pct: float = BASE_WIDTH
    max_sessions: int = 10


POLICY = ReplayPolicy()


@dataclass(frozen=True, slots=True)
class OrdinaryCashDistribution:
    """Verified per-share USD entitlement and pre-open cash availability."""

    event_id: str
    cash_per_share: Decimal
    payment_date: date
    cash_available_on: date

    def __post_init__(self) -> None:
        if (
            not self.event_id
            or self.event_id != self.event_id.strip()
            or not isinstance(self.cash_per_share, Decimal)
            or not self.cash_per_share.is_finite()
            or self.cash_per_share <= ZERO
            or self.cash_available_on < self.payment_date
        ):
            raise ValueError("invalid ordinary cash distribution")


@dataclass(frozen=True, slots=True)
class UnsupportedAction:
    """A verified event that requires different execution mechanics."""

    event_id: str
    reason: str

    def __post_init__(self) -> None:
        if not self.event_id or self.event_id != self.event_id.strip() or not self.reason.strip():
            raise ValueError("unsupported action requires an event identity and reason")


Action = OrdinaryCashDistribution | UnsupportedAction
ActionCoverage = Mapping[BarKey, tuple[Action, ...]]


@dataclass(frozen=True, slots=True)
class ReplayCandidate:
    packet_id: str
    accession_number: str
    symbol: str
    signal_at: datetime
    instrument: str
    entry_day: date
    planning_close: float
    widths: ExitWidths


@dataclass(frozen=True, slots=True)
class ReplayCosts:
    """A research commission proxy and separately charged adverse slippage."""

    one_way_commission: Decimal
    one_way_slippage_bps: Decimal

    def __post_init__(self) -> None:
        if (
            not self.one_way_commission.is_finite()
            or not self.one_way_slippage_bps.is_finite()
            or not ZERO <= self.one_way_commission <= COMMISSION_CAP
            or not ZERO <= self.one_way_slippage_bps < Decimal("10000")
        ):
            raise ValueError("invalid replay costs")

    def friction(self, notional: Decimal) -> Decimal:
        slippage = notional * self.one_way_slippage_bps / Decimal("10000")
        return (self.one_way_commission + slippage).quantize(CENT, rounding=ROUND_HALF_UP)


@dataclass(frozen=True, slots=True)
class EntryDecision:
    packet_id: str
    state: Literal[
        "opened",
        "overlap_suppressed",
        "capacity_suppressed",
        "cash_suppressed",
        "commission_suppressed",
        "no_auction_fill",
    ]


@dataclass(frozen=True, slots=True)
class PositionClose:
    packet_id: str
    session: date
    reason: str
    same_bar_collision: bool
    ambiguous_collision: bool
    net_pnl: Decimal
    cash_distributions: Decimal


@dataclass(frozen=True, slots=True)
class SessionResult:
    session: date
    equity: Decimal
    settled_cash: Decimal
    unsettled_cash: Decimal
    open_positions: int
    entries: tuple[EntryDecision, ...]
    exits: tuple[PositionClose, ...]
    distribution_receivable: Decimal


@dataclass(frozen=True, slots=True)
class PortfolioResult:
    initial_cash: Decimal
    sessions: tuple[SessionResult, ...]

    @property
    def net_pnl(self) -> Decimal:
        return self.sessions[-1].equity - self.initial_cash


@dataclass(frozen=True, slots=True)
class ExitReplay:
    fixed: PortfolioResult
    scaled: PortfolioResult
    matched: PortfolioResult
    action_coverage_sha256: str
    execution_evidence_sha256: str

    @property
    def paired_daily_pnl(self) -> tuple[Decimal, ...]:
        return self._paired_daily_pnl(self.fixed)

    @property
    def scaled_vs_matched_daily_pnl(self) -> tuple[Decimal, ...]:
        return self._paired_daily_pnl(self.matched)

    def _paired_daily_pnl(self, comparison: PortfolioResult) -> tuple[Decimal, ...]:
        difference = ZERO
        increments = []
        for other, scaled in zip(comparison.sessions, self.scaled.sessions, strict=True):
            cumulative = scaled.equity - other.equity
            increments.append(cumulative - difference)
            difference = cumulative
        return tuple(increments)


@dataclass(slots=True)
class _Position:
    candidate: ReplayCandidate
    quantity: int
    entry_debit: Decimal
    stop: float
    target: float
    entry_path: EntryPath
    completed_sessions: int = 0
    cash_distributions: Decimal = ZERO


@dataclass(frozen=True, slots=True)
class _Commitment:
    candidate: ReplayCandidate
    quantity: int


def _notional(quantity: int, price: float) -> Decimal:
    return (quantity * Decimal(str(price))).quantize(CENT, rounding=ROUND_HALF_UP)


def _bar(bars: Mapping[BarKey, DailyBar], candidate: ReplayCandidate, session: date) -> DailyBar:
    bar = bars.get((candidate.instrument, session))
    if bar is None or not valid_bar(bar, candidate.symbol, session):
        raise UnavailableExecution(
            f"missing or invalid execution bar for {candidate.packet_id} on {session}"
        )
    return bar


def _ranked_entries(
    candidates: Sequence[ReplayCandidate], sessions: Sequence[date], previous_session: date
) -> dict[date, list[ReplayCandidate]]:
    groups: dict[date, list[ReplayCandidate]] = defaultdict(list)
    allowed = set(sessions[: len(sessions) - POLICY.max_sessions + 1])
    cohort_calendar = (previous_session, *sessions)
    seen: set[str] = set()
    for candidate in candidates:
        if (
            not candidate.packet_id
            or candidate.packet_id in seen
            or not candidate.accession_number
            or not candidate.symbol
            or not candidate.instrument
            or candidate.entry_day not in allowed
            or candidate.signal_at.utcoffset() is None
            or e07.entry_session(
                POLICY, candidate.signal_at, cohort_calendar, now=candidate.signal_at
            )
            != candidate.entry_day
            or not POLICY.min_price <= candidate.planning_close <= POLICY.max_price
            or candidate.widths.instrument != candidate.instrument
            or candidate.widths.cutoff.utcoffset() is None
            or candidate.widths.cutoff
            != datetime.combine(candidate.entry_day, FEATURE_CUTOFF, e07.NEW_YORK)
            or not MIN_WIDTH <= candidate.widths.scaled <= MAX_WIDTH
            or not MIN_WIDTH <= candidate.widths.matched <= MAX_WIDTH
            or (
                candidate.widths.missing is not None
                and (
                    candidate.widths.scaled != BASE_WIDTH or candidate.widths.matched != BASE_WIDTH
                )
            )
        ):
            raise ValueError(f"invalid replay candidate {candidate.packet_id!r}")
        seen.add(candidate.packet_id)
        groups[candidate.entry_day].append(candidate)

    for session, entries in groups.items():
        entries.sort(key=lambda row: (e07.deterministic_rank(POLICY, row, session), row.packet_id))
    return groups


def _select_auctions(
    arrivals: Mapping[date, list[ReplayCandidate]], evidence: Mapping[BarKey, Auction]
) -> tuple[dict[BarKey, str], dict[BarKey, Auction], frozenset[BarKey]]:
    names: dict[BarKey, str] = {}
    auctions: dict[BarKey, Auction] = {}
    for day, candidates in arrivals.items():
        for candidate in candidates:
            key = (candidate.instrument, day)
            if names.setdefault(key, candidate.symbol) != candidate.symbol:
                raise UnavailableExecution("conflicting symbols for one opening execution key")
            observation = evidence.get(key)
            if not isinstance(observation, (AuctionFill, AuctionMiss)):
                raise UnavailableExecution(
                    f"missing or invalid opening evidence for {candidate.packet_id} on {day}"
                )
            auctions[key] = observation
    return (
        names,
        auctions,
        frozenset(
            key for key, observation in auctions.items() if isinstance(observation, AuctionMiss)
        ),
    )


def _holding_bars(
    sessions: Sequence[date],
    arrivals: Mapping[date, list[ReplayCandidate]],
    misses: frozenset[BarKey],
    bars: Mapping[BarKey, DailyBar],
) -> dict[BarKey, DailyBar]:
    covered = {}
    for index, entry in enumerate(sessions):
        for candidate in arrivals.get(entry, []):
            if (candidate.instrument, entry) in misses:
                continue
            for held in sessions[index + 1 : index + POLICY.max_sessions]:
                covered[candidate.instrument, held] = _bar(bars, candidate, held)
    return covered


def _width(candidate: ReplayCandidate, arm: Arm) -> float:
    if arm == "scaled":
        return candidate.widths.scaled
    if arm == "matched":
        return candidate.widths.matched
    return BASE_WIDTH


def _canonical_amount(amount: Decimal) -> str:
    _, digits, exponent = amount.as_tuple()
    coefficient = "".join(map(str, digits))
    significant = coefficient.rstrip("0")
    scale = int(exponent) + len(coefficient) - len(significant)
    return f"{significant}e{scale}"


def _bind_actions(
    sessions: Sequence[date],
    arrivals: Mapping[date, list[ReplayCandidate]],
    coverage: ActionCoverage,
    confirmed_auction_misses: frozenset[BarKey],
) -> tuple[dict[BarKey, tuple[OrdinaryCashDistribution, ...]], str]:
    """Freeze action coverage for every potential holding window before any fills."""
    required: dict[BarKey, bool] = {}
    for index, entry_day in enumerate(sessions):
        holding_window = sessions[index : index + POLICY.max_sessions]
        for candidate in arrivals.get(entry_day, []):
            can_fill = (candidate.instrument, entry_day) not in confirmed_auction_misses
            for held_day in holding_window:
                key = (candidate.instrument, held_day)
                required[key] = can_fill or required.get(key, False)
    missing = required.keys() - coverage.keys()
    if missing:
        raise UnavailableExecution(
            f"missing corporate-action coverage for {len(missing)} instrument sessions"
        )

    bound: dict[BarKey, tuple[OrdinaryCashDistribution, ...]] = {}
    cash_events: set[tuple[str, str]] = set()
    material = []
    live_sessions = set(sessions)
    for instrument, session in sorted(required):
        key = (instrument, session)
        events = coverage[key]
        if any(
            not isinstance(event, (OrdinaryCashDistribution, UnsupportedAction)) for event in events
        ):
            raise ValueError("unknown corporate-action type")
        if len({event.event_id for event in events}) != len(events):
            raise ValueError("duplicate corporate-action event in one instrument session")
        distributions = []
        encoded = []
        for event in sorted(events, key=lambda event: event.event_id):
            if isinstance(event, OrdinaryCashDistribution):
                available = event.cash_available_on
                if event.payment_date < session or available <= session:
                    raise ValueError("invalid distribution timing for ex-session")
                if available <= sessions[-1] and available not in live_sessions:
                    raise ValueError("distribution cash availability requires an exchange session")
                event_key = (instrument, event.event_id)
                if event_key in cash_events:
                    raise ValueError("duplicate distribution event for an instrument")
                cash_events.add(event_key)
                distributions.append(event)
                encoded.append(
                    [
                        "ordinary_cash",
                        event.event_id,
                        _canonical_amount(event.cash_per_share),
                        event.payment_date.isoformat(),
                        available.isoformat(),
                    ]
                )
            else:
                if required[key]:
                    raise UnavailableExecution(
                        f"unsupported corporate action for {instrument} on {session}: "
                        f"{event.event_id} ({event.reason})"
                    )
                encoded.append(["unsupported", event.event_id, event.reason])
        bound[key] = tuple(distributions)
        material.append([instrument, session.isoformat(), encoded])
    digest = hashlib.sha256(rfc8785.dumps(["e07-replay-action-coverage-v1", material])).hexdigest()
    return bound, digest


def _barriers(price: float, width: float) -> tuple[float, float]:
    return price * (1.0 - width), price * (1.0 + width)


def _require_resolved_entry_prices(
    arrivals: Mapping[date, list[ReplayCandidate]],
    auctions: Mapping[BarKey, Auction],
    arms: tuple[Arm, ...],
) -> None:
    for day, candidates in arrivals.items():
        for candidate in candidates:
            auction = auctions[candidate.instrument, day]
            if (
                isinstance(auction, AuctionMiss)
                or auction.path.first_low == auction.path.first_high
            ):
                continue
            for arm in arms:
                stop, target = _barriers(auction.price, _width(candidate, arm))
                if auction.path.first_low <= stop or auction.path.first_high >= target:
                    raise UnavailableExecution(
                        f"first protected second can touch a barrier for {candidate.packet_id}"
                    )


def _exit(
    position: _Position,
    prices: DailyBar | EntryPath,
    collision_rule: CollisionPolicy,
    *,
    close_can_trigger: bool,
) -> tuple[float, str, bool, bool] | None:
    stop, target = position.stop, position.target
    first = prices.first_low if isinstance(prices, EntryPath) else prices.open
    stop_hit, target_hit = prices.low <= stop, prices.high >= target
    collision = stop_hit and target_hit
    ambiguous = collision and stop < first < target
    if isinstance(prices, EntryPath):
        if first <= stop:
            return first, "stop", collision, False
        if first >= target:
            return first, "target", collision, False
    if collision and collision_rule == "target_first" and first > stop:
        return max(target, first), "stop_and_target_same_day_target_assumed", True, ambiguous
    if stop_hit:
        reason = "stop_and_target_same_day_stop_assumed" if collision else "stop"
        return min(stop, first), reason, collision, ambiguous
    if target_hit:
        return max(target, first), "target", False, False
    if isinstance(prices, EntryPath) and close_can_trigger:
        if prices.close <= stop:
            return prices.close, "closing_stop", False, False
        if prices.close >= target:
            return prices.close, "closing_target", False, False
    if position.completed_sessions == POLICY.max_sessions:
        return prices.close, "time", False, False
    return None


@dataclass(slots=True)
class _Book:
    cash: Decimal
    costs: ReplayCosts
    arm: Arm
    collision: CollisionPolicy
    positions: dict[str, _Position] = field(default_factory=dict)
    receivables: dict[date, Decimal] = field(default_factory=dict)
    distribution_receivables: dict[date, Decimal] = field(default_factory=dict)

    def settle(self, session: date) -> None:
        for pending in (self.receivables, self.distribution_receivables):
            for due in tuple(pending):
                if due <= session:
                    self.cash += pending.pop(due)

    def accrue_distributions(
        self, coverage: Mapping[BarKey, tuple[OrdinaryCashDistribution, ...]], session: date
    ) -> None:
        for instrument, position in self.positions.items():
            for event in coverage[instrument, session]:
                amount = (position.quantity * event.cash_per_share).quantize(
                    CENT, rounding=ROUND_HALF_UP
                )
                due = event.cash_available_on
                self.distribution_receivables[due] = (
                    self.distribution_receivables.get(due, ZERO) + amount
                )
                position.cash_distributions += amount

    def _commit(
        self, candidates: Sequence[ReplayCandidate]
    ) -> tuple[list[_Commitment], dict[str, EntryDecision]]:
        commitments: list[_Commitment] = []
        decisions: dict[str, EntryDecision] = {}
        symbols = {position.candidate.symbol for position in self.positions.values()}
        instruments = set(self.positions)
        available = self.cash - RESERVE
        for candidate in candidates:
            packet = candidate.packet_id
            if candidate.symbol in symbols or candidate.instrument in instruments:
                decisions[packet] = EntryDecision(packet, "overlap_suppressed")
                continue
            symbols.add(candidate.symbol)
            instruments.add(candidate.instrument)
            if len(self.positions) + len(commitments) >= SLOT_COUNT:
                decisions[packet] = EntryDecision(packet, "capacity_suppressed")
                continue

            quantity = e07.planned_quantity(POLICY, candidate.planning_close)
            planned = Decimal(str(candidate.planning_close)) * quantity
            required = planned * CUSHION + 2 * COMMISSION_CAP
            if required > available:
                decisions[packet] = EntryDecision(packet, "cash_suppressed")
            elif self.costs.one_way_commission > planned * COMMISSION_RATE_CAP:
                decisions[packet] = EntryDecision(packet, "commission_suppressed")
            else:
                reserve = planned * CUSHION + self.costs.one_way_commission + COMMISSION_CAP
                available -= reserve
                commitments.append(_Commitment(candidate, quantity))
        return commitments, decisions

    def enter(
        self,
        candidates: Sequence[ReplayCandidate],
        auctions: Mapping[BarKey, Auction],
        session: date,
    ) -> tuple[EntryDecision, ...]:
        commitments, decisions = self._commit(candidates)
        for commitment in commitments:
            candidate = commitment.candidate
            packet = candidate.packet_id
            auction = auctions[candidate.instrument, session]
            if isinstance(auction, AuctionMiss):
                decisions[packet] = EntryDecision(packet, "no_auction_fill")
                continue

            notional = _notional(commitment.quantity, auction.price)
            debit = notional + self.costs.friction(notional)
            if debit > self.cash:
                raise UnavailableExecution(
                    f"auction fills exceed settled cash on {session}; broker outcome required"
                )

            stop, target = _barriers(auction.price, _width(candidate, self.arm))
            self.positions[candidate.instrument] = _Position(
                candidate,
                commitment.quantity,
                debit,
                stop,
                target,
                auction.path,
            )
            self.cash -= debit
            decisions[packet] = EntryDecision(packet, "opened")

        return tuple(decisions[candidate.packet_id] for candidate in candidates)

    def close_and_mark(
        self,
        bars: Mapping[BarKey, DailyBar],
        session: date,
        settlement: date,
        entries: tuple[EntryDecision, ...],
        *,
        close_can_trigger: bool,
    ) -> SessionResult:
        exits = []
        market_value = ZERO
        for instrument, position in tuple(self.positions.items()):
            candidate = position.candidate
            prices = (
                position.entry_path
                if position.completed_sessions == 0
                else bars[instrument, session]
            )
            position.completed_sessions += 1
            outcome = _exit(position, prices, self.collision, close_can_trigger=close_can_trigger)
            if outcome is None:
                market_value += _notional(position.quantity, prices.close)
                continue

            price, reason, collision, ambiguous = outcome
            gross_sale = _notional(position.quantity, price)
            proceeds = gross_sale - self.costs.friction(gross_sale)
            self.receivables[settlement] = self.receivables.get(settlement, ZERO) + proceeds
            exits.append(
                PositionClose(
                    candidate.packet_id,
                    session,
                    reason,
                    collision,
                    ambiguous,
                    proceeds - position.entry_debit + position.cash_distributions,
                    position.cash_distributions,
                )
            )
            del self.positions[instrument]

        distribution_receivable = sum(self.distribution_receivables.values(), ZERO)
        unsettled = sum(self.receivables.values(), distribution_receivable)
        return SessionResult(
            session,
            self.cash + unsettled + market_value,
            self.cash,
            unsettled,
            len(self.positions),
            entries,
            tuple(exits),
            distribution_receivable,
        )


def compare_exit_widths(
    sessions: Sequence[date],
    settlements: Mapping[date, date],
    candidates: Sequence[ReplayCandidate],
    bars: Mapping[BarKey, DailyBar],
    *,
    action_coverage: ActionCoverage,
    opening_evidence: Mapping[BarKey, Auction],
    session_hours: SessionHours,
    entry_convention: EntryConvention,
    initial_cash: Decimal,
    costs: ReplayCosts,
    previous_session: date,
    collision: CollisionPolicy = "stop_first",
) -> ExitReplay:
    """Replay three books with the same qualified execution evidence.

    Sessions must include inactive dates and the complete drain period. Supply
    the last exchange session before the evaluation period to reject stale entries. The
    caller qualifies equity identity, price continuity, actions and the calendar.
    Every potential holding window needs action coverage. An empty tuple
    attests to a covered session without events. Unsupported actions in a
    possible holding window make the comparison unavailable before replay.
    An opening miss requires independent evidence. Require protected prices
    and a qualified closing mark on the entry day, and a qualified holding
    bar on every possible later day.
    """
    if (
        len(sessions) < POLICY.max_sessions
        or previous_session >= sessions[0]
        or list(sessions) != sorted(set(sessions))
        or any(
            session not in settlements or settlements[session] <= session for session in sessions
        )
        or not initial_cash.is_finite()
        or initial_cash <= RESERVE
        or initial_cash != initial_cash.quantize(CENT)
        or collision not in ("stop_first", "target_first")
    ):
        raise ValueError("invalid replay calendar, capital, or collision rule")

    arrivals = _ranked_entries(candidates, sessions, previous_session)
    names, auctions, misses = _select_auctions(arrivals, opening_evidence)
    coverage, action_digest = _bind_actions(sessions, arrivals, action_coverage, misses)
    holding = _holding_bars(sessions, arrivals, misses, bars)
    execution_digest = validate_opening_evidence(
        names, auctions, session_hours, entry_convention, holding
    )
    arms: tuple[Arm, ...] = ("fixed", "scaled", "matched")
    _require_resolved_entry_prices(arrivals, auctions, arms)
    books = [_Book(initial_cash, costs, arm, collision) for arm in arms]
    paths: list[list[SessionResult]] = [[], [], []]
    for session in sessions:
        entries = arrivals.get(session, ())
        for book, path in zip(books, paths, strict=True):
            book.settle(session)
            book.accrue_distributions(coverage, session)
            decisions = book.enter(entries, auctions, session)
            path.append(
                book.close_and_mark(
                    holding,
                    session,
                    settlements[session],
                    decisions,
                    close_can_trigger=entry_convention.close_can_trigger,
                )
            )

    if any(book.positions for book in books):
        raise ValueError("replay ended with unliquidated positions")
    fixed, scaled, matched = (PortfolioResult(initial_cash, tuple(path)) for path in paths)
    return ExitReplay(fixed, scaled, matched, action_digest, execution_digest)
