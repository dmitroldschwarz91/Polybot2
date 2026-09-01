"""
Risk Management — the heart of the refactored bot.

In the original script the stop-loss computation was duplicated in four
places (check_and_handle_urgent_sl, check_sl_inline, monitor_positions_async,
execute_cascading_sl_sell). Here it lives in ONE class so it is:

  * testable without the exchange,
  * configurable without touching strategy code,
  * consistent — every position is evaluated against the same rules.

The RiskManager also adds portfolio-level guards that were missing entirely:
daily loss limit, absolute drawdown kill-switch, and a cap on concurrent
positions.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import ROUND_DOWN, Decimal
from typing import Dict, Optional, Tuple

from ..config import Settings
from ..domain.enums import CloseReason, EntryType
from ..domain.models import Position


@dataclass
class RiskState:
    """Mutable, in-memory portfolio risk state (snapshotted to DB periodically)."""

    day_start_ts: float = field(default_factory=time.time)
    day_start_balance: float = 0.0
    peak_balance: float = 0.0
    halted: bool = False
    halt_reason: Optional[str] = None
    daily_realized_pnl: float = 0.0

    def new_day(self, balance: float) -> None:
        self.day_start_ts = self._today_midnight()
        self.day_start_balance = balance
        self.daily_realized_pnl = 0.0
        self.halted = False
        self.halt_reason = None

    @property
    def needs_rollover(self) -> bool:
        return time.time() - self.day_start_ts >= 86400

    @staticmethod
    def _today_midnight() -> float:
        import datetime as _dt
        now = _dt.datetime.now(_dt.timezone.utc)
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return midnight.timestamp()


@dataclass
class RiskDecision:
    """Outcome of evaluating a position against risk rules."""

    breached: bool = False
    reason: Optional[CloseReason] = None
    trigger_price: float = 0.0
    nuclear: bool = False
    note: str = ""


class RiskManager:
    """Single source of truth for every risk decision.

    Callers (the engine, the SL guard, the monitor) only ever ask the manager
    *what* should happen; they never re-derive thresholds themselves.
    """

    def __init__(self, settings: Settings) -> None:
        self.s = settings
        self.state = RiskState()

    # ── Thresholds ───────────────────────────────────────────────────────

    def stop_loss_threshold(self, position: Position) -> float:
        """The price at/below which a position must be liquidated."""
        ep = position.entry_price
        if position.entry_type == EntryType.EARLY_TREND:
            if position.partial_tp_taken:
                return position.trailing_stop_price
            return round(ep * (1 - self.s.early_trend_sl_pct), 4)
        if position.entry_type == EntryType.VACUUM_SCALP:
            return round(ep * (1 - self.s.vacuum_scalp_sl_pct), 4)
        return round(ep * (1 - self.s.standard_sl_pct), 4)

    def take_profit_price(self, position: Position) -> float:
        ep = position.entry_price
        if position.entry_type == EntryType.EARLY_TREND:
            return round(ep * (1 + self.s.early_trend_tp_pct), 4)
        # vacuum / standard have TP set at entry time; kept here for completeness
        return 0.0

    def trailing_threshold(self, position: Position, current_price: float) -> Optional[float]:
        if not position.trailing_active:
            return None
        return max(
            current_price * (1 - self.s.trailing_stop_distance_pct),
            position.entry_price * (1 + self.s.trailing_stop_min_profit_pct),
        )

    def nuclear_threshold(self, sl_trigger: float) -> float:
        """Below this price we bypass the normal chase and dump immediately."""
        return sl_trigger * (1 - self.s.nuclear_crash_pct)

    def is_nuclear(self, current_price: float, sl_trigger: float) -> bool:
        return current_price <= self.nuclear_threshold(sl_trigger)

    def is_fill_anomaly(self, expected_price: float, actual_price: float) -> bool:
        return expected_price > 0 and actual_price < expected_price * (1 - self.s.fill_anomaly_pct)

    # ── Evaluation ───────────────────────────────────────────────────────

    def evaluate(self, position: Position, current_price: Optional[float]) -> RiskDecision:
        """Pure check: given a price, should this position be exited now?

        Does not touch the exchange. Returns a decision the engine acts on.
        """
        now = time.time()
        if position.closed or position.sl_in_progress:
            return RiskDecision()
        if now >= position.end_ts:
            return RiskDecision(breached=False, note="expiring_handled_elsewhere")
        if now - position.entry_timestamp < self.s.monitor_grace_period:
            return RiskDecision()
        if current_price is None:
            return RiskDecision()

        sl_trigger = self.stop_loss_threshold(position)

        # Trailing-stop positions are evaluated against their moving stop.
        if position.entry_type == EntryType.EARLY_TREND and position.partial_tp_taken:
            if current_price <= position.trailing_stop_price:
                return RiskDecision(
                    breached=True, reason=CloseReason.TRAILING_STOP,
                    trigger_price=position.trailing_stop_price, current_price=current_price,
                    note="trailing_breach",
                )
            return RiskDecision()

        if self.is_nuclear(current_price, sl_trigger):
            return RiskDecision(
                breached=True, reason=CloseReason.STOP_LOSS,
                trigger_price=sl_trigger, nuclear=True,
                note=f"nuclear_crash ({current_price:.4f} <= "
                     f"{self.nuclear_threshold(sl_trigger):.4f})",
            )
        if current_price <= sl_trigger:
            return RiskDecision(
                breached=True, reason=CloseReason.STOP_LOSS,
                trigger_price=sl_trigger, note="sl_breach",
            )
        return RiskDecision()

    # ── Position sizing ──────────────────────────────────────────────────

    def base_stake(self, balance: float) -> float:
        if not balance or balance <= 0:
            return 0.0
        return float((Decimal(str(balance)) / 2).quantize(Decimal("0.01"), rounding=ROUND_DOWN))

    def stake_with_imbalance(self, balance: float, imbalance: float) -> float:
        base = self.base_stake(balance)
        if not self.s.imbalance_enabled or imbalance < self.s.moderate_imbalance_threshold:
            return base
        mult = 1.0
        for th, m in sorted(self.s.imbalance_stake_multipliers.items(), reverse=True):
            if imbalance >= th:
                mult = m
                break
        return min(base * mult, balance * self.s.max_stake_ratio)

    def early_trend_stake(self, balance: float) -> float:
        if not balance or balance <= 0:
            return 0.0
        return float(
            Decimal(str(balance * self.s.early_trend_max_stake_ratio))
            .quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        )

    def vacuum_scalp_stake(self, balance: float, imbalance: float) -> float:
        if not balance or balance <= 0:
            return 0.0
        base = float(
            Decimal(str(balance * self.s.vacuum_scalp_max_stake_ratio))
            .quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        )
        if not self.s.imbalance_enabled or imbalance < self.s.moderate_imbalance_threshold:
            return base
        mult = 1.0
        for th, m in sorted(self.s.imbalance_stake_multipliers.items(), reverse=True):
            if imbalance >= th:
                mult = m
                break
        return min(base * mult, balance * self.s.max_stake_ratio)

    def imbalance_confidence_boost(self, imbalance: float) -> float:
        if not self.s.imbalance_enabled:
            return 0.0
        for th, b in sorted(self.s.imbalance_confidence_boost.items(), reverse=True):
            if imbalance >= th:
                return b
        return 0.0

    # ── Portfolio-level guards (NEW) ─────────────────────────────────────

    def can_open_new(self, positions: Dict[str, Position], bot_balance: float) -> Tuple[bool, str]:
        """Hard gate called before *every* entry."""
        if self.state.halted:
            return False, f"halted: {self.state.halt_reason}"

        # ── Data freshness watchdog ──
        # If Chainlink data is >120s old, halt new entries (trading blind)
        import time as _t
        for asset in self.s.assets:
            age = _t.time() - self.state.day_start_ts  # placeholder
        # Actual check is done in the engine via LivePriceStore

        open_count = sum(1 for p in positions.values() if not p.closed)
        if open_count >= self.s.max_concurrent_positions:
            return False, f"max_concurrent_positions ({open_count}/{self.s.max_concurrent_positions})"

        # Absolute drawdown vs. initial balance — the kill switch.
        init = self.s.initial_balance
        if init > 0 and bot_balance <= init * (1 - self.s.max_drawdown_pct):
            self.state.halted = True
            self.state.halt_reason = (
                f"max_drawdown: ${bot_balance:.2f} <= "
                f"${init * (1 - self.s.max_drawdown_pct):.2f}"
            )
            return False, self.state.halt_reason

        # Daily loss limit.
        if self.state.day_start_balance > 0:
            day_loss = self.state.day_start_balance - bot_balance
            if day_loss >= self.state.day_start_balance * self.s.max_daily_loss_pct:
                self.state.halted = True
                self.state.halt_reason = (
                    f"max_daily_loss: -${day_loss:.2f} "
                    f"(-{self.s.max_daily_loss_pct * 100:.0f}% of day start)"
                )
                return False, self.state.halt_reason
        return True, "ok"

    def record_realized_pnl(self, pnl: float) -> None:
        self.state.daily_realized_pnl += pnl

    def rollover_if_needed(self, bot_balance: float) -> bool:
        if self.state.needs_rollover:
            self.state.new_day(bot_balance)
            return True
        return False

    def update_peak(self, bot_balance: float) -> None:
        if bot_balance > self.state.peak_balance:
            self.state.peak_balance = bot_balance

    def snapshot(self) -> dict:
        st = self.state
        return {
            "halted": st.halted,
            "halt_reason": st.halt_reason,
            "day_start_balance": round(st.day_start_balance, 4),
            "daily_realized_pnl": round(st.daily_realized_pnl, 4),
            "peak_balance": round(st.peak_balance, 4),
            "max_concurrent_positions": self.s.max_concurrent_positions,
            "max_daily_loss_pct": self.s.max_daily_loss_pct,
            "max_drawdown_pct": self.s.max_drawdown_pct,
        }
