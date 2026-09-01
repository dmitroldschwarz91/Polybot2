"""Domain entities: Position, TradeStats, BalanceState.

These are pure data objects with *no* dependency on config, the exchange, or
the web layer — they can be unit-tested and serialised independently.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

from .enums import CloseReason, EntryType


@dataclass
class Position:
    """A single open or closed position."""

    slug: str
    asset: str
    token_id: str
    direction: str
    entry_price: float
    entry_size: int
    entry_cost: float
    entry_type: EntryType = EntryType.STANDARD
    entry_timestamp: float = field(default_factory=time.time)
    order_id: Optional[str] = None
    current_size: int = 0
    end_ts: int = 0

    take_profit_price: float = 0.0
    stop_loss_price: float = 0.0
    partial_tp_taken: bool = False
    trailing_active: bool = False
    max_price_seen: float = 0.0
    trailing_stop_price: float = 0.0
    partial_tp_pnl: float = 0.0
    partial_tp_size: int = 0
    real_cost: float = 0.0
    order_locked_cost: float = 0.0
    confidence: float = 0.0
    target_price: Optional[float] = None
    entry_deviation: Optional[float] = None

    # ── Vacuum-scalp TP plumbing ──
    sl_in_progress: bool = False
    tp_order_id: Optional[str] = None
    tp_order_placed: bool = False
    tp_order_timestamp: float = 0.0
    tp_pending_priority: bool = False

    # ── FavDip pair (two-leg) ──
    is_pair: bool = False
    leg2_token_id: Optional[str] = None
    leg2_price: float = 0.0
    leg2_filled: bool = False

    # ── Close state ──
    closed: bool = False
    close_reason: Optional[CloseReason] = None
    close_pnl: float = 0.0
    close_proceeds: float = 0.0
    close_timestamp: Optional[float] = None

    def __post_init__(self) -> None:
        if self.current_size == 0:
            self.current_size = self.entry_size
        if self.max_price_seen == 0:
            self.max_price_seen = self.entry_price
        if self.real_cost == 0:
            self.real_cost = self.entry_cost

    @property
    def total_pnl(self) -> float:
        return self.partial_tp_pnl + self.close_pnl

    def update_trailing(self, cp: float, distance_pct: float, min_profit_pct: float) -> bool:
        """Advance the trailing stop. Returns True if the stop moved up."""
        if not self.trailing_active or cp <= self.max_price_seen:
            return False
        self.max_price_seen = cp
        nt = max(cp * (1 - distance_pct), self.entry_price * (1 + min_profit_pct))
        if nt > self.trailing_stop_price:
            self.trailing_stop_price = nt
            return True
        return False

    def record_partial_tp(self, size: int, pnl: float, trailing: float) -> None:
        self.partial_tp_taken = True
        self.partial_tp_pnl = pnl
        self.partial_tp_size = size
        self.current_size -= size
        self.trailing_active = True
        self.trailing_stop_price = trailing

    def record_close(self, reason: CloseReason, pnl: float, proceeds: float = 0.0) -> None:
        self.closed = True
        self.close_reason = reason
        self.close_pnl = pnl
        self.close_proceeds = proceeds
        self.close_timestamp = time.time()
        self.current_size = 0

    def to_dict(self) -> dict:
        return {
            "slug": self.slug,
            "asset": self.asset,
            "direction": self.direction,
            "entry_type": self.entry_type.value,
            "entry_price": self.entry_price,
            "entry_size": self.entry_size,
            "current_size": self.current_size,
            "take_profit_price": self.take_profit_price,
            "stop_loss_price": self.stop_loss_price,
            "trailing_stop_price": self.trailing_stop_price,
            "unrealized_pnl": self.total_pnl,
            "closed": self.closed,
            "close_reason": self.close_reason.value if self.close_reason else None,
            "close_pnl": self.close_pnl,
            "entry_ts": self.entry_timestamp,
            "end_ts": self.end_ts,
        }


@dataclass
class TradeStats:
    early_trend_count: int = 0
    early_trend_wins: int = 0
    early_trend_losses: int = 0
    early_trend_pnl: float = 0.0
    standard_count: int = 0
    standard_wins: int = 0
    standard_losses: int = 0
    standard_pnl: float = 0.0
    vacuum_scalp_count: int = 0
    vacuum_scalp_wins: int = 0
    vacuum_scalp_losses: int = 0
    vacuum_scalp_pnl: float = 0.0
    partial_tps: int = 0
    trailing_exits: int = 0
    stop_losses: int = 0
    early_exits: int = 0
    vacuum_tps: int = 0
    expired_count: int = 0
    nuclear_count: int = 0

    @property
    def total_trades(self) -> int:
        return self.early_trend_count + self.standard_count + self.vacuum_scalp_count

    @property
    def total_pnl(self) -> float:
        return self.early_trend_pnl + self.standard_pnl + self.vacuum_scalp_pnl

    @property
    def win_rate(self) -> float:
        w = self.early_trend_wins + self.standard_wins + self.vacuum_scalp_wins
        return w / self.total_trades if self.total_trades else 0.0

    def record(self, pnl: float, et: EntryType, reason: CloseReason) -> None:
        if et == EntryType.EARLY_TREND:
            self.early_trend_count += 1
            self.early_trend_pnl += pnl
            if pnl > 0:
                self.early_trend_wins += 1
            elif pnl < 0:
                self.early_trend_losses += 1
        elif et == EntryType.VACUUM_SCALP:
            self.vacuum_scalp_count += 1
            self.vacuum_scalp_pnl += pnl
            if pnl > 0:
                self.vacuum_scalp_wins += 1
            elif pnl < 0:
                self.vacuum_scalp_losses += 1
        else:
            self.standard_count += 1
            self.standard_pnl += pnl
            if pnl > 0:
                self.standard_wins += 1
            elif pnl < 0:
                self.standard_losses += 1

        _by_reason = {
            CloseReason.TRAILING_STOP: "trailing_exits",
            CloseReason.STOP_LOSS: "stop_losses",
            CloseReason.NUCLEAR_CRASH: "nuclear_count",
            CloseReason.EARLY_EXIT: "early_exits",
            CloseReason.PARTIAL_TP: "partial_tps",
            CloseReason.VACUUM_TP: "vacuum_tps",
            CloseReason.EXPIRED: "expired_count",
        }
        attr = _by_reason.get(reason)
        if attr is not None:
            setattr(self, attr, getattr(self, attr) + 1)

    def to_dict(self) -> dict:
        return {
            "total_trades": self.total_trades,
            "win_rate": self.win_rate,
            "total_pnl": self.total_pnl,
            "early_trend": {"count": self.early_trend_count, "pnl": self.early_trend_pnl,
                            "wins": self.early_trend_wins, "losses": self.early_trend_losses},
            "standard": {"count": self.standard_count, "pnl": self.standard_pnl,
                         "wins": self.standard_wins, "losses": self.standard_losses},
            "vacuum_scalp": {"count": self.vacuum_scalp_count, "pnl": self.vacuum_scalp_pnl,
                             "wins": self.vacuum_scalp_wins, "losses": self.vacuum_scalp_losses},
            "exits": {"partial_tps": self.partial_tps, "trailing_exits": self.trailing_exits,
                      "stop_losses": self.stop_losses, "early_exits": self.early_exits,
                      "vacuum_tps": self.vacuum_tps, "expired": self.expired_count,
                      "nuclear": self.nuclear_count},
        }


@dataclass
class BalanceState:
    """Reconciles the exchange wallet USDC with the bot's logical balance."""

    prev_wallet_usdc: Optional[float] = None
    prev_bot_snap: float = 0.0
    total_profit: float = 0.0
    intervals_passed: int = 0

    def to_dict(self, bot_balance: float) -> dict:
        return {
            "bot_balance": round(bot_balance, 4),
            "total_profit": round(self.total_profit, 4),
            "intervals_passed": self.intervals_passed,
        }
