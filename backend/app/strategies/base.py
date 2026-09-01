"""Strategy base class + entry-opportunity dataclass."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set

from ..domain.enums import EntryType
from ..domain.models import Position


@dataclass
class Opportunity:
    """A signal produced by a strategy's `check` method."""

    can_enter: bool = False
    direction: Optional[str] = None
    token_id: Optional[str] = None
    entry_price: float = 0.0
    target_price: Optional[float] = None
    oracle_price: Optional[float] = None
    deviation: Optional[float] = None
    volatility: Optional[float] = None
    bid_volume: float = 0.0
    imbalance: float = 0.5
    potential_size: int = 0
    secs_to_close: float = 0.0
    confidence: float = 0.0
    reason: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)


class BaseStrategy:
    """Common interface: check() → Opportunity, then build_position() on fill."""

    entry_type: EntryType = EntryType.STANDARD

    def __init__(self, settings) -> None:
        self.s = settings

    def enabled(self) -> bool:
        return False

    def check(self, market: dict, asset: str, traded: Set[str],
              bot_balance: float, prices, market_data) -> Opportunity:
        raise NotImplementedError

    def target_tp_price(self, actual_price: float, tick_size: str) -> float:
        return actual_price

    def target_sl_price(self, actual_price: float) -> float:
        return 0.0

    def build_position(self, slug: str, asset: str, opp: Opportunity,
                       fill: dict, end_ts: int, **extra) -> Position:
        """Create a Position object from a successful fill."""
        return Position(
            slug=slug, asset=asset, token_id=opp.token_id, direction=opp.direction,
            entry_price=fill["price"], entry_size=fill["size"], entry_cost=fill["cost"],
            entry_type=self.entry_type, order_id=fill["order_id"], end_ts=end_ts,
            real_cost=fill["cost"],
            order_locked_cost=round(fill["price"] * fill["size"], 4),
            confidence=opp.confidence, target_price=opp.target_price,
            entry_deviation=opp.deviation,
        )
