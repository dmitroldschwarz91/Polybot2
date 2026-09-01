"""
Spread Capture / Hedge-Lite — gabagool-inspired strategy.

Different philosophy from Vacuum Scalp: instead of a pure directional bet,
this strategy opens a position on the leading side AND opportunistically buys
the opposite (trailing) side when the combined pair cost drops below a
threshold (e.g. $0.97), locking a profit regardless of outcome — after fees.

For a small budget we do NOT hold both sides continuously (capital-intensive).
We only hedge an already-open directional position when the math genuinely
works (pair < $1 − fees − edge buffer).

Entry logic (primary side):
  * Same late-window timing as vacuum scalp.
  * Enters the side favoured by oracle deviation.

Hedge logic (opposite side), triggered when a primary position is open:
  * If up_ask + down_ask < pair_threshold AND (1 − pair_cost) > min_edge+fees,
    buy enough of the opposite side to cover `hedge_ratio` of the primary size.
  * This converts a directional bet into a (partially) risk-neutral pair.

This is the 'Level 1 — Hedge-Lite' adaptation from GABAGOOL_ANALYSIS.md.
"""

from __future__ import annotations

import time
from typing import Dict, Optional, Set

from ..domain.enums import EntryType
from ..execution.orders import round_size
from ..marketdata.stores import LivePriceStore
from ..marketdata.markets import MarketData
from ..risk.manager import RiskManager
from .base import BaseStrategy, Opportunity
from .vacuum_scalp import VacuumScalpStrategy


class SpreadCaptureStrategy(VacuumScalpStrategy):
    """Extends Vacuum Scalp with an optional hedge on the opposite side."""

    entry_type = EntryType.VACUUM_SCALP  # reuses vacuum scalp type for risk/monitoring

    def enabled(self) -> bool:
        return self.s.spread_capture_enabled

    # ------------------------------------------------------------------
    # The entry decision is inherited from Vacuum Scalp (same thresholds).
    # We only add hedge evaluation, exposed for the engine/monitor to act on.
    # ------------------------------------------------------------------

    def evaluate_hedge(self, primary_position, prices: LivePriceStore,
                       market: dict, bot_balance: float,
                       risk: RiskManager) -> Dict:
        """Given an open directional position, decide whether to buy the hedge.

        Returns a dict with `should_hedge`, `token_id`, `price`, `size`, `edge`.
        """
        s = self.s
        result = {"should_hedge": False}
        if primary_position is None or primary_position.closed:
            return result

        primary_side = primary_position.direction  # UP / DOWN
        opp_token_id = (market["down_token_id"] if primary_side == "UP"
                        else market["up_token_id"])
        if not opp_token_id:
            return result

        opp_book = prices.get_book_with_max_age(opp_token_id, s.vacuum_scalp_book_max_age)
        if not opp_book or opp_book.best_ask is None:
            return result

        prim_book = prices.get_book(primary_position.token_id)
        prim_mid = prim_book.best_bid if prim_book and prim_book.best_bid else primary_position.entry_price

        # pair cost = our realised primary cost (entry) + current opposite ask
        pair_cost = primary_position.entry_price + opp_book.best_ask
        gross_edge = 1.0 - pair_cost
        fee_cost = pair_cost * s.backtest_taker_fee + 0.01  # approx fee + gas
        net_edge = gross_edge - fee_cost

        result.update({
            "pair_cost": round(pair_cost, 4),
            "gross_edge": round(gross_edge, 4),
            "net_edge": round(net_edge, 4),
            "opp_ask": opp_book.best_ask,
            "opp_token_id": opp_token_id,
        })

        if (gross_edge >= s.spread_capture_min_edge
                and pair_cost < s.spread_capture_pair_threshold
                and net_edge > 0):
            hedge_size = round_size(primary_position.current_size * s.spread_capture_hedge_ratio)
            stake = hedge_size * opp_book.best_ask
            if (hedge_size >= s.min_order_size
                    and stake <= bot_balance * s.spread_capture_max_stake_ratio):
                result["should_hedge"] = True
                result["price"] = opp_book.best_ask
                result["size"] = hedge_size
        return result
