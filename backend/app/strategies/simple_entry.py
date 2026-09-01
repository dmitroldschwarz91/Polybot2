"""
Simple Entry Strategy — mirrors backtest's extract_entry_opportunities logic.

This strategy uses ONLY the conditions that the walk-forward backtest uses:
  1. Time window: T-150 to T-30 seconds before close
  2. Leader determination: oracle price vs start price
  3. Entry trigger: leader token price >= threshold

NO other filters (volatility, deviation, liquidity, book staleness, etc.).
This ensures the demo produces exactly the same entries as the backtest.
"""

from __future__ import annotations

import time
from typing import Set

from ..domain.enums import EntryType
from ..marketdata.stores import LivePriceStore
from ..marketdata.markets import MarketData
from ..risk.manager import RiskManager
from .base import BaseStrategy, Opportunity


class SimpleEntryStrategy(BaseStrategy):
    """Simplified entry — mirrors backtest logic exactly."""

    entry_type = EntryType.VACUUM_SCALP

    def __init__(self, settings, threshold: float = 0.75) -> None:
        super().__init__(settings)
        self.threshold = threshold

    def enabled(self) -> bool:
        return True

    def check(self, market: dict, asset: str, traded: Set[str],
              bot_balance: float, prices: LivePriceStore,
              market_data: MarketData, risk: RiskManager,
              **kwargs) -> Opportunity:
        """Check entry conditions — simplified to match backtest."""
        s = self.s
        slug = market["slug"]

        if slug in traded:
            return Opportunity(can_enter=False, reason="already_traded")

        interval_duration = s.interval_minutes * 60
        stc = market["end_ts"] - time.time()
        time_since_start = interval_duration - stc

        # ── FILTER 1: Time window (T-150 to T-30) ──
        if time_since_start < s.vacuum_scalp_entry_start_secs:
            return Opportunity(can_enter=False, reason="too_early")
        if stc < s.vacuum_scalp_entry_end_secs:
            return Opportunity(can_enter=False, reason="too_late")

        # ── FILTER 2: Get oracle + start price ──
        tp = market.get("target_price")
        op = prices.get_oracle_price(asset)

        if tp is None or tp <= 0:
            return Opportunity(can_enter=False, reason="no_target_price")
        if op is None:
            return Opportunity(can_enter=False, reason="no_oracle_price")

        # ── FILTER 3: Determine leader direction ──
        deviation = (op - tp) / tp
        direction = "UP" if deviation >= 0 else "DOWN"
        token_id = market["up_token_id"] if direction == "UP" else market["down_token_id"]

        if not token_id:
            return Opportunity(can_enter=False, reason="no_token_id")

        # ── FILTER 4: Check leader token price >= threshold ──
        # Use lot_price (last trade) OR book best_ask — whichever is available
        token_price = prices.get_lot_price(token_id)
        if token_price is None:
            book = prices.get_book(token_id)
            token_price = book.best_ask if book else None

        if token_price is None:
            return Opportunity(can_enter=False, reason="no_token_data")

        if token_price < self.threshold:
            return Opportunity(can_enter=False, reason="token_price_too_low",
                               token_id=token_id, entry_price=token_price,
                               direction=direction)

        # ── ENTRY! ──
        imb = prices.get_book_imbalance(token_id) if token_id else 0.5

        return Opportunity(
            can_enter=True, direction=direction, token_id=token_id,
            entry_price=token_price, target_price=tp, oracle_price=op,
            deviation=deviation, secs_to_close=stc,
            confidence=0.9, reason="ok", imbalance=imb,
        )
