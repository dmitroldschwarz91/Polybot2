"""Early Trend strategy — enter when the oracle consistently deviates
from the interval start price, with micro-trend confirmation."""

from __future__ import annotations

import time
from typing import Set

from ..domain.enums import EntryType
from ..execution.orders import round_size
from ..marketdata.stores import LivePriceStore
from ..marketdata.markets import MarketData
from ..risk.manager import RiskManager
from .base import BaseStrategy, Opportunity


class EarlyTrendStrategy(BaseStrategy):
    entry_type = EntryType.EARLY_TREND

    def enabled(self) -> bool:
        return self.s.early_trend_enabled

    def check(self, market: dict, asset: str, traded: Set[str],
              bot_balance: float, prices: LivePriceStore,
              market_data: MarketData, risk: RiskManager) -> Opportunity:
        s = self.s
        slug = market["slug"]
        if slug in traded:
            return Opportunity(can_enter=False, reason="already_traded")
        stc = market["end_ts"] - time.time()
        if stc <= 0 or stc <= s.early_trend_cutoff_secs:
            return Opportunity(can_enter=False, reason="outside_window")

        tp = market.get("target_price")
        if tp is None:
            return Opportunity(can_enter=False, reason="no_target")
        op = prices.get_oracle_price(asset)
        if op is None:
            return Opportunity(can_enter=False, reason="no_oracle")

        market_data.trend.track(slug, asset, op, tp)
        trend = market_data.trend.analyze(slug, asset, prices)
        if not trend["is_consistent"]:
            return Opportunity(can_enter=False, reason="inconsistent_trend")
        if abs(trend["current_deviation"]) < s.early_trend_min_deviation:
            return Opportunity(can_enter=False, reason="deviation_too_small")

        direction = trend["direction"]
        token_id = market["up_token_id"] if direction == "UP" else market["down_token_id"]
        if not token_id:
            return Opportunity(can_enter=False, reason="no_token")
        book = prices.get_book(token_id)
        if book is None or book.best_ask is None:
            return Opportunity(can_enter=False, reason="no_book")
        ba = book.best_ask
        if not (s.early_trend_min_price <= ba <= s.early_trend_max_price):
            return Opportunity(can_enter=False, reason="price_out_of_range")
        if book.spread is not None and book.spread > s.early_trend_max_spread:
            return Opportunity(can_enter=False, reason="spread_too_wide")

        micro = market_data.check_micro_trend(asset, direction)
        if not micro["confirmed"]:
            return Opportunity(can_enter=False, reason="micro_trend_unconfirmed")

        return Opportunity(
            can_enter=True, direction=direction, token_id=token_id, entry_price=ba,
            target_price=tp, oracle_price=op, deviation=trend["current_deviation"],
            secs_to_close=stc, confidence=0.7, reason="ok",
            extra={"micro_trend_pct": micro.get("price_change_pct", 0)},
        )

    def target_tp_price(self, actual_price: float, tick_size: str) -> float:
        return round(actual_price * (1 + self.s.early_trend_tp_pct), 4)

    def target_sl_price(self, actual_price: float) -> float:
        return round(actual_price * (1 - self.s.early_trend_sl_pct), 4)
