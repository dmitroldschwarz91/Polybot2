"""Standard Entry — last-second entry based on lot-price trend analysis.
Disabled by default in the original config; preserved for completeness."""

from __future__ import annotations

import time
from collections import deque
from typing import Dict, Set

from ..domain.enums import EntryType
from ..marketdata.stores import LivePriceStore
from ..marketdata.markets import MarketData
from ..risk.manager import RiskManager
from .base import BaseStrategy, Opportunity


class StandardEntryStrategy(BaseStrategy):
    entry_type = EntryType.STANDARD

    def enabled(self) -> bool:
        return self.s.standard_entry_enabled

    def check(self, market: dict, asset: str, traded: Set[str],
              bot_balance: float, prices: LivePriceStore,
              market_data: MarketData, risk: RiskManager,
              price_history: Dict[str, deque] = None) -> Opportunity:
        s = self.s
        slug = market["slug"]
        if slug in traded:
            return Opportunity(can_enter=False, reason="already_traded")

        up_id, dn_id = market["up_token_id"], market["down_token_id"]
        end_ts = market["end_ts"]
        stc = end_ts - time.time()
        up_p = prices.get_lot_price(up_id) if up_id else None
        dn_p = prices.get_lot_price(dn_id) if dn_id else None
        if up_p is None and dn_p is None:
            return Opportunity(can_enter=False, reason="no_prices")

        # record into shared price history
        now_ts = time.time()
        if price_history is not None:
            for sfx, pr in (("_UP", up_p), ("_DOWN", dn_p)):
                key = slug + sfx
                if key not in price_history:
                    price_history[key] = deque(maxlen=s.deque_maxlen)
                if pr is not None:
                    price_history[key].append((now_ts, pr))

        if not (0 < stc <= s.entry_window_secs):
            return Opportunity(can_enter=False, reason="outside_window")
        if price_history is None:
            return Opportunity(can_enter=False, reason="no_history")

        analysis = market_data.analyze_market(price_history, slug)
        if analysis["both_choppy"]:
            return Opportunity(can_enter=False, reason="both_choppy")

        conf = analysis["confidence"]
        if conf < s.min_confidence:
            return Opportunity(can_enter=False, reason="low_confidence")

        rec, hpe = analysis["recommended"], analysis["high_price_entry"]
        direction = token_id = current_price = None
        if rec == "UP" and up_p is not None and up_p >= s.min_lot_price:
            direction, token_id, current_price = "UP", up_id, up_p
        elif rec == "DOWN" and dn_p is not None and dn_p >= s.min_lot_price:
            direction, token_id, current_price = "DOWN", dn_id, dn_p
        if not direction or not current_price:
            return Opportunity(can_enter=False, reason="no_signal")

        imb = prices.get_book_imbalance(token_id)
        conf = min(1.0, conf + risk.imbalance_confidence_boost(imb))
        if conf < s.min_confidence:
            return Opportunity(can_enter=False, reason="low_confidence_after_boost")

        if hpe:
            book = prices.get_book(token_id)
            if not book or not book.asks:
                return Opportunity(can_enter=False, reason="no_ask_liquidity")

        return Opportunity(
            can_enter=True, direction=direction, token_id=token_id,
            entry_price=current_price, secs_to_close=stc, confidence=conf,
            reason="ok", extra={"high_price_entry": hpe},
        )

    def target_sl_price(self, actual_price: float) -> float:
        return round(actual_price * (1 - self.s.standard_sl_pct), 4)
