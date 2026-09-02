"""
TWAP Inertia & Barrier Lock Strategy for Polymarket 5-min Crypto Markets.

Exploits the mathematical irreversibility of 60-second Chainlink TWAP resolution
on Polymarket UP/DOWN binary markets. When the accumulated TWAP deviation in the
final 60s is sufficiently strong, the required counter-move in remaining seconds
exceeds physical market limits (Barrier Factor B(t) >= min_barrier_pct).

Key Features:
- Reversal Impossibility Barrier calculation: B(t) = |dev_twap| * (60 - stc) / stc
- Real-time Frozen Book Guard: detects & rejects untradable 0.50/0.51 stale books
- Orderbook Depth & Liquidity Guard: checks actual depth at best_ask level
- Outage & Feed Staleness Watchdog: halts on stale TWAP / oracle ticks
- HOLD to resolution mode for maximum compounding without SL noise
"""

from __future__ import annotations

import time
from typing import Optional, Set

from ..domain.enums import EntryType
from ..marketdata.stores import LivePriceStore
from ..marketdata.markets import MarketData
from ..risk.manager import RiskManager
from .base import BaseStrategy, Opportunity


class TWAPInertiaStrategy(BaseStrategy):
    entry_type = EntryType.VACUUM_SCALP

    def enabled(self) -> bool:
        return getattr(self.s, "twap_inertia_enabled", True)

    def check(self, market: dict, asset: str, traded: Set[str],
              bot_balance: float, prices: LivePriceStore,
              market_data: MarketData, risk: RiskManager,
              price_history: Optional[dict] = None) -> Opportunity:
        s = self.s
        opp = Opportunity(can_enter=False)
        slug = market.get("slug", "")

        if slug in traded:
            return Opportunity(can_enter=False, reason="already_traded")

        stc = market.get("end_ts", 0) - time.time()

        min_stc = getattr(s, "twap_stc_min", 10.0)
        max_stc = getattr(s, "twap_stc_max", 35.0)

        if stc < min_stc:
            return Opportunity(can_enter=False, reason="too_late")
        if stc > max_stc:
            return Opportunity(can_enter=False, reason="too_early")

        # 1. Feed freshness guard (Outage / Maintenance watchdog)
        now = time.time()
        max_age = getattr(s, "twap_max_feed_age", 3.0)

        twap_ts = prices.chainlink_twap_ts.get(asset, 0.0)
        oracle_ts = max(
            prices.chainlink_ts.get(asset, 0.0),
            prices.binance_direct_ts.get(asset, 0.0),
            prices.binance_ts.get(asset, 0.0),
        )

        if (now - twap_ts > max_age) or (now - oracle_ts > max_age):
            return Opportunity(can_enter=False, reason="stale_twap_or_oracle_feed")

        # 2. Reference prices (Current TWAP and Interval TWAP Open)
        cur_twap = prices.get_chainlink_twap(asset, max_age=max_age)
        cur_interval = market_data.current_interval_ts()
        twap_open = prices.get_twap_at(asset, float(cur_interval)) or market.get("target_price")

        if not cur_twap or not twap_open or twap_open <= 0:
            return Opportunity(can_enter=False, reason="missing_twap_reference")

        dev_twap_pct = (cur_twap - twap_open) / twap_open * 100.0
        abs_dev = abs(dev_twap_pct)

        min_dev = getattr(s, "twap_min_dev_pct", 0.015)
        if abs_dev < min_dev:
            return Opportunity(can_enter=False, reason="deviation_too_small")

        # 3. TWAP Mathematical Barrier Calculation
        # Window is 60s for 5m crypto markets
        elapsed_twap = 60.0 - stc
        if elapsed_twap <= 0:
            return Opportunity(can_enter=False, reason="twap_not_started")

        barrier_factor = abs_dev * (elapsed_twap / stc)
        min_barrier = getattr(s, "twap_min_barrier_pct", 0.070)

        if barrier_factor < min_barrier:
            return Opportunity(can_enter=False, reason="insufficient_twap_barrier")

        direction = "UP" if dev_twap_pct > 0 else "DOWN"
        token_id = market.get("up_token_id") if direction == "UP" else market.get("down_token_id")
        opp_token_id = market.get("down_token_id") if direction == "UP" else market.get("up_token_id")

        if not token_id:
            return Opportunity(can_enter=False, reason="no_token_id")

        book = prices.get_book_with_max_age(token_id, max_age=5.0)
        opp_book = prices.get_book_with_max_age(opp_token_id, max_age=5.0) if opp_token_id else None

        if not book or book.best_ask is None:
            return Opportunity(can_enter=False, reason="no_orderbook")

        token_ask = book.best_ask
        opp_ask = opp_book.best_ask if opp_book else None

        # 4. Real-time Frozen Market Filter (0.50 / 0.51 freeze guard)
        if opp_ask is not None:
            if (0.49 <= token_ask <= 0.52) and (0.49 <= opp_ask <= 0.52):
                return Opportunity(can_enter=False, reason="frozen_market_50_50")

        min_ask = getattr(s, "twap_min_token_ask", 0.55)
        max_ask = getattr(s, "twap_max_token_ask", 0.92)

        if token_ask < min_ask:
            return Opportunity(can_enter=False, reason="token_price_too_low")
        if token_ask > max_ask:
            return Opportunity(can_enter=False, reason="token_price_too_high")

        # 5. Liquidity & Depth at best_ask check
        min_depth = getattr(s, "twap_min_level_depth", 5)
        depth_size = prices.ask_size_at(token_id, token_ask) if hasattr(prices, "ask_size_at") else None
        if depth_size is None and getattr(book, "ask_volume", 0) > 0 and token_ask > 0:
            depth_size = book.ask_volume / token_ask

        if depth_size is not None and depth_size < min_depth:
            return Opportunity(can_enter=False, reason="insufficient_level_depth")

        # 6. Sizing and Risk Gate
        imb = prices.get_book_imbalance(token_id) if token_id else 0.5
        stake = risk.vacuum_scalp_stake(bot_balance, imb)
        shares = int(stake / token_ask) if token_ask > 0 else 0

        # Cap shares to available depth if depth is known
        if depth_size is not None and depth_size >= min_depth:
            shares = min(shares, int(depth_size))

        min_order = getattr(s, "min_order_size", 5)
        if shares < min_order:
            if bot_balance >= min_order * token_ask:
                shares = min_order
            else:
                return Opportunity(can_enter=False, reason="insufficient_balance")

        return Opportunity(
            can_enter=True,
            direction=direction,
            token_id=token_id,
            entry_price=token_ask,
            target_price=twap_open,
            oracle_price=cur_twap,
            deviation=dev_twap_pct / 100.0,
            volatility=barrier_factor,
            bid_volume=getattr(book, "bid_volume", 0.0),
            imbalance=imb,
            potential_size=shares,
            secs_to_close=stc,
            confidence=0.99,
            reason="twap_barrier_locked",
            extra={
                "barrier_pct": round(barrier_factor, 4),
                "dev_twap_pct": round(dev_twap_pct, 4),
                "elapsed_twap": round(elapsed_twap, 1),
                "depth_size": depth_size,
            },
        )

    def target_tp_price(self, actual_price: float, tick_size: str) -> float:
        # HOLD mode default (never sells early)
        return 999.0

    def target_sl_price(self, actual_price: float) -> float:
        # HOLD mode default
        return 0.0
