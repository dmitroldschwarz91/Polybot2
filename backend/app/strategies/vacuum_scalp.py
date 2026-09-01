"""Vacuum Scalp — the main active strategy.

Enters late in the interval on a token whose price is firm (0.95–0.98) while
the underlying oracle is deviating from the interval start price, with low
volatility and strong bid support. Exits via a GTC take-profit limit or SL.

All thresholds now come from Settings; the checks are unchanged in spirit.
"""

from __future__ import annotations

import time
from typing import Set

from ..domain.enums import EntryType
from ..execution.orders import round_size
from ..marketdata.stores import LivePriceStore
from ..marketdata.markets import MarketData
from ..risk.manager import RiskManager
from .base import BaseStrategy, Opportunity


class VacuumScalpStrategy(BaseStrategy):
    entry_type = EntryType.VACUUM_SCALP

    def enabled(self) -> bool:
        return self.s.vacuum_scalp_enabled

    def check(self, market: dict, asset: str, traded: Set[str],
              bot_balance: float, prices: LivePriceStore,
              market_data: MarketData, risk: RiskManager) -> Opportunity:
        s = self.s
        opp = Opportunity(can_enter=False)
        slug = market["slug"]
        if slug in traded:
            return Opportunity(can_enter=False, reason="already_traded")

        interval_duration = s.interval_minutes * 60
        stc = market["end_ts"] - time.time()
        time_since_start = interval_duration - stc

        if time_since_start < s.vacuum_scalp_entry_start_secs:
            return Opportunity(can_enter=False, reason="too_early")
        if stc < s.vacuum_scalp_entry_end_secs:
            return Opportunity(can_enter=False, reason="too_late")

        tp = market.get("target_price")
        op = prices.get_oracle_price(asset)
        deviation = (op - tp) / tp if (tp and op and tp > 0) else None
        direction = "UP" if (deviation and deviation > 0) else "DOWN"
        token_id = market["up_token_id"] if direction == "UP" else market["down_token_id"]

        book = prices.get_book_with_max_age(token_id, s.vacuum_scalp_book_max_age) if token_id else None
        if book is None:
            token_price = None
        elif book.stale:
            # Fallback: book data is old. Use last trade price to evaluate entry,
            # but real execution will still check the live book.
            token_price = prices.get_lot_price(token_id) or book.best_ask
        else:
            token_price = book.best_ask
        bid_volume = book.bid_volume if book else 0
        volatility = prices.get_volatility(asset, s.vacuum_scalp_volatility_window)
        imb = prices.get_book_imbalance(token_id) if token_id else 0.5

        stake = risk.vacuum_scalp_stake(bot_balance, imb)
        potential_size = round_size(stake / token_price) if token_price else 0
        potential_value = potential_size * token_price if token_price else 0
        required_liquidity = potential_size * s.vacuum_scalp_liquidity_ratio

        # ── checks (same order as original) ──────────────────────────────
        if tp is None or tp <= 0:
            return Opportunity(can_enter=False, reason="no_target_price")
        if op is None:
            return Opportunity(can_enter=False, reason="no_oracle_price")
        if abs(deviation) < s.vacuum_scalp_min_deviation:
            return Opportunity(can_enter=False, reason="deviation_too_small")
        if not token_id:
            return Opportunity(can_enter=False, reason="no_token_id")
        if book is None:
            return Opportunity(can_enter=False, reason="book_stale")
        if token_price is None or token_price < s.vacuum_scalp_min_token_price:
            return Opportunity(can_enter=False, reason="token_price_too_low")
        if token_price > s.vacuum_scalp_max_token_price:
            return Opportunity(can_enter=False, reason="token_price_too_high")

        # cross-validate: opposite token shouldn't also be expensive
        opp_token_id = market["down_token_id"] if direction == "UP" else market["up_token_id"]
        if opp_token_id:
            ob = prices.get_book_with_max_age(opp_token_id, s.vacuum_scalp_book_max_age)
            if ob and ob.best_ask and ob.best_ask >= s.vacuum_scalp_min_token_price:
                return Opportunity(can_enter=False, reason="direction_conflict")

        # multi-confirmation of deviation: the oracle must have stayed on ONE
        # side of target for `confirmation_secs` AND the deviation must already
        # have reached the min_dev threshold confirmation_secs ago (STRICT —
        # "strong AND stable", not just direction-stable). WFO on real data:
        # this lifts WR from ~87% to ~92%, above the 91% break-even.
        # NOTE: history formats differ — binance_direct/binance store 2-tuples
        # (ts, price); chainlink stores 3-tuples (oracle_ts_ms, ts, price).
        # Normalize to (ts_seconds, price) to avoid ValueError on fallback.
        # Diagnostics in opp.extra["confirm"] detect SILENT skips (when the
        # filter can't run due to missing/short history — a likely cause of
        # entries that should have been rejected).
        confirm_info = {"status": "not_checked"}
        hist_raw = prices.binance_direct_history.get(asset) or prices.chainlink_history.get(asset)
        history = None
        if hist_raw:
            history = [(it[1], it[2]) if len(it) >= 3 else (it[0], it[1]) for it in hist_raw]
        if history and tp > 0:
            now = time.time()
            recent = [(ts, p) for ts, p in history if ts >= now - s.vacuum_scalp_confirmation_secs]
            if len(recent) >= 2:
                # 1) direction stable: no crossing of target in the window
                above = sum(1 for _, p in recent if p > tp)
                below = sum(1 for _, p in recent if p < tp)
                # 2) STRICT: deviation was already >= min_dev confirmation_secs ago
                target_ts = now - s.vacuum_scalp_confirmation_secs
                past_price = min(recent, key=lambda pt: abs(pt[0] - target_ts))[1]
                past_dev = abs(past_price - tp) / tp
                confirm_info = {"status": "applied", "points": len(recent),
                                "past_dev": round(past_dev, 5),
                                "crossed": bool(above > 0 and below > 0)}
                if above > 0 and below > 0:
                    return Opportunity(can_enter=False, reason="price_crossed_target",
                                       extra={"confirm": confirm_info})
                if past_dev < s.vacuum_scalp_min_deviation:
                    return Opportunity(can_enter=False, reason="confirmation_too_weak",
                                       extra={"confirm": confirm_info})
            else:
                confirm_info = {"status": "skipped_few_points", "points": len(recent)}
        else:
            confirm_info = {"status": "skipped_no_history", "points": 0}

        # range5 filter: reject "nervous" markets where BTC ranged > range5_max
        # over the last range5_window (WFO strict config E).
        range5 = prices.get_range_ratio(asset, s.vacuum_scalp_range5_window)
        if range5 is not None and range5 >= s.vacuum_scalp_range5_max:
            return Opportunity(can_enter=False, reason="market_too_volatile")

        if volatility is None:
            return Opportunity(can_enter=False, reason="no_volatility_data")
        if volatility > s.vacuum_scalp_max_volatility:
            return Opportunity(can_enter=False, reason="volatility_too_high")
        if potential_value < s.min_order_value:
            return Opportunity(can_enter=False, reason="order_value_too_small")
        if potential_size < s.min_order_size:
            return Opportunity(can_enter=False, reason="insufficient_balance")
        if bid_volume < required_liquidity and not (book and book.stale):
            pass  # Liquidity check disabled for small capital (min order 5 shares)

        return Opportunity(
            can_enter=True, direction=direction, token_id=token_id,
            entry_price=token_price, target_price=tp, oracle_price=op,
            deviation=deviation, volatility=volatility, bid_volume=bid_volume,
            imbalance=imb, potential_size=potential_size, secs_to_close=stc,
            confidence=0.9, reason="ok",
            extra={"confirm": confirm_info},
        )

    def target_tp_price(self, actual_price: float, tick_size: str) -> float:
        from ..execution.orders import round_to_tick
        delta = self.s.vacuum_scalp_tp_delta if actual_price < 0.98 else 0.01
        return round_to_tick(actual_price + delta, tick_size)

    def target_sl_price(self, actual_price: float) -> float:
        return round(actual_price * (1 - self.s.vacuum_scalp_sl_pct), 4)
