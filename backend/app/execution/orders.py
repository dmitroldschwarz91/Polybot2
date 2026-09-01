"""
Order execution — buy, sell, cascading stop-loss, fill waiting.

The cascading SL is preserved exactly from the original: FAK attempt →
rounds of GTC chase with widening price → nuclear dump if everything fails.
The difference now: thresholds come from the RiskManager, not magic constants.
"""

from __future__ import annotations

import asyncio
import time
from decimal import ROUND_FLOOR, Decimal, InvalidOperation
from typing import Any, Dict, Optional

from ..config import Settings
from ..core.http import run_sync
from ..core.logging import StructuredLogger
from ..marketdata.stores import FillStore, LivePriceStore
from ..risk.manager import RiskManager
from .client import TradingClient, extract_order_id


def round_to_tick(price: float, ts: str = "0.01") -> float:
    try:
        t = Decimal(str(ts)) if ts else Decimal("0.01")
        if t <= 0:
            t = Decimal("0.01")
        p = Decimal(str(price))
        if p <= 0:
            return 0.001
        r = (p / t).to_integral_value(rounding=ROUND_FLOOR) * t
        return float(max(Decimal("0.001"), min(r, Decimal("0.999"))))
    except (InvalidOperation, ValueError, TypeError):
        return 0.001


def round_size(size) -> int:
    if size is None:
        return 0
    try:
        v = Decimal(str(size))
        return max(0, int(v.to_integral_value(rounding=ROUND_FLOOR))) if v >= 0 else 0
    except (InvalidOperation, ValueError, TypeError):
        return 0


class OrderExecutor:
    """All order placement + fill handling."""

    def __init__(self, settings: Settings, client: TradingClient,
                 prices: LivePriceStore, fills: FillStore,
                 risk: RiskManager, log: StructuredLogger) -> None:
        self.s = settings
        self.client = client
        self.prices = prices
        self.fills = fills
        self.risk = risk
        self.log = log

    # ── fill waiting ─────────────────────────────────────────────────────

    async def wait_for_fill(self, order_id: str, expected_size: int, timeout: float,
                            fallback_price: float) -> Dict[str, Any]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if (snap := self.fills.snapshot(order_id)) is not None:
                matched = max(float(snap.get("filled_size", 0)), float(snap.get("size_matched", 0)))
                if matched > 0:
                    afp = snap.get("avg_fill_price", 0)
                    avg = afp if afp > 0 else (snap.get("limit_price", 0) or fallback_price)
                    return {"filled": True, "matched_size": int(matched),
                            "avg_fill_price": float(avg),
                            "actual_value": round(float(avg) * float(matched), 4),
                            "status": snap.get("status")}
            await asyncio.sleep(self.s.fill_wait_interval)

        snap = self.fills.snapshot(order_id)
        if snap:
            matched = max(float(snap.get("filled_size", 0)), float(snap.get("size_matched", 0)))
            if matched > 0:
                afp = snap.get("avg_fill_price", 0)
                avg = afp if afp > 0 else (snap.get("limit_price", 0) or fallback_price)
                return {"filled": True, "matched_size": int(matched),
                        "avg_fill_price": float(avg),
                        "actual_value": round(float(avg) * float(matched), 4),
                        "status": snap.get("status"), "timed_out": True}
        return {"filled": False, "matched_size": 0, "avg_fill_price": fallback_price,
                "actual_value": 0.0, "status": None}

    # ── buy ──────────────────────────────────────────────────────────────

    async def execute_buy(self, token_id: str, price: float, size: int, asset: str,
                          max_budget: Optional[float] = None) -> Dict[str, Any]:
        try:
            avail = max_budget if max_budget is not None else await run_sync(self.client.get_real_balance)
            if avail is not None:
                needed = round(size * price, 4)
                if needed > avail:
                    size = round_size(avail / price)
                    if size < self.s.min_order_size:
                        self.log.warning(f"[{asset}] BUY rejected: insufficient balance",
                                         needed=f"${needed:.2f}", available=f"${avail:.2f}")
                        return {"success": False}

            tick_size, neg_risk = self.client.get_market_params(token_id) if not self.client.paper else ("0.01", False)
            op = round_to_tick(price, tick_size)
            resp = await self.client.post_order_async(token_id, op, size, "BUY", tick_size, neg_risk)
            oid = extract_order_id(resp)
            self.log.info(f"[{asset}] BUY placed", price=op, size=size, order_id=oid)
            if not oid:
                return {"success": False, "error": "no_order_id"}

            fill = await self.wait_for_fill(oid, size, self.s.buy_fill_timeout_vacuum, op)
            if not fill["filled"] or fill["matched_size"] < self.s.min_order_size:
                self.client.cancel(oid)
                self.log.warning(f"[{asset}] BUY not filled in time", order_id=oid)
                return {"success": False, "error": "not_filled"}

            actual_price = round(fill["avg_fill_price"], 4)
            actual_size = int(fill["matched_size"])
            actual_cost = round(fill["actual_value"], 4)
            if actual_size < size:
                self.client.cancel(oid)
                self.log.warning(f"[{asset}] BUY partial", filled=actual_size, requested=size)
            self.log.info(f"[{asset}] BUY filled", price=actual_price, size=actual_size,
                          cost=actual_cost, order_id=oid)
            return {"success": True, "order_id": oid, "price": actual_price,
                    "size": actual_size, "cost": actual_cost}
        except Exception as e:
            self.log.error(f"[{asset}] BUY failed", error=str(e))
            return {"success": False, "error": str(e)}

    # ── sell ─────────────────────────────────────────────────────────────

    async def execute_sell(self, token_id: str, size: int, asset: str,
                           entry_price: float = 0.0) -> Dict[str, Any]:
        try:
            tick_size, neg_risk = self.client.get_market_params(token_id) if not self.client.paper else ("0.01", False)
            lp = self.prices.get_lot_price(token_id)
            sell_price = lp if lp and lp > 0 else entry_price
            if sell_price <= 0:
                return {"success": False, "error": "No price available"}

            fak_price = max(round_to_tick(0.01, tick_size),
                            round_to_tick(sell_price - float(tick_size) * 2, tick_size))
            try:
                resp = await self.client.post_order_async(
                    token_id, fak_price, size, "SELL", tick_size, neg_risk, fak=True)
                oid = extract_order_id(resp)
                if oid:
                    fill = await self.wait_for_fill(oid, size, 1.5, fak_price)
                    if fill["filled"]:
                        matched = fill["matched_size"]
                        proceeds = fill["actual_value"]
                        ot = "FAK"
                        if matched >= size:
                            self.log.info(f"[{asset}] SELL FAK filled", size=matched,
                                          price=fill["avg_fill_price"], proceeds=proceeds)
                            return {"success": True, "proceeds": proceeds, "type": ot}
                        remainder = size - matched
                        self.log.warning(f"[{asset}] SELL partial", filled=matched, remainder=remainder)
                        if remainder >= self.s.min_order_size:
                            limit = max(round_to_tick(0.01, tick_size),
                                        round_to_tick(sell_price - float(tick_size) * 5, tick_size))
                            await self.client.post_order_async(
                                token_id, limit, remainder, "SELL", tick_size, neg_risk)
                            return {"success": True, "proceeds": proceeds, "type": "FAK+GTC", "pending": True}
                        return {"success": True, "proceeds": proceeds, "type": ot}
            except Exception as e:
                self.log.debug(f"[{asset}] FAK failed: {e}")

            limit = max(round_to_tick(0.01, tick_size),
                        round_to_tick(sell_price - float(tick_size) * 5, tick_size))
            resp = await self.client.post_order_async(
                token_id, limit, size, "SELL", tick_size, neg_risk)
            oid = extract_order_id(resp)
            proceeds = round(size * limit, 4)
            self.log.info(f"[{asset}] SELL GTC", price=limit, size=size, order_id=oid)
            return {"success": True, "proceeds": proceeds, "type": "GTC", "pending": True}
        except Exception as e:
            self.log.error(f"[{asset}] SELL failed", error=str(e))
            return {"success": False, "error": str(e)}

    # ── GTC sell (vacuum TP) ─────────────────────────────────────────────

    async def place_gtc_buy(self, token_id: str, size: int, price: float, asset: str) -> Dict[str, Any]:
        """Place a resting GTC BUY. Returns immediately with order_id (no fill wait)."""
        try:
            tick_size, neg_risk = self.client.get_market_params(token_id) if not self.client.paper else ("0.01", False)
            buy_price = round_to_tick(price, tick_size)
            resp = await self.client.post_order_async(token_id, buy_price, size, "BUY", tick_size, neg_risk)
            oid = extract_order_id(resp)
            if oid:
                self.log.info(f"[{asset}] GTC BUY placed", price=buy_price, size=size, order_id=oid)
                return {"success": True, "order_id": oid, "price": buy_price}
            return {"success": False, "error": "no_order_id"}
        except Exception as e:
            self.log.error(f"[{asset}] GTC BUY failed", error=str(e))
            return {"success": False, "error": str(e)}

    async def place_gtc_sell(self, token_id: str, size: int, price: float, asset: str) -> Dict[str, Any]:
        try:
            tick_size, neg_risk = self.client.get_market_params(token_id) if not self.client.paper else ("0.01", False)
            sell_price = round_to_tick(price, tick_size)
            resp = await self.client.post_order_async(
                token_id, sell_price, size, "SELL", tick_size, neg_risk)
            oid = extract_order_id(resp)
            if oid:
                self.log.info(f"[{asset}] VACUUM TP placed", price=sell_price, size=size, order_id=oid)
                return {"success": True, "order_id": oid, "price": sell_price}
            return {"success": False, "error": "no_order_id"}
        except Exception as e:
            self.log.error(f"[{asset}] VACUUM TP failed", error=str(e))
            return {"success": False, "error": str(e)}

    # ── cascading stop-loss (preserved from original) ────────────────────

    async def execute_cascading_sl(self, token_id: str, size: int, asset: str,
                                   entry_price: float, current_price: float,
                                   sl_trigger: float) -> Dict[str, Any]:
        tick_size, neg_risk = self.client.get_market_params(token_id) if not self.client.paper else ("0.01", False)
        nuclear_threshold = self.risk.nuclear_threshold(sl_trigger)
        remaining = size
        total_proceeds = 0.0

        if self.risk.is_nuclear(current_price, sl_trigger):
            self.log.error(f"[{asset}] NUCLEAR EXIT", price=f"${current_price:.4f}",
                           threshold=f"${nuclear_threshold:.4f}")
            await self.client.post_order_async(
                token_id, self.s.nuclear_sell_price, remaining, "SELL", tick_size, neg_risk)
            return {"success": True, "proceeds": round(remaining * current_price * 0.5, 4),
                    "exit_type": "NUCLEAR_CRASH"}

        lp = self.prices.get_lot_price(token_id)
        sell_price = lp if lp and lp > 0 else current_price
        slippage_price = max(round_to_tick(0.01, tick_size),
                             round_to_tick(sell_price * (1 - self.s.sl_dynamic_slippage_pct), tick_size))

        try:
            resp = await self.client.post_order_async(
                token_id, slippage_price, remaining, "SELL", tick_size, neg_risk, fak=True)
            oid = extract_order_id(resp)
            if oid:
                fill = await self.wait_for_fill(oid, remaining, 1.5, slippage_price)
                if fill["filled"]:
                    matched = fill["matched_size"]
                    total_proceeds += fill["actual_value"]
                    if matched >= remaining:
                        self.log.info(f"[{asset}] SL FAK filled", size=matched, proceeds=total_proceeds)
                        return {"success": True, "proceeds": total_proceeds, "exit_type": "FAK"}
                    remaining -= matched
        except Exception as e:
            self.log.debug(f"[{asset}] SL FAK failed: {e}")

        chase_price = slippage_price
        for chase_round in range(self.s.sl_max_chase_rounds):
            if remaining < self.s.min_order_size:
                break
            fresh = self.prices.get_lot_price(token_id)
            if fresh and self.risk.is_nuclear(fresh, sl_trigger):
                await self.client.post_order_async(
                    token_id, self.s.nuclear_sell_price, remaining, "SELL", tick_size, neg_risk)
                return {"success": True,
                        "proceeds": total_proceeds + round(remaining * fresh * 0.5, 4),
                        "exit_type": f"NUCLEAR_CHASE_R{chase_round + 1}"}

            chase_price = max(round_to_tick(0.01, tick_size),
                              round_to_tick(chase_price * (1 - self.s.sl_chase_step_pct), tick_size))
            try:
                resp = await self.client.post_order_async(
                    token_id, chase_price, remaining, "SELL", tick_size, neg_risk)
                oid = extract_order_id(resp)
                self.log.info(f"[{asset}] SL CHASE R{chase_round + 1}", price=chase_price,
                              size=remaining, order_id=oid)
                if oid:
                    fill = await self.wait_for_fill(oid, remaining, self.s.sl_chase_timeout, chase_price)
                    if fill["filled"]:
                        matched = fill["matched_size"]
                        total_proceeds += fill["actual_value"]
                        remaining -= matched
                        if remaining < self.s.min_order_size:
                            return {"success": True, "proceeds": total_proceeds,
                                    "exit_type": f"CHASE_R{chase_round + 1}"}
                    self.client.cancel(oid)
            except Exception:
                continue

        if remaining >= self.s.min_order_size:
            self.log.error(f"[{asset}] NUCLEAR FINAL", remaining=remaining)
            await self.client.post_order_async(
                token_id, self.s.nuclear_sell_price, remaining, "SELL", tick_size, neg_risk)
            fresh = self.prices.get_lot_price(token_id) or 0.01
            total_proceeds += round(remaining * fresh * 0.5, 4)
        return {"success": True, "proceeds": total_proceeds, "exit_type": "NUCLEAR_FINAL"}
