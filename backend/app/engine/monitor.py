"""Position monitoring — TP/SL/trailing/early-exit evaluation every loop tick.

Refactored: instead of each branch recomputing its own SL threshold, every
position is run through RiskManager.evaluate(). The monitor only decides
*how* to act on a breach (cascade SL vs direct sell vs trailing exit).
"""

from __future__ import annotations

import asyncio
import time
from typing import Dict

from ..config import Settings
from ..core.logging import StructuredLogger
from ..domain.enums import CloseReason, EntryType
from ..domain.models import Position, TradeStats
from ..execution.orders import OrderExecutor, round_size
from ..marketdata.stores import LivePriceStore
from ..risk.manager import RiskManager


class PositionMonitor:
    def __init__(self, settings: Settings, executor: OrderExecutor,
                 prices: LivePriceStore, risk: RiskManager, log: StructuredLogger) -> None:
        self.s = settings
        self.executor = executor
        self.prices = prices
        self.risk = risk
        self.log = log

    async def monitor(self, positions: Dict[str, Position], bot_balance: float,
                      stats: TradeStats) -> float:
        """Returns the balance delta from any closes this tick."""
        now = time.time()
        bd = 0.0
        active = [p for p in positions.values()
                  if not p.closed and now < p.end_ts
                  and now - p.entry_timestamp >= self.s.monitor_grace_period]
        for pos in active:
            cp = self.prices.get_lot_price(pos.token_id)
            if cp is None:
                continue

            if pos.entry_type == EntryType.VACUUM_SCALP:
                bd += await self._monitor_vacuum(pos, cp, stats)
            elif pos.entry_type == EntryType.EARLY_TREND:
                bd += await self._monitor_early_trend(pos, cp, stats)
            else:
                bd += await self._monitor_standard(pos, cp, stats)
        return bd

    # ── Vacuum scalp ─────────────────────────────────────────────────────

    async def _monitor_vacuum(self, pos: Position, cp: float, stats: TradeStats) -> float:
        if pos.sl_in_progress:
            return 0.0
        # TP filled?
        if pos.tp_order_id and pos.tp_order_placed:
            if self.executor.fills.is_filled(pos.tp_order_id):
                snap = self.executor.fills.snapshot(pos.tp_order_id)
                if snap:
                    fs = max(snap.get("filled_size", 0), snap.get("size_matched", 0))
                    ap = snap.get("avg_fill_price", 0) or pos.take_profit_price
                    proceeds = round(fs * ap, 4)
                    pnl = proceeds - (fs * pos.entry_price)
                    pos.record_close(CloseReason.VACUUM_TP, pnl, proceeds)
                    stats.record(pnl, pos.entry_type, CloseReason.VACUUM_TP)
                    self.risk.record_realized_pnl(pnl)
                    self.log.info(f"[{pos.asset}] VACUUM TP HIT",
                                  entry=f"${pos.entry_price:.4f}", tp=f"${ap:.4f}", pnl=f"+${pnl:.4f}")
                    return 0.0
            # TP timeout
            if pos.tp_order_timestamp and (now := time.time()) - pos.tp_order_timestamp > self.s.vacuum_scalp_tp_timeout_secs:
                self.log.warning(f"[{pos.asset}] VACUUM TP timeout, cancelling")
                self.executor.client.cancel(pos.tp_order_id)
                pos.tp_order_id = None
                pos.tp_order_placed = False

        # place TP if missing
        if not pos.tp_order_placed and pos.take_profit_price > 0:
            r = await self.executor.place_gtc_sell(
                pos.token_id, pos.current_size, pos.take_profit_price, pos.asset)
            if r.get("success"):
                pos.tp_order_id = r.get("order_id")
                pos.tp_order_placed = True
                pos.tp_order_timestamp = time.time()

        # SL
        decision = self.risk.evaluate(pos, cp)
        if decision.breached:
            pos.sl_in_progress = True
            asyncio.create_task(self._run_cascade_sl(pos, decision.trigger_price, cp, stats))
        return 0.0

    # ── Early trend ──────────────────────────────────────────────────────

    async def _monitor_early_trend(self, pos: Position, cp: float, stats: TradeStats) -> float:
        pp = (cp - pos.entry_price) / pos.entry_price if pos.entry_price > 0 else 0
        if not pos.partial_tp_taken:
            if pp >= self.s.early_trend_tp_pct:
                cs = round_size(pos.current_size * self.s.early_trend_partial_tp_ratio)
                if cs >= self.s.min_order_size:
                    r = await self.executor.execute_sell(pos.token_id, cs, pos.asset, pos.entry_price)
                    if r["success"]:
                        pnl = r["proceeds"] - (cs * pos.entry_price)
                        trail = max(cp * (1 - self.s.trailing_stop_distance_pct),
                                    pos.entry_price * (1 + self.s.trailing_stop_min_profit_pct))
                        pos.record_partial_tp(cs, pnl, trail)
                        stats.record(pnl, pos.entry_type, CloseReason.PARTIAL_TP)
                        self.log.info(f"[{pos.asset}] PARTIAL TP", pnl=f"+${pnl:.2f}", trailing=f"${trail:.4f}")
                        return r["proceeds"]
            if pp <= -self.s.early_trend_sl_pct:
                sz = pos.current_size
                r = await self.executor.execute_sell(pos.token_id, sz, pos.asset, pos.entry_price)
                if r["success"]:
                    pnl = r["proceeds"] - (sz * pos.entry_price)
                    pos.record_close(CloseReason.STOP_LOSS, pnl, r["proceeds"])
                    stats.record(pnl, pos.entry_type, CloseReason.STOP_LOSS)
                    self.risk.record_realized_pnl(pnl)
                    self.log.warning(f"[{pos.asset}] STOP LOSS", pnl=f"${pnl:.2f}")
                    return r["proceeds"]
            return 0.0

        pos.update_trailing(cp, self.s.trailing_stop_distance_pct, self.s.trailing_stop_min_profit_pct)
        if cp <= pos.trailing_stop_price:
            sz = pos.current_size
            r = await self.executor.execute_sell(pos.token_id, sz, pos.asset, pos.entry_price)
            if r["success"]:
                pnl = r["proceeds"] - (sz * pos.entry_price)
                pos.record_close(CloseReason.TRAILING_STOP, pnl, r["proceeds"])
                stats.record(pos.total_pnl, pos.entry_type, CloseReason.TRAILING_STOP)
                self.risk.record_realized_pnl(pnl)
                self.log.info(f"[{pos.asset}] TRAILING EXIT", pnl=f"${pnl:.2f}")
                return r["proceeds"]
        return 0.0

    # ── Standard ─────────────────────────────────────────────────────────

    async def _monitor_standard(self, pos: Position, cp: float, stats: TradeStats) -> float:
        if pos.sl_in_progress:
            return 0.0
        if self.s.early_exit_enabled:
            if self._check_early_exit(pos):
                r = await self.executor.execute_sell(pos.token_id, pos.current_size, pos.asset, pos.entry_price)
                if r["success"]:
                    profit = r["proceeds"] - (pos.current_size * pos.entry_price)
                    pos.record_close(CloseReason.EARLY_EXIT, profit, r["proceeds"])
                    stats.record(profit, pos.entry_type, CloseReason.EARLY_EXIT)
                    self.risk.record_realized_pnl(profit)
                    self.log.info(f"[{pos.asset}] EARLY EXIT", profit=f"${profit:+.2f}")
                    return r["proceeds"]
        decision = self.risk.evaluate(pos, cp)
        if decision.breached:
            pos.sl_in_progress = True
            asyncio.create_task(self._run_cascade_sl(pos, decision.trigger_price, cp, stats))
        return 0.0

    def _check_early_exit(self, pos: Position) -> bool:
        s = self.s
        stc = pos.end_ts - time.time()
        if stc > s.early_exit_window or stc <= 0:
            return False
        if pos.entry_price <= 0 or pos.current_size <= 0:
            return False
        if pos.entry_price >= s.early_exit_skip_above_price:
            return False
        book = self.prices.get_book(pos.token_id)
        if book is None:
            return False
        bv, av = book.bid_volume, book.ask_volume
        t = bv + av
        imb = bv / t if t > 0 else 0.5
        is_vacuum = (av == 0 and bv > 0) or (imb >= s.vacuum_imbalance_threshold and bv > 0)
        if not is_vacuum or book.best_bid is None or book.best_bid <= 0:
            return False
        return (book.best_bid - pos.entry_price) >= s.early_exit_min_profit

    # ── cascade SL background task ───────────────────────────────────────

    async def _run_cascade_sl(self, pos: Position, sl_trigger: float,
                              current_price: float, stats: TradeStats) -> None:
        try:
            if pos.tp_order_id:
                self.executor.client.cancel(pos.tp_order_id)
                self.log.info(f"[{pos.asset}] Cancelled TP before SL", order_id=pos.tp_order_id)
            sz = pos.current_size
            r = await self.executor.execute_cascading_sl(
                pos.token_id, sz, pos.asset, pos.entry_price, current_price, sl_trigger)
            if r["success"]:
                pnl = r["proceeds"] - (sz * pos.entry_price)
                reason = CloseReason.NUCLEAR_CRASH if "NUCLEAR" in r.get("exit_type", "") else CloseReason.STOP_LOSS
                pos.record_close(reason, pnl, r["proceeds"])
                stats.record(pnl, pos.entry_type, reason)
                self.risk.record_realized_pnl(pnl)
                self.log.warning(f"[{pos.asset}] SL [{r.get('exit_type', '?')}]",
                                 price=f"${current_price:.4f}", pnl=f"${pnl:.2f}")
            else:
                pos.sl_in_progress = False
        except Exception as e:
            self.log.error(f"[{pos.asset}] SL task error", error=str(e))
            pos.sl_in_progress = False
