"""
The trading engine — the main async loop.

Ties together: market-data feeds, strategies, execution, risk and balance.
Exposes a BotStatus object that the FastAPI dashboard reads from.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

from ..config import Settings
from ..core.http import AsyncHTTP, run_sync
from ..core.logging import StructuredLogger, build_logger
from ..db.database import Database
from ..domain.enums import CloseReason, EntryType
from ..domain.models import Position, TradeStats
from ..domain.resolution import (
    cross_validate, gamma_outcome, leader_token_outcome, late_truth_outcome,
)
from ..execution.client import TradingClient
from ..execution.orders import OrderExecutor, round_size
from ..marketdata.markets import MarketData
from ..marketdata.stores import FillStore, LivePriceStore
from ..marketdata.websockets import WebSocketManager
from ..marketdata.book_poller import BookPoller
from ..risk.manager import RiskManager
from ..strategies import all_strategies
from ..strategies.base import Opportunity
from ..strategies.favdip import check_entry as _favdip_check
from .balance import BalanceManager
from .monitor import PositionMonitor


class BotStatus:
    """Live snapshot consumed by the dashboard + WS subscribers."""

    def __init__(self) -> None:
        self.running = False
        self.started_at: Optional[float] = None
        self.bot_balance = 0.0
        self.initial_balance = 0.0

    def to_dict(self, positions, stats, risk, prices, balance_state, paper: bool,
                hold: bool = True) -> dict:
        open_positions = [p.to_dict() for p in positions.values() if not p.closed]
        return {
            "running": self.running,
            "paper_trading": paper,
            "hold_to_resolution": hold,
            "uptime": round(time.time() - self.started_at, 0) if self.started_at else 0,
            "balance": balance_state.to_dict(self.bot_balance),
            "initial_balance": self.initial_balance,
            "positions": open_positions,
            "open_count": len(open_positions),
            "stats": stats.to_dict(),
            "risk": risk.snapshot(),
            "prices": prices.snapshot(),
        }


class TradingEngine:
    def __init__(self, settings: Settings, log: Optional[StructuredLogger] = None) -> None:
        self.s = settings
        self.log = log or build_logger("polybot", json_file=f"{settings.log_dir}/polybot.json")
        self.status = BotStatus()

        # core services
        self.prices = LivePriceStore(settings.assets, settings.ws_book_stale_secs)
        self.fills = FillStore()
        self.risk = RiskManager(settings)
        self.http = AsyncHTTP()
        self.client = TradingClient(settings, self.log)
        self.market_data = MarketData(settings, self.prices, self.http, self.log)
        self.executor = OrderExecutor(settings, self.client, self.prices, self.fills, self.risk, self.log)
        self.monitor = PositionMonitor(settings, self.executor, self.prices, self.risk, self.log)
        self.balance = BalanceManager(settings, self.client, self.log)
        self.ws = WebSocketManager(settings, self.prices, self.fills, self.log)
        self.book_poller = BookPoller(settings, self.prices, self.log, poll_interval=5.0)
        self.strategies = all_strategies(settings)
        self.db = Database(settings.database_url)

        # runtime state
        self.positions: Dict[str, Position] = {}
        self.stats = TradeStats()
        self.traded: Set[str] = set()
        self.price_history: Dict[str, deque] = {}
        self.known_tokens: Set[str] = set()
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        # FavDip state
        self._zpair_prices: Dict[str, list] = {}
        self._zpair_last_ts: Dict[str, float] = {}
        self._pending_leg1: Dict[str, dict] = {}

    # ── lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self.status.running:
            return
        await self.db.init()
        self.log.info("=" * 60)
        self.log.info("POLYMARKET BOT v6.0.0 (refactored)")
        active = [type(st).__name__ for st in self.strategies if st.enabled()]
        self.log.info(f"Active strategies: {', '.join(active) or 'none'}")
        self.log.info(f"Paper trading: {self.s.paper_trading}")
        self.log.info(f"Strategy: {self.s.trading_strategy}")
        self.log.info(f"Risk: max_concurrent={self.s.max_concurrent_positions}, "
                      f"daily_loss={self.s.max_daily_loss_pct:.0%}, "
                      f"drawdown={self.s.max_drawdown_pct:.0%}")
        self.log.info("=" * 60)

        await self.client.init()
        self.ws.api_creds = self.client.api_creds
        self.ws.start()
        self.book_poller.start()
        self.risk.state.new_day(self.s.initial_balance)
        self.risk.state.peak_balance = self.s.initial_balance

        self.status.running = True
        self.status.started_at = time.time()
        self.status.initial_balance = self.s.initial_balance
        self.status.bot_balance = self.s.initial_balance
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="bot-engine")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        self._graceful_shutdown()
        await self.book_poller.stop()
        await self.ws.stop()
        await self.http.close()
        await self.db.close()
        self.status.running = False
        self.log.info("Bot stopped.")

    # ── main loop ────────────────────────────────────────────────────────

    async def _run(self) -> None:
        try:
            await self._wait_for_feeds()
            last_interval = 0
            last_monitor = 0.0
            last_blog = 0.0
            snapshot_done = False

            while not self._stop_event.is_set():
                t0 = time.time()
                cur = self.market_data.current_interval_ts()
                secs_from_start = time.time() - cur

                # resolve interval start prices
                await self._resolve_start_prices(cur, secs_from_start)

                # daily rollover
                if self.risk.rollover_if_needed(self.status.bot_balance):
                    self.log.info(f"New trading day. Reset daily loss counter.")

                # monitor open positions
                ac = sum(1 for p in self.positions.values() if not p.closed and time.time() < p.end_ts)
                if ac > 0 and (t0 - last_monitor) >= self.s.monitor_interval:
                    bd = await self.monitor.monitor(self.positions, self.status.bot_balance, self.stats)
                    self.status.bot_balance += bd
                    last_monitor = t0

                # reconcile completed cascade-SL positions
                for pos in self.positions.values():
                    if pos.closed and pos.sl_in_progress:
                        self.status.bot_balance += pos.close_proceeds
                        pos.sl_in_progress = False
                        await self.db.save_trade(pos)

                self.risk.update_peak(self.status.bot_balance)

                ste = self.market_data.next_interval_ts() - int(t0)
                if 0 < ste <= self.s.snapshot_before_end and not snapshot_done:
                    is_first = self.balance.state.prev_wallet_usdc is None
                    r = self.balance.process_interval_snapshot(self.balance.state.intervals_passed + 1, is_first)
                    if r["success"]:
                        self.status.bot_balance = r["bot_snap"]
                    snapshot_done = True

                if last_interval != cur:
                    if last_interval != 0:
                        self.balance.state.intervals_passed += 1
                        self.traded = await self._cleanup(cur)
                        if self.known_tokens:
                            self.prices.cleanup_old_tokens(self.known_tokens)
                            self.known_tokens.clear()
                    last_interval = cur
                    snapshot_done = False
                    # FavDip: reset oracle accumulator on new interval
                    self._zpair_prices = {a: [] for a in self.s.assets}
                    self._zpair_last_ts = {}
                    self._pending_leg1 = {}

                # discover markets + subscribe to token books
                markets = await asyncio.gather(*[self.market_data.fetch_market(a) for a in self.s.assets], return_exceptions=True)
                new_tokens = set()
                for m in markets:
                    if m:
                        for tid in (m.get("up_token_id"), m.get("down_token_id")):
                            if tid and tid not in self.known_tokens:
                                new_tokens.add(tid)
                                self.known_tokens.add(tid)
                if new_tokens:
                    await self.ws.subscribe_market_tokens(new_tokens)
                    self.book_poller.watch(new_tokens)

                # evaluate strategies per asset
                for asset, market in zip(self.s.assets, markets):
                    if self.status.bot_balance <= 0 or not market:
                        continue

                    if self.s.trading_strategy == "favdip":
                        op_now = self.prices.get_oracle_price(asset)
                        if op_now:
                            _tn = time.time()
                            if _tn - self._zpair_last_ts.get(asset, 0.0) >= 1.0:
                                self._zpair_last_ts[asset] = _tn
                                self._zpair_prices.setdefault(asset, []).append(op_now)
                        await self._favdip_check_pending()
                        await self._favdip_check_complete()
                        if market["slug"] not in self.traded:
                            stc = market["end_ts"] - time.time()
                            if self.s.favdip_win_lo <= stc <= self.s.favdip_win_hi:
                                await self._favdip_enter(asset, market, stc)
                        continue

                    if market["slug"] in self.traded:
                        continue
                    if await self._evaluate_strategies(asset, market):
                        break  # one entry per loop tick

                if self.s.quiet_mode and (t0 - last_blog) >= self.s.balance_log_interval:
                    cl_age = self.prices.get_chainlink_age("BTC")
                    self.log.info(f"Balance: ${self.status.bot_balance:.2f} | Pos: {ac}",
                                  chainlink_age=f"{cl_age:.0f}s")
                    last_blog = t0

                elapsed = time.time() - t0
                if elapsed > 0.5:
                    self.log.warning(f"Loop lag: {elapsed:.2f}s")
                await asyncio.sleep(max(0.0, self.s.poll_interval - elapsed))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log.error("Engine crashed", error=str(e), exc_info=True)
            raise

    async def _favdip_enter(self, asset: str, market: dict, stc: float) -> None:
        """FavDip leg1: place GTC BUY limit at best_ask (resting, waits for seller)."""
        sig = _favdip_check(
            market=market, asset=asset, stc=stc,
            prices=self.prices,
            start_prices=self.market_data.start_prices,
            cur_interval=self.market_data.current_interval_ts(),
            zpair_prices=self._zpair_prices,
            stake_ratio=self.s.max_stake_ratio,
            virtual_capital=self.status.bot_balance,
            risk=self.risk, positions=self.positions)
        if sig is None:
            return
        result = await self.executor.place_gtc_buy(
            sig.token_id, sig.shares, sig.limit_price, asset)
        if not result.get("success"):
            self.log.info(f"[{asset}] FavDip GTC FAILED: {result.get('error','?')}")
            return
        self.traded.add(market["slug"])
        self._pending_leg1[market["slug"]] = dict(
            order_id=result["order_id"], token_id=sig.token_id, other_id=sig.other_id,
            direction=sig.direction, limit_price=result["price"], shares=sig.shares,
            end_ts=market["end_ts"], asset=asset)
        self.log.info(f"[{asset}] FAVDIP LEG1 (GTC) {sig.direction} | "
            f"{sig.shares}sh @ ${sig.limit_price:.3f} | mom={sig.momentum:+.1f}")

    async def _favdip_check_pending(self) -> None:
        """Check pending FavDip GTC orders: fill→Position, expire→cancel."""
        now = time.time()
        for slug, p in list(self._pending_leg1.items()):
            snap = self.fills.snapshot(p["order_id"])
            if snap:
                matched = max(float(snap.get("filled_size", 0)),
                              float(snap.get("size_matched", 0)))
                if matched >= self.s.min_order_size:
                    avg = float(snap.get("avg_fill_price", 0)) or p["limit_price"]
                    size = int(matched); cost = round(size * avg, 4)
                    self.status.bot_balance = max(0.0, self.status.bot_balance - cost)
                    pos = Position(slug=slug, asset=p["asset"], token_id=p["token_id"],
                        direction=p["direction"], entry_price=avg, entry_size=size,
                        entry_cost=cost, entry_type=EntryType.VACUUM_SCALP,
                        end_ts=p["end_ts"], is_pair=True, leg2_token_id=p["other_id"],
                        take_profit_price=999.0, stop_loss_price=0.0)
                    self.positions[slug] = pos
                    del self._pending_leg1[slug]
                    self.log.info(f"[{p['asset']}] FAVDIP LEG1 FILLED | {size}sh @ ${avg:.3f}")
                    continue
            if now > p["end_ts"]:
                self.client.cancel(p["order_id"])
                del self._pending_leg1[slug]
                self.log.info(f"[{p['asset']}] FAVDIP leg1 expired")

    async def _favdip_check_complete(self) -> None:
        """FavDip leg2: buy opposite token when pair sum < target -> lock."""
        now = time.time()
        for slug, pos in list(self.positions.items()):
            if pos.closed or not pos.is_pair or pos.leg2_filled:
                continue
            if now > pos.end_ts:
                continue
            book = self.prices.get_book(pos.leg2_token_id)
            if not book or book.best_ask is None:
                continue
            if pos.entry_price + book.best_ask >= self.s.favdip_target_sum:
                continue
            from ..execution.orders import round_to_tick
            ts, _ = self.client.get_market_params(pos.leg2_token_id) if not self.client.paper else ("0.01", False)
            buy2 = round_to_tick(book.best_ask + 0.01, ts)
            result = await self.executor.execute_buy(pos.leg2_token_id, buy2,
                pos.current_size, pos.asset, max_budget=pos.current_size * buy2 * 1.05)
            if not result.get("success"):
                continue
            ap2, _, acost2 = result["price"], result["size"], result["cost"]
            self.status.bot_balance = max(0.0, self.status.bot_balance - acost2)
            pos.leg2_price = ap2
            pos.leg2_filled = True
            lock = pos.current_size * (0.98 - pos.entry_price - ap2)
            self.log.info(f"[{pos.asset}] FAVDIP PAIR LOCKED | leg2 @ ${ap2:.3f} | sum=${pos.entry_price+ap2:.3f} | LOCK ${lock:+.2f}")

    async def _evaluate_strategies(self, asset: str, market: dict) -> bool:
        """Try each enabled strategy; return True if an entry was placed."""
        for strat in self.strategies:
            if not strat.enabled():
                continue
            stc = market["end_ts"] - time.time()
            # window pre-checks to skip cheap
            if strat.entry_type == EntryType.EARLY_TREND and stc <= self.s.early_trend_cutoff_secs:
                continue
            if strat.entry_type == EntryType.STANDARD and not (0 < stc <= self.s.entry_window_secs):
                continue
            if strat.entry_type == EntryType.VACUUM_SCALP and stc <= self.s.vacuum_scalp_entry_end_secs:
                continue

            opp = strat.check(market, asset, self.traded, self.status.bot_balance,
                              self.prices, self.market_data, self.risk,
                              price_history=self.price_history)
            if not opp.can_enter:
                continue

            # ★ portfolio-level risk gate (NEW)
            ok, reason = self.risk.can_open_new(self.positions, self.status.bot_balance)
            if not ok:
                self.log.warning(f"[{asset}] Entry blocked by risk: {reason}")
                return False

            await self._enter(strat, asset, market, opp)
            return True
        return False

    async def _enter(self, strat, asset: str, market: dict, opp: Opportunity) -> None:
        token_id = opp.token_id
        direction = opp.direction
        imb = opp.imbalance
        tick_size, neg_risk = self.client.get_market_params(token_id) if not self.client.paper else ("0.01", False)
        from ..execution.orders import round_to_tick
        buy_price = round_to_tick(opp.entry_price, tick_size)

        # sizing
        if strat.entry_type == EntryType.EARLY_TREND:
            stake = self.risk.early_trend_stake(self.status.bot_balance)
        elif strat.entry_type == EntryType.VACUUM_SCALP:
            stake = self.risk.vacuum_scalp_stake(self.status.bot_balance, imb)
        else:
            avail = await run_sync(self.client.get_real_balance)
            effective = min(self.status.bot_balance, avail) if avail is not None else self.status.bot_balance
            stake = self.risk.stake_with_imbalance(effective, imb)
        if stake <= 0:
            return
        size = round_size(stake / buy_price)
        if size < self.s.min_order_size:
            return
        cost = round(size * buy_price, 4)
        while cost > stake and size >= self.s.min_order_size:
            size -= 1
            cost = round(size * buy_price, 4)
        if size < self.s.min_order_size or cost < self.s.min_order_value:
            return

        self.log.info(f"[{asset}] ENTRY {direction} [{strat.entry_type.value}]",
                      price=buy_price, lots=size, dev=f"{(opp.deviation or 0)*100:+.3f}%")

        result = await self.executor.execute_buy(token_id, buy_price, size, asset,
                                                 max_budget=cost * 1.05)
        if not result.get("success"):
            return

        ap, asize, acost = result["price"], result["size"], result["cost"]
        self.status.bot_balance = max(0.0, self.status.bot_balance - acost)
        self.traded.add(market["slug"])

        # anomalous fill → nuclear exit
        if self.risk.is_fill_anomaly(opp.entry_price, ap):
            self.log.error(f"[{asset}] ANOMALOUS FILL — nuclear exit",
                           actual=f"${ap:.4f}", expected=f"${opp.entry_price:.4f}")
            pos = strat.build_position(market["slug"], asset, opp, result, market["end_ts"])
            pos.sl_in_progress = True
            self.positions[market["slug"]] = pos
            asyncio.create_task(self._nuclear_exit(pos))
            return

        pos = strat.build_position(market["slug"], asset, opp, result, market["end_ts"])
        if self.s.hold_to_resolution:
            pos.take_profit_price = 999.0  # never triggers
            pos.stop_loss_price = 0.0      # never triggers
        else:
            pos.take_profit_price = strat.target_tp_price(ap, tick_size)
            pos.stop_loss_price = strat.target_sl_price(ap)

        if strat.entry_type == EntryType.VACUUM_SCALP and not self.s.hold_to_resolution:
            await asyncio.sleep(2.0)  # let tokens settle
            tp_r = await self._place_vacuum_tp(pos, asize, tick_size)
            pos.tp_order_id = tp_r.get("order_id")
            pos.tp_order_placed = tp_r.get("order_id") is not None
            pos.tp_order_timestamp = time.time() if pos.tp_order_placed else 0.0
            pos.tp_pending_priority = not pos.tp_order_placed
            if pos.tp_pending_priority:
                asyncio.create_task(self._retry_vacuum_tp(pos))

        self.positions[market["slug"]] = pos
        self.log.info(f"[{asset}] FILLED at ${ap:.4f}", cost=f"${acost:.2f}", size=asize,
                      tp=f"${pos.take_profit_price:.4f}", sl=f"${pos.stop_loss_price:.4f}")

    async def _place_vacuum_tp(self, pos: Position, size: int, tick_size: str) -> dict:
        for attempt in range(3):
            r = await self.executor.place_gtc_sell(pos.token_id, size, pos.take_profit_price, pos.asset)
            if r.get("success"):
                return r
            await asyncio.sleep(0.5 * (attempt + 1))
        return {"success": False}

    async def _retry_vacuum_tp(self, pos: Position) -> None:
        for attempt in range(1, 6):
            if pos.closed or pos.sl_in_progress or pos.tp_order_placed:
                return
            await asyncio.sleep(0.5 * attempt)
            if pos.closed or pos.sl_in_progress:
                return
            r = await self.executor.place_gtc_sell(
                pos.token_id, pos.current_size, pos.take_profit_price, pos.asset)
            if r.get("success"):
                pos.tp_order_id = r.get("order_id")
                pos.tp_order_placed = True
                pos.tp_pending_priority = False
                pos.tp_order_timestamp = time.time()
                self.log.info(f"[{pos.asset}] VACUUM TP retry #{attempt} ok", order_id=pos.tp_order_id)
                return

    async def _nuclear_exit(self, pos: Position) -> None:
        try:
            sz = pos.current_size
            r = await self.executor.execute_cascading_sl(
                pos.token_id, sz, pos.asset, pos.entry_price, pos.entry_price * 0.5, pos.stop_loss_price)
            pnl = r.get("proceeds", 0) - (sz * pos.entry_price)
            pos.record_close(CloseReason.NUCLEAR_CRASH, pnl, r.get("proceeds", 0))
            self.stats.record(pnl, pos.entry_type, CloseReason.NUCLEAR_CRASH)
            self.risk.record_realized_pnl(pnl)
            self.log.error(f"[{pos.asset}] NUCLEAR EXIT done", pnl=f"${pnl:.2f}")
        except Exception as e:
            self.log.error(f"[{pos.asset}] Nuclear exit failed", error=str(e))
            pos.sl_in_progress = False

    # ── helpers ──────────────────────────────────────────────────────────

    async def _resolve_start_prices(self, cur: int, secs_from_start: float) -> None:
        for asset in self.s.assets:
            key = str(cur)
            cached = key in self.market_data.start_prices and asset in self.market_data.start_prices.get(key, {})
            if not cached:
                allow_fb = secs_from_start >= self.s.start_price_chainlink_grace_secs
                sp = await self.market_data.get_or_set_start_price(asset, cur, allow_fallback=allow_fb)
                if sp is not None:
                    self.log.info(f"[{asset}] Start price set", price=f"${sp:.2f}")

    async def _wait_for_feeds(self) -> None:
        self.log.info("Waiting for WebSocket data...")
        for _ in range(50):
            if self.prices.chainlink or self.prices.binance:
                break
            await asyncio.sleep(0.1)
        if self.prices.chainlink:
            self.log.info("Chainlink OK", assets=list(self.prices.chainlink.keys()))
        if self.prices.binance:
            self.log.info("Binance OK", assets=list(self.prices.binance.keys()))

    async def _cleanup(self, cur_ts: int) -> Set[str]:
        """Clean up expired positions with REAL winner check."""
        now = time.time()
        cutoff = now - self.s.interval_minutes * 60 * 2
        self.fills.orders = {k: v for k, v in self.fills.orders.items() if v.get("last_ts", 0) > cutoff}
        self.market_data.cleanup_caches()
        
        for slug, pos in list(self.positions.items()):
            # FavDip pair_locked: both legs filled -> guaranteed 0.98
            if (not pos.closed and pos.is_pair and pos.leg2_filled
                    and now >= pos.end_ts + 30):
                proceeds = pos.current_size * 1.0
                fee = proceeds * self.s.backtest_taker_fee
                cost2 = pos.current_size * pos.leg2_price
                pnl = proceeds - fee - pos.entry_cost - cost2
                pos.record_close(CloseReason.EXPIRED, pnl, proceeds)
                self.stats.record(pnl, pos.entry_type, CloseReason.EXPIRED)
                self.risk.record_realized_pnl(pnl)
                self.status.bot_balance += proceeds - fee  # FIX: was += pnl (double-counted cost)
                self.log.info(f"[{pos.asset}] RESOLVE PAIR LOCKED pnl=${pnl:+.2f}")
                await self.db.save_trade(pos)
                self.positions.pop(slug, None)
                continue

            if not pos.closed and now >= pos.end_ts + 30:
                resolve_method = None
                won = None
                _cl_won: Optional[bool] = None
                _tok_won: Optional[bool] = None
                _agree: Optional[bool] = None

                # both sources computed once; reused by cross-validation & cascade.
                # Chainlink source prefers the OFFICIAL TWAP stream (authoritative);
                # falls back to the local tick-average reconstruction. _cl_src records
                # which one supplied the value (results log / re-analysis).
                _cl_won = self._resolve_from_twap(pos)
                _cl_src = "twap" if _cl_won is not None else None
                if _cl_won is None:
                    _cl_won = self._resolve_from_history(pos)
                    if _cl_won is not None:
                        _cl_src = "chainlink_local"
                _tok_won = await leader_token_outcome(
                    pos.token_id, self.prices, self.http, self.s)

                # ── 1. cross-validation FIRST (conclusive only when BOTH present) ──
                if self.s.cross_validate_resolution:
                    xcc = cross_validate(_cl_won, _tok_won)
                    _agree = xcc.agree
                    if xcc.method == "cross_disagree":
                        # two trusted sources conflict → refuse to guess (подстраховка)
                        resolve_method = "cross_disagree"
                    elif xcc.method == "cross_ok":
                        won, resolve_method = xcc.won, "cross_ok"

                # ── 2. cascade fallback (cross-validation off OR inconclusive) ──
                if won is None and resolve_method != "cross_disagree":
                    won = _cl_won
                    if won is not None:
                        resolve_method = _cl_src            # "twap" | "chainlink_local"
                    if won is None:
                        _gw = await gamma_outcome(slug, self.http, self.s)
                        if _gw is not None:
                            won = _gw if pos.direction == "UP" else (not _gw)
                            resolve_method = "gamma"
                    if won is None:
                        won = _tok_won
                        if won is not None:
                            resolve_method = "token_price"

                # UNRESOLVED (neutral): disagreement (immediate) OR no data +300s.
                # Cost is returned; excluded from win rate. For real money these
                # flagged cases need manual reconciliation against the chain.
                is_disagree = (resolve_method == "cross_disagree")
                if won is None and (is_disagree or now >= pos.end_ts + 300):
                    if is_disagree:
                        self.log.warning(
                            f"[{pos.asset}] CROSS-DISAGREE {slug}: Chainlink="
                            f"{'WIN' if _cl_won else 'LOSS'} vs token="
                            f"{'WIN' if _tok_won else 'LOSS'} → UNRESOLVED (neutral)",
                            slug=slug, chainlink_won=_cl_won, token_won=_tok_won)
                    else:
                        resolve_method = "unresolved"
                        self.log.error(
                            f"[{pos.asset}] UNRESOLVED: no resolution data for {slug} → neutral")
                    pos.record_close(CloseReason.EXPIRED, 0.0, pos.entry_cost)
                    # neutral: return the cost deducted at entry (NOT a loss)
                    self.status.bot_balance += pos.entry_cost
                    self._log_resolution(pos, None, 0.0, 0.0, 0.0, resolve_method,
                                         _cl_won, _tok_won, _agree)
                    await self.db.save_trade(pos)
                    self.positions.pop(slug, None)
                    continue

                if won is None:
                    continue

                # Balance FIX: add proceeds (cost was already deducted at entry).
                # The old `+= pnl` double-counted entry_cost on every WIN/LOSS.
                # Now matches the demo engine's accounting convention.
                if won:
                    proceeds = pos.current_size * 1.0
                    fee = proceeds * self.s.backtest_taker_fee
                    pnl = proceeds - pos.entry_cost - fee
                    self.status.bot_balance += proceeds - fee
                else:
                    pnl = -pos.entry_cost
                    self.status.bot_balance += 0.0

                pos.record_close(CloseReason.EXPIRED, pnl)
                self.stats.record(pnl, pos.entry_type, CloseReason.EXPIRED)
                self.risk.record_realized_pnl(pnl)
                tag = "✓ WIN" if won else "✗ LOSS"
                self.log.info(f"[{pos.asset}] RESOLVE {tag} pnl=${pnl:+.2f} method={resolve_method}",
                              chainlink_won=_cl_won, token_won=_tok_won, agree=_agree)
                self._log_resolution(pos, won, pnl,
                                     proceeds if won else 0.0,
                                     fee if won else 0.0,
                                     resolve_method, _cl_won, _tok_won, _agree)
                # late ground-truth recheck (measures real Chainlink error rate)
                if self.s.cross_late_snapshot:
                    asyncio.create_task(self._late_cross_check({
                        "token_id": pos.token_id, "slug": slug, "asset": pos.asset,
                        "direction": pos.direction, "end_ts": pos.end_ts,
                        "resolved_won": won, "resolved_method": resolve_method,
                        "chainlink_won_at_resolve": _cl_won,
                        "token_won_at_resolve": _tok_won,
                    }))
                
            if pos.closed:
                self.positions.pop(slug, None)
                
        return {s for s in self.traded if str(cur_ts) in s}

    def _resolve_from_twap(self, pos: Position) -> Optional[bool]:
        """Authoritative resolution via the OFFICIAL Chainlink TWAP stream.

        close = official TWAP at end_ts; open = official TWAP at start_ts
        (= end_ts - interval; identical to the previous interval's close — hence
        open(N) == close(N-1)). Falls back to the captured start_price if the
        open TWAP sample is missing. Returns None when official TWAP data is
        unavailable (caller falls back to the local tick-average reconstruction).
        """
        if not self.s.chainlink_twap_enabled:
            return None
        close_val = self.prices.get_twap_at(pos.asset, float(pos.end_ts))
        if close_val is None:
            return None
        start_ts = pos.end_ts - self.s.interval_minutes * 60
        open_val = self.prices.get_twap_at(pos.asset, float(start_ts))
        if open_val is None:
            open_val = self.market_data.start_prices.get(str(start_ts), {}).get(pos.asset)
        if open_val is None:
            return None
        up_won = close_val >= open_val
        self.log.info(f"[{pos.asset}] official TWAP: close=${close_val:.2f} vs open=${open_val:.2f}")
        return up_won if pos.direction == "UP" else (not up_won)

    def _resolve_from_history(self, pos: Position) -> Optional[bool]:
        """TWAP (30-sec average) of Chainlink/Binance prices before end_ts."""
        history = self.prices.chainlink_history.get(pos.asset, [])
        if not history:
            history = self.prices.binance_direct_history.get(pos.asset, [])
        if not history:
            return None
        twap = []
        for item in history:
            ts_sec = item[1] if len(item) == 3 else item[0]
            price = item[2] if len(item) == 3 else item[1]
            if pos.end_ts - 30 <= ts_sec <= pos.end_ts + 5:
                twap.append(price)
        if twap:
            best_price = sum(twap) / len(twap)
        else:
            best_price = None; best_diff = float("inf")
            for item in history:
                ts_sec = item[1] if len(item) == 3 else item[0]
                price = item[2] if len(item) == 3 else item[1]
                diff = abs(ts_sec - pos.end_ts)
                if diff < best_diff:
                    best_diff = diff; best_price = price
            if best_price is None or best_diff > 120:
                return None
        key = str(pos.end_ts - self.s.interval_minutes * 60)
        start_price = self.market_data.start_prices.get(key, {}).get(pos.asset)
        if not start_price:
            return None
        up_won = best_price >= start_price
        return up_won if pos.direction == "UP" else (not up_won)

    def _log_resolution(self, pos: Position, won: Optional[bool], pnl: float,
                        proceeds: float, fee: float, method: Optional[str],
                        cl_won: Optional[bool] = None,
                        tok_won: Optional[bool] = None,
                        agree: Optional[bool] = None) -> None:
        """Structured resolution record → logs/live_results.jsonl.

        Mirrors the demo's _log_result. One line per resolved position; records
        BOTH resolution sources so the cross-validation (подстраховка) can be
        audited offline: how often Chainlink and the leader token price agree,
        and on which side they diverge.
        """
        now = time.time()
        # official TWAP boundary values used for resolution (None if unavailable)
        _start_ts = pos.end_ts - self.s.interval_minutes * 60
        twap_open = self.prices.get_twap_at(pos.asset, float(_start_ts)) if self.s.chainlink_twap_enabled else None
        twap_close = self.prices.get_twap_at(pos.asset, float(pos.end_ts)) if self.s.chainlink_twap_enabled else None
        rec = {
            "ts": int(now),
            "iso_ts": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
            "event": "RESOLVE",
            "slug": pos.slug,
            "asset": pos.asset,
            "direction": pos.direction,
            "won": won,
            "pnl": round(pnl, 4),
            "entry_price": pos.entry_price,
            "shares": pos.entry_size,
            "cost": pos.entry_cost,
            "proceeds": round(proceeds, 4),
            "fee": round(fee, 4),
            "resolve_method": method,
            "chainlink_won": cl_won,
            "token_won": tok_won,
            "cross_agree": agree,
            "twap_open": round(twap_open, 2) if twap_open is not None else None,
            "twap_close": round(twap_close, 2) if twap_close is not None else None,
            "end_ts": int(pos.end_ts),
            "balance_after": round(self.status.bot_balance, 4),
        }
        try:
            p = Path(self.s.log_dir) / "live_results.jsonl"
            with open(p, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except OSError:
            pass

    async def _late_cross_check(self, snap: dict) -> None:
        """Ground-truth recheck `cross_late_snapshot_secs` after close.

        Reads the eventual token outcome (CLOB /price → Gamma) once the market
        has fully settled and compares it to what we resolved. Logs to
        logs/cross_snapshots.jsonl so the real Chainlink-vs-token agreement rate
        can be measured on live data — including trades the cascade closed early
        before the token polarised. Fire-and-forget; never affects accounting.
        """
        try:
            delay = snap["end_ts"] + self.s.cross_late_snapshot_secs - time.time()
            if delay > 0:
                await asyncio.sleep(delay)
            late_won, late_src = await late_truth_outcome(
                snap["token_id"], snap["slug"], snap["direction"],
                self.http, self.s)
        except asyncio.CancelledError:
            return
        except Exception as e:
            self.log.warning(f"[{snap['asset']}] late cross-check failed", error=str(e))
            return
        agrees = (late_won == snap["resolved_won"]) if (
            late_won is not None and snap["resolved_won"] is not None) else None
        rec = {
            "ts": int(time.time()),
            "iso_ts": datetime.fromtimestamp(time.time(), tz=timezone.utc).isoformat(),
            "event": "LATE_SNAPSHOT",
            "slug": snap["slug"], "asset": snap["asset"],
            "direction": snap["direction"],
            "resolved_won": snap["resolved_won"],
            "resolved_method": snap["resolved_method"],
            "chainlink_won_at_resolve": snap["chainlink_won_at_resolve"],
            "token_won_at_resolve": snap["token_won_at_resolve"],
            "late_won": late_won, "late_source": late_src,
            "late_agrees_with_resolve": agrees,
        }
        try:
            p = Path(self.s.log_dir) / "cross_snapshots.jsonl"
            with open(p, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except OSError:
            pass
        if agrees is False:
            self.log.warning(
                f"[{snap['asset']}] LATE MISMATCH {snap['slug']}: resolved="
                f"{'WIN' if snap['resolved_won'] else 'LOSS'} but late="
                f"{'WIN' if late_won else 'LOSS'} ({late_src}) — possible mis-resolution",
                slug=snap["slug"], late_source=late_src)

    def _graceful_shutdown(self) -> None:
        for pos in self.positions.values():
            if not pos.closed:
                for oid in (pos.tp_order_id, pos.order_id):
                    if oid:
                        self.client.cancel(oid)
