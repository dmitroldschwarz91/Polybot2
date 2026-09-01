"""
Demo trading engine — live market data, virtual money.

Connects to the SAME real-time feeds as the live bot (Binance, Polymarket
market channel, RTDS Chainlink) but executes trades in a virtual account:
  * Entries are filled instantly at the real best_ask (+slippage) from the
    live order book — no CLOB API key needed.
  * Capital is virtual ($15 by default) and compounds: wins grow it, losses
    shrink it, just like the compounding analysis showed.
  * Positions resolve at real market close (HOLD mode — our walk-forward
    proved HOLD beats TP/SL).
  * Settings default to the walk-forward consensus: threshold 0.75, sr ~30%.

This lets you watch the strategy run against the real market — see real
entries, real fills, real outcomes — with zero financial risk.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import aiohttp

from ..config import Settings
from ..core.http import AsyncHTTP
from ..core.logging import StructuredLogger, build_logger
from ..domain.enums import CloseReason, EntryType
from ..domain.models import Position, TradeStats
from ..domain.resolution import (
    cross_validate, gamma_outcome, leader_token_outcome, late_truth_outcome,
)
from ..marketdata.stores import LivePriceStore, OrderBook
from ..marketdata.websockets import WebSocketManager
from ..marketdata.book_poller import BookPoller
from ..risk.manager import RiskManager
from ..strategies.vacuum_scalp import VacuumScalpStrategy
from ..strategies.simple_entry import SimpleEntryStrategy
from ..strategies.base import Opportunity
from ..strategies.favdip import (
    check_entry as _favdip_check,
    FAVDIP_CAP, FAVDIP_MOM_K, FAVDIP_MOM_MIN, FAVDIP_MIN_LEG1,
)
from ..strategies.pair_first import PairFirstStrategy as _PairFirstStrategy
from ..strategies.longshot import check_entry as _longshot_check
from ..marketdata.markets import MarketData
from ..sample_io import append_sample


# ── demo defaults from walk-forward analysis ──────────────────────────────

DEMO_THRESHOLD = 0.75          # optimal entry threshold (walk-forward consensus)
DEMO_SLIPPAGE = 0.01           # buy at ask + 1 tick (pessimistic)
DEMO_FEE = 0.02                # Polymarket taker fee on winning side
DEMO_STAKE_RATIO = 0.05        # Kelly ~20% -> quarter-Kelly 5% (WR~55%, иначе ruin)
DEMO_ENTRY_START = 150         # vacuum scalp window start (secs before close)
DEMO_ENTRY_END = 90            # vacuum scalp window end (no entries after 90s)
DEMO_START_CAPITAL = 50.0

# ZScore Reversal strategy (mean reversion + trailing stop)
ZSCORE_MIN_Z = 1.5             # min |Z| for entry (moderate, not extreme)
ZSCORE_MAX_Z = 3.0             # max |Z| (above = trend, no reversion)
ZSCORE_OPP_MAX_PRICE = 0.30    # only buy opposite if <= this price
ZSCORE_TRAIL_PCT = 0.05        # trailing stop: sell when fallen 5% from peak
ZSCORE_MIN_GROWTH = 0.05      # min growth guard: only sell after 5%+ rise from entry
ZSCORE_STD_WINDOW = 20         # points for std calculation
ZSCORE_PAIR_SUM = 1.01         # assumed up+down token price sum

# ZPair: Z-триггер + асинхронная пара (финальная стратегия). Z — ВЕРХНИЙ порог.
ZPAIR_MAX_Z        = 3.0       # ВЕРХНИЙ порог |Z|: отсекаем большие (тренды); Z<3
ZPAIR_MIN_LEG1     = 0.04      # мин. цена ноги1 (отсек $0.02-артефакт)
ZPAIR_MAX_LEG1     = 0.30      # макс. цена ноги1 (opposite умеренно дёшев)
ZPAIR_TARGET_SUM   = 0.40      # замыкаем пару когда p1 + other_ask < этого (0.40 — оптимум для обеих)
ZPAIR_WIN_LO       = 30        # окно входа (secs to close), начало
ZPAIR_WIN_HI       = 270       # окно входа, конец (входить рано — больше времени на откат)
ZPAIR_LIMIT_MODE   = True      # True = рестинг-лимит на ask (ждём продавца); False = мгновенный market
ZPAIR_MIN_FILL_VOL = 5         # мин. ask_volume для заполнения ордера (мин-ордер 5 акций)
ZPAIR_BOOK_STALE_SECS = 5.0    # не заполнять лимит, если книга не обновлялась > 5с (ask_vol протух)
# FavDip constants — импортированы из strategies/favdip.py


@dataclass
class DemoPosition:
    """A virtual open position."""
    slug: str
    asset: str
    token_id: str
    direction: str           # UP / DOWN
    entry_price: float
    shares: int
    cost: float
    entry_ts: float
    end_ts: int
    interval_ts: int
    closed: bool = False
    close_reason: str = ""
    pnl: float = 0.0
    won: Optional[bool] = None
    peak_price: float = 0.0  # ZScore Reversal: peak opposite price since entry
    is_reversal: bool = False  # True = ZScoreReversal (trailing exit)
    # ZPair
    is_pair: bool = False
    leg2_token_id: Optional[str] = None
    leg2_price: float = 0.0
    leg2_filled: bool = False


@dataclass
class DemoStatus:
    """Live snapshot for the dashboard."""
    running: bool = False
    started_at: Optional[float] = None
    virtual_capital: float = DEMO_START_CAPITAL
    start_capital: float = DEMO_START_CAPITAL

    def to_dict(self, positions, stats, risk, prices, pending=None) -> dict:
        open_pos = []
        for p in positions.values():
            if p.closed:
                continue
            cur_price = prices.get_lot_price(p.token_id) or p.entry_price
            unrealized = (cur_price - p.entry_price) * p.shares
            rec = {
                "asset": p.asset, "direction": p.direction,
                "entry_price": p.entry_price, "shares": p.shares,
                "cost": p.cost, "unrealized_pnl": round(unrealized, 4),
                "current_price": cur_price,
                "secs_to_close": max(0, p.end_ts - int(time.time())),
                "is_pair": getattr(p, "is_pair", False),
                "leg2_filled": getattr(p, "leg2_filled", False),
                "leg2_price": p.leg2_price if getattr(p, "leg2_filled", False) else None,
            }
            if getattr(p, "is_pair", False) and getattr(p, "leg2_filled", False):
                rec["locked_pnl"] = round(p.shares * (0.98 - p.entry_price - p.leg2_price), 2)
                rec["pair_sum"] = round(p.entry_price + p.leg2_price, 3)
            open_pos.append(rec)
        return {
            "running": self.running,
            "demo": True,   # ← dashboard shows DEMO badge
            "uptime": round(time.time() - self.started_at, 0) if self.started_at else 0,
            "virtual_capital": round(self.virtual_capital, 2),
            "start_capital": self.start_capital,
            "return_pct": round((self.virtual_capital / self.start_capital - 1) * 100, 2),
            "positions": open_pos,
            "open_count": len(open_pos),
            "pending_leg1": len(pending) if pending else 0,
            "stats": stats.to_dict(),
            "risk": risk.snapshot() if risk else {},
            "prices": prices.snapshot(),
        }


class DemoEngine:
    """Runs the strategy on live data with virtual execution."""

    def __init__(self, settings: Settings,
                 log: Optional[StructuredLogger] = None,
                 start_capital: float = DEMO_START_CAPITAL,
                 threshold: float = DEMO_THRESHOLD,
                 stake_ratio: float = DEMO_STAKE_RATIO,
                 strategy: str = "vacuum_scalp") -> None:
        self.s = settings
        # Duplicate the full demo log stream to logs/demo.log (rotating JSON),
        # so trade outcomes (RESOLVE WIN/LOSS) are persisted to file, not only console.
        self.log = log or build_logger(
            "demo", json_file=str(Path(settings.log_dir) / "demo.log")
        )
        self.start_capital = start_capital
        self.threshold = threshold
        self.stake_ratio = stake_ratio
        self.strategy_name = strategy

        # ── shared infrastructure with the live bot ──
        self.prices = LivePriceStore(settings.assets, settings.ws_book_stale_secs)
        self.fills = type("F", (), {"orders": {}})()  # dummy fill store
        self.http = AsyncHTTP()
        self.ws = WebSocketManager(settings, self.prices, self.fills, self.log)
        self.book_poller = BookPoller(settings, self.prices, self.log, poll_interval=5.0)
        self.market_data = MarketData(settings, self.prices, self.http, self.log)
        # strategy + risk
        self.s_demo = settings.model_copy()
        self.s_demo.vacuum_scalp_enabled = True
        self.s_demo.max_stake_ratio = stake_ratio
        self.s_demo.assets = settings.assets
        self.s_demo.max_concurrent_positions = 1
        # демо: отключаем лимиты убытков — при WR 55% просадки 40%+ штатны,
        # лимиты 30/50% будут ложно стопить. 1.0 = никогда не сработают.
        self.s_demo.max_daily_loss_pct = 1.0
        self.s_demo.max_drawdown_pct = 1.0

        self._pf = None                       # PairFirst strategy instance (if active)
        self._pair_target = ZPAIR_TARGET_SUM  # leg2 completion target (overridden by pair_first)

        if self.strategy_name == "zscore_reversal":
            self.strategy = None  # ZScoreReversal has its own logic, no strategy.check()
            self.log.info("DEMO: Using ZScoreReversal (mean reversion + trailing stop)")
            self.log.info(f"  Z∈[{ZSCORE_MIN_Z},{ZSCORE_MAX_Z}] opp≤${ZSCORE_OPP_MAX_PRICE} "
                          f"trail={ZSCORE_TRAIL_PCT:.0%}")
        elif self.strategy_name == "zpair":
            self.strategy = None  # ZPair: своя логика (leg1 по Z, leg2 по сумме)
            self.log.info("DEMO: Using ZPair (Z-trigger + async pair, без trailing)")
            self.log.info(f"  Z<{ZPAIR_MAX_Z} leg1∈[${ZPAIR_MIN_LEG1},{ZPAIR_MAX_LEG1}] "
                          f"target_sum≤${ZPAIR_TARGET_SUM} limit={ZPAIR_LIMIT_MODE}")
        elif self.strategy_name == "favdip":
            self.strategy = None  # FavDip: фаворит на dip + momentum revert
            self.log.info("DEMO: Using FavDip (фаворит на dipе + momentum revert)")
            self.log.info(f"  cap≤${FAVDIP_CAP} mom_K={FAVDIP_MOM_K}s |mom|≥${FAVDIP_MOM_MIN} "
                          f"target≤${ZPAIR_TARGET_SUM}")
        elif self.strategy_name == "pair_first":
            self.strategy = None  # PairFirst: rolling-low leg1 + async pair
            self._pf = _PairFirstStrategy(
                cap=self.s.pair_entry_cap, min_leg1=self.s.pair_min_leg1,
                window=self.s.pair_rolling_window, target=self.s.pair_target,
                win_lo=self.s.pair_win_lo, win_hi=self.s.pair_win_hi)
            self._pair_target = self.s.pair_target
            self.log.info("DEMO: Using PairFirst (rolling-low leg1 + async pair)")
            self.log.info(f"  cap≤${self.s.pair_entry_cap} rolling={self.s.pair_rolling_window:.0f}s "
                          f"target≤${self.s.pair_target} twap_alive≤{self.s.pair_twap_alive_max_age:.0f}s")
        elif self.strategy_name == "longshot":
            self.strategy = None  # Longshot: favorite-longshot bias (buy the underdog)
            self.log.info("DEMO: Using Longshot (favorite-longshot bias: buy underdog, HOLD)")
            self.log.info(f"  underdog∈[${self.s.longshot_min},{self.s.longshot_max}] "
                          f"stc∈[{self.s.longshot_win_lo},{self.s.longshot_win_hi}] "
                          f"kelly={self.s.longshot_kelly:.1%}")
        elif self.strategy_name == "simple":
            self.strategy = SimpleEntryStrategy(self.s_demo, threshold=threshold)
            self.log.info("DEMO: Using SimpleEntryStrategy (backtest mode)")
        else:
            self.s_demo.vacuum_scalp_min_token_price = threshold
            self.s_demo.vacuum_scalp_max_token_price = 0.92   # live data: 0.88 too tight on volatile market -> 0.92
            self.s_demo.vacuum_scalp_max_volatility = 0.0010
            self.s_demo.vacuum_scalp_min_deviation = 0.0005
            self.s_demo.vacuum_scalp_book_max_age = 30.0
            self.strategy = VacuumScalpStrategy(self.s_demo)
            self.log.info("DEMO: Using VacuumScalpStrategy (full filters)")

        self.risk = RiskManager(self.s_demo)
        self.risk.state.new_day(start_capital)
        self.risk.state.new_day(start_capital)

        # ── virtual state ──
        self.status = DemoStatus(
            virtual_capital=start_capital, start_capital=start_capital,
        )
        self._zpair_prices: Dict[str, list] = {}      # asset -> [oracle с начала интервала]
        self._zpair_last_ts: Dict[str, float] = {}     # asset -> ts последней точки (троттлинг 1/сек)
        self._pending_leg1: Dict[str, dict] = {}       # slug -> pending limit-ордер ноги1
        self.positions: Dict[str, DemoPosition] = {}
        self.stats = TradeStats()
        self.traded: Set[str] = set()
        self.known_tokens: Set[str] = set()
        self._cur_interval = 0
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        # persisted closed-trade history (survives restarts; feeds compare())
        self.history_path = Path(settings.backtest_data_dir) / "demo_history.json"
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        self.closed_history: List[dict] = self._load_history()
        # JSONL log for decision tracking (entries and skips)
        self.decisions_log_path = Path(settings.log_dir) / "demo_decisions.jsonl"
        self.decisions_log_path.parent.mkdir(parents=True, exist_ok=True)
        # JSONL log for trade RESULTS (win/loss + pnl + resolution method).
        # Mirrors decisions_log: structured, one record per resolved position,
        # survives restarts — replaces fragile capital-delta reconstruction.
        self.results_log_path = Path(settings.log_dir) / "demo_results.jsonl"
        self.results_log_path.parent.mkdir(parents=True, exist_ok=True)
        # JSONL log for interval SAMPLES — second-by-second market state across
        # the WHOLE interval (oracle/deviation/range5/book), not just the entry
        # window. range5 looks 5 min back, confirmation 60s back — both outside
        # the window — so the full path is needed to reconstruct filters later.
        self.samples_log_path = Path(settings.log_dir) / "demo_interval_samples.jsonl"
        self.samples_log_path.parent.mkdir(parents=True, exist_ok=True)
        self._last_reject_log = {}  # throttle dict
        self._last_sample_ts = {}   # interval-sampler throttle dict

    # ── lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self.status.running:
            return
        self.log.info("=" * 60)
        self.log.info("DEMO ENGINE — live data, virtual money")
        self.log.info(f"Capital: ${self.start_capital} | Threshold: {self.threshold} | "
                      f"Stake: {self.stake_ratio:.0%} | Mode: HOLD")
        self.log.info(f"NO real orders. NO API key needed.")
        self.log.info("=" * 60)

        await self.http.session()  # init async HTTP
        self.ws.start()
        self.book_poller.start()
        self.status.running = True
        self.status.started_at = time.time()
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="demo-engine")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        await self.book_poller.stop()
        await self.ws.stop()
        await self.http.close()
        self.status.running = False
        self.log.info("Demo engine stopped.")

    @property
    def running(self) -> bool:
        return self.status.running

    # ── main loop ────────────────────────────────────────────────────────

    async def _run(self) -> None:
        try:
            await self._wait_for_feeds()
            last_resolve = 0.0
            while not self._stop.is_set():
                t0 = time.time()
                try:
                    await self._tick()
                except Exception as e:
                    self.log.error("[DEMO] tick error", error=str(e))

                # resolve closed positions every 2s
                if t0 - last_resolve >= 2.0:
                    await self._resolve_positions()
                    last_resolve = t0

                elapsed = time.time() - t0
                await asyncio.sleep(max(0.1, 0.5 - elapsed))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log.error("[DEMO] crashed", error=str(e), exc_info=True)

    async def _tick(self) -> None:
        """One evaluation pass — discover markets, check entries."""
        cur = self.market_data.current_interval_ts()
        if cur != self._cur_interval:
            # ZPair: свежий аккумулятор oracle и сброс pending (старые истекли)
            self._zpair_prices = {a: [] for a in self.s_demo.assets}
            self._zpair_last_ts = {}
            self._pending_leg1 = {}
            if self._pf is not None:
                self._pf.reset()   # drop stale token rolling-low history
            # new interval — clear traded set
            if self._cur_interval > 0:
                self.traded.clear()
                # Unwatch old tokens from BookPoller (they belong to expired markets)
                if self.known_tokens:
                    self.book_poller.unwatch(self.known_tokens)
                    self.prices.cleanup_old_tokens(self.known_tokens)
                    self.known_tokens.clear()
                # Prune _last_reject_log (keep only last 20 entries)
                if len(self._last_reject_log) > 20:
                    sorted_keys = sorted(self._last_reject_log.items(),
                                         key=lambda x: x[1], reverse=True)
                    self._last_reject_log = dict(sorted_keys[:20])
                # Prune _last_sample_ts (keep only last 40 entries)
                if len(self._last_sample_ts) > 40:
                    sorted_keys = sorted(self._last_sample_ts.items(),
                                         key=lambda x: x[1], reverse=True)
                    self._last_sample_ts = dict(sorted_keys[:40])
            self._cur_interval = cur
            # set start price + reset VWAP accumulators for the new interval
            for asset in self.s_demo.assets:
                op = self.prices.get_oracle_price(asset)
                if op:
                    self.market_data.start_prices.setdefault(str(cur), {})[asset] = op
                self.prices.reset_vwap(asset)

        # fetch markets + subscribe to token books
        markets = await asyncio.gather(
            *[self.market_data.fetch_market(a) for a in self.s_demo.assets],
            return_exceptions=True
        )
        new_tokens = set()
        for m in markets:
            if m:
                for tid in (m.get("up_token_id"), m.get("down_token_id")):
                    if tid and tid not in self.known_tokens:
                        new_tokens.add(tid)
                        self.known_tokens.add(tid)
        if new_tokens:
            self.log.info(f"[DEMO] Subscribing to {len(new_tokens)} new market tokens...")
            await self.ws.subscribe_market_tokens(new_tokens)
            self.book_poller.watch(new_tokens)

        # evaluate entries
        for asset, market in zip(self.s_demo.assets, markets):
            if not market:
                continue

            stc = market["end_ts"] - time.time()

            # ── interval sample: whole interval (1s), for later filter reconstruction ──
            self._sample_interval(market, asset, stc)

            # ── ZPair: накопление oracle с начала интервала, 1 точка/сек (как в бэктесте) ──
            _op_now = self.prices.get_oracle_price(asset)
            if _op_now:
                _tn = time.time()
                if _tn - self._zpair_last_ts.get(asset, 0.0) >= 1.0:
                    self._zpair_last_ts[asset] = _tn
                    self._zpair_prices.setdefault(asset, []).append(_op_now)

            # ── ZScoreReversal: trailing exits (every tick) + entry in window ──
            if self.strategy_name == "zscore_reversal":
                self._check_trailing_exits()
                if 60 <= stc <= 240 and market["slug"] not in self.traded:
                    self._check_zscore_entry(market, asset, stc)
                continue  # skip VacuumScalp logic

            # ── ZPair: заполняем pending-лимиты, докупаем ногу2, открываем ногу1 ──
            if self.strategy_name == "zpair":
                self._check_zpair_pending()              # fill рестинг-лимитов ноги1
                self._check_zpair_complete()             # докупка ноги2 (замыкание пары)
                if (ZPAIR_WIN_LO <= stc <= ZPAIR_WIN_HI
                        and market["slug"] not in self.traded
                        and market["slug"] not in self._pending_leg1):
                    self._check_zpair_entry(market, asset, stc)
                continue

            # ── FavDip: фаворит на dip + momentum revert ──
            if self.strategy_name == "favdip":
                self._check_zpair_pending()
                self._check_zpair_complete()
                if (ZPAIR_WIN_LO <= stc <= ZPAIR_WIN_HI
                        and market["slug"] not in self.traded
                        and market["slug"] not in self._pending_leg1):
                    self._check_favdip_entry(market, asset, stc)
                continue

            # ── PairFirst: rolling-low leg1 + async pair ──
            if self.strategy_name == "pair_first":
                self._check_zpair_pending()
                self._check_zpair_complete()
                if (self.s.pair_win_lo <= stc <= self.s.pair_win_hi
                        and market["slug"] not in self.traded
                        and market["slug"] not in self._pending_leg1):
                    self._check_pair_first_entry(market, asset, stc)
                continue

            # ── Longshot: buy underdog, HOLD to resolution ──
            if self.strategy_name == "longshot":
                if (self.s.longshot_win_lo <= stc <= self.s.longshot_win_hi
                        and market["slug"] not in self.traded):
                    self._check_longshot_entry(market, asset, stc)
                continue

            op = self.prices.get_oracle_price(asset)
            start_p = market.get("target_price")
            leader_token_id = (market["up_token_id"] if (op and start_p and op >= start_p) else market.get("down_token_id")) if op and start_p else None
            tok_price = self.prices.get_lot_price(leader_token_id) if leader_token_id else None
            
            # Log market status every ~60 seconds for visibility
            key = f"status_{asset}_{market.get('slug', '')}"
            last_ts = self._last_reject_log.get(key, 0)
            if time.time() - last_ts > 60:
                self._last_reject_log[key] = time.time()
                stc_str = f"{int(stc)}s"
                op_str = f"${op:.2f}" if op else "None"
                tok_str = f"${tok_price:.3f}" if tok_price else "None"
                phase = "WINDOW" if (DEMO_ENTRY_END < stc <= DEMO_ENTRY_START) else "WAIT"
                self.log.info(
                    f"[DEMO] STATUS {asset} | stc={stc_str} ({phase}) | "
                    f"oracle={op_str} | leader_tok={tok_str}"
                )

            # Skip if outside the entry window
            if stc <= DEMO_ENTRY_END or stc > DEMO_ENTRY_START:
                continue

            if market["slug"] in self.traded:
                continue

            # risk gate (pass REAL open positions, not empty dict!)
            open_positions = {s: p for s, p in self.positions.items() if not p.closed}
            ok, reason = self.risk.can_open_new(open_positions, self.status.virtual_capital)
            if not ok:
                self.log.info(f"[DEMO] {asset} blocked by risk: {reason}")
                continue
            # ── Data freshness watchdog ──
            op = self.prices.get_oracle_price(asset)
            if op is None:
                self.log.warning(f"[DEMO] {asset} Oracle data STALE, halting entries")
                continue

            # strategy check
            opp = self._check_entry(market, asset)
            if opp and opp.can_enter:
                self._simulate_entry(market, asset, opp)

    def _check_entry(self, market: dict, asset: str) -> Optional[Opportunity]:
        """Run the vacuum scalp strategy on live data. Logs rejection reasons."""
        try:
            stc = market["end_ts"] - time.time()
            opp = self.strategy.check(
                market, asset, self.traded, self.status.virtual_capital,
                self.prices, self.market_data, self.risk,
            )

            if opp and opp.can_enter:
                self._log_decision(market, asset, opp, stc)
                return opp

            # ── Log skips to JSONL (throttled per market) ──
            # Default 1s: dense second-by-second path of oracle/token prices in
            # the entry window → enables later analysis WITHOUT interpolation.
            key = f"{asset}_{market.get('slug', '')}"
            last_ts = self._last_reject_log.get(key, 0)
            if time.time() - last_ts >= self.s.demo_decision_log_interval_secs:
                self._last_reject_log[key] = time.time()
                self._log_decision(market, asset, opp, stc)

            return None
        except Exception as e:
            self.log.error("[DEMO] check error", error=str(e), exc_info=True)
            return None

    def _check_zscore_entry(self, market: dict, asset: str, stc: float) -> None:
        """ZScoreReversal entry: buy OPPOSITE token on moderate Z-Score extremum."""
        op = self.prices.get_oracle_price(asset)
        if not op:
            return
        vwap = self.prices.get_vwap(asset)
        if not vwap:
            return
        hist = self.prices.binance_direct_history.get(asset) or self.prices.chainlink_history.get(asset)
        if not hist or len(hist) < 5:
            return
        recent = [item[-1] if len(item) == 2 else (item[2] if len(item) >= 3 else None)
                  for item in list(hist)[-ZSCORE_STD_WINDOW:]]
        recent = [p for p in recent if p is not None]
        if len(recent) < 5:
            return
        mean_p = sum(recent) / len(recent)
        var = sum((p - mean_p) ** 2 for p in recent) / len(recent)
        std = math.sqrt(var)
        if std <= 0:
            return
        z = (op - vwap) / std
        az = abs(z)
        if not (ZSCORE_MIN_Z <= az < ZSCORE_MAX_Z):
            return
        direction = "DOWN" if z > 0 else "UP"
        token_id = market["down_token_id"] if direction == "DOWN" else market["up_token_id"]
        if not token_id:
            return
        book = self.prices.get_book(token_id)
        if not book or book.best_ask is None:
            return
        opp_price = book.best_ask
        if opp_price <= 0.005 or opp_price > ZSCORE_OPP_MAX_PRICE:
            return
        open_positions = {s: p for s, p in self.positions.items() if not p.closed}
        ok, _ = self.risk.can_open_new(open_positions, self.status.virtual_capital)
        if not ok:
            return
        fill_price = min(0.999, opp_price + DEMO_SLIPPAGE)
        stake = self.status.virtual_capital * self.stake_ratio
        shares = int(stake / fill_price) if fill_price > 0 else 0
        if shares < 5:
            if self.status.virtual_capital >= 5 * fill_price:
                shares = 5
            else:
                return
        cost = round(shares * fill_price, 4)
        if cost > self.status.virtual_capital:
            shares = max(5, int(self.status.virtual_capital / fill_price))
            if shares < 5:
                return
            cost = round(shares * fill_price, 4)
        self.status.virtual_capital = round(self.status.virtual_capital - cost, 4)
        pos = DemoPosition(
            slug=market["slug"], asset=asset, token_id=token_id,
            direction=direction, entry_price=fill_price, shares=shares, cost=cost,
            entry_ts=time.time(), end_ts=market["end_ts"],
            interval_ts=self._cur_interval,
            peak_price=fill_price, is_reversal=True,
        )
        self.positions[market["slug"]] = pos
        self.traded.add(market["slug"])
        self.log.info(
            f"[DEMO] ZSCORE ENTRY {asset} {direction} | Z={z:+.1f} | "
            f"{shares}sh @ ${fill_price:.3f}=${cost:.2f} | "
            f"oracle=${op:.1f} vwap=${vwap:.1f}"
        )

    def _check_trailing_exits(self) -> None:
        """Check trailing stop for open ZScoreReversal positions."""
        for slug, pos in list(self.positions.items()):
            if pos.closed or not pos.is_reversal:
                continue
            current = self.prices.get_lot_price(pos.token_id)
            if current is None:
                book = self.prices.get_book(pos.token_id)
                current = book.best_ask if book else None
            if current is None or current <= 0:
                continue
            if current > pos.peak_price:
                pos.peak_price = current
            if pos.peak_price >= pos.entry_price * (1 + ZSCORE_MIN_GROWTH) \
                    and current <= pos.peak_price * (1 - ZSCORE_TRAIL_PCT):
                proceeds = current * pos.shares
                # Polymarket CLOB: 0% trading fee (fee only at resolution on winning side).
                # Trailing exit = sell on CLOB → no fee.
                fee = 0.0
                pnl = proceeds - pos.cost
                pos.closed = True
                pos.won = pnl > 0
                pos.pnl = round(pnl, 4)
                pos.close_reason = "trailing_stop"
                self.status.virtual_capital = round(
                    self.status.virtual_capital + proceeds - fee, 4)
                self.risk.record_realized_pnl(pnl)
                self.stats.record(pnl, EntryType.VACUUM_SCALP, CloseReason.TAKE_PROFIT)
                self._log_result(pos, pos.won, pnl, proceeds, fee, "trailing_stop")
                self.closed_history.append({
                    "interval_ts": pos.interval_ts, "asset": pos.asset,
                    "direction": pos.direction, "entry_price": pos.entry_price,
                    "shares": pos.shares, "cost": pos.cost,
                    "won": pos.won, "pnl": pos.pnl,
                    "entry_ts": pos.entry_ts, "end_ts": pos.end_ts,
                    "fill_mode": "ask_plus_slippage",
                    "resolve_method": "trailing_stop",
                    "peak_price": pos.peak_price,
                })
                self._save_history()
                self.positions.pop(slug, None)
                self.log.info(
                    f"[DEMO] TRAILING EXIT {pos.asset} {pos.direction} | "
                    f"sell=${current:.3f} peak=${pos.peak_price:.3f} | "
                    f"pnl=${pnl:+.2f} cap=${self.status.virtual_capital:.2f}"
                )

    def _check_zpair_entry(self, market: dict, asset: str, stc: float) -> None:
        """ZPair нога1: на спокойном |Z| (< ZPAIR_MAX_Z) покупаем opposite-токен.
        Логирует КАЖДОЕ решение в demo_decisions.jsonl (entry + все skip-причины).
        limit-режим = рестинг-лимит на ask, иначе мгновенный fill."""
        op = self.prices.get_oracle_price(asset)
        vwap = self.prices.get_vwap(asset)
        ps = self._zpair_prices.get(asset) or []
        z = None
        direction = None
        p1 = None
        token_id = None
        other_id = None
        reason = "entry"
        if not op or not vwap or len(ps) < 5:
            reason = "no_data"
        else:
            mean_p = sum(ps) / len(ps)
            std = math.sqrt(sum((x - mean_p) ** 2 for x in ps) / len(ps))
            if std <= 0:
                reason = "no_data"
            else:
                z = (op - vwap) / std
                direction = "DOWN" if z > 0 else "UP"
                token_id = market["down_token_id"] if direction == "DOWN" else market["up_token_id"]
                other_id = market["up_token_id"] if direction == "DOWN" else market["down_token_id"]
                if abs(z) >= ZPAIR_MAX_Z:
                    reason = "z_too_high"
                elif not token_id or not other_id:
                    reason = "no_token"
                else:
                    book = self.prices.get_book(token_id)
                    if not book or book.best_ask is None:
                        reason = "no_book"
                    else:
                        p1 = book.best_ask
                        if p1 < ZPAIR_MIN_LEG1:
                            reason = "too_cheap"
                        elif p1 > ZPAIR_MAX_LEG1:
                            reason = "too_expensive"
                        else:
                            open_positions = {s: p for s, p in self.positions.items() if not p.closed}
                            ok, _ = self.risk.can_open_new(open_positions, self.status.virtual_capital)
                            if not ok:
                                reason = "risk_blocked"
        key = f"{asset}_{market.get('slug', '')}"
        last = self._last_reject_log.get(key, 0)
        if time.time() - last >= self.s.demo_decision_log_interval_secs:
            self._last_reject_log[key] = time.time()
            _ub = self.prices.get_book(market.get("up_token_id")) if market.get("up_token_id") else None
            _db = self.prices.get_book(market.get("down_token_id")) if market.get("down_token_id") else None
            _drec = {
                "ts": int(time.time()),
                "iso_ts": datetime.fromtimestamp(time.time(), tz=timezone.utc).isoformat(),
                "asset": asset,
                "action": "ENTRY" if reason == "entry" else "SKIP",
                "reason": reason,
                "strategy": "zpair",
                "secs_to_close": int(stc),
                "z_score": round(z, 2) if z is not None else None,
                "oracle_price": op,
                "vwap": round(vwap, 2) if vwap is not None else None,
                "direction": direction,
                "bought_ask": round(p1, 4) if p1 is not None else None,
                "up_ask": _ub.best_ask if _ub else None,
                "dn_ask": _db.best_ask if _db else None,
                "up_ask_vol": getattr(_ub, "ask_volume", 0.0) if _ub else 0.0,
                "dn_ask_vol": getattr(_db, "ask_volume", 0.0) if _db else 0.0,
                "capital": self.status.virtual_capital,
                "slug": market.get("slug", ""),
            }
            try:
                with open(self.decisions_log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(_drec, ensure_ascii=False) + "\n")
            except OSError:
                pass
        if reason != "entry":
            return
        fill = min(0.999, p1 + DEMO_SLIPPAGE)
        stake = self.status.virtual_capital * self.stake_ratio
        shares = int(stake / fill) if fill > 0 else 0
        if shares < 5:
            shares = 5 if self.status.virtual_capital >= 5 * fill else 0
        if shares == 0:
            return
        if ZPAIR_LIMIT_MODE:
            self._pending_leg1[market["slug"]] = dict(
                asset=asset, token_id=token_id, other_id=other_id, direction=direction,
                limit_price=p1, shares=shares, end_ts=market["end_ts"],
                interval_ts=self._cur_interval, z=z,
            )
            self.traded.add(market["slug"])
            self.log.info(
                f"[DEMO] ZPAIR LEG1 (limit) {asset} {direction} | Z={z:+.1f} | "
                f"{shares}sh @ ask ${p1:.3f} — ждём продавца (ask_vol≥{ZPAIR_MIN_FILL_VOL})"
            )
        else:
            cost = round(shares * fill, 4)
            if cost > self.status.virtual_capital:
                shares = max(5, int(self.status.virtual_capital / fill))
                if shares < 5:
                    return
                cost = round(shares * fill, 4)
            self.status.virtual_capital = round(self.status.virtual_capital - cost, 4)
            self._open_zpair_leg1(market, asset, token_id, other_id, direction, fill, shares)

    def _check_favdip_entry(self, market: dict, asset: str, stc: float) -> None:
        """FavDip нога1: thin wrapper around strategies/favdip.check_entry."""
        sig = _favdip_check(
            market=market, asset=asset, stc=stc,
            prices=self.prices,
            start_prices=self.market_data.start_prices,
            cur_interval=self._cur_interval,
            zpair_prices=self._zpair_prices,
            stake_ratio=self.stake_ratio,
            virtual_capital=self.status.virtual_capital,
            slippage=DEMO_SLIPPAGE,
            risk=self.risk,
            positions=self.positions,
        )
        if sig is None:
            return
        if ZPAIR_LIMIT_MODE:
            self._pending_leg1[market["slug"]] = dict(
                asset=asset, token_id=sig.token_id, other_id=sig.other_id,
                direction=sig.direction, limit_price=sig.limit_price,
                shares=sig.shares, end_ts=market["end_ts"],
                interval_ts=self._cur_interval, z=0,
            )
            self.traded.add(market["slug"])
            self.log.info(
                f"[DEMO] FAVDIP LEG1 (limit) {asset} {sig.direction} | "
                f"mom={sig.momentum:+.1f} | {sig.shares}sh @ ${sig.limit_price:.3f}"
            )
        else:
            fill = min(0.999, sig.limit_price + DEMO_SLIPPAGE)
            cost = round(sig.shares * fill, 4)
            if cost > self.status.virtual_capital:
                return
            self.status.virtual_capital = round(self.status.virtual_capital - cost, 4)
            self._open_zpair_leg1(market, asset, sig.token_id, sig.other_id,
                                 sig.direction, fill, sig.shares)

    def _check_pair_first_entry(self, market: dict, asset: str, stc: float) -> None:
        """PairFirst нога1: rolling-low дешёвого токена + guard живого TWAP."""
        if self._pf is None:
            return
        sig = self._pf.check_entry(
            market=market, asset=asset, stc=stc, prices=self.prices,
            stake_ratio=self.stake_ratio, capital=self.status.virtual_capital,
            risk=self.risk, positions=self.positions,
            twap_max_age=self.s.pair_twap_alive_max_age, slippage=DEMO_SLIPPAGE)
        if sig is None:
            return
        self._pending_leg1[market["slug"]] = dict(
            asset=asset, token_id=sig.token_id, other_id=sig.other_id,
            direction=sig.direction, limit_price=sig.limit_price,
            shares=sig.shares, end_ts=market["end_ts"],
            interval_ts=self._cur_interval, z=0)
        self.traded.add(market["slug"])
        self.log.info(
            f"[DEMO] PAIRFIRST LEG1 (limit) {asset} {sig.direction} | "
            f"new low ${sig.rolling_min:.3f}->{sig.limit_price:.3f} | "
            f"{sig.shares}sh | twap=${sig.twap_value:.0f} alive"
        )

    def _check_longshot_entry(self, market: dict, asset: str, stc: float) -> None:
        """Longshot: buy the underdog (cheap token) and HOLD to resolution.
        Direct instant fill at ask+slippage — NO liquidity/ask_volume check
        (operator request: do not block on thin books)."""
        sig = _longshot_check(
            market=market, asset=asset, stc=stc, prices=self.prices,
            capital=self.status.virtual_capital, kelly_frac=self.s.longshot_kelly,
            price_min=self.s.longshot_min, price_max=self.s.longshot_max,
            win_lo=self.s.longshot_win_lo, win_hi=self.s.longshot_win_hi,
            risk=self.risk, positions=self.positions,
            require_twap_alive=False, slippage=DEMO_SLIPPAGE)
        if sig is None:
            return
        book = self.prices.get_book(sig.token_id)
        if not book or book.best_ask is None:
            return
        fill = min(0.999, book.best_ask + DEMO_SLIPPAGE)
        shares = sig.shares
        cost = round(shares * fill, 4)
        if cost > self.status.virtual_capital:           # shrink to affordable
            shares = max(5, int(self.status.virtual_capital / fill))
            cost = round(shares * fill, 4)
        if shares < 5 or cost > self.status.virtual_capital:
            return
        self.status.virtual_capital = round(self.status.virtual_capital - cost, 4)
        pos = DemoPosition(
            slug=market["slug"], asset=asset, token_id=sig.token_id,
            direction=sig.direction, entry_price=fill, shares=shares, cost=cost,
            entry_ts=time.time(), end_ts=market["end_ts"], interval_ts=self._cur_interval)
        self.positions[market["slug"]] = pos
        self.traded.add(market["slug"])
        self.log.info(
            f"[DEMO] LONGSHOT ENTRY {asset} {sig.direction} | "
            f"underdog @ ${fill:.3f} (leader ${sig.leader_ask:.2f}) | "
            f"{shares}sh=${cost:.2f} | cap=${self.status.virtual_capital:.2f}"
        )

    def _check_zpair_pending(self) -> None:
        """Заполняем рестинг-лимиты ноги1, когда ask дошёл до лимита с объёмом."""
        now = time.time()
        for slug, o in list(self._pending_leg1.items()):
            if now > o["end_ts"]:
                self._pending_leg1.pop(slug, None)
                continue
            book = self.prices.get_book(o["token_id"])
            if not book or book.best_ask is None:
                continue
            bts = getattr(book, "ts", 0.0) or 0.0
            if now - bts > ZPAIR_BOOK_STALE_SECS:
                continue
            ask_vol = getattr(book, "ask_volume", 0.0) or 0.0
            liq_ok = (ask_vol >= ZPAIR_MIN_FILL_VOL) if self.s.require_fill_liquidity else True
            if book.best_ask <= o["limit_price"] and liq_ok:
                fill = min(0.999, o["limit_price"])
                cost = round(o["shares"] * fill, 4)
                if cost > self.status.virtual_capital:
                    self._pending_leg1.pop(slug, None)
                    continue
                self.status.virtual_capital = round(self.status.virtual_capital - cost, 4)
                self._open_zpair_leg1(
                    {"slug": slug, "end_ts": o["end_ts"]}, o["asset"], o["token_id"],
                    o["other_id"], o["direction"], fill, o["shares"], interval_ts=o["interval_ts"])
                self._pending_leg1.pop(slug, None)

    def _open_zpair_leg1(self, market, asset, token_id, other_id, direction, fill, shares,
                         interval_ts=None) -> None:
        """Создаёт DemoPosition ноги1 (общий хелпер для market- и limit-fill)."""
        cost = round(shares * fill, 4)
        pos = DemoPosition(
            slug=market["slug"], asset=asset, token_id=token_id,
            direction=direction, entry_price=fill, shares=shares, cost=cost,
            entry_ts=time.time(), end_ts=market["end_ts"],
            interval_ts=interval_ts if interval_ts is not None else self._cur_interval,
            peak_price=fill, is_pair=True, leg2_token_id=other_id,
        )
        self.positions[market["slug"]] = pos
        self.log.info(
            f"[DEMO] ZPAIR LEG1 FILLED {asset} {direction} | {shares}sh @ ${fill:.3f}=${cost:.2f}"
        )

    def _check_zpair_complete(self) -> None:
        """ZPair нога2: докупаем второй токен, когда сумма < target -> lock прибыли."""
        now = time.time()
        for slug, pos in list(self.positions.items()):
            if pos.closed or not pos.is_pair or pos.leg2_filled:
                continue
            if now > pos.end_ts:
                continue
            book = self.prices.get_book(pos.leg2_token_id)
            if not book or book.best_ask is None:
                continue
            other_ask = book.best_ask
            if pos.entry_price + other_ask >= self._pair_target:
                continue
            fill2 = min(0.999, other_ask + DEMO_SLIPPAGE)
            cost2 = round(pos.shares * fill2, 4)
            if cost2 > self.status.virtual_capital:
                continue
            self.status.virtual_capital = round(self.status.virtual_capital - cost2, 4)
            pos.leg2_price = fill2
            pos.leg2_filled = True
            lock = pos.shares * (0.98 - pos.entry_price - fill2)
            self.log.info(
                f"[DEMO] ZPAIR LEG2 {pos.asset} | {pos.leg2_token_id[:8]}.. @ ${fill2:.3f} | "
                f"sum=${pos.entry_price + fill2:.3f} | LOCKED ${lock:+.2f} "
                f"({pos.shares}sh) | cap=${self.status.virtual_capital:.2f}"
            )

    def _simulate_entry(self, market: dict, asset: str, opp: Opportunity) -> None:
        """Simulate an instant fill at the real best_ask + slippage."""
        token_id = opp.token_id
        book = self.prices.get_book(token_id)
        if not book or book.best_ask is None:
            self.log.debug(f"[DEMO] {asset} no book for entry")
            return

        # fill at real ask + slippage (pessimistic)
        fill_price = min(0.999, book.best_ask + DEMO_SLIPPAGE)

        # compounding sizing: stake = capital * stake_ratio
        stake = self.status.virtual_capital * self.stake_ratio
        shares = int(stake / fill_price) if fill_price > 0 else 0
        if shares < 5:   # min order
            if self.status.virtual_capital >= 5 * fill_price:
                shares = 5
            else:
                return
        cost = round(shares * fill_price, 4)
        if cost > self.status.virtual_capital:
            shares = max(5, int(self.status.virtual_capital / fill_price))
            if shares < 5:
                return
            cost = round(shares * fill_price, 4)

        # deduct from virtual capital
        self.status.virtual_capital = round(self.status.virtual_capital - cost, 4)

        pos = DemoPosition(
            slug=market["slug"], asset=asset, token_id=token_id,
            direction=opp.direction, entry_price=fill_price,
            shares=shares, cost=cost, entry_ts=time.time(),
            end_ts=market["end_ts"], interval_ts=self._cur_interval,
        )
        self.positions[market["slug"]] = pos
        self.traded.add(market["slug"])

        self.log.info(
            f"[DEMO] ENTRY {asset} {opp.direction} | "
            f"{shares} shares @ ${fill_price:.3f} = ${cost:.2f} | "
            f"capital: ${self.status.virtual_capital:.2f} | "
            f"oracle: ${opp.oracle_price:.2f} dev: {(opp.deviation or 0)*100:+.3f}%"
        )

    async def _resolve_positions(self) -> None:
        """Resolve positions whose market has closed (HOLD mode)."""
        now = time.time()
        for slug, pos in list(self.positions.items()):
            if pos.closed:
                continue

            # Wait 30s after market end before first attempt
            if now < pos.end_ts + 30:
                continue

            # замкнутая пара: исход гарантирован (0.98 за пару акций)
            if pos.is_pair and pos.leg2_filled:
                proceeds = pos.shares * 1.0
                fee = proceeds * DEMO_FEE
                pnl = proceeds - fee - pos.cost - round(pos.shares * pos.leg2_price, 4)
                pos.closed = True
                pos.won = pnl > 0
                pos.pnl = round(pnl, 4)
                pos.close_reason = "pair_locked"
                self.status.virtual_capital = round(self.status.virtual_capital + proceeds - fee, 4)
                self.risk.record_realized_pnl(pnl)
                self.stats.record(pnl, EntryType.VACUUM_SCALP, CloseReason.TAKE_PROFIT)
                self._log_result(pos, pos.won, pnl, proceeds, fee, "pair_locked")
                self.closed_history.append({
                    "interval_ts": pos.interval_ts, "asset": pos.asset,
                    "direction": pos.direction, "entry_price": pos.entry_price,
                    "shares": pos.shares, "cost": pos.cost, "won": pos.won, "pnl": pos.pnl,
                    "entry_ts": pos.entry_ts, "end_ts": pos.end_ts,
                    "fill_mode": "limit_or_market", "resolve_method": "pair_locked",
                    "leg2_price": pos.leg2_price,
                })
                self._save_history()
                self.positions.pop(slug, None)
                continue

            resolve_method = None
            won = None
            _cl_won: Optional[bool] = None
            _tok_won: Optional[bool] = None
            _agree: Optional[bool] = None

            # both sources computed once; reused by cross-validation, cascade & log.
            # Chainlink source prefers the OFFICIAL TWAP stream (authoritative);
            # falls back to the local tick-average reconstruction. _cl_src records
            # which one supplied the value (results log / re-analysis).
            _cl_won = self._resolve_from_twap(pos)
            _cl_src = "twap" if _cl_won is not None else None
            if _cl_won is None:
                _cl_won = self._resolve_from_chainlink_history(pos)
                if _cl_won is not None:
                    _cl_src = "chainlink_local"
            _tok_won = await leader_token_outcome(
                pos.token_id, self.prices, self.http, self.s)

            # ── 1. cross-validation FIRST (only conclusive when BOTH present) ──
            if self.s.cross_validate_resolution:
                xcc = cross_validate(_cl_won, _tok_won)
                _agree = xcc.agree
                if xcc.method == "cross_disagree":
                    # two trusted sources conflict → refuse to guess (подстраховка)
                    resolve_method = "cross_disagree"
                elif xcc.method == "cross_ok":
                    won, resolve_method = xcc.won, "cross_ok"

            # ── 2. cascade fallback (cross-validation off OR inconclusive:
            #        only one / no source available) ──
            if won is None and resolve_method != "cross_disagree":
                won = _cl_won
                if won is not None:
                    resolve_method = _cl_src            # "twap" | "chainlink_local"
                if won is None:
                    _gw = await gamma_outcome(pos.slug, self.http, self.s)
                    if _gw is not None:
                        won = _gw if pos.direction == "UP" else (not _gw)
                        resolve_method = "gamma"
                if won is None:
                    won = _tok_won
                    if won is not None:
                        resolve_method = "token_price"

            # UNRESOLVED (neutral): cross-validation disagreement (immediate) OR
            # no resolution data after 5 min. DON'T assume loss — assuming LOSS
            # biased WR down (spoiled stats repeatedly). Neutral PnL (cost
            # returned), excluded from WR.
            is_disagree = (resolve_method == "cross_disagree")
            if won is None and (is_disagree or now >= pos.end_ts + 300):
                if is_disagree:
                    self.log.warning(
                        f"[DEMO] CROSS-DISAGREE {pos.slug}: Chainlink="
                        f"{'WIN' if _cl_won else 'LOSS'} vs token="
                        f"{'WIN' if _tok_won else 'LOSS'} → UNRESOLVED (neutral)",
                        slug=pos.slug, asset=pos.asset,
                        chainlink_won=_cl_won, token_won=_tok_won, agree=_agree)
                else:
                    resolve_method = "unresolved"
                    self.log.warning(
                        f"[DEMO] UNRESOLVED {pos.slug}: no resolution data -- "
                        f"marking unresolved (NOT counted as loss)"
                    )
                pos.closed = True
                pos.won = None
                pos.pnl = 0.0
                pos.close_reason = "unresolved"
                self.status.virtual_capital = round(
                    self.status.virtual_capital + pos.cost, 4)
                self._log_result(pos, None, 0.0, pos.cost, 0.0, resolve_method,
                                 cl_won=_cl_won, tok_won=_tok_won, agree=_agree)
                self.closed_history.append({
                    "interval_ts": pos.interval_ts, "asset": pos.asset,
                    "direction": pos.direction, "entry_price": pos.entry_price,
                    "shares": pos.shares, "cost": pos.cost,
                    "won": None, "pnl": 0.0,
                    "entry_ts": pos.entry_ts, "end_ts": pos.end_ts,
                    "fill_mode": "ask_plus_slippage",
                    "resolve_method": resolve_method,
                    "chainlink_won": _cl_won, "token_won": _tok_won, "agree": _agree,
                })
                self._save_history()
                self.positions.pop(slug, None)
                continue

            if won is None:
                continue  # keep retrying

            # compute P&L (HOLD to resolution)
            if won:
                proceeds = pos.shares * 1.0
                fee = proceeds * DEMO_FEE
                pnl = proceeds - pos.cost - fee
            else:
                proceeds = 0.0
                pnl = -pos.cost

            pos.closed = True
            pos.won = won
            pos.pnl = round(pnl, 4)
            pos.close_reason = "expired_win" if won else "expired_loss"

            # add proceeds back to virtual capital
            self.status.virtual_capital = round(self.status.virtual_capital + proceeds - (fee if won else 0), 4)
            self.risk.record_realized_pnl(pnl)
            self.stats.record(pnl, EntryType.VACUUM_SCALP,
                              CloseReason.EXPIRED if not won else CloseReason.TAKE_PROFIT)

            # ── structured trade result → logs/demo_results.jsonl + console + demo.log ──
            self._log_result(pos, won, pnl, proceeds, fee if won else 0.0, resolve_method,
                             cl_won=_cl_won, tok_won=_tok_won, agree=_agree)

            # persist to history for later comparison with backtest
            self.closed_history.append({
                "interval_ts": pos.interval_ts, "asset": pos.asset,
                "direction": pos.direction, "entry_price": pos.entry_price,
                "shares": pos.shares, "cost": pos.cost,
                "won": pos.won, "pnl": pos.pnl,
                "entry_ts": pos.entry_ts, "end_ts": pos.end_ts,
                "fill_mode": "ask_plus_slippage",
            })
            self._save_history()

            # schedule a late ground-truth recheck (measures real Chainlink
            # error rate incl. trades the cascade closed before token polarised)
            if self.s.cross_late_snapshot and won is not None:
                asyncio.create_task(self._late_cross_check({
                    "token_id": pos.token_id, "slug": pos.slug, "asset": pos.asset,
                    "direction": pos.direction, "end_ts": pos.end_ts,
                    "resolved_won": won, "resolved_method": resolve_method,
                    "chainlink_won_at_resolve": _cl_won,
                    "token_won_at_resolve": _tok_won,
                }))

            # ALWAYS cleanup position from active memory immediately
            self.positions.pop(slug, None)

    def _resolve_from_twap(self, pos: DemoPosition) -> Optional[bool]:
        """Authoritative resolution via the OFFICIAL Chainlink TWAP stream.

        close = official TWAP at end_ts; open = official TWAP at start_ts
        (= interval_ts; identical to the previous interval's close — hence
        open(N) == close(N-1)). Falls back to the captured start_price if the
        open TWAP sample is missing. Returns None when official TWAP data is
        unavailable (caller falls back to the local tick-average reconstruction).
        """
        if not self.s.chainlink_twap_enabled:
            return None
        close_val = self.prices.get_twap_at(pos.asset, float(pos.end_ts))
        if close_val is None:
            return None
        open_val = self.prices.get_twap_at(pos.asset, float(pos.interval_ts))
        if open_val is None:
            open_val = self.market_data.start_prices.get(str(pos.interval_ts), {}).get(pos.asset)
        if open_val is None:
            return None
        up_won = close_val >= open_val
        result = up_won if pos.direction == "UP" else (not up_won)
        self.log.info(
            f"[DEMO] official TWAP: close=${close_val:.2f} vs open=${open_val:.2f} → {'WIN' if result else 'LOSS'}")
        return result

    def _resolve_from_chainlink_history(self, pos: DemoPosition) -> Optional[bool]:
        """LOCAL tick-average reconstruction of the close (FALLBACK only).

        Used when the official Chainlink TWAP stream (_resolve_from_twap) is
        unavailable. Per Chainlink docs this local average is NOT guaranteed to
        match the signed settlement value (sampling/weighting undisclosed), so
        it is a last-resort approximation, not the authoritative source.
        """
        history = self.prices.chainlink_history.get(pos.asset, [])
        if not history:
            history = self.prices.binance_direct_history.get(pos.asset, [])
        if not history:
            return None
        # TWAP: average prices in [end_ts-30, end_ts+5]
        twap = []
        for item in history:
            ts_sec = item[1] if len(item) == 3 else item[0]
            price = item[2] if len(item) == 3 else item[1]
            if pos.end_ts - 30 <= ts_sec <= pos.end_ts + 5:
                twap.append(price)
        if twap:
            best_price = sum(twap) / len(twap)
        else:
            best_price = None; best_diff = float('inf')
            for item in history:
                ts_sec = item[1] if len(item) == 3 else item[0]
                price = item[2] if len(item) == 3 else item[1]
                diff = abs(ts_sec - pos.end_ts)
                if diff < best_diff:
                    best_diff = diff; best_price = price
            if best_price is None or best_diff > 120:
                return None
        sp = self.market_data.start_prices.get(str(pos.interval_ts), {}).get(pos.asset)
        if sp is None or sp <= 0:
            return None
        up_won = best_price >= sp
        result = up_won if pos.direction == "UP" else (not up_won)
        self.log.info(f"[DEMO] local TWAP reconst({len(twap)}pts): ${best_price:.2f} vs ${sp:.2f} → {'WIN' if result else 'LOSS'}")
        return result

    async def _wait_for_feeds(self) -> None:
        self.log.info("[DEMO] Waiting for WebSocket data...")
        for _ in range(50):
            if self.prices.chainlink or self.prices.binance or self.prices.binance_direct:
                break
            await asyncio.sleep(0.1)
        src = []
        if self.prices.chainlink:
            src.append(f"Chainlink({list(self.prices.chainlink.keys())})")
        if self.prices.binance_direct:
            src.append(f"Binance-direct({list(self.prices.binance_direct.keys())})")
        if self.prices.lot_prices:
            src.append(f"books({len(self.prices.books)})")
        self.log.info(f"[DEMO] Feeds active: {', '.join(src) or 'none'}")

    def _sample_interval(self, market: dict, asset: str, stc: float) -> None:
        """Append a second-by-second market-state sample for the WHOLE interval.

        Unlike _log_decision (entry-window only), this runs across the entire
        5-min interval so the full oracle/deviation/range5 path is captured —
        needed to reconstruct filters that look back beyond the entry window
        (range5 = 5 min back, confirmation = 60s back). Throttled per market.
        """
        key = f"{asset}_{market.get('slug', '')}"
        last = self._last_sample_ts.get(key, 0)
        if time.time() - last < self.s.demo_sample_interval_secs:
            return
        self._last_sample_ts[key] = time.time()

        op = self.prices.get_oracle_price(asset)
        start_p = market.get("target_price")
        _now = time.time()
        _ages = []
        for _src in (self.prices.chainlink_ts.get(asset),
                     self.prices.binance_direct_ts.get(asset),
                     self.prices.binance_ts.get(asset)):
            if _src:
                _ages.append(round(_now - _src, 1))
        oracle_age = min(_ages) if _ages else None
        range5 = self.prices.get_range_ratio(asset, 300.0)
        vwap = self.prices.get_vwap(asset)
        # official Chainlink TWAP (boundary resolution feed) + interval TWAP-open
        twap = self.prices.get_chainlink_twap(asset)
        _twap_ts = self.prices.chainlink_twap_ts.get(asset)
        twap_age = round(_now - _twap_ts, 1) if _twap_ts else None
        twap_open = (self.prices.get_twap_at(asset, float(self._cur_interval))
                     if self.s.chainlink_twap_enabled else None)
        leader = (market["up_token_id"] if (op and start_p and op >= start_p)
                  else market.get("down_token_id")) if (op and start_p) else None
        tok, bid_vol, ask_vol, alive = None, 0, 0, False
        if leader:
            book = self.prices.get_book(leader)
            if book:
                tok, bid_vol, ask_vol = book.best_ask, book.bid_volume, book.ask_volume
                alive = (bid_vol > 0 or ask_vol > 0)
        # полные стаканы ОБЕИХ токенов (ask/bid/vol) — для валидации fills и ZPair
        up_bid = up_ask = dn_bid = dn_ask = None
        up_av = dn_av = 0.0
        up_asize = dn_asize = None
        utid = market.get("up_token_id"); dtid = market.get("down_token_id")
        bu = self.prices.get_book(utid) if utid else None
        if bu:
            up_ask = bu.best_ask
            up_bid = getattr(bu, "best_bid", None)
            up_av = getattr(bu, "ask_volume", 0.0) or 0.0
            up_asize = getattr(bu, "best_ask_size", None)
        bd = self.prices.get_book(dtid) if dtid else None
        if bd:
            dn_ask = bd.best_ask
            dn_bid = getattr(bd, "best_bid", None)
            dn_av = getattr(bd, "ask_volume", 0.0) or 0.0
            dn_asize = getattr(bd, "best_ask_size", None)
        pair_ask_sum = round(up_ask + dn_ask, 4) if (up_ask is not None and dn_ask is not None) else None

        # book-quality flag (full / one-sided / stale) + size AT the live best_ask
        def _bq(book):
            if book is None:
                return "none"
            if book.ts and _now - book.ts > self.s.ws_book_stale_secs:
                return "stale"
            ha, hb = book.ask_volume > 0, book.bid_volume > 0
            return "full" if (ha and hb) else ("asks_only" if ha else ("bids_only" if hb else "empty"))
        up_bq, dn_bq = _bq(bu), _bq(bd)
        up_ask_at = self.prices.ask_size_at(utid, up_ask) if (utid and up_ask is not None) else None
        dn_ask_at = self.prices.ask_size_at(dtid, dn_ask) if (dtid and dn_ask is not None) else None

        rec = {
            "ts": int(_now),
            "asset": asset,
            "slug": market.get("slug", ""),
            "secs_to_close": int(stc),
            "oracle_price": op,
            "oracle_age": oracle_age,
            "deviation_pct": round((op - start_p) / start_p * 100, 3) if (op and start_p and start_p > 0) else None,
            "range5": round(range5, 5) if range5 is not None else None,
            "vwap": round(vwap, 2) if vwap is not None else None,
            "twap": round(twap, 2) if twap is not None else None,
            "twap_age": twap_age,
            "twap_open": round(twap_open, 2) if twap_open is not None else None,
            "deviation_twap_pct": round((twap - twap_open) / twap_open * 100, 3) if (twap and twap_open and twap_open > 0) else None,
            "leader_token_price": tok,
            "bid_volume": bid_vol,
            "ask_volume": ask_vol,
            "book_alive": alive,
            "imbalance": round(bid_vol / (bid_vol + ask_vol), 3) if (bid_vol + ask_vol) > 0 else 0.5,
            "pair_ask_sum": pair_ask_sum,
            "up_ask": up_ask, "up_bid": up_bid, "up_ask_vol": up_av,
            "up_ask_size": up_asize, "up_ask_size_at": up_ask_at, "up_bq": up_bq,
            "dn_ask": dn_ask, "dn_bid": dn_bid, "dn_ask_vol": dn_av,
            "dn_ask_size": dn_asize, "dn_ask_size_at": dn_ask_at, "dn_bq": dn_bq,
        }
        append_sample(self.samples_log_path, rec)

    def _log_decision(self, market: dict, asset: str, opp: Optional[Opportunity], stc: float) -> None:
        """Append a decision record (entry or skip) to the JSONL log."""
        op = self.prices.get_oracle_price(asset)
        start_p = market.get("target_price")
        vol = self.prices.get_volatility(asset, self.s_demo.vacuum_scalp_volatility_window)
        leader_token_id = market["up_token_id"] if (op and start_p and op >= start_p) else market.get("down_token_id")
        tok_price, bid_vol, ask_vol = None, 0, 0
        if leader_token_id:
            book = self.prices.get_book(leader_token_id)
            if book:
                tok_price, bid_vol, ask_vol = book.best_ask, book.bid_volume, book.ask_volume

        # oracle freshness (min age across feeds) + book liveness — expose
        # stale oracle / dead order book DIRECTLY in the log, so later analysis
        # can filter low-quality decision points (no interpolation needed).
        _now = time.time()
        _ages = []
        for _src_ts in (self.prices.chainlink_ts.get(asset),
                        self.prices.binance_direct_ts.get(asset),
                        self.prices.binance_ts.get(asset)):
            if _src_ts:
                _ages.append(round(_now - _src_ts, 1))
        oracle_age = min(_ages) if _ages else None

        # official Chainlink TWAP + interval TWAP-open (boundary resolution feed)
        twap = self.prices.get_chainlink_twap(asset)
        twap_open = (self.prices.get_twap_at(asset, float(self._cur_interval))
                     if self.s.chainlink_twap_enabled else None)

        record = {
            "ts": int(_now),
            "iso_ts": datetime.fromtimestamp(_now, tz=timezone.utc).isoformat(),
            "asset": asset,
            "action": "ENTRY" if (opp and opp.can_enter) else "SKIP",
            "reason": opp.reason if opp else "check_returned_none",
            "secs_to_close": int(stc),
            "oracle_price": op,
            "oracle_age": oracle_age,
            "deviation_pct": round((op - start_p) / start_p * 100, 3) if (op and start_p and start_p > 0) else None,
            "twap": round(twap, 2) if twap is not None else None,
            "twap_open": round(twap_open, 2) if twap_open is not None else None,
            "deviation_twap_pct": round((twap - twap_open) / twap_open * 100, 3) if (twap and twap_open and twap_open > 0) else None,
            "volatility": vol,
            "leader_token_price": tok_price,
            "bid_volume": bid_vol,
            "ask_volume": ask_vol,
            "book_alive": (bid_vol > 0 or ask_vol > 0),
            "imbalance": round(bid_vol / (bid_vol + ask_vol), 3) if (bid_vol + ask_vol) > 0 else 0.5,
            "capital": self.status.virtual_capital,
            "slug": market.get("slug", ""),
            "confirm": opp.extra.get("confirm") if (opp and opp.extra) else None,
        }
        try:
            with open(self.decisions_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception:
            pass

        # ── persistence + comparison ──

    def _log_result(self, pos: "DemoPosition", won: bool, pnl: float,
                    proceeds: float, fee: float, method: Optional[str],
                    cl_won: Optional[bool] = None,
                    tok_won: Optional[bool] = None,
                    agree: Optional[bool] = None) -> None:
        """Persist a structured trade RESULT to JSONL AND log to console+file.

        One record per resolved position, written to logs/demo_results.jsonl
        (mirrors the decisions log). This records outcomes EXPLICITLY — the
        previous flow only printed them to console, so win/loss had to be
        reconstructed from capital deltas. The self.log.info call below is
        duplicated to BOTH the console and logs/demo.log (the demo logger has
        a RotatingFileHandler attached).
        """
        now = time.time()
        cap = self.status.virtual_capital
        # official TWAP boundary values used for resolution (None if unavailable)
        twap_open = self.prices.get_twap_at(pos.asset, float(pos.interval_ts)) if self.s.chainlink_twap_enabled else None
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
            "shares": pos.shares,
            "cost": pos.cost,
            "proceeds": round(proceeds, 4),
            "fee": round(fee, 4),
            "entry_ts": int(pos.entry_ts),
            "end_ts": int(pos.end_ts),
            "secs_held": round(now - pos.entry_ts, 1),
            "resolve_method": method,
            "strategy": self.strategy_name,
            "chainlink_won": cl_won,
            "token_won": tok_won,
            "cross_agree": agree,
            "twap_open": round(twap_open, 2) if twap_open is not None else None,
            "twap_close": round(twap_close, 2) if twap_close is not None else None,
            "capital_after": round(cap, 4),
            "return_pct": round((cap / self.start_capital - 1) * 100, 2),
        }
        try:
            with open(self.results_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except OSError:
            pass

        tag = "WIN" if won else ("UNRESOLVED" if won is None else "LOSS")
        self.log.info(
            f"[DEMO] RESULT {pos.asset} {pos.direction} {tag} | "
            f"pnl: ${pnl:+.2f} | entry: ${pos.entry_price:.3f} x{pos.shares} "
            f"(cost ${pos.cost:.2f}) | held {rec['secs_held']:.0f}s | "
            f"method={method} | capital: ${cap:.2f} "
            f"(return {rec['return_pct']:+.1f}%)",
            slug=pos.slug, won=won, pnl=round(pnl, 4), method=method,
        )

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
            self.log.warning("[DEMO] late cross-check failed", error=str(e))
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
            with open(Path(self.s.log_dir) / "cross_snapshots.jsonl",
                      "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except OSError:
            pass
        if agrees is False:
            self.log.warning(
                f"[DEMO] LATE MISMATCH {snap['slug']}: resolved="
                f"{'WIN' if snap['resolved_won'] else 'LOSS'} but late="
                f"{'WIN' if late_won else 'LOSS'} ({late_src}) — possible mis-resolution",
                slug=snap["slug"], late_source=late_src)

    def _load_history(self) -> List[dict]:
        if not self.history_path.exists():
            return []
        try:
            with open(self.history_path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return []

    def _save_history(self) -> None:
        try:
            with open(self.history_path, "w") as f:
                json.dump(self.closed_history[-500:], f)  # cap at 500
        except OSError:
            pass

    def clear_history(self) -> None:
        self.closed_history = []
        self._save_history()

    async def compare_with_backtest(self, lookback_intervals: int = 50) -> dict:
        """Compare live demo performance vs a backtest run on the SAME real
        intervals the demo just traded.

        This measures the gap between theory (backtest) and reality (live):
          * Did demo enter as often as backtest predicted?
          * Did entries happen at the same price?
          * Did win rate match?
          * What's the execution cost (slippage)?

        Requires the demo to have at least a few closed trades.
        """
        if not self.closed_history:
            return {"error": "no closed demo trades yet; let the demo run longer"}

        # determine the time range from closed trades
        ts_list = sorted(t["interval_ts"] for t in self.closed_history)
        start = ts_list[0]
        end = ts_list[-1] + self.s.interval_minutes * 60

        # ── fetch real data for the same intervals ──
        from ..backtest.poly_fetcher import PolymarketDataFetcher
        from ..backtest.compounding import (
            extract_entry_opportunities, simulate_compounding,
        )
        fetcher = PolymarketDataFetcher(self.s, self.log)
        dataset = await fetcher.build_dataset("BTC", start, end, 5, fidelity=1)
        st = dataset.stats()
        if st["intervals_with_token_data"] == 0:
            return {"error": "no real data for the demo's interval range",
                    "range": [start, end]}

        # ── backtest: extract entries at the SAME threshold demo used ──
        bt_entries = extract_entry_opportunities(dataset, self.threshold)
        bt_result = simulate_compounding(bt_entries, self.start_capital, self.stake_ratio)

        # ── demo aggregate ──
        demo_trades = self.closed_history[-lookback_intervals:]
        demo_wins = sum(1 for t in demo_trades if t["won"])
        demo_pnl = sum(t["pnl"] for t in demo_trades)
        demo_capital = self.start_capital + demo_pnl
        demo_return = (demo_capital / self.start_capital - 1) * 100

        # ── execution gap analysis ──
        # match demo trades to backtest entries by interval_ts
        bt_by_ts = {e["ts"]: e for e in bt_entries}
        gaps = []
        for dt in demo_trades:
            bt = bt_by_ts.get(dt["interval_ts"])
            if bt:
                # backtest assumed entry at last-trade price; demo filled at ask+slippage
                price_gap = dt["entry_price"] - bt["entry_price"]
                gaps.append({
                    "interval_ts": dt["interval_ts"],
                    "demo_price": dt["entry_price"],
                    "backtest_price": bt["entry_price"],
                    "price_gap": round(price_gap, 4),
                    "demo_won": dt["won"],
                    "bt_won": bt["won"],
                })

        avg_price_gap = (sum(g["price_gap"] for g in gaps) / len(gaps)) if gaps else 0

        # ── verdict ──
        n_match = len(gaps)
        n_demo = len(demo_trades)
        n_bt = len(bt_entries)
        win_rate_gap = (demo_wins / n_demo - bt_result.win_rate) if n_demo and bt_result.n_trades else 0
        return_gap = demo_return - bt_result.return_pct

        if abs(return_gap) < 5:
            verdict = "✓ согласуется — live подтверждает бэктест"
        elif return_gap < -10:
            verdict = "⚠ live ХУЖЕ бэктеста — реальное исполнение съедает edge (slippage/latency)"
        elif return_gap > 10:
            verdict = "⚠ live ЛУЧШЕ бэктеста — возможно, повезло на малой выборке"
        else:
            verdict = "≈ частично согласуется"

        return {
            "range": {"start": start, "end": end,
                      "intervals": st["intervals_total"]},
            "demo": {
                "trades": n_demo,
                "wins": demo_wins,
                "win_rate": round(demo_wins / n_demo, 3) if n_demo else 0,
                "total_pnl": round(demo_pnl, 2),
                "final_capital": round(demo_capital, 2),
                "return_pct": round(demo_return, 1),
                "threshold": self.threshold,
            },
            "backtest": {
                "trades": bt_result.n_trades,
                "wins": bt_result.n_wins,
                "win_rate": round(bt_result.win_rate, 3),
                "total_pnl": round(bt_result.final_capital - self.start_capital, 2),
                "final_capital": round(bt_result.final_capital, 2),
                "return_pct": round(bt_result.return_pct, 1),
            },
            "comparison": {
                "trade_count_gap": n_demo - n_bt,
                "win_rate_gap": round(win_rate_gap, 3),
                "return_gap_pct": round(return_gap, 1),
                "matched_trades": n_match,
                "avg_entry_price_gap": round(avg_price_gap, 4),
                "avg_slippage_cost_per_trade": round(avg_price_gap * 5, 4),  # ~5 shares
                "verdict": verdict,
            },
            "trade_gaps": gaps[-10:],   # last 10 for inspection
        }

        # ── recent trades for dashboard ──

    def recent_trades(self, limit: int = 20) -> List[dict]:
        out = []
        for pos in sorted(self.positions.values(),
                          key=lambda p: p.entry_ts, reverse=True):
            if not pos.closed:
                continue
            out.append({
                "asset": pos.asset, "direction": pos.direction,
                "entry_price": pos.entry_price, "shares": pos.shares,
                "pnl": pos.pnl, "won": pos.won,
                "close_reason": pos.close_reason,
                "entry_ts": datetime.fromtimestamp(pos.entry_ts, tz=timezone.utc).isoformat(),
            })
            if len(out) >= limit:
                break
        return out
