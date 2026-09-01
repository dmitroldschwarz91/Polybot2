"""
Backtest engine — replays historical/synthetic price data through the bot.

Design principle: reuse the REAL strategy decision logic (same thresholds from
Settings + RiskManager) so a profitable backtest means the strategy's *edge*
exists — only execution is simulated.

Two data modes:
  * MODEL — token books come from MarketModel (parametric). Fast, no network.
  * POLY  — token books + winners come from REAL Polymarket history
            (see poly_fetcher.py). Decisions still run on the real 1s oracle.

Deliberately pessimistic: we buy at ask, sell at bid, pay the taker fee, and
apply slippage. If it survives that, live trading has a chance.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from ..config import Settings
from ..core.logging import StructuredLogger, build_logger
from ..domain.enums import EntryType
from ..marketdata.stores import LivePriceStore
from ..marketdata.markets import MarketData
from ..risk.manager import RiskManager
from ..strategies import (
    VacuumScalpStrategy, EarlyTrendStrategy, StandardEntryStrategy,
)
from ..strategies.base import Opportunity
from .data import PriceTick
from .metrics import BacktestMetrics, compute_metrics
from .poly_fetcher import BacktestDataset
from .simulator import MarketModel, SimulatedExchange


STRATEGY_REGISTRY = {
    "vacuum_scalp": VacuumScalpStrategy,
    "early_trend": EarlyTrendStrategy,
    "standard": StandardEntryStrategy,
}


@dataclass
class BacktestPosition:
    """A simulated open position (directional)."""
    token_id: str
    asset: str
    side: str            # UP / DOWN
    entry_price: float
    size: int
    sl: float
    tp: float
    entry_type: EntryType
    interval_start: int
    end_ts: int
    closed: bool = False
    close_reason: str = ""
    pnl: float = 0.0


@dataclass
class BacktestConfig:
    strategy: str = "vacuum_scalp"
    capital: float = 15.0
    asset: str = "BTC"
    interval_minutes: int = 5
    sl_pct_override: Optional[float] = None   # tighten SL for small budgets
    tp_delta_override: Optional[float] = None
    book_spread: float = 0.005
    book_liquidity: float = 200.0
    book_noise: float = 0.01
    step_secs: int = 1
    use_fees: bool = True
    data_mode: str = "model"   # "model" or "poly"
    # ── entry threshold override (for threshold sweep) ──
    entry_threshold_min: Optional[float] = None   # vacuum_scalp_min_token_price
    entry_threshold_max: Optional[float] = None   # vacuum_scalp_max_token_price
    # ── exit mode ──
    hold_to_resolution: bool = False               # if True, skip TP/SL → pure directional


class BacktestEngine:
    def __init__(self, settings: Settings, ticks: List[PriceTick],
                 cfg: BacktestConfig, log: Optional[StructuredLogger] = None,
                 dataset: Optional[BacktestDataset] = None) -> None:
        self.s = settings
        self.ticks = ticks
        self.cfg = cfg
        self.log = log or build_logger("backtest")

        # apply overrides to a local settings copy
        self.s_local = settings.model_copy()
        if cfg.sl_pct_override is not None:
            self.s_local.vacuum_scalp_sl_pct = cfg.sl_pct_override
            self.s_local.standard_sl_pct = cfg.sl_pct_override
            self.s_local.early_trend_sl_pct = cfg.sl_pct_override
        if cfg.tp_delta_override is not None:
            self.s_local.vacuum_scalp_tp_delta = cfg.tp_delta_override
        self.s_local.assets = [cfg.asset]
        self.s_local.interval_minutes = cfg.interval_minutes

        self.model = MarketModel(spread=cfg.book_spread, liquidity=cfg.book_liquidity,
                                 noise=cfg.book_noise)
        if not cfg.use_fees:
            self.s_local.backtest_taker_fee = 0.0
        self.exchange = SimulatedExchange(self.s_local, self.model)
        self.exchange.fund(cfg.capital)

        self.prices = LivePriceStore([cfg.asset], book_stale_secs=9999)
        self.risk = RiskManager(self.s_local)
        # MarketData needs an http; we bypass fetch by pre-seeding start prices
        self.market_data = MarketData(self.s_local, self.prices, _NullHTTP(), self.log)

        strat_cls = STRATEGY_REGISTRY.get(cfg.strategy, VacuumScalpStrategy)
        self.strategy = strat_cls(self.s_local)
        self._forced_enabled = cfg.strategy
        self._cur_ts = ticks[0].ts if ticks else 0

        # ── real-data mode: token price lookup + winners from Polymarket ──
        self._token_lookup: Dict[int, Tuple[Optional[float], Optional[float]]] = {}
        self._real_winners: Dict[int, bool] = {}
        self._has_real_data = False
        if dataset is not None and dataset.token_history:
            self._token_lookup = dataset.token_lookup()
            self._real_winners = {
                i.interval_ts: i.up_won for i in dataset.intervals
                if i.up_won is not None
            }
            self._has_real_data = bool(self._token_lookup)
            # If token data is dense (trade-level), step the oracle to 1-second
            # resolution so the dense prices actually drive decisions.
            total_tok_pts = sum(len(h.get("up", [])) + len(h.get("down", []))
                                for h in dataset.token_history.values())
            if self._has_real_data and total_tok_pts > len(dataset.intervals) * 20:
                before = len(self.ticks)
                dataset.densify_oracle_to_seconds()
                self.ticks = dataset.oracle_ticks
                # rebuild lookups at the new 1s resolution
                self._token_lookup = dataset.token_lookup()
                self.log.info(
                    f"[BACKTEST] REAL Polymarket data (trade-level): "
                    f"{total_tok_pts} token trades, oracle densified "
                    f"{before}->{len(self.ticks)} ticks (1s), "
                    f"{len(self._token_lookup)} lookups, "
                    f"{len(self._real_winners)} winners")
            elif self._has_real_data:
                self.log.info(f"[BACKTEST] REAL Polymarket data: "
                              f"{len(self._token_lookup)} lookups, "
                              f"{len(self._real_winners)} winners")

    # ── public ───────────────────────────────────────────────────────────

    def run(self) -> BacktestMetrics:
        self.log.info(f"[BACKTEST] strategy={self.cfg.strategy} "
                      f"capital=${self.cfg.capital} ticks={len(self.ticks)} "
                      f"mode={self.cfg.data_mode}")
        # Strategies call time.time() internally for window/volatility logic.
        # In a backtest we replay historical timestamps, so we redirect the
        # process clock to the current tick's ts for the duration of the run.
        import time as _time
        _orig_time = _time.time
        _time.time = lambda: self._cur_ts
        try:
            return self._run_loop()
        finally:
            _time.time = _orig_time

    def _run_loop(self) -> BacktestMetrics:
        equity_curve: List[dict] = []
        positions: List[BacktestPosition] = []
        traded: set = set()
        cur_interval = None
        start_price = None
        bp = self.model.token_prices(1.0, 1.0, 300, 300)  # init placeholder

        for i, tick in enumerate(self.ticks):
            ts = tick.ts
            self._cur_ts = ts
            # ── feed the price store (oracle) ──
            self.prices.update_binance_direct(self.cfg.asset, tick.price)
            self.prices.update_binance(self.cfg.asset, tick.price)
            self.prices.update_chainlink(self.cfg.asset, tick.price, oracle_ts_ms=ts * 1000)

            # ── interval boundary ──
            boundary = ts - (ts % (self.cfg.interval_minutes * 60))
            if cur_interval != boundary:
                if cur_interval is not None and start_price is not None:
                    self._resolve_interval(positions, tick, start_price, cur_interval)
                    traded.clear()
                cur_interval = boundary
                start_price = tick.price
                self.market_data.start_prices[str(boundary)] = {self.cfg.asset: tick.price}

            interval_secs = self.cfg.interval_minutes * 60
            end_ts = boundary + interval_secs
            secs_to_close = end_ts - ts

            # ── build book: REAL token prices if available, else model ──
            bp = self._build_book(ts, start_price or tick.price, secs_to_close, interval_secs)
            market = {
                "slug": f"{self.cfg.asset.lower()}-updown-{self.cfg.interval_minutes}m-{boundary}",
                "asset": self.cfg.asset,
                "up_token_id": "UP_TOKEN", "down_token_id": "DOWN_TOKEN",
                "end_ts": end_ts, "target_price": start_price,
            }
            self._seed_books(bp)

            # ── monitor open positions (TP/SL) ──
            self._monitor(positions, bp, ts)

            # ── try entry ──
            bot_balance = self.exchange.account.cash
            ok, reason = self.risk.can_open_new({}, bot_balance)
            if ok and market["slug"] not in traded:
                opp = self._safe_check(market, bp, bot_balance, traded, secs_to_close)
                if opp and opp.can_enter:
                    self._enter(opp, market, bp, ts, positions)
                    traded.add(market["slug"])

            # ── equity sample (every N ticks to keep curve compact) ──
            if i % max(1, len(self.ticks) // 500) == 0:
                mid = {"UP_TOKEN": bp["up_mid"], "DOWN_TOKEN": bp["down_mid"]}
                eq = self.exchange.equity(mid)
                equity_curve.append({"ts": ts, "equity": round(eq, 4)})

        # resolve final interval
        if cur_interval is not None and start_price is not None and positions:
            self._resolve_interval(positions, self.ticks[-1], start_price, cur_interval)

        if equity_curve:
            final_mid = {"UP_TOKEN": bp["up_mid"], "DOWN_TOKEN": bp["down_mid"]}
            equity_curve.append({"ts": self.ticks[-1].ts,
                                 "equity": round(self.exchange.equity(final_mid), 4)})

        return compute_metrics(equity_curve, self.exchange.account.trade_log,
                               self.cfg.capital, self.exchange.account.fees_paid)

    # ── book construction (real or modelled) ─────────────────────────────

    def _build_book(self, ts: int, start_price: float,
                    secs_to_close: float, interval_secs: int) -> dict:
        """Return book dict. Uses REAL Polymarket token prices when present."""
        if self._has_real_data:
            real = self._token_lookup.get(ts)
            if real is not None and (real[0] is not None or real[1] is not None):
                up_mid = real[0] if real[0] is not None else round(1.0 - (real[1] or 0.5), 4)
                down_mid = real[1] if real[1] is not None else round(1.0 - (real[0] or 0.5), 4)
                # prices-history 'p' is a mid/last-trade; derive ask/bid via spread
                sp = self.cfg.book_spread
                up_mid = max(0.001, min(0.999, up_mid))
                down_mid = max(0.001, min(0.999, down_mid))
                return {
                    "up_mid": up_mid, "down_mid": down_mid,
                    "up_ask": min(0.999, up_mid + sp), "up_bid": max(0.001, up_mid - sp),
                    "down_ask": min(0.999, down_mid + sp), "down_bid": max(0.001, down_mid - sp),
                    "bid_volume": self.cfg.book_liquidity,
                    "ask_volume": self.cfg.book_liquidity,
                }
        # fallback to parametric model
        return self.model.token_prices(self.prices.get_oracle_price(self.cfg.asset),
                                       start_price, secs_to_close, interval_secs)

    # ── helpers ──────────────────────────────────────────────────────────

    def _seed_books(self, bp: dict) -> None:
        for tid, ask_key, bid_key in (
            ("UP_TOKEN", "up_ask", "up_bid"),
            ("DOWN_TOKEN", "down_ask", "down_bid"),
        ):
            self.prices.update_full_book(
                tid,
                bids=[{"price": str(bp[bid_key]), "size": str(bp["bid_volume"])}],
                asks=[{"price": str(bp[ask_key]), "size": str(bp["ask_volume"])}],
            )

    def _safe_check(self, market, bp, bot_balance, traded, secs_to_close) -> Optional[Opportunity]:
        try:
            if isinstance(self.strategy, StandardEntryStrategy):
                return self.strategy.check(market, self.cfg.asset, set(traded), bot_balance,
                                           self.prices, self.market_data, self.risk,
                                           price_history={})
            return self.strategy.check(market, self.cfg.asset, set(traded), bot_balance,
                                       self.prices, self.market_data, self.risk)
        except Exception as e:
            self.log.debug(f"[BACKTEST] check error: {e}")
            return None

    def _enter(self, opp: Opportunity, market, bp, ts, positions) -> None:
        token_id = opp.token_id
        side = "UP" if token_id == "UP_TOKEN" else "DOWN"
        ask = bp["up_ask"] if side == "UP" else bp["down_ask"]
        if self.strategy.entry_type == EntryType.EARLY_TREND:
            stake = self.risk.early_trend_stake(self.exchange.account.cash)
        else:
            stake = self.risk.vacuum_scalp_stake(self.exchange.account.cash, opp.imbalance)
        size = max(1, int(stake / ask)) if ask > 0 else 0
        if size < self.s_local.min_order_size:
            return
        order = self.exchange.buy(token_id, ask, size, ts, side)
        if not order:
            return
        entry = order.price
        # ── exit mode: HOLD ignores TP/SL; TP/SL sets real levels ──
        if self.cfg.hold_to_resolution:
            sl = 0.0       # never triggers (price can't go below 0 in practice)
            tp = 999.0     # never triggers
        else:
            sl = self.strategy.target_sl_price(entry)
            tp = self.strategy.target_tp_price(entry, "0.01")
        positions.append(BacktestPosition(
            token_id=token_id, asset=self.cfg.asset, side=side,
            entry_price=entry, size=order.size, sl=sl, tp=tp,
            entry_type=self.strategy.entry_type,
            interval_start=market["end_ts"] - self.cfg.interval_minutes * 60,
            end_ts=market["end_ts"],
        ))

    def _monitor(self, positions: List[BacktestPosition], bp: dict, ts: int) -> None:
        for p in positions:
            if p.closed:
                continue
            mid = bp["up_mid"] if p.side == "UP" else bp["down_mid"]
            bid = bp["up_bid"] if p.side == "UP" else bp["down_bid"]
            if mid >= p.tp:
                self.exchange.sell(p.token_id, bid, p.size, ts, p.side)
                p.closed, p.close_reason, p.pnl = True, "tp", 0.0
            elif mid <= p.sl:
                self.exchange.sell(p.token_id, bid, p.size, ts, p.side)
                p.closed, p.close_reason = True, "sl"

    def _resolve_interval(self, positions, last_tick, start_price, interval_ts) -> None:
        # Prefer the REAL Polymarket winner; fall back to oracle close>=open.
        if interval_ts in self._real_winners:
            up_won = self._real_winners[interval_ts]
        else:
            up_won = last_tick.price > start_price
        for p in positions:
            if p.closed:
                continue
            side_won = (up_won and p.side == "UP") or ((not up_won) and p.side == "DOWN")
            self.exchange.redeem(p.token_id, side_won, last_tick.ts, p.side)
            p.closed = True
            p.close_reason = "expired_win" if side_won else "expired_loss"


class _NullHTTP:
    """No-op async HTTP so MarketData doesn't try to fetch during backtest."""
    async def get(self, *a, **k):
        return None
    async def close(self):
        pass
