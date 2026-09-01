"""
Backtest API routes — run historical / synthetic / REAL-Polymarket backtests.

  POST /api/backtest/run         — run a backtest (model / synthetic / historical / poly)
  POST /api/backtest/fetch-data  — download + cache Binance klines (historical)
  POST /api/backtest/fetch-poly  — download + cache REAL Polymarket interval data
  GET  /api/backtest/strategies  — list available strategies
  GET  /api/backtest/cache-stats — show how many intervals are cached locally
"""

from __future__ import annotations

import asyncio
import time
from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel

from ..backtest import (
    BacktestConfig, BacktestEngine, HistoricalData,
    PolymarketDataFetcher,
)
from ..backtest.engine import STRATEGY_REGISTRY
from ..config import Settings


class BacktestRequest(BaseModel):
    strategy: str = "vacuum_scalp"
    capital: float = 7.0
    asset: str = "BTC"
    interval_minutes: int = 5
    sl_pct_override: Optional[float] = None
    tp_delta_override: Optional[float] = None
    book_spread: float = 0.005
    book_liquidity: float = 200.0
    book_noise: float = 0.01
    use_fees: bool = True
    hold_to_resolution: bool = False  # if True, ignore TP/SL → pure directional hold
    # data source: "synthetic", "historical", or "poly"
    mode: str = "synthetic"
    duration_secs: int = 3600
    start_price: float = 100000.0
    volatility: float = 0.0002
    drift: float = 0.0
    trend_changes: int = 0
    seed: int = 42
    historical_start: Optional[int] = None
    historical_end: Optional[int] = None
    poly_fidelity: int = 1
    token_source: str = "trades"      # "trades" (per-trade) or "prices" (prices-history)


class FetchDataRequest(BaseModel):
    asset: str = "BTC"
    start_ts: int
    end_ts: int


class FetchPolyRequest(BaseModel):
    asset: str = "BTC"
    start_ts: int
    end_ts: int
    interval_minutes: int = 5
    fidelity: int = 1


def create_backtest_router(engine, settings: Settings) -> APIRouter:
    router = APIRouter()
    hist = HistoricalData(settings)
    poly_fetcher = PolymarketDataFetcher(settings)

    @router.get("/api/backtest/strategies")
    async def strategies():
        return [{"name": k, "enabled_default": getattr(settings, f"{k}_enabled", False)}
                for k in STRATEGY_REGISTRY]

    @router.get("/api/backtest/cache-stats")
    async def cache_stats():
        from pathlib import Path
        poly_dir = Path(settings.backtest_data_dir) / "poly"
        poly_files = list(poly_dir.glob("*.json")) if poly_dir.exists() else []
        bin_dir = Path(settings.backtest_data_dir)
        bin_files = list(bin_dir.glob("*.csv"))
        return {
            "polymarket_intervals_cached": len(poly_files),
            "binance_files_cached": len(bin_files),
            "cache_dir": settings.backtest_data_dir,
        }

    @router.post("/api/backtest/run")
    async def run_backtest(req: BacktestRequest):
        ticks = None
        dataset = None
        data_mode = "model"

        if req.mode == "poly":
            if not req.historical_start or not req.historical_end:
                return {"error": "poly mode requires historical_start/end (unix sec)"}
            poly_fetcher.token_source = req.token_source
            dataset = await poly_fetcher.build_dataset(
                req.asset, req.historical_start, req.historical_end,
                req.interval_minutes, fidelity=req.poly_fidelity,
            )
            if not dataset.oracle_ticks:
                return {"error": "no data downloaded; check range / network / geoblock"}
            ticks = dataset.oracle_ticks
            data_mode = "poly"

        elif req.mode == "historical":
            if not req.historical_start or not req.historical_end:
                return {"error": "historical mode requires historical_start/end (unix sec)"}
            ticks = await hist.fetch_klines(req.asset, req.historical_start, req.historical_end)
            if not ticks:
                return {"error": "no data fetched; check range / network"}
            data_mode = "model"

        else:  # synthetic
            ticks = HistoricalData.synthetic_random_walk(
                req.start_price, req.duration_secs, step_secs=1,
                volatility=req.volatility, seed=req.seed,
                drift=req.drift, trend_changes=req.trend_changes,
            )
            data_mode = "model"

        cfg = BacktestConfig(
            strategy=req.strategy, capital=req.capital, asset=req.asset,
            interval_minutes=req.interval_minutes,
            sl_pct_override=req.sl_pct_override, tp_delta_override=req.tp_delta_override,
            book_spread=req.book_spread, book_liquidity=req.book_liquidity,
            book_noise=req.book_noise, use_fees=req.use_fees, data_mode=data_mode,
            hold_to_resolution=req.hold_to_resolution,
        )
        bt_engine = BacktestEngine(settings, ticks, cfg, dataset=dataset)
        t0 = time.time()
        metrics = await asyncio.get_event_loop().run_in_executor(None, bt_engine.run)
        elapsed = time.time() - t0

        result = metrics.to_dict()
        result["strategy"] = req.strategy
        result["asset"] = req.asset
        result["capital"] = req.capital
        result["mode"] = data_mode
        result["real_data"] = bt_engine._has_real_data
        result["num_ticks"] = len(ticks)
        result["elapsed_ms"] = round(elapsed * 1000, 0)
        result["trade_log_tail"] = bt_engine.exchange.account.trade_log[-25:]
        result["final_cash"] = round(bt_engine.exchange.account.cash, 4)
        if dataset is not None:
            result["dataset_stats"] = dataset.stats()
        return result

    @router.post("/api/backtest/fetch-data")
    async def fetch_data(req: FetchDataRequest):
        ticks = await hist.fetch_klines(req.asset, req.start_ts, req.end_ts)
        return {"asset": req.asset, "count": len(ticks),
                "first_ts": ticks[0].ts if ticks else None,
                "last_ts": ticks[-1].ts if ticks else None}

    @router.post("/api/backtest/fetch-poly")
    async def fetch_poly(req: FetchPolyRequest):
        """Pre-download and cache REAL Polymarket intervals for later backtests."""
        dataset = await poly_fetcher.build_dataset(
            req.asset, req.start_ts, req.end_ts,
            req.interval_minutes, fidelity=req.fidelity,
        )
        return {"asset": req.asset, **dataset.stats()}

    return router
