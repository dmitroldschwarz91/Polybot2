"""
Walk-forward optimization routes.

  POST /api/optimize/run    — run walk-forward optimization (returns summary)
  GET  /api/optimize/spaces — list the search space for each strategy
"""

from __future__ import annotations

import asyncio
import time

from fastapi import APIRouter
from pydantic import BaseModel

from ..backtest.optimizer import WalkForwardOptimizer, default_search_space
from ..config import Settings


class OptimizeRequest(BaseModel):
    strategy: str = "vacuum_scalp"
    asset: str = "BTC"
    interval_minutes: int = 5
    capital: float = 7.0
    start_ts: int
    end_ts: int
    train_intervals: int = 24    # 24 × 5m = 2h train window
    test_intervals: int = 12     # 12 × 5m = 1h test window
    max_combos: int = 60


def create_optimizer_router(settings: Settings) -> APIRouter:
    router = APIRouter()
    _current: dict = {"running": False, "progress": None}

    @router.get("/api/optimize/spaces")
    async def spaces():
        out = {}
        for strat in ("vacuum_scalp", "early_trend", "standard"):
            out[strat] = {sp.name: sp.values for sp in default_search_space(strat)}
        return out

    @router.get("/api/optimize/status")
    async def status():
        return _current

    @router.post("/api/optimize/run")
    async def run_optimize(req: OptimizeRequest):
        if _current["running"]:
            return {"error": "an optimization is already running; wait or check /status"}
        _current["running"] = True
        _current["progress"] = {"done": 0, "total": None}

        def cb(done, total):
            _current["progress"] = {"done": done, "total": total}

        opt = WalkForwardOptimizer(
            settings, strategy=req.strategy, capital=req.capital, asset=req.asset,
            interval_minutes=req.interval_minutes, max_combos=req.max_combos,
        )

        # run in a thread so the request doesn't block; metrics are CPU-bound
        loop = asyncio.get_event_loop()
        t0 = time.time()
        try:
            result = await loop.run_in_executor(
                None,
                lambda: asyncio.run(opt.run(
                    req.start_ts, req.end_ts,
                    train_intervals=req.train_intervals,
                    test_intervals=req.test_intervals,
                    progress_cb=cb,
                )),
            )
        except Exception as e:
            _current["running"] = False
            return {"error": str(e)}
        finally:
            _current["running"] = False

        summary = result.summary()
        summary["elapsed_ms"] = round((time.time() - t0) * 1000, 0)
        return summary

    return router
