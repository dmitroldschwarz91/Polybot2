"""
Demo trading API routes.

  GET  /api/demo/status        — live demo status (virtual capital, positions)
  POST /api/demo/start         — start demo on live data
  POST /api/demo/stop          — stop demo
  GET  /api/demo/trades        — recent virtual trades
  POST /api/demo/config        — adjust threshold / capital / stake ratio
"""

from __future__ import annotations

from typing import Optional
import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from ..config import Settings
from ..demo.engine import DemoEngine, DEMO_START_CAPITAL, DEMO_THRESHOLD, DEMO_STAKE_RATIO


class DemoConfigRequest(BaseModel):
    start_capital: Optional[float] = None
    threshold: Optional[float] = None
    stake_ratio: Optional[float] = None
    strategy: Optional[str] = "twap_inertia"  # "twap_inertia" | "vacuum_scalp" | "simple" | "zscore_reversal" | "zpair"


def create_demo_router(settings: Settings) -> APIRouter:
    router = APIRouter()
    demo_engine: Optional[DemoEngine] = None

    def get_demo() -> Optional[DemoEngine]:
        nonlocal demo_engine
        return demo_engine

    create_demo_router.get_demo = get_demo  # type: ignore[attr-defined]

    @router.get("/api/demo/status")
    async def demo_status():
        eng = get_demo()
        if not eng or not eng.running:
            return {"running": False, "demo": True,
                    "virtual_capital": DEMO_START_CAPITAL,
                    "start_capital": DEMO_START_CAPITAL}
        return eng.status.to_dict(eng.positions, eng.stats, eng.risk, self.prices,
                                  getattr(eng, "_pending_leg1", {}))

    @router.post("/api/demo/start")
    async def demo_start(req: DemoConfigRequest = DemoConfigRequest()):
        nonlocal demo_engine
        eng = get_demo()
        if eng and eng.running:
            return {"ok": True, "msg": "already running"}
        capital = req.start_capital or DEMO_START_CAPITAL
        thr = req.threshold or DEMO_THRESHOLD
        sr = req.stake_ratio or DEMO_STAKE_RATIO
        strat = req.strategy or "twap_inertia"
        if eng:
            await eng.stop()
        demo_engine = DemoEngine(settings, start_capital=capital,
                                 threshold=thr, stake_ratio=sr, strategy=strat)
        await demo_engine.start()
        return {"ok": True, "config": {"start_capital": capital,
                                        "threshold": thr, "stake_ratio": sr,
                                        "strategy": strat}}

    @router.post("/api/demo/stop")
    async def demo_stop():
        eng = get_demo()
        if eng and eng.running:
            await eng.stop()
            return {"ok": True}
        return {"ok": True, "msg": "not running"}

    @router.get("/api/demo/trades")
    async def demo_trades(limit: int = 20):
        eng = get_demo()
        if not eng:
            return []
        return eng.recent_trades(limit)

    @router.post("/api/demo/compare")
    async def demo_compare(lookback: int = 50):
        """Compare live demo performance vs a backtest on the SAME intervals."""
        eng = get_demo()
        if not eng:
            return {"error": "demo engine not initialised"}
        return await eng.compare_with_backtest(lookback_intervals=lookback)

    @router.post("/api/demo/clear-history")
    async def demo_clear_history():
        eng = get_demo()
        if eng:
            eng.clear_history()
            return {"ok": True}
        return {"error": "no engine"}

    @router.get("/api/demo/history")
    async def demo_history(limit: int = 50):
        eng = get_demo()
        if not eng:
            return []
        return eng.closed_history[-limit:]

    @router.get("/api/demo/config")
    async def demo_config():
        eng = get_demo()
        if eng:
            return {"start_capital": eng.start_capital,
                    "threshold": eng.threshold,
                    "stake_ratio": eng.stake_ratio,
                    "strategy": eng.strategy_name,
                    "running": eng.running}
        return {"start_capital": DEMO_START_CAPITAL,
                "threshold": DEMO_THRESHOLD,
                "stake_ratio": DEMO_STAKE_RATIO,
                "strategy": "twap_inertia",
                "running": False}

    @router.websocket("/ws/demo")
    async def ws_demo(ws: WebSocket):
        await ws.accept()
        try:
            while True:
                eng = get_demo()
                if eng and eng.running:
                    payload = eng.status.to_dict(eng.positions, eng.stats, eng.risk, eng.prices,
                                                getattr(eng, "_pending_leg1", {}))
                else:
                    payload = {"running": False, "demo": True,
                               "virtual_capital": DEMO_START_CAPITAL,
                               "start_capital": DEMO_START_CAPITAL}
                await ws.send_text(json.dumps(payload, default=str))
                await asyncio.sleep(1.5)
        except (WebSocketDisconnect, Exception):
            pass

    return router
