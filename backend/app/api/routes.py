"""
FastAPI routes — dashboard backend.

Exposes: live status, trade history, config (safe view), bot start/stop,
risk hot-pause, and a WebSocket that pushes status every second.
"""

from __future__ import annotations

import asyncio
import json
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..config import Settings


def create_router(engine, settings: Settings) -> APIRouter:
    router = APIRouter()

    @router.get("/api/status")
    async def status():
        return engine.status.to_dict(engine.positions, engine.stats, engine.risk,
                                     engine.prices, engine.balance.state, settings.paper_trading)

    @router.post("/api/bot/start")
    async def start_bot(strategy: Optional[str] = None):
        if engine.status.running:
            return {"ok": True, "msg": "already running"}
        if strategy:
            settings.trading_strategy = strategy
        await engine.start()
        return {"ok": True, "strategy": settings.trading_strategy}

    @router.post("/api/bot/stop")
    async def stop_bot():
        await engine.stop()
        return {"ok": True}

    @router.get("/api/trades")
    async def trades(limit: int = 100):
        return await engine.db.recent_trades(limit)

    @router.get("/api/pnl")
    async def pnl(limit: int = 200):
        return await engine.db.pnl_series(limit)

    @router.get("/api/config")
    async def config():
        return settings.safe_view

    @router.post("/api/risk/halt")
    async def halt(reason: str = "manual"):
        engine.risk.state.halted = True
        engine.risk.state.halt_reason = reason
        return engine.risk.snapshot()

    @router.post("/api/risk/resume")
    async def resume():
        engine.risk.state.halted = False
        engine.risk.state.halt_reason = None
        return engine.risk.snapshot()

    @router.get("/api/positions")
    async def positions():
        return [p.to_dict() for p in engine.positions.values()]

    @router.websocket("/ws/status")
    async def ws_status(ws: WebSocket):
        await ws.accept()
        try:
            while True:
                payload = engine.status.to_dict(
                    engine.positions, engine.stats, engine.risk,
                    engine.prices, engine.balance.state, settings.paper_trading,
                )
                await ws.send_text(json.dumps(payload, default=str))
                await asyncio.sleep(1.0)
        except WebSocketDisconnect:
            pass
        except Exception:
            pass

    return router
