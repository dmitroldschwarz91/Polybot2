"""
Data management routes — controls the real-time collector + cache inspection.

  GET  /api/collector/status          — is the recorder running?
  POST /api/collector/start           — start continuous recording
  POST /api/collector/stop            — stop it
  POST /api/collector/backfill        — fetch a past range now
  GET  /api/data/inventory            — what's cached locally (per asset)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter
from pydantic import BaseModel

from ..backtest.collector import RealTimeCollector
from ..config import Settings


class CollectorStartRequest(BaseModel):
    assets: Optional[List[str]] = None
    interval_minutes: Optional[int] = None
    grace_secs: float = 150.0


class BackfillRequest(BaseModel):
    asset: str = "BTC"
    start_ts: int
    end_ts: int


def create_data_router(settings: Settings) -> APIRouter:
    router = APIRouter()
    collector = RealTimeCollector(settings, grace_secs=settings.collector_grace_secs)

    # expose the singleton so main.py lifespan can stop it on shutdown
    create_data_router.collector = collector  # type: ignore[attr-defined]

    @router.get("/api/collector/status")
    async def collector_status():
        return collector.stats.to_dict()

    @router.post("/api/collector/start")
    async def collector_start(req: CollectorStartRequest):
        if collector.running:
            return {"ok": True, "msg": "already running", **collector.stats.to_dict()}
        # reconfigure if requested
        if req.assets:
            collector.assets = list(req.assets)
            collector.stats.assets = list(req.assets)
            collector._seen = {a: 0 for a in collector.assets}
        if req.interval_minutes:
            collector.interval_minutes = req.interval_minutes
            collector.stats.interval_minutes = req.interval_minutes
        collector.grace_secs = req.grace_secs
        collector.start()
        return {"ok": True, **collector.stats.to_dict()}

    @router.post("/api/collector/stop")
    async def collector_stop():
        await collector.stop()
        return {"ok": True, **collector.stats.to_dict()}

    @router.post("/api/collector/backfill")
    async def collector_backfill(req: BackfillRequest):
        """Synchronously fetch a past range and cache it."""
        task = collector.backfill(req.asset, req.start_ts, req.end_ts)
        result = await task
        return {"ok": True, "asset": req.asset, "stats": result}

    @router.get("/api/data/inventory")
    async def inventory():
        """Summarise what's cached locally across all assets/intervals."""
        poly_dir = Path(settings.backtest_data_dir) / "poly"
        by_asset: dict = {}
        if poly_dir.exists():
            for f in poly_dir.glob("*.json"):
                # filename: {asset}_{interval}m_{epoch}.json
                parts = f.stem.split("_")
                if len(parts) < 3:
                    continue
                asset = parts[0]
                try:
                    epoch = int(parts[-1])
                except ValueError:
                    continue
                rec = by_asset.setdefault(asset, {"count": 0, "first": None, "last": None,
                                                  "resolved": 0, "total_volume": 0.0})
                rec["count"] += 1
                if rec["first"] is None or epoch < rec["first"]:
                    rec["first"] = epoch
                if rec["last"] is None or epoch > rec["last"]:
                    rec["last"] = epoch
                try:
                    with open(f) as fh:
                        payload = json.load(fh)
                        if payload.get("winner"):
                            rec["resolved"] += 1
                        rec["total_volume"] += float(payload.get("volume", 0) or 0)
                except (json.JSONDecodeError, OSError):
                    pass
        return {"assets": by_asset,
                "poly_cache_dir": str(poly_dir),
                "collector": collector.stats.to_dict()}

    return router
