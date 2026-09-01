"""
FastAPI application entry point.

Run:  uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
The trading engine is created once at import; the dashboard starts/stops it.
The real-time collector and optimizer are independent services.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse
import base64
import secrets
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import Settings, get_settings
from .api.routes import create_router
from .api.backtest_routes import create_backtest_router
from .api.data_routes import create_data_router
from .api.optimizer_routes import create_optimizer_router
from .api.demo_routes import create_demo_router
from .engine.bot import TradingEngine


_SETTINGS: Settings | None = None
_ENGINE: TradingEngine | None = None
_DATA_ROUTER = None
_DEMO_ROUTER = None


def get_engine() -> TradingEngine:
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = TradingEngine(get_settings())
    return _ENGINE


def get_app_settings() -> Settings:
    global _SETTINGS
    if _SETTINGS is None:
        _SETTINGS = get_settings()
    return _SETTINGS


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_app_settings()
    Path(settings.log_dir).mkdir(parents=True, exist_ok=True)
    # autostart only if AUTOSTART_BOT=1
    if os.getenv("AUTOSTART_BOT", "0") == "1":
        engine = get_engine()
        await engine.start()
    # autostart the data collector if AUTOSTART_COLLECTOR=1
    if os.getenv("AUTOSTART_COLLECTOR", "0") == "1" and _DATA_ROUTER is not None:
        collector = getattr(_DATA_ROUTER, "collector", None)
        if collector and not collector.running:
            collector.start()
    yield
    engine = _ENGINE
    if engine and engine.status.running:
        await engine.stop()
    if _DATA_ROUTER is not None:
        collector = getattr(_DATA_ROUTER, "collector", None)
        if collector and collector.running:
            await collector.stop()
    if _DEMO_ROUTER is not None:
        demo_eng = _DEMO_ROUTER.get_demo() if hasattr(_DEMO_ROUTER, "get_demo") else None
        if demo_eng and demo_eng.running:
            await demo_eng.stop()


def create_app() -> FastAPI:
    global _DATA_ROUTER
    settings = get_app_settings()
    app = FastAPI(title="Polymarket UP/DOWN Bot", version="6.0.0", lifespan=lifespan)

    # ── Optional Basic Auth ──
    pwd = settings.dashboard_password
    if pwd:
        @app.middleware("http")
        async def auth_middleware(request: Request, call_next):
            # Allow WebSocket upgrades (auth via query param or header)
            if request.url.path.startswith("/ws/"):
                # WS connections pass auth via query param ?token=
                token = request.query_params.get("token", "")
                if token == pwd:
                    return await call_next(request)
                return JSONResponse({"error": "unauthorized"}, status_code=401)

            auth = request.headers.get("Authorization")
            if auth and auth.startswith("Basic "):
                try:
                    decoded = base64.b64decode(auth[6:]).decode("utf-8")
                    if ":" in decoded:
                        _, password = decoded.split(":", 1)
                        if password == pwd:
                            return await call_next(request)
                except Exception:
                    pass
            # Return 401 with WWW-Authenticate header (browsers show login dialog)
            return JSONResponse(
                {"detail": "Not authenticated"},
                status_code=401,
                headers={"WWW-Authenticate": "Basic realm=Polymarket-Bot"}
            )
    engine = get_engine()

    app.include_router(create_router(engine, settings))
    app.include_router(create_backtest_router(engine, settings))
    _DATA_ROUTER = create_data_router(settings)
    app.include_router(_DATA_ROUTER)
    app.include_router(create_optimizer_router(settings))
    global _DEMO_ROUTER
    _DEMO_ROUTER = create_demo_router(settings)
    app.include_router(_DEMO_ROUTER)

    # Use the same robust root resolution as config.py (parents[2], not [3])
    from .config import PROJECT_ROOT
    static_dir = PROJECT_ROOT / "frontend" / "static"
    static_dir.mkdir(parents=True, exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/")
    async def index():
        return FileResponse(str(static_dir / "index.html"))

    return app


app = create_app()
