"""
Historical data — fetches and caches Binance klines for backtesting.

Binance provides genuine OHLCV + trade data that drove the real outcomes of
Polymarket UP/DOWN markets. We fetch 1-second klines (or 1-minute where 1s
is unavailable) and cache locally as CSV so repeated backtests are fast.

The Polymarket token order-books are NOT historically retrievable in bulk, so
those are modelled (see simulator.py) — UNLESS poly mode is used, in which case
real token prices come from poly_fetcher.py. But the underlying oracle price
path is always real — that is the part that determines whether a market
resolved UP or DOWN.
"""

from __future__ import annotations

import asyncio
import csv
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import aiohttp

from ..config import Settings


SYMBOL_MAP = {"BTC": "BTCUSDT", "ETH": "ETHUSDT"}


@dataclass
class PriceTick:
    """A single price observation at a UTC timestamp (seconds)."""
    ts: int        # unix seconds
    price: float
    source: str = "binance_kline"


class HistoricalData:
    """Downloads + caches Binance klines for a given asset / date range."""

    def __init__(self, settings: Settings) -> None:
        self.s = settings
        self.cache_dir = Path(settings.backtest_data_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    # ── download ─────────────────────────────────────────────────────────

    async def fetch_klines(
        self, asset: str, start_ts: int, end_ts: int, interval: str = "1s"
    ) -> List[PriceTick]:
        """Fetch Binance klines between start_ts and end_ts (unix seconds).

        1s klines are available for ~last 6 months; falls back to 1m automatically.
        Uses data-api.binance.vision as a geo-block fallback (same kline format).
        """
        symbol = SYMBOL_MAP.get(asset, f"{asset}USDT")
        cached = self._load_cache(asset, start_ts, end_ts, interval)
        if cached is not None:
            return cached

        ticks: List[PriceTick] = []
        cursor = start_ts * 1000
        end_ms = end_ts * 1000

        # api.binance.com is geo-blocked in some regions; data-api.binance.vision
        # is a public market-data mirror (no auth, same kline format) that works.
        base_urls = [self.s.binance_api, "https://data-api.binance.vision/api/v3"]

        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while cursor < end_ms:
                params = {
                    "symbol": symbol, "interval": interval,
                    "startTime": cursor, "endTime": end_ms, "limit": 1000,
                }
                data = None
                for base in base_urls:
                    url = f"{base}/klines"
                    try:
                        async with session.get(url, params=params) as r:
                            if r.status == 400 and interval == "1s":
                                return await self.fetch_klines(asset, start_ts, end_ts, "1m")
                            if r.status in (403, 451):
                                continue  # geoblock → try next base
                            r.raise_for_status()
                            payload = await r.json()
                            # geo-blocks sometimes return 200 + error dict — skip
                            if isinstance(payload, list):
                                data = payload
                                break
                    except (aiohttp.ClientError, asyncio.TimeoutError):
                        continue
                if not data:
                    break
                for k in data:
                    # k = [open_time, open, high, low, close, volume, close_time, ...]
                    ts = int(k[0] // 1000)
                    ticks.append(PriceTick(ts=ts, price=float(k[4])))  # close
                    cursor = int(k[6]) + 1  # close_time + 1 ms
                if len(data) < 1000:
                    break
                await asyncio.sleep(0.1)  # be polite to the API

        ticks.sort(key=lambda t: t.ts)
        if ticks:
            self._save_cache(asset, start_ts, end_ts, interval, ticks)
        return ticks

    # ── cache ────────────────────────────────────────────────────────────

    def _cache_path(self, asset: str, start_ts: int, end_ts: int, interval: str) -> Path:
        return self.cache_dir / f"{asset}_{start_ts}_{end_ts}_{interval}.csv"

    def _load_cache(self, asset, start_ts, end_ts, interval) -> Optional[List[PriceTick]]:
        p = self._cache_path(asset, start_ts, end_ts, interval)
        if not p.exists():
            return None
        ticks = []
        with open(p, newline="") as f:
            for row in csv.DictReader(f):
                ticks.append(PriceTick(ts=int(row["ts"]), price=float(row["price"]),
                                       source=row.get("source", "binance_kline")))
        return ticks if ticks else None

    def _save_cache(self, asset, start_ts, end_ts, interval, ticks: List[PriceTick]) -> None:
        p = self._cache_path(asset, start_ts, end_ts, interval)
        with open(p, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["ts", "price", "source"])
            w.writeheader()
            for t in ticks:
                w.writerow({"ts": t.ts, "price": t.price, "source": t.source})

    # ── synthetic data (for when no network / quick tests) ───────────────

    @staticmethod
    def synthetic_random_walk(
        start_price: float, duration_secs: int, step_secs: int = 1,
        volatility: float = 0.0002, seed: int = 42, drift: float = 0.0,
        trend_changes: int = 0
    ) -> List[PriceTick]:
        """Generate a deterministic price path (seeded).

        `drift` adds a constant per-step drift (momentum). `trend_changes`
        injects N regime flips to mimic trending markets that the late-window
        scalper thrives on (calm recent vol + a decided cumulative move).
        """
        import random
        rng = random.Random(seed)
        ticks = []
        price = start_price
        base_ts = int(time.time()) - duration_secs
        seg_len = max(1, duration_secs // (trend_changes + 1))
        segments = []
        for s in range(trend_changes + 1):
            d = drift * (1 if rng.random() > 0.5 else -1)
            segments.append((s * seg_len, d))
        seg_idx = 0
        cur_drift = segments[0][1] if segments else drift
        for i in range(0, duration_secs, step_secs):
            if segments and seg_idx < len(segments) - 1 and i >= segments[seg_idx + 1][0]:
                seg_idx += 1
                cur_drift = segments[seg_idx][1]
            price *= (1 + rng.gauss(0, volatility) + cur_drift)
            ticks.append(PriceTick(ts=base_ts + i, price=price))
        return ticks
