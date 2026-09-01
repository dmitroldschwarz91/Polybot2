"""
Market discovery, interval start-price resolution, and trend analysis.

Consolidates: fetch_market_async, parse_market_data, get_interval_start_price_*,
analyze_market, analyze_trend, track_oracle_price, check_micro_trend_ws.
"""

from __future__ import annotations

import json
import re
import time
from collections import deque
from typing import Dict, List, Optional, Tuple

from ..config import Settings
from ..core.http import AsyncHTTP, run_sync
from ..core.logging import StructuredLogger
from .stores import LivePriceStore


class TrendTracker:
    """Tracks whether the oracle price stayed consistently above/below target."""

    def __init__(self) -> None:
        self.state: Dict[str, dict] = {}

    def track(self, slug: str, asset: str, oracle_price: Optional[float],
              target: Optional[float]) -> dict:
        if slug not in self.state:
            self.state[slug] = {"asset": asset, "target": target,
                                "always_above": True, "always_below": True}
        rec = self.state[slug]
        if oracle_price is None or target is None:
            return rec
        if oracle_price < target:
            rec["always_above"] = False
        if oracle_price > target:
            rec["always_below"] = False
        return rec

    def analyze(self, slug: str, asset: str, prices: LivePriceStore) -> dict:
        rec = self.state.get(slug)
        if not rec:
            return {"direction": None, "is_consistent": False, "current_deviation": 0}
        target = rec["target"]
        op = prices.get_oracle_price(asset)
        if op is None or not target:
            return {"direction": None, "is_consistent": False, "current_deviation": 0}
        dev = (op - target) / target
        if rec["always_above"]:
            d, ic = "UP", True
        elif rec["always_below"]:
            d, ic = "DOWN", True
        else:
            d, ic = "UP" if dev > 0 else "DOWN", False
        return {"direction": d, "is_consistent": ic, "current_deviation": dev,
                "oracle_price": op, "chainlink_age": prices.get_chainlink_age(asset)}


class MarketData:
    """Fetches Polymarket markets and resolves the interval 'price to beat'."""

    def __init__(self, settings: Settings, prices: LivePriceStore,
                 http: AsyncHTTP, log: StructuredLogger) -> None:
        self.s = settings
        self.prices = prices
        self.http = http
        self.log = log
        self.gamma_cache: Dict[str, Tuple[Optional[dict], float]] = {}
        self.start_prices: Dict[str, Dict[str, float]] = {}
        self.trend = TrendTracker()

    # ── interval timing ──────────────────────────────────────────────────

    def current_interval_ts(self) -> int:
        n = int(time.time())
        return n - (n % (self.s.interval_minutes * 60))

    def next_interval_ts(self) -> int:
        return self.current_interval_ts() + self.s.interval_minutes * 60

    # ── market fetch ─────────────────────────────────────────────────────

    async def fetch_market(self, asset: str) -> Optional[dict]:
        slug = f"{asset.lower()}-updown-{self.s.interval_minutes}m-{self.current_interval_ts()}"
        cached = self.gamma_cache.get(slug)
        if cached:
            result, ts = cached
            # Cache market data for 10 seconds to avoid hammering Gamma API
            if time.time() - ts < 10.0:
                if result and result.get("target_price") is None:
                    result["target_price"] = self._cached_start_price(asset)
                return result

        data = await self.http.get(
            f"{self.s.gamma_api}/markets",
            {"slug": slug, "active": "true", "closed": "false", "limit": "5"},
        )
        if not data or not isinstance(data, list) or not data:
            self.gamma_cache[slug] = (None, time.time())
            return None

        result = self._parse_market(data[0], slug)
        if result:
            result["asset"] = asset
            if result.get("target_price") is None:
                result["target_price"] = self._cached_start_price(asset)
        self.gamma_cache[slug] = (result, time.time())
        return result

    def _parse_market(self, raw: dict, slug: str) -> Optional[dict]:
        if not isinstance(raw, dict):
            return None
        ids = json.loads(raw.get("clobTokenIds", "[]"))
        asset = None
        slug_lower, question_lower = slug.lower(), (raw.get("question") or "").lower()
        for a in self.s.assets:
            if a.lower() in slug_lower:
                asset = a
                break
        if asset is None:
            if "bitcoin" in question_lower or "btc" in question_lower:
                asset = "BTC"
            elif "ethereum" in question_lower or "eth" in question_lower:
                asset = "ETH"
        return {
            "slug": slug,
            "asset": asset,
            "up_token_id": ids[0] if ids else None,
            "down_token_id": ids[1] if len(ids) > 1 else None,
            "end_ts": self.next_interval_ts(),
            "target_price": self._cached_start_price(asset) if asset else None,
        }

    def _cached_start_price(self, asset: Optional[str]) -> Optional[float]:
        if not asset:
            return None
        return self.start_prices.get(str(self.current_interval_ts()), {}).get(asset)

    # ── interval start price (the 'price to beat') ───────────────────────

    async def get_or_set_start_price(self, asset: str, interval_ts: int,
                                     allow_fallback: bool = True) -> Optional[float]:
        key = str(interval_ts)
        if key in self.start_prices and asset in self.start_prices[key]:
            return self.start_prices[key][asset]
        self.start_prices.setdefault(key, {})

        price = self._chainlink_boundary_price(asset, interval_ts)
        if price is not None:
            self.start_prices[key][asset] = price
            self.log.info(f"[{asset}] Start price from Chainlink",
                          interval=interval_ts, price=f"${price:.2f}")
            return price

        if not allow_fallback:
            return None

        price = await self._binance_open_price(asset, interval_ts)
        if price is not None:
            self.start_prices[key][asset] = price
            self.log.warning(f"[{asset}] Start price from Binance fallback",
                             interval=interval_ts, price=f"${price:.2f}")
            return price

        price = self.prices.get_oracle_price(asset)
        if price is not None:
            self.start_prices[key][asset] = price
            self.log.warning(f"[{asset}] Using current oracle as start price", price=f"${price:.2f}")
            return price
        return None

    def _chainlink_boundary_price(self, asset: str, interval_ts: int,
                                  max_drift_ms: int = 5000) -> Optional[float]:
        history = self.prices.chainlink_history.get(asset)
        if not history:
            return None
        boundary_ms = interval_ts * 1000
        best_price, best_ts = None, None
        for oracle_ts_ms, _, price in history:
            if oracle_ts_ms >= boundary_ms:
                if best_ts is None or oracle_ts_ms < best_ts:
                    best_ts, best_price = oracle_ts_ms, price
        if best_ts is not None and (best_ts - boundary_ms) <= max_drift_ms:
            return best_price
        for oracle_ts_ms, _, price in reversed(list(history)):
            if oracle_ts_ms < boundary_ms and (boundary_ms - oracle_ts_ms) <= max_drift_ms:
                return price
        return None

    async def _binance_open_price(self, asset: str, interval_ts: int) -> Optional[float]:
        symbol = {"BTC": "BTCUSDT", "ETH": "ETHUSDT"}.get(asset)
        if not symbol:
            return None
        data = await self.http.get(
            f"{self.s.binance_api}/klines",
            {"symbol": symbol, "interval": "1m", "startTime": interval_ts * 1000, "limit": 1},
        )
        if data and len(data) > 0:
            return float(data[0][1])
        return None

    # ── micro trend (for early-trend confirmation) ───────────────────────

    def check_micro_trend(self, asset: str, direction: str) -> dict:
        data = self.prices.get_micro_trend_data(
            asset, self.s.early_trend_micro_window, self.s.early_trend_micro_min_points
        )
        if len(data) < self.s.early_trend_micro_min_points:
            return {"confirmed": False, "reason": "insufficient_data", "points": len(data)}
        rp = [p for _, p in data]
        avg = sum(rp) / len(rp)
        pct = (rp[-1] - avg) / avg if avg > 0 else 0.0
        if direction == "UP":
            confirmed = pct > self.s.early_trend_micro_min_change_pct
        elif direction == "DOWN":
            confirmed = pct < -self.s.early_trend_micro_min_change_pct
        else:
            confirmed = False
        return {"confirmed": confirmed,
                "reason": "confirmed" if confirmed else "wrong_direction",
                "price_change_pct": pct, "points": len(rp)}

    # ── standard-entry market analysis ───────────────────────────────────

    def analyze_market(self, ph: Dict[str, deque], slug: str,
                       window: Optional[int] = None) -> dict:
        ws = window or self.s.price_hist_window
        now = time.time()
        cutoff = now - ws
        up_p = [p for ts, p in ph.get(f"{slug}_UP", []) if ts >= cutoff]
        dn_p = [p for ts, p in ph.get(f"{slug}_DOWN", []) if ts >= cutoff]
        up, dn = self._series_stats(up_p), self._series_stats(dn_p)
        result = {
            "up_trend": 0.0, "down_trend": 0.0, "up_current": None, "down_current": None,
            "recommended": "NONE", "confidence": 0.0, "reason": "No data",
            "high_price_entry": False, "up_is_choppy": False, "down_is_choppy": False,
            "both_choppy": False,
        }
        if up:
            result["up_current"], result["up_trend"], result["up_is_choppy"] = up["current"], up["trend"], up["is_choppy"]
        if dn:
            result["down_current"], result["down_trend"], result["down_is_choppy"] = dn["current"], dn["trend"], dn["is_choppy"]
        result["both_choppy"] = result["up_is_choppy"] and result["down_is_choppy"]

        if up is None and dn is None:
            return result
        if up is None or dn is None:
            s = up or dn
            d = "UP" if up else "DOWN"
            ic = result["up_is_choppy"] if up else result["down_is_choppy"]
            if ic:
                result["reason"] = f"Only {d} (chop)"
            elif s["trend"] > self.s.min_trend_diff:
                result.update(recommended=d, confidence=0.5, reason=f"Only {d}^")
            elif s["min"] >= self.s.high_price_threshold:
                result.update(recommended=d, confidence=0.5, high_price_entry=True)
            return result

        td = abs(up["trend"] - dn["trend"])
        md = up["momentum"] - dn["momentum"]
        uc, dc = result["up_is_choppy"], result["down_is_choppy"]

        def _set(d, c, r, fb=False):
            result.update(recommended=d, confidence=c * self.s.fallback_confidence_multiplier if fb else c, reason=r)

        def _try_high_price():
            uo = up["min"] >= self.s.high_price_threshold and not uc
            do = dn["min"] >= self.s.high_price_threshold and not dc
            if uo and do:
                w = "UP" if up["min"] >= dn["min"] else "DOWN"
                _set(w, 0.5, f"{w} high"); result["high_price_entry"] = True
            elif uo:
                _set("UP", 0.5, "UP high"); result["high_price_entry"] = True
            elif do:
                _set("DOWN", 0.5, "DOWN high"); result["high_price_entry"] = True

        if up["trend"] > 0 and dn["trend"] <= 0:
            if not uc: _set("UP", min(1.0, td / 0.02), "UP^ DOWNv")
        elif dn["trend"] > 0 and up["trend"] <= 0:
            if not dc: _set("DOWN", min(1.0, td / 0.02), "DOWN^ UPv")
        elif up["trend"] > 0 and dn["trend"] > 0:
            if md > self.s.min_trend_diff and not uc:
                _set("UP", min(1.0, abs(md) / 0.02), "Both^, UP faster")
            elif md < -self.s.min_trend_diff and not dc:
                _set("DOWN", min(1.0, abs(md) / 0.02), "Both^, DOWN faster")
            else:
                _try_high_price()
        else:
            if up["trend"] > dn["trend"] + self.s.min_trend_diff:
                if not uc: _set("UP", min(1.0, td / 0.02), "Bothv, UP slower")
            elif dn["trend"] > up["trend"] + self.s.min_trend_diff:
                if not dc: _set("DOWN", min(1.0, td / 0.02), "Bothv, DOWN slower")
            else:
                _try_high_price()
        return result

    def _series_stats(self, p: List[float]) -> Optional[dict]:
        n = len(p)
        if n == 0:
            return None
        cur = p[-1]
        mn = min(p)
        avg = sum(p) / n
        trend = cur - avg
        mom = trend / avg if avg > 0 else 0.0
        cv, dc = 0.0, 0
        if n >= self.s.min_price_points:
            vs = sum((x - avg) ** 2 for x in p)
            for i in range(2, n):
                if (p[i - 1] - p[i - 2]) * (p[i] - p[i - 1]) < 0:
                    dc += 1
            cv = (vs / n) ** 0.5 / avg if avg > 0 else 0.0
        choppy = n >= self.s.min_price_points and (cv > self.s.max_cv or dc >= self.s.max_direction_changes)
        return {"current": cur, "min": mn, "avg": avg, "trend": trend, "momentum": mom, "is_choppy": choppy}

    # ── cleanup ──────────────────────────────────────────────────────────

    def cleanup_caches(self) -> None:
        now = time.time()
        self.gamma_cache.clear()
        for key in list(self.start_prices.keys()):
            try:
                if now - int(key) > self.s.interval_minutes * 60 * 3:
                    self.start_prices.pop(key, None)
            except (ValueError, TypeError):
                self.start_prices.pop(key, None)
        for slug in list(self.trend.state.keys()):
            parts = slug.rsplit("-", 1)
            if len(parts) == 2:
                try:
                    if now - int(parts[-1]) > self.s.interval_minutes * 60 * 2:
                        self.trend.state.pop(slug, None)
                except ValueError:
                    pass
