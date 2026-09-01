"""
Real Polymarket historical data fetcher.

Pulls genuine market data straight from Polymarket's APIs so backtests run on
REAL token prices and REAL outcomes instead of a parametric model:

  * Gamma  GET /events/slug/{asset}-updown-{m}m-{epoch}
           → token IDs, outcome winner (outcomePrices), volume, boundaries
  * CLOB   GET /prices-history?market={token_id}&startTs=&endTs=&fidelity=
           → per-token price timeseries [{"t":..,"p":..}, ...] (~min granularity)
  * Binance klines → the oracle price path that drove resolution

Each interval is cached as its own JSON file so data accumulates incrementally
and repeat backtests are instant. Network is only hit for missing intervals.

⚠️ Granularity caveat: prices-history returns roughly one point per minute for
fresh crypto markets, and may downsample to ~12h for very old/resolved markets
(see Polymarket issue #216). We therefore always pair token prices with the
1-second Binance oracle path: decisions are made on the real oracle, fills are
priced at the nearest real token price point.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp

from ..config import Settings
from ..core.logging import StructuredLogger, build_logger
from .data import HistoricalData, PriceTick


@dataclass
class IntervalMeta:
    """A single resolved UP/DOWN market window."""
    interval_ts: int          # window start (unix sec)
    asset: str
    up_token_id: str
    down_token_id: str
    winner: Optional[str]     # "Up" / "Down" / None (unresolved)
    volume: float
    end_ts: int

    @property
    def up_won(self) -> Optional[bool]:
        if self.winner is None:
            return None
        return self.winner.lower().startswith("up")


@dataclass
class BacktestDataset:
    """Combined real dataset for a backtest run."""
    oracle_ticks: List[PriceTick] = field(default_factory=list)
    intervals: List[IntervalMeta] = field(default_factory=list)
    # interval_ts -> {"up": [(t,p)...], "down": [(t,p)...]}
    token_history: Dict[int, dict] = field(default_factory=dict)
    asset: str = "BTC"
    interval_minutes: int = 5
    source: str = "polymarket"

    def token_lookup(self) -> Dict[int, Tuple[Optional[float], Optional[float]]]:
        """Map oracle tick ts -> (up_mid, down_mid) using step interpolation.

        For a given timestamp we take the most recent token price point at or
        before it (price held until the next trade) — a realistic model of
        walking a sparse tick into the engine's per-second loop.
        """
        lookup: Dict[int, Tuple[Optional[float], Optional[float]]] = {}
        # group token points by interval
        for its, hist in self.token_history.items():
            up_pts = sorted(hist.get("up", []), key=lambda x: x[0])
            down_pts = sorted(hist.get("down", []), key=lambda x: x[0])
            up_pts.sort(key=lambda x: x[0]); down_pts.sort(key=lambda x: x[0])
            # for every oracle tick within this interval window
            win_start = its
            win_end = its + self.interval_minutes * 60
            for tick in self.oracle_ticks:
                if tick.ts < win_start or tick.ts > win_end:
                    continue
                up = _step_value(up_pts, tick.ts)
                down = _step_value(down_pts, tick.ts)
                if up is not None or down is not None:
                    # complement: if only one side known, derive the other (~1 - p)
                    if up is None and down is not None:
                        up = round(1.0 - down, 4)
                    if down is None and up is not None:
                        down = round(1.0 - up, 4)
                    lookup[tick.ts] = (up, down)
        return lookup

    def densify_oracle_to_seconds(self) -> None:
        """Interpolate 1-minute oracle ticks to 1-second resolution.

        Binance klines are 1-minute; for a 5-min market that's ~6 ticks. But
        trade-level token data has sub-second resolution. To actually USE that
        density, the engine must step at second resolution — otherwise the 3292
        token trades collapse to ~6 lookups.

        We linearly interpolate between consecutive 1m closes to produce a
        1 tick/second path. This is an approximation of the intra-minute oracle
        (which is unknowable from free data), but it lets the dense token prices
        drive realistic entry/exit timing.
        """
        if not self.oracle_ticks or len(self.oracle_ticks) < 2:
            return
        from .data import PriceTick
        dense: List[PriceTick] = []
        ticks = sorted(self.oracle_ticks, key=lambda t: t.ts)
        for i in range(len(ticks) - 1):
            a, b = ticks[i], ticks[i + 1]
            dense.append(a)
            span = b.ts - a.ts
            if span <= 1:
                continue
            for s in range(1, span):
                frac = s / span
                price = a.price + (b.price - a.price) * frac
                dense.append(PriceTick(ts=a.ts + s, price=price, source="interp_1s"))
        dense.append(ticks[-1])
        self.oracle_ticks = dense

    def stats(self) -> dict:
        resolved = [i for i in self.intervals if i.winner is not None]
        up_wins = sum(1 for i in resolved if i.up_won)
        # data density: average token price points per interval
        total_pts = sum(len(h.get("up", [])) + len(h.get("down", []))
                        for h in self.token_history.values())
        intervals_with_data = len(self.token_history)
        avg_pts = (total_pts / intervals_with_data) if intervals_with_data else 0.0
        return {
            "intervals_total": len(self.intervals),
            "intervals_resolved": len(resolved),
            "up_wins": up_wins,
            "down_wins": len(resolved) - up_wins,
            "oracle_ticks": len(self.oracle_ticks),
            "intervals_with_token_data": intervals_with_data,
            "token_points_total": total_pts,
            "avg_token_points_per_interval": round(avg_pts, 1),
            "data_resolution": "trades (sub-second)" if avg_pts > 20 else "prices-history (~1/min)",
            "total_volume": round(sum(i.volume for i in self.intervals), 2),
        }


def _step_value(points: List[Tuple[int, float]], ts: int) -> Optional[float]:
    """Most recent point at or before ts (None if none yet)."""
    best = None
    for t, p in points:
        if t <= ts:
            best = p
        else:
            break
    return best


class PolymarketDataFetcher:
    """Downloads real Gamma + CLOB + Binance data, cached per interval."""

    def __init__(self, settings: Settings,
                 log: Optional[StructuredLogger] = None) -> None:
        self.s = settings
        self.log = log or build_logger("poly-fetch")
        self.cache_dir = Path(settings.backtest_data_dir) / "poly"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._hist = HistoricalData(settings)
        self._gamma = settings.gamma_api
        self._clob = settings.clob_api
        self._data_api = "https://data-api.polymarket.com"
        # "trades" (per-trade, ~287x denser) or "prices" (prices-history, ~5pts/5min)
        self.token_source = getattr(settings, "backtest_token_source", "trades")

    # ── interval epochs ──────────────────────────────────────────────────

    @staticmethod
    def interval_epochs(start_ts: int, end_ts: int, interval_minutes: int) -> List[int]:
        step = interval_minutes * 60
        first = start_ts - (start_ts % step)
        return list(range(first, end_ts, step))

    # ── public: build a full dataset ─────────────────────────────────────

    async def build_dataset(
        self, asset: str, start_ts: int, end_ts: int,
        interval_minutes: int = 5, fidelity: int = 1,
        rate_limit_delay: float = 0.15,
    ) -> BacktestDataset:
        epochs = self.interval_epochs(start_ts, end_ts, interval_minutes)
        self.log.info(f"[POLY-FETCH] {len(epochs)} intervals for {asset} "
                      f"{interval_minutes}m [{epochs[0]}..{epochs[-1]}]" if epochs else
                      f"[POLY-FETCH] no intervals in range")

        dataset = BacktestDataset(asset=asset, interval_minutes=interval_minutes)

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            for epoch in epochs:
                rec = await self._load_or_fetch_interval(session, asset, epoch,
                                                          interval_minutes, fidelity)
                if rec is None:
                    continue
                meta, token_hist, cache_hit = rec
                dataset.intervals.append(meta)
                if token_hist:
                    dataset.token_history[epoch] = token_hist
                if not cache_hit:
                    await asyncio.sleep(rate_limit_delay)   # only throttle real fetches

        # oracle path from Binance (1s if available, else 1m)
        if dataset.intervals:
            o_start = dataset.intervals[0].interval_ts
            o_end = dataset.intervals[-1].end_ts
            dataset.oracle_ticks = await self._hist.fetch_klines(asset, o_start, o_end, "1m")

        st = dataset.stats()
        self.log.info(f"[POLY-FETCH] done: {st['intervals_resolved']}/{st['intervals_total']} "
                      f"resolved, {st['intervals_with_token_data']} w/ token data, "
                      f"{st['oracle_ticks']} oracle ticks")
        return dataset

    # ── per-interval fetch (with cache) ──────────────────────────────────

    async def _load_or_fetch_interval(
        self, session: aiohttp.ClientSession, asset: str, epoch: int,
        interval_minutes: int, fidelity: int,
    ) -> Optional[Tuple[IntervalMeta, Optional[dict], bool]]:
        cache_path = self.cache_dir / f"{asset}_{interval_minutes}m_{epoch}.json"
        cached = self._load_cache(cache_path)
        # invalidate cache if built with a different token source
        if cached is not None and cached.get("source") != self.token_source:
            cached = None
        if cached is not None:
            meta, th = self._unpack_cached(asset, epoch, interval_minutes, cached)
            return meta, th, True   # cache hit

        meta = await self._fetch_gamma(session, asset, epoch, interval_minutes)
        if meta is None:
            return None
        token_hist: Optional[dict] = None
        if meta.winner is not None or True:  # try token history regardless
            token_hist = await self._fetch_token_history(session, meta, fidelity)

        payload = {
            "up_token_id": meta.up_token_id, "down_token_id": meta.down_token_id,
            "winner": meta.winner, "volume": meta.volume, "end_ts": meta.end_ts,
            "token_history": token_hist,
            "condition_id": getattr(meta, "condition_id", None),
            "source": self.token_source,
        }
        self._save_cache(cache_path, payload)
        return meta, token_hist, False   # fresh fetch

    # ── Gamma: market metadata + winner ──────────────────────────────────

    async def _fetch_gamma(
        self, session: aiohttp.ClientSession, asset: str, epoch: int,
        interval_minutes: int,
    ) -> Optional[IntervalMeta]:
        slug = f"{asset.lower()}-updown-{interval_minutes}m-{epoch}"
        url = f"{self._gamma}/events/slug/{slug}"
        try:
            async with session.get(url) as r:
                if r.status != 200:
                    return None
                data = await r.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return None
        if not data or not data.get("markets"):
            return None

        m = data["markets"][0]
        token_ids = json.loads(m.get("clobTokenIds", "[]"))
        if len(token_ids) < 2:
            return None
        outcomes = json.loads(m.get("outcomes", "[]"))
        prices = json.loads(m.get("outcomePrices", "[]"))
        winner = _resolve_winner(outcomes, prices)

        end_ts = _iso_to_epoch(m.get("endDate") or data.get("endDate"))
        if end_ts == 0:
            end_ts = epoch + interval_minutes * 60
        return IntervalMeta(
            interval_ts=epoch, asset=asset,
            up_token_id=token_ids[0], down_token_id=token_ids[1],
            winner=winner, volume=float(m.get("volume", 0) or 0), end_ts=end_ts,
        )

    # ── CLOB: token price history ────────────────────────────────────────

    async def _fetch_token_history(
        self, session: aiohttp.ClientSession, meta: IntervalMeta, fidelity: int,
    ) -> Optional[dict]:
        start, end = meta.interval_ts, meta.end_ts
        up = await self._prices_history(session, meta.up_token_id, start, end, fidelity)
        down = await self._prices_history(session, meta.down_token_id, start, end, fidelity)
        if not up and not down:
            return None
        return {"up": up or [], "down": down or []}

    async def _prices_history(
        self, session: aiohttp.ClientSession, token_id: str,
        start_ts: int, end_ts: int, fidelity: int,
    ) -> List[Tuple[int, float]]:
        params = {"market": token_id, "startTs": start_ts, "endTs": end_ts, "fidelity": fidelity}
        url = f"{self._clob}/prices-history"
        try:
            async with session.get(url, params=params) as r:
                if r.status != 200:
                    return []
                data = await r.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return []
        history = data.get("history", []) if isinstance(data, dict) else []
        return [(int(h["t"]), float(h["p"])) for h in history if "t" in h and "p" in h]

    # ── Data API: per-trade history (sub-minute resolution) ──────────────
    #
    # prices-history stores ~1 point/minute; for 5-min markets that's ~5 pts.
    # The Data API /trades endpoint returns EVERY executed trade with a
    # second-resolution timestamp — for a liquid 5-min market that's thousands
    # of trades (measured: 3292 trades vs 5 price-history points = 287x denser).
    # Trades carry real execution prices, which is exactly what a backtest
    # should fill at. We page backwards (DESC order) with rate limiting.

    async def _fetch_token_history(
        self, session: aiohttp.ClientSession, meta: IntervalMeta, fidelity: int,
    ) -> Optional[dict]:
        start, end = meta.interval_ts, meta.end_ts
        if self.token_source == "trades":
            # need the condition_id; fetch it once and stash on the meta
            if not getattr(meta, "condition_id", None):
                cid = await self._fetch_condition_id(session, meta)
                if cid:
                    meta.condition_id = cid  # type: ignore[attr-defined]
            cid = getattr(meta, "condition_id", None)
            if cid:
                up_trades, down_trades = await self._fetch_trades_split(
                    session, cid, meta.up_token_id, meta.down_token_id, start, end)
                if up_trades or down_trades:
                    return {"up": up_trades, "down": down_trades}
            # fall through to prices-history if trades unavailable
        up = await self._prices_history(session, meta.up_token_id, start, end, fidelity)
        down = await self._prices_history(session, meta.down_token_id, start, end, fidelity)
        if not up and not down:
            return None
        return {"up": up or [], "down": down or []}

    async def _fetch_condition_id(
        self, session: aiohttp.ClientSession, meta: IntervalMeta,
    ) -> Optional[str]:
        """The /trades endpoint keys on condition_id, not token_id."""
        slug = f"{meta.asset.lower()}-updown-{int(self.s.interval_minutes)}m-{meta.interval_ts}" \
               if not hasattr(self, "_im") else None
        # rebuild slug from meta (interval_minutes lives on the dataset, but the
        # slug format is fixed: {asset}-updown-{m}m-{epoch})
        import re
        # determine minutes from the window length
        mins = (meta.end_ts - meta.interval_ts) // 60
        slug = f"{meta.asset.lower()}-updown-{mins}m-{meta.interval_ts}"
        url = f"{self._gamma}/events/slug/{slug}"
        try:
            async with session.get(url) as r:
                if r.status != 200:
                    return None
                data = await r.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return None
        markets = (data or {}).get("markets") or []
        if markets:
            return markets[0].get("conditionId")
        return None

    async def _fetch_trades_split(
        self, session: aiohttp.ClientSession, condition_id: str,
        up_token_id: str, down_token_id: str,
        start_ts: int, end_ts: int,
    ) -> Tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
        """Page through /trades for a market and split by outcome token."""
        all_trades: list = []
        offset = 0
        page_size = 500
        max_pages = 20
        headers = {"User-Agent": "polymarket-bot/6.0"}
        for _ in range(max_pages):
            url = f"{self._data_api}/trades"
            params = {"market": condition_id, "limit": page_size, "offset": offset}
            try:
                async with session.get(url, params=params, headers=headers) as r:
                    if r.status != 200:
                        break
                    batch = await r.json()
            except (aiohttp.ClientError, asyncio.TimeoutError):
                break
            if not isinstance(batch, list) or not batch:
                break
            in_win = [t for t in batch if start_ts <= int(t.get("timestamp", 0)) <= end_ts + 15]
            all_trades.extend(in_win)
            earliest = min(int(t.get("timestamp", end_ts)) for t in batch)
            offset += len(batch)
            if earliest <= start_ts or len(batch) < page_size:
                break
            await asyncio.sleep(0.35)  # respect rate limits (403 otherwise)

        up_pts = [(int(t["timestamp"]), float(t["price"]))
                  for t in all_trades if str(t.get("asset")) == str(up_token_id)]
        down_pts = [(int(t["timestamp"]), float(t["price"]))
                    for t in all_trades if str(t.get("asset")) == str(down_token_id)]
        up_pts.sort(key=lambda x: x[0])
        down_pts.sort(key=lambda x: x[0])
        return up_pts, down_pts

    # ── cache IO ─────────────────────────────────────────────────────────

    @staticmethod
    def _load_cache(path: Path) -> Optional[dict]:
        if not path.exists():
            return None
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

    @staticmethod
    def _save_cache(path: Path, payload: dict) -> None:
        try:
            with open(path, "w") as f:
                json.dump(payload, f)
        except OSError:
            pass

    @staticmethod
    def _unpack_cached(asset, epoch, interval_minutes, cached: dict):
        meta = IntervalMeta(
            interval_ts=epoch, asset=asset,
            up_token_id=cached["up_token_id"], down_token_id=cached["down_token_id"],
            winner=cached.get("winner"), volume=cached.get("volume", 0.0),
            end_ts=cached.get("end_ts", epoch + interval_minutes * 60),
        )
        if cached.get("condition_id"):
            meta.condition_id = cached["condition_id"]  # type: ignore[attr-defined]
        return meta, cached.get("token_history")


def _resolve_winner(outcomes: List[str], prices: List[str]) -> Optional[str]:
    """Determine winner from outcomePrices.

    CRITICAL: After UMA finalization, prices are EXACTLY ["1","0"] or ["0","1"].
    Before finalization, they are live trading prices (e.g. ["0.52","0.48"]).
    We require at least one price to be >= 0.95 to consider it resolved.
    This prevents reading 'proposed' results that might change during UMA liveness.
    """
    if not outcomes or not prices:
        return None
    for outcome, price in zip(outcomes, prices):
        try:
            p = float(price)
            if p >= 0.95:
                return outcome
        except (ValueError, TypeError):
            continue
    return None


def _iso_to_epoch(iso: Optional[str]) -> int:
    if not iso:
        return 0
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except (ValueError, TypeError):
        return 0
