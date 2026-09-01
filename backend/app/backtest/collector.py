"""
Real-time data collector.

A background service that continuously accumulates FRESH Polymarket intervals as
they close + resolve, so the cache grows automatically and walk-forward
optimization always has up-to-date real data.

Design:
  * Runs as a single asyncio task per asset/interval pair.
  * Aligns to interval boundaries (5m → every multiple of 300s).
  * After a boundary it waits a grace period (resolution happens ~30s after
    close), then fetches the just-closed interval via the fetcher (which caches
    it). Retries on failure.
  * FAILED intervals go into a retry queue (pending list) and are re-checked
    with exponential backoff — Polymarket sometimes indexes ETH markets 1-3 min
    after they close, so a fresh interval may be "unresolved" at first but
    appear a few minutes later.
  * Also streams the live Binance oracle ticks into a rolling file so the
    oracle path is captured at 1m resolution even between backfills.

It is fully decoupled from the trading engine — you can record data with the
bot stopped, and the bot can trade while recording runs.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from ..config import Settings
from ..core.logging import StructuredLogger, build_logger
from .data import HistoricalData
from .poly_fetcher import PolymarketDataFetcher
from .book_recorder import OrderBookRecorder
from ..marketdata.stores import LivePriceStore
from ..marketdata.websockets import WebSocketManager


# ── retry queue config ────────────────────────────────────────────────────

MAX_PENDING_RETRIES = 6         # total attempts per interval (1 fresh + 5 retries)
PENDING_BACKOFF_BASE = 60       # seconds; grows: 60, 120, 240, 480, 960
MAX_PENDING_QUEUE = 50          # don't let the queue grow unbounded


@dataclass
class PendingInterval:
    """A failed interval awaiting retry."""
    asset: str
    epoch: int
    attempts: int = 1           # starts at 1 (the fresh attempt that failed)
    next_retry_ts: float = 0.0  # unix seconds; when to try again
    last_error: str = ""

    def schedule_retry(self) -> None:
        """Compute next retry time with exponential backoff."""
        delay = PENDING_BACKOFF_BASE * (2 ** (self.attempts - 1))
        self.next_retry_ts = time.time() + delay

    @property
    def is_due(self) -> bool:
        return time.time() >= self.next_retry_ts

    @property
    def is_exhausted(self) -> bool:
        return self.attempts >= MAX_PENDING_RETRIES


@dataclass
class CollectorStats:
    """Live status of the recording service, exposed to the dashboard."""
    running: bool = False
    started_at: Optional[float] = None
    assets: List[str] = field(default_factory=list)
    interval_minutes: int = 5
    intervals_collected: int = 0          # successfully fetched this session
    intervals_failed: int = 0             # exhausted all retries
    intervals_pending: int = 0            # in retry queue right now
    intervals_recovered: int = 0          # succeeded on a retry (were pending)
    last_interval_ts: Optional[int] = None
    last_fetch_at: Optional[float] = None
    last_error: Optional[str] = None
    next_collection_ts: Optional[int] = None
    oracle_ticks_written: int = 0

    def to_dict(self) -> dict:
        return {
            "running": self.running,
            "uptime": round(time.time() - self.started_at, 0) if self.started_at else 0,
            "assets": self.assets,
            "interval_minutes": self.interval_minutes,
            "intervals_collected": self.intervals_collected,
            "intervals_failed": self.intervals_failed,
            "intervals_pending": self.intervals_pending,
            "intervals_recovered": self.intervals_recovered,
            "last_interval": (
                datetime.fromtimestamp(self.last_interval_ts, tz=timezone.utc).isoformat()
                if self.last_interval_ts else None
            ),
            "last_fetch_at": (
                datetime.fromtimestamp(self.last_fetch_at, tz=timezone.utc).isoformat()
                if self.last_fetch_at else None
            ),
            "next_collection": (
                datetime.fromtimestamp(self.next_collection_ts, tz=timezone.utc).isoformat()
                if self.next_collection_ts else None
            ),
            "last_error": self.last_error,
            "oracle_ticks_written": self.oracle_ticks_written,
        }


class RealTimeCollector:
    """Continuously records fresh resolved intervals to the cache."""

    def __init__(
        self,
        settings: Settings,
        assets: Optional[List[str]] = None,
        interval_minutes: Optional[int] = None,
        grace_secs: float = 150.0,         # wait after close (UMA needs 2-3 min)
        retry_secs: float = 20.0,          # gap between in-line retries (fresh attempt)
        log: Optional[StructuredLogger] = None,
    ) -> None:
        self.s = settings
        self.assets = list(assets or settings.assets)
        self.interval_minutes = interval_minutes or settings.interval_minutes
        self.grace_secs = grace_secs
        self.retry_secs = retry_secs
        self.log = log or build_logger("collector")
        self.fetcher = PolymarketDataFetcher(settings, self.log)
        self.hist = HistoricalData(settings)
        self.stats = CollectorStats(assets=self.assets, interval_minutes=self.interval_minutes)

        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._seen: Dict[str, int] = {a: 0 for a in self.assets}  # last SUCCESS epoch per asset
        # ── retry queue for intervals that weren't resolved on first try ──
        self._pending: List[PendingInterval] = []
        # ── live order-book recording ──
        self._book_prices = LivePriceStore(self.assets, book_stale_secs=9999)
        self._book_fills = type("F", (), {"orders": {}})()
        self._book_ws = WebSocketManager(settings, self._book_prices, self._book_fills, self.log)
        self._book_recorder = OrderBookRecorder(self._book_prices, self.fetcher.cache_dir, self.log)
        # token_ids currently being recorded for the active interval
        self._active_book_tokens: Dict[str, Set[str]] = {}   # asset -> token_ids

    # ── lifecycle ────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self.stats.running = True
        self.stats.started_at = time.time()
        self._task = asyncio.create_task(self._run(), name="rt-collector")
        # start the live order-book recording subsystem
        self._book_ws.start()
        self._book_recorder.start()
        self.log.info(f"[COLLECTOR] started for {self.assets} ({self.interval_minutes}m, "
                      f"grace={self.grace_secs}s, book-recording=ON)")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        # stop book recording
        self._book_recorder.stop()
        await self._book_ws.stop()
        self.stats.running = False
        self.log.info("[COLLECTOR] stopped")

    @property
    def running(self) -> bool:
        return self.stats.running

    # ── main loop ────────────────────────────────────────────────────────

    async def _run(self) -> None:
        try:
            while not self._stop.is_set():
                now = time.time()
                boundary = self._last_boundary(int(now))
                # time at which the just-closed interval is safe to fetch
                ready_at = boundary + self.interval_minutes * 60 + self.grace_secs
                wait = ready_at - now

                # ── process pending retries while waiting for next boundary ──
                # This is the key: failed ETH intervals get re-checked here,
                # filling the gaps in the cache without blocking fresh collection.
                await self._process_pending(until_ts=ready_at)

                if wait > 0:
                    self.stats.next_collection_ts = int(ready_at)
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=max(0.1, wait))
                        return  # stop signaled
                    except asyncio.TimeoutError:
                        pass

                # subscribe to book updates for the NEW interval (before it closes)
                for asset in self.assets:
                    if self._stop.is_set():
                        break
                    asyncio.ensure_future(self._subscribe_books_for_interval(asset, boundary))

                # collect every asset for the interval that just closed
                await self._collect_all(boundary)
                await self._sleep_until_next(now)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log.error("[COLLECTOR] fatal", error=str(e))
            self.stats.last_error = str(e)
            self.stats.running = False

    async def _collect_all(self, boundary: int) -> None:
        """Collect the interval that started at `boundary` for all assets.
        Also flushes any order-book snapshots recorded for that interval."""
        target_epoch = boundary  # the interval [boundary, boundary+step) just ended
        for asset in self.assets:
            if self._stop.is_set():
                return
            # flush order-book snapshots collected during this interval
            self._flush_book_recordings(asset, target_epoch)
            await self._collect_one(asset, target_epoch)

    async def _collect_one(self, asset: str, epoch: int) -> None:
        """Fetch a single interval, retrying a few times on failure.

        If all in-line retries fail, the interval is added to the pending queue
        for later re-checking (with exponential backoff). This handles the case
        where Polymarket indexes a market 1-3 minutes after it closes.
        """
        # skip if already collected this session (idempotent)
        if self._seen.get(asset, 0) >= epoch:
            return

        import aiohttp
        max_attempts = 3
        last_err = ""
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            for attempt in range(1, max_attempts + 1):
                if self._stop.is_set():
                    return
                try:
                    rec = await self.fetcher._load_or_fetch_interval(
                        session, asset, epoch, self.interval_minutes, fidelity=1,
                    )
                    if rec is not None:
                        meta, token_hist, _cache_hit = rec
                        self._on_success(asset, epoch, meta, token_hist)
                        return
                    # interval not resolved yet → retry after delay
                    last_err = "unresolved"
                except Exception as e:
                    last_err = str(e)
                    self.stats.last_error = f"{asset}@{epoch}: {e}"
                    self.log.debug(f"[COLLECTOR] attempt {attempt} failed for {asset}@{epoch}: {e}")
                if attempt < max_attempts:
                    await asyncio.sleep(self.retry_secs)

        # ── all in-line retries failed → enqueue for later ──
        self._enqueue_pending(asset, epoch, last_err)

    def _on_success(self, asset: str, epoch: int, meta, token_hist) -> None:
        """Called when an interval is successfully fetched."""
        self.stats.intervals_collected += 1
        self.stats.last_interval_ts = epoch
        self.stats.last_fetch_at = time.time()
        self._seen[asset] = epoch
        win = meta.winner or "?"
        self.log.info(
            f"[COLLECTOR] {asset} interval {epoch} "
            f"({datetime.fromtimestamp(epoch, tz=timezone.utc).strftime('%H:%M')}Z) "
            f"winner={win} tok_pts={len(token_hist) if token_hist else 0}",
        )
        # opportunistically refresh oracle cache for this window
        # (skip if no event loop is running, e.g. in tests)
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self._refresh_oracle(asset, epoch))
        except RuntimeError:
            pass

    # ── pending (retry) queue ────────────────────────────────────────────

    def _enqueue_pending(self, asset: str, epoch: int, error: str) -> None:
        """Add a failed interval to the retry queue."""
        # don't duplicate
        for p in self._pending:
            if p.asset == asset and p.epoch == epoch:
                return
        if len(self._pending) >= MAX_PENDING_QUEUE:
            # drop oldest to make room
            self._pending.pop(0)
        p = PendingInterval(asset=asset, epoch=epoch, attempts=1, last_error=error)
        p.schedule_retry()
        self._pending.append(p)
        self.stats.intervals_pending = len(self._pending)
        retry_in = int(p.next_retry_ts - time.time())
        self.log.warning(
            f"[COLLECTOR] {asset}@{epoch} not resolved yet — "
            f"enqueued for retry in {retry_in}s "
            f"(queue: {len(self._pending)}, error: {error})"
        )

    async def _process_pending(self, until_ts: Optional[float] = None) -> None:
        """Re-check pending (failed) intervals whose retry timer has elapsed.

        Called between fresh-collection cycles. Stops when `until_ts` is
        reached (so it never delays a fresh collection) or the queue is empty.
        """
        if not self._pending:
            return

        import aiohttp
        due = [p for p in self._pending if p.is_due]
        if not due:
            # log the earliest pending for visibility
            next_p = min(self._pending, key=lambda p: p.next_retry_ts)
            wait = int(next_p.next_retry_ts - time.time())
            if wait > 0:
                self.log.debug(f"[COLLECTOR] {len(self._pending)} pending, "
                               f"next retry in {wait}s ({next_p.asset}@{next_p.epoch})")
            return

        self.log.info(f"[COLLECTOR] retrying {len(due)} pending interval(s)...")
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            for p in due:
                if self._stop.is_set():
                    return
                # stop if we'd delay the next fresh collection
                if until_ts and time.time() > until_ts:
                    break
                success = False
                try:
                    rec = await self.fetcher._load_or_fetch_interval(
                        session, p.asset, p.epoch, self.interval_minutes, fidelity=1,
                    )
                    if rec is not None:
                        meta, token_hist, _ = rec
                        self._on_success(p.asset, p.epoch, meta, token_hist)
                        self.stats.intervals_recovered += 1
                        success = True
                        self.log.info(
                            f"[COLLECTOR] ✓ RECOVERED {p.asset}@{p.epoch} "
                            f"on attempt {p.attempts + 1}"
                        )
                except Exception as e:
                    p.last_error = str(e)

                if success:
                    self._pending.remove(p)
                else:
                    p.attempts += 1
                    if p.is_exhausted:
                        self._pending.remove(p)
                        self.stats.intervals_failed += 1
                        self.stats.last_error = (
                            f"{p.asset}@{p.epoch}: exhausted {MAX_PENDING_RETRIES} retries "
                            f"({p.last_error})"
                        )
                        self.log.error(
                            f"[COLLECTOR] ✗ GIVING UP {p.asset}@{p.epoch} "
                            f"after {p.attempts} attempts ({p.last_error})"
                        )
                    else:
                        p.schedule_retry()
                        self.log.debug(
                            f"[COLLECTOR] {p.asset}@{p.epoch} still unresolved, "
                            f"retry #{p.attempts} scheduled "
                            f"(+{int(p.next_retry_ts - time.time())}s)"
                        )
                # small delay between pending checks
                if not success and not self._stop.is_set():
                    await asyncio.sleep(1.0)

        self.stats.intervals_pending = len(self._pending)

    async def _subscribe_books_for_interval(self, asset: str, epoch: int) -> None:
        """Subscribe to the market channel for the CURRENT interval's tokens.

        Called at the start of each interval (not after close) so the WS
        streams live book updates throughout the interval's life. Token IDs
        are fetched from Gamma (the market is created slightly before open).
        """
        import aiohttp
        slug = f"{asset.lower()}-updown-{self.interval_minutes}m-{epoch}"
        token_ids = set()
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(f"{self.s.gamma_api}/events/slug/{slug}") as r:
                    if r.status == 200:
                        data = await r.json()
                        markets = (data or {}).get("markets") or []
                        if markets:
                            import json as _json
                            ids = _json.loads(markets[0].get("clobTokenIds", "[]"))
                            token_ids = {t for t in ids if t}
        except Exception:
            pass

        if token_ids:
            self._active_book_tokens[asset] = token_ids
            await self._book_ws.subscribe_market_tokens(token_ids)
            self._book_recorder.watch(token_ids)
            self.log.debug(f"[COLLECTOR] {asset}@{epoch}: subscribed to "
                           f"{len(token_ids)} tokens for book recording")

    def _flush_book_recordings(self, asset: str, epoch: int) -> None:
        """Save collected order-book snapshots for the just-closed interval."""
        token_ids = self._active_book_tokens.pop(asset, None)
        if not token_ids:
            return
        results = self._book_recorder.flush(asset, epoch, self.interval_minutes)
        if results:
            total = sum(results.values())
            self.log.info(f"[COLLECTOR] {asset}@{epoch}: saved {total} book snapshots "
                          f"({len(results)} tokens)")
        self._book_recorder.unwatch(token_ids)

    async def _refresh_oracle(self, asset: str, epoch: int) -> None:
        """Ensure the Binance oracle kline cache covers this interval."""
        try:
            start = epoch
            end = epoch + self.interval_minutes * 60
            before = self._count_oracle_ticks(asset, start, end)
            ticks = await self.hist.fetch_klines(asset, start, end, "1m")
            after = self._count_oracle_ticks(asset, start, end)
            added = max(0, after - before)
            if added:
                self.stats.oracle_ticks_written += added
        except Exception:
            pass  # oracle is best-effort; token data is the priority

    def _count_oracle_ticks(self, asset: str, start: int, end: int) -> int:
        """Count cached oracle ticks for a range without re-fetching."""
        from pathlib import Path
        p = Path(self.s.backtest_data_dir) / f"{asset}_{start}_{end}_1m.csv"
        if not p.exists():
            return 0
        try:
            with open(p) as f:
                return sum(1 for _ in f) - 1  # minus header
        except OSError:
            return 0

    async def _sleep_until_next(self, now: float) -> None:
        """Sleep until shortly after the next boundary's grace period.

        Wakes every 30s to process pending retries between fresh collections.
        """
        next_boundary = self._last_boundary(int(now)) + self.interval_minutes * 60
        wake = next_boundary + self.grace_secs
        self.stats.next_collection_ts = int(wake)
        while time.time() < wake:
            if self._stop.is_set():
                return
            # process pending every 30s during the wait
            chunk = min(30, max(1, wake - time.time()))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=chunk)
                return  # stop signaled
            except asyncio.TimeoutError:
                # opportunistically retry pending intervals while waiting
                if self._pending:
                    await self._process_pending(until_ts=wake)

    # ── helpers ──────────────────────────────────────────────────────────

    def _last_boundary(self, ts: int) -> int:
        step = self.interval_minutes * 60
        return ts - (ts % step)

    def backfill(self, asset: str, start_ts: int, end_ts: int) -> "asyncio.Future":
        """Synchronously backfill a range of past intervals (returns a task)."""
        async def _do():
            self.log.info(f"[COLLECTOR] backfill {asset} {start_ts}->{end_ts}")
            ds = await self.fetcher.build_dataset(
                asset, start_ts, end_ts, self.interval_minutes, fidelity=1,
            )
            st = ds.stats()
            self.stats.intervals_collected += st["intervals_resolved"]
            return st
        return asyncio.create_task(_do())
