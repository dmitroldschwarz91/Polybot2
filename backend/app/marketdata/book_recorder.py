"""
Order-book DEPTH recorder — separate high-volume log for maker/микроструктура research.

Why a separate file (not the interval-sample TSV):
  * The interval samples log only best-level (best_ask/best_bid + size AT best).
    Maker / spread-capture research needs the FULL ladder (N levels per side),
    which is much larger — so it lives in its own file `book_depth_samples.jsonl`
    and can be rotated / archived / deleted independently of the strategy logs.
  * It is event-driven: it hooks LivePriceStore.add_book_listener and captures a
    snapshot whenever a full book update arrives, so it needs no polling loop.

Volume control (this is a firehose by nature):
  * Only records tokens explicitly registered via `watch()` — the engine registers
    only the CURRENT interval's up/down tokens, so we don't log expired markets.
  * Per-token throttle (`min_interval_secs`) collapses burst updates.
  * `max_levels` caps how deep the ladder is stored per side.
  * Optional `only_near_close_secs`: only record when the market is within N seconds
    of close (the window where maker fills actually matter), gated by a cheap
    `seconds_to_close` callback the engine supplies.

Format: JSONL (one compact JSON object per snapshot). Each row:
  {
    "ts": <float epoch>, "asset": "BTC", "slug": "...", "token_id": "...",
    "side_of": "UP"|"DOWN"|"?", "stc": <int|null>,
    "best_bid": .., "best_ask": .., "spread": ..,
    "bids": [[price,size], ...up to max_levels],
    "asks": [[price,size], ...up to max_levels]
  }
JSONL (not TSV) because the ladder is a variable-length nested array.
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple


class BookDepthRecorder:
    """Event-driven full-depth order-book recorder → own JSONL file."""

    def __init__(
        self,
        path,
        *,
        max_levels: int = 10,
        min_interval_secs: float = 1.0,
        only_near_close_secs: Optional[float] = None,
        flush_interval: float = 2.0,
        max_batch: int = 200,
        enabled: bool = True,
    ) -> None:
        self.path = Path(path)
        self.max_levels = int(max_levels)
        self.min_interval_secs = float(min_interval_secs)
        self.only_near_close_secs = only_near_close_secs
        self.flush_interval = float(flush_interval)
        self.max_batch = int(max_batch)
        self.enabled = bool(enabled)

        # token_id -> metadata for enrichment {asset, slug, side_of}
        self._watched: Dict[str, Dict[str, str]] = {}
        # token_id -> callable() -> Optional[float] seconds_to_close
        self._stc_fns: Dict[str, Callable[[], Optional[float]]] = {}
        self._last_ts: Dict[str, float] = {}

        self.queue: "asyncio.Queue[dict]" = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
        self._stopped = False
        self._written = 0

    # ── lifecycle ────────────────────────────────────────────────────────
    def start(self) -> None:
        if not self.enabled:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self._task is None:
            self._task = asyncio.create_task(self._flush_loop(), name="book-depth-recorder")

    async def stop(self) -> None:
        self._stopped = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        rem: List[dict] = []
        while not self.queue.empty():
            try:
                rem.append(self.queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if rem:
            self._sync_write(rem)

    # ── registration ─────────────────────────────────────────────────────
    def watch(
        self,
        token_id: str,
        *,
        asset: str = "?",
        slug: str = "",
        side_of: str = "?",
        stc_fn: Optional[Callable[[], Optional[float]]] = None,
    ) -> None:
        """Start recording snapshots for `token_id` (current-interval tokens only)."""
        if not token_id:
            return
        self._watched[token_id] = {"asset": asset, "slug": slug, "side_of": side_of}
        if stc_fn is not None:
            self._stc_fns[token_id] = stc_fn

    def unwatch(self, token_ids) -> None:
        for tid in token_ids:
            self._watched.pop(tid, None)
            self._stc_fns.pop(tid, None)
            self._last_ts.pop(tid, None)

    # ── the book listener hook ───────────────────────────────────────────
    def on_book(self, token_id: str, book) -> None:
        """LivePriceStore book-listener callback. Cheap & non-blocking."""
        if not self.enabled or self._stopped:
            return
        meta = self._watched.get(token_id)
        if meta is None:
            return
        now = time.time()
        if now - self._last_ts.get(token_id, 0.0) < self.min_interval_secs:
            return

        stc: Optional[float] = None
        fn = self._stc_fns.get(token_id)
        if fn is not None:
            try:
                stc = fn()
            except Exception:
                stc = None
        if self.only_near_close_secs is not None and stc is not None and stc > self.only_near_close_secs:
            return

        bids = self._ladder(getattr(book, "bids", None))
        asks = self._ladder(getattr(book, "asks", None))
        if not bids and not asks:
            return

        self._last_ts[token_id] = now
        rec = {
            "ts": round(now, 3),
            "asset": meta.get("asset", "?"),
            "slug": meta.get("slug", ""),
            "token_id": token_id,
            "side_of": meta.get("side_of", "?"),
            "stc": int(stc) if stc is not None else None,
            "best_bid": getattr(book, "best_bid", None),
            "best_ask": getattr(book, "best_ask", None),
            "spread": getattr(book, "spread", None),
            "bids": bids,
            "asks": asks,
        }
        try:
            self.queue.put_nowait(rec)
        except asyncio.QueueFull:
            pass

    def _ladder(self, levels) -> List[Tuple[float, float]]:
        out: List[Tuple[float, float]] = []
        if not levels:
            return out
        for lvl in levels[: self.max_levels]:
            try:
                p = float(lvl.get("price", 0))
                sz = float(lvl.get("size", 0))
            except (AttributeError, TypeError, ValueError):
                continue
            if p > 0:
                out.append((round(p, 4), round(sz, 2)))
        return out

    # ── async batched writer ─────────────────────────────────────────────
    async def _flush_loop(self) -> None:
        batch: List[dict] = []
        while not self._stopped:
            try:
                while len(batch) < self.max_batch:
                    rec = await asyncio.wait_for(self.queue.get(), timeout=self.flush_interval)
                    batch.append(rec)
            except asyncio.TimeoutError:
                pass
            if batch:
                await self._write_batch(batch)
                batch.clear()

    async def _write_batch(self, batch: List[dict]) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sync_write, batch)

    def _sync_write(self, batch: List[dict]) -> None:
        try:
            with self.path.open("a", encoding="utf-8", buffering=65536) as f:
                for rec in batch:
                    f.write(json.dumps(rec, separators=(",", ":")) + "\n")
            self._written += len(batch)
        except OSError:
            pass

    @property
    def written(self) -> int:
        return self._written
