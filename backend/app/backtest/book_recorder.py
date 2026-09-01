"""
Live order-book recorder — captures top-of-book snapshots via WebSocket.

Records best_bid/best_ask + volumes for each token over the life of a market
window, then flushes to a compact JSONL file. This data is ONLY obtainable
in real time (order books are not historically available in bulk), so the
recorder must run continuously while markets are live.

Format: {asset}_{interval}m_{epoch}_book.jsonl
Each line: {"ts":..,"bb":..,"ba":..,"bv":..,"av":..,"spread":..}

Throttled to 1 snapshot/second/token to keep files small (~300 lines/5min)
while still capturing every meaningful price level change.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

from ..core.logging import StructuredLogger, build_logger
from ..marketdata.stores import LivePriceStore, OrderBook


# max snapshots per second per token (throttle)
THROTTLE_SECS = 1.0


class BookSnapshot:
    """One top-of-book observation."""
    __slots__ = ("ts", "best_bid", "best_ask", "bid_volume", "ask_volume", "spread")

    def __init__(self, book: OrderBook) -> None:
        self.ts = int(book.ts)
        self.best_bid = book.best_bid
        self.best_ask = book.best_ask
        self.bid_volume = round(book.bid_volume, 2)
        self.ask_volume = round(book.ask_volume, 2)
        self.spread = round(book.spread, 4) if book.spread is not None else None

    def to_dict(self) -> dict:
        return {
            "ts": self.ts,
            "bb": self.best_bid,
            "ba": self.best_ask,
            "bv": self.bid_volume,
            "av": self.ask_volume,
            "spread": self.spread,
        }


class OrderBookRecorder:
    """Records live order-book snapshots for a set of tokens.

    Lifecycle:
      1. start() — registers a listener on LivePriceStore
      2. watch(token_ids) — begins recording snapshots for these tokens
      3. flush(asset, epoch) — saves collected snapshots to JSONL, clears buffer
      4. stop() — removes listener
    """

    def __init__(self, prices: LivePriceStore, cache_dir: Path,
                 log: Optional[StructuredLogger] = None) -> None:
        self.prices = prices
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.log = log or build_logger("book-recorder")

        # token_id -> list of BookSnapshot
        self._buffer: Dict[str, List[BookSnapshot]] = {}
        # token_id -> last snapshot ts (for throttle)
        self._last_ts: Dict[str, int] = {}
        # which tokens we're actively recording
        self._watching: Set[str] = set()
        self._active = False

    def start(self) -> None:
        """Register the book-update listener."""
        if self._active:
            return
        self.prices.add_book_listener(self._on_book_update)
        self._active = True
        self.log.info("[BOOK-REC] started, listening for book updates")

    def stop(self) -> None:
        """Stop recording (listener stays registered but inert)."""
        self._watching.clear()
        self._active = False
        self.log.info("[BOOK-REC] stopped")

    def watch(self, token_ids: Set[str]) -> None:
        """Begin recording snapshots for these token IDs."""
        self._watching |= token_ids
        for tid in token_ids:
            self._buffer.setdefault(tid, [])
        self.log.debug(f"[BOOK-REC] watching {len(token_ids)} tokens "
                       f"(total: {len(self._watching)})")

    def unwatch(self, token_ids: Set[str]) -> None:
        """Stop recording for these tokens (buffer preserved for flush)."""
        self._watching -= token_ids

    def _on_book_update(self, token_id: str, book: OrderBook) -> None:
        """Called by LivePriceStore on every book update. Throttled."""
        if not self._active or token_id not in self._watching:
            return
        now_ts = int(book.ts)
        # throttle: skip if we recorded this token < THROTTLE_SECS ago
        last = self._last_ts.get(token_id, 0)
        if now_ts - last < THROTTLE_SECS:
            return
        # only record if we have at least a best_ask (meaningful book)
        if book.best_ask is None and book.best_bid is None:
            return
        snap = BookSnapshot(book)
        self._buffer.setdefault(token_id, []).append(snap)
        self._last_ts[token_id] = now_ts

    def flush(self, asset: str, epoch: int, interval_minutes: int) -> Dict[str, int]:
        """Save all buffered snapshots to JSONL files. Returns {token: count}.

        Each token gets its own file: {asset}_{interval}m_{epoch}_book_{side}.jsonl
        We don't know which token is UP/DOWN here, so we save by token_id suffix.
        """
        results = {}
        for token_id, snaps in self._buffer.items():
            if not snaps:
                continue
            # short token id for filename (last 8 chars)
            short = token_id[-8:]
            fname = f"{asset}_{interval_minutes}m_{epoch}_book_{short}.jsonl"
            fpath = self.cache_dir / fname
            with open(fpath, "w") as f:
                for s in snaps:
                    f.write(json.dumps(s.to_dict()) + "\n")
            results[token_id] = len(snaps)
            self.log.info(f"[BOOK-REC] flushed {len(snaps)} snapshots → {fname}")
            # clear buffer for this token
            self._buffer[token_id] = []
        return results

    def stats(self) -> dict:
        """Quick status for logging."""
        total = sum(len(s) for s in self._buffer.values())
        return {
            "watching": len(self._watching),
            "buffered_snapshots": total,
            "tokens_with_data": len([1 for s in self._buffer.values() if s]),
        }
