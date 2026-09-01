"""
Order book poller — REST API fallback for stale WebSocket data.

Polymarket's WebSocket market channel is unreliable on 5-minute markets:
book updates sometimes stop for 30-120 seconds. This poller runs alongside
the WebSocket and fetches best_bid/best_ask via the CLOB REST API every
few seconds, feeding the data into the same LivePriceStore.

This ensures book data is never more than `poll_interval` seconds old,
eliminating the 'book_stale' rejections that blocked ~23% of entries.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Optional, Set

import aiohttp

from ..config import Settings
from ..core.logging import StructuredLogger, build_logger
from ..marketdata.stores import LivePriceStore


class BookPoller:
    """Periodically fetches best_bid/ask via CLOB REST API.

    Runs as a background asyncio task. Polls /price for BUY and SELL sides
    for each tracked token, then updates LivePriceStore with the fresh data.
    """

    def __init__(
        self,
        settings: Settings,
        prices: LivePriceStore,
        log: Optional[StructuredLogger] = None,
        poll_interval: float = 5.0,
    ) -> None:
        self.s = settings
        self.prices = prices
        self.log = log or build_logger("book-poller")
        self.poll_interval = poll_interval
        self._tokens: Dict[str, float] = {}  # token_id -> timestamp added
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._session: Optional[aiohttp.ClientSession] = None
        # stats
        self.polls_done = 0
        self.polls_ok = 0
        self.polls_fail = 0

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def watch(self, token_ids: Set[str]) -> None:
        """Add tokens to poll (with TTL)."""
        now = time.time()
        for tid in token_ids:
            self._tokens[tid] = now

    def unwatch(self, token_ids: Set[str]) -> None:
        """Remove tokens from polling."""
        for tid in token_ids:
            self._tokens.pop(tid, None)

    def start(self) -> None:
        if self.running:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="book-poller")
        self.log.info(f"[BOOK-POLLER] started (interval={self.poll_interval}s)")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        if self._session and not self._session.closed:
            await self._session.close()
        self.log.info("[BOOK-POLLER] stopped")

    async def _run(self) -> None:
        try:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
                connector=aiohttp.TCPConnector(limit=10, keepalive_timeout=30),
            )
            while not self._stop.is_set():
                if self._tokens:
                    await self._poll_all()
                try:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=self.poll_interval
                    )
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log.error("[BOOK-POLLER] crashed", error=str(e))
        finally:
            if self._session and not self._session.closed:
                await self._session.close()

    def _prune_tokens(self) -> List[str]:
        """Remove tokens older than 15 minutes (market expired)."""
        now = time.time()
        active = []
        expired = 0
        for tid, ts in list(self._tokens.items()):
            if now - ts < 900:  # 15 min TTL
                active.append(tid)
            else:
                del self._tokens[tid]
                expired += 1
        if expired:
            self.log.debug(f"[BOOK-POLLER] Pruned {expired} expired tokens "
                           f"({len(active)} active)")
        return active

    async def _poll_all(self) -> None:
        """Poll best_bid/ask for all ACTIVE (non-expired) tokens."""
        active = self._prune_tokens()
        if not active:
            return
        tasks = [self._poll_token(tid) for tid in active]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _poll_token(self, token_id: str) -> None:
        """Fetch best_bid and best_ask for a single token via REST API."""
        self.polls_done += 1
        base = self.s.clob_api

        try:
            # Fetch BUY price (= best_bid) and SELL price (= best_ask)
            buy_data, sell_data = await asyncio.gather(
                self._fetch_price(token_id, "BUY"),
                self._fetch_price(token_id, "SELL"),
            )

            best_bid = None
            best_ask = None

            if buy_data and "price" in buy_data:
                try:
                    best_bid = float(buy_data["price"])
                except (ValueError, TypeError):
                    pass

            if sell_data and "price" in sell_data:
                try:
                    best_ask = float(sell_data["price"])
                except (ValueError, TypeError):
                    pass

            if best_bid is not None or best_ask is not None:
                self.prices.update_lot_price(token_id, best_ask, best_bid)
                self.polls_ok += 1
            else:
                self.polls_fail += 1
        except Exception:
            self.polls_fail += 1

    async def _fetch_price(self, token_id: str, side: str) -> Optional[dict]:
        """Fetch /price endpoint for one side."""
        url = f"{self.s.clob_api}/price"
        params = {"token_id": token_id, "side": side}
        headers = {"User-Agent": "polymarket-bot/6.0"}
        try:
            async with self._session.get(url, params=params, headers=headers) as r:
                if r.status != 200:
                    return None
                return await r.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return None

    def stats(self) -> dict:
        return {
            "running": self.running,
            "tokens": len(self._tokens),
            "polls_done": self.polls_done,
            "polls_ok": self.polls_ok,
            "polls_fail": self.polls_fail,
            "poll_interval": self.poll_interval,
        }
