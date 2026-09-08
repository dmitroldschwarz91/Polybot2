"""
Market-data stores — price feeds and order-fill tracking.

LivePriceStore: aggregates Chainlink / Binance-direct / Binance-RTDS feeds,
order books and token (lot) prices.
FillStore: tracks order + trade WS events to determine fill state.

Both are plain data containers; the WebSocket managers write into them, the
engine/strategies read from them. Thread-safety note: they're only mutated
from the asyncio event loop, so no locks are needed within the loop.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

# Downsample interval for the range5 filter's price history. Binance aggTrade
# fires dozens of times/sec, so the maxlen=600 binance_direct deque covers only
# ~30s — too short for a 5-min range. We keep one point every RANGE_SAMPLE_SECS
# in a separate deque (range_history) -> maxlen=120 covers ~10 min.
RANGE_SAMPLE_SECS = 5.0


@dataclass
class OrderBook:
    bids: List[dict] = field(default_factory=list)
    asks: List[dict] = field(default_factory=list)
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    bid_volume: float = 0.0
    ask_volume: float = 0.0
    best_bid_size: float = 0.0   # size AT the best bid level (top-of-book depth)
    best_ask_size: float = 0.0   # size AT the best ask level (top-of-book depth)
    spread: Optional[float] = None
    ts: float = 0.0
    stale: bool = False


class LivePriceStore:
    """Multi-source price + order-book store with cached volatility."""

    def __init__(self, assets: List[str], book_stale_secs: float = 30.0,
                 vol_cache_ttl: float = 0.5) -> None:
        self.assets = list(assets)
        self.book_stale_secs = book_stale_secs

        self.chainlink: Dict[str, float] = {}
        self.chainlink_ts: Dict[str, float] = {}
        self.chainlink_history: Dict[str, Deque] = {a: deque(maxlen=600) for a in assets}

        # Official Chainlink TWAP stream (RTDS crypto_prices_twap_thirty) — the
        # AUTHORITATIVE resolution feed (per Chainlink docs: do not recompute).
        # History holds (obs_ts_sec, value), throttled ~1/sec (a 30-sec average
        # needs no denser sampling for boundary lookups).
        self.chainlink_twap: Dict[str, float] = {}
        self.chainlink_twap_ts: Dict[str, float] = {}
        self.chainlink_twap_history: Dict[str, Deque] = {a: deque(maxlen=600) for a in assets}

        self.binance: Dict[str, float] = {}
        self.binance_ts: Dict[str, float] = {}
        self.binance_history: Dict[str, Deque] = {a: deque(maxlen=600) for a in assets}

        self.binance_direct: Dict[str, float] = {}
        self.binance_direct_ts: Dict[str, float] = {}
        self.binance_direct_history: Dict[str, Deque] = {a: deque(maxlen=600) for a in assets}

        # Downsampled price history for the range5 filter: one point every
        # RANGE_SAMPLE_SECS -> maxlen=120 covers ~10 min (aggTrade deque is too
        # short at maxlen=600). Populated in update_binance_direct.
        self.range_history: Dict[str, Deque] = {a: deque(maxlen=120) for a in assets}

        # VWAP accumulators (BTC oracle): sum(price*qty) and sum(qty), reset at
        # the start of each 5-min interval. Populated from Binance aggTrade volume.
        self.vwap_num: Dict[str, float] = {a: 0.0 for a in assets}
        self.vwap_den: Dict[str, float] = {a: 0.0 for a in assets}

        self.books: Dict[str, OrderBook] = {}
        self.lot_prices: Dict[str, float] = {}
        self.lot_prices_ts: Dict[str, float] = {}
        # ── book update listeners (for live order-book recording) ──
        self._book_listeners: List = []

        self._volatility_cache: Dict[str, Tuple[float, float]] = {}
        self._vol_cache_ttl = vol_cache_ttl

    def set_assets(self, assets: List[str]) -> None:
        """Dynamically update active tracked assets."""
        self.assets = list(assets)
        for a in assets:
            if a not in self.chainlink_history:
                self.chainlink_history[a] = deque(maxlen=600)
            if a not in self.chainlink_twap_history:
                self.chainlink_twap_history[a] = deque(maxlen=600)
            if a not in self.binance_history:
                self.binance_history[a] = deque(maxlen=600)
            if a not in self.binance_direct_history:
                self.binance_direct_history[a] = deque(maxlen=600)
            if a not in self.range_history:
                self.range_history[a] = deque(maxlen=120)
            if a not in self.vwap_num:
                self.vwap_num[a] = 0.0
                self.vwap_den[a] = 0.0

    # ── Updaters ─────────────────────────────────────────────────────────

    def update_chainlink(self, asset: str, price: float, oracle_ts_ms: Optional[int] = None) -> None:
        if asset not in self.chainlink_history:
            return
        now = time.time()
        self.chainlink[asset] = price
        self.chainlink_ts[asset] = now
        ots = oracle_ts_ms if oracle_ts_ms is not None else int(now * 1000)
        self.chainlink_history[asset].append((ots, now, price))
        self._volatility_cache.pop(asset, None)

    def update_chainlink_twap(self, asset: str, value: float,
                              oracle_ts_ms: Optional[int] = None) -> None:
        """Store an official Chainlink TWAP update (30-sec lookback).

        History is throttled to ~1 point/sec by observation timestamp (the TWAP
        is already a 30-sec average, so denser sampling adds nothing for the
        boundary lookups resolution needs).
        """
        if asset not in self.chainlink_twap_history:
            return
        now = time.time()
        self.chainlink_twap[asset] = value
        self.chainlink_twap_ts[asset] = now
        ts_sec = (oracle_ts_ms / 1000.0) if oracle_ts_ms else now
        hist = self.chainlink_twap_history[asset]
        if not hist or (ts_sec - hist[-1][0]) >= 1.0:
            hist.append((ts_sec, value))

    def get_chainlink_twap(self, asset: str, max_age: float = 120.0) -> Optional[float]:
        """Latest official TWAP value (None if older than max_age)."""
        if asset in self.chainlink_twap:
            if time.time() - self.chainlink_twap_ts.get(asset, 0) <= max_age:
                return self.chainlink_twap[asset]
        return None

    def get_twap_at(self, asset: str, t_sec: float, tolerance: float = 10.0) -> Optional[float]:
        """Official TWAP value whose observation time is nearest to t_sec.

        Used to read the boundary TWAP at an interval's open/close timestamps
        (open = TWAP at start_ts, close = TWAP at end_ts — the same boundary
        value for consecutive intervals, hence open(N) == close(N-1)).
        Returns None if no sample is within `tolerance` seconds of t_sec.
        """
        hist = self.chainlink_twap_history.get(asset)
        if not hist:
            return None
        best_val, best_d = None, None
        for ts, val in hist:
            d = abs(ts - t_sec)
            if best_d is None or d < best_d:
                best_d, best_val = d, val
        if best_val is not None and best_d is not None and best_d <= tolerance:
            return best_val
        return None

    def update_binance(self, asset: str, price: float) -> None:
        if asset not in self.binance_history:
            return
        now = time.time()
        self.binance[asset] = price
        self.binance_ts[asset] = now
        self.binance_history[asset].append((now, price))
        self._volatility_cache.pop(asset, None)

    def update_binance_direct(self, asset: str, price: float, qty: Optional[float] = None) -> None:
        if asset not in self.binance_direct_history:
            return
        now = time.time()
        self.binance_direct[asset] = price
        self.binance_direct_ts[asset] = now
        self.binance_direct_history[asset].append((now, price))
        self._volatility_cache.pop(asset, None)
        # downsampled point for range5 (the maxlen=600 aggTrade deque is too
        # short for a 5-min window; keep one sample every RANGE_SAMPLE_SECS)
        rh = self.range_history.get(asset)
        if rh is not None and (not rh or (now - rh[-1][0]) >= RANGE_SAMPLE_SECS):
            rh.append((now, price))
        # VWAP accumulation (BTC, from Binance aggTrade volume)
        if qty is not None and qty > 0:
            self.vwap_num[asset] = self.vwap_num.get(asset, 0.0) + price * qty
            self.vwap_den[asset] = self.vwap_den.get(asset, 0.0) + qty

    def get_vwap(self, asset: str) -> Optional[float]:
        """Current interval VWAP (since last reset_vwap). None if no volume yet."""
        den = self.vwap_den.get(asset, 0)
        if den <= 0:
            return None
        return self.vwap_num.get(asset, 0) / den

    def reset_vwap(self, asset: str) -> None:
        """Reset VWAP accumulators at the start of a new interval."""
        self.vwap_num[asset] = 0.0
        self.vwap_den[asset] = 0.0

    def update_lot_price(self, token_id: str, best_ask: Optional[float],
                         best_bid: Optional[float] = None) -> None:
        now = time.time()
        if best_ask is not None and best_ask > 0:
            self.lot_prices[token_id] = best_ask
            self.lot_prices_ts[token_id] = now
        book = self.books.setdefault(token_id, OrderBook())
        if best_ask is not None:
            book.best_ask = best_ask
        if best_bid is not None:
            book.best_bid = best_bid
        if book.best_ask and book.best_bid:
            book.spread = book.best_ask - book.best_bid
        book.ts = now
        self._notify_book_listeners(token_id, book)

    def update_full_book(self, token_id: str, bids: List[dict], asks: List[dict]) -> None:
        now = time.time()

        def _num(d: dict, key: str = "price") -> float:
            try:
                return float(d.get(key, 0))
            except (TypeError, ValueError):
                return 0.0

        # sort so asks[0]/bids[0] are the TRUE best levels regardless of the
        # order Polymarket sends (ascending asks, descending bids).
        a_sorted = sorted([a for a in asks if isinstance(a, dict)], key=_num)
        b_sorted = sorted([b for b in bids if isinstance(b, dict)], key=_num, reverse=True)
        ba = _num(a_sorted[0]) if a_sorted else None
        bb = _num(b_sorted[0]) if b_sorted else None
        ask_vol = sum(_num(a, "size") for a in a_sorted)
        bid_vol = sum(_num(b, "size") for b in b_sorted)
        ba_size = _num(a_sorted[0], "size") if a_sorted else 0.0
        bb_size = _num(b_sorted[0], "size") if b_sorted else 0.0
        self.books[token_id] = OrderBook(
            bids=b_sorted, asks=a_sorted, best_bid=bb, best_ask=ba,
            bid_volume=bid_vol, ask_volume=ask_vol,
            best_bid_size=bb_size, best_ask_size=ba_size,
            spread=(ba - bb) if (ba is not None and bb is not None) else None, ts=now,
        )
        if ba and ba > 0:
            self.lot_prices[token_id] = ba
            self.lot_prices_ts[token_id] = now
        self._notify_book_listeners(token_id, self.books[token_id])

    def add_book_listener(self, callback) -> None:
        """Register a callback(token_id, book) invoked on every book update.
        Used by the order-book recorder to capture live snapshots."""
        self._book_listeners.append(callback)

    def _notify_book_listeners(self, token_id: str, book: OrderBook) -> None:
        if not self._book_listeners:
            return
        for cb in self._book_listeners:
            try:
                cb(token_id, book)
            except Exception:
                pass  # never let a listener break the price feed

    def cleanup_old_tokens(self, active_token_ids: Set[str]) -> int:
        """Remove books/lot_prices for tokens that are no longer active."""
        if not active_token_ids:
            return 0
        removed = 0
        for tid in list(self.books.keys()):
            if tid not in active_token_ids:
                del self.books[tid]
                removed += 1
        for tid in list(self.lot_prices.keys()):
            if tid not in active_token_ids:
                del self.lot_prices[tid]
        for tid in list(self.lot_prices_ts.keys()):
            if tid not in active_token_ids:
                del self.lot_prices_ts[tid]
        return removed

        # ── Readers ──────────────────────────────────────────────────────────

    def get_oracle_price(self, asset: str) -> Optional[float]:
        """Returns the freshest oracle price (max 120s old, else None)."""
        now = time.time()
        max_age = 120  # 2 minutes

        # Chainlink (highest priority)
        if asset in self.chainlink:
            age = now - self.chainlink_ts.get(asset, 0)
            if age <= max_age:
                return self.chainlink[asset]

        # Binance direct (second priority)
        if asset in self.binance_direct:
            age = now - self.binance_direct_ts.get(asset, 0)
            if age <= max_age:
                return self.binance_direct[asset]

        # Binance RTDS (last resort)
        if asset in self.binance:
            age = now - self.binance_ts.get(asset, 0)
            if age <= max_age:
                return self.binance[asset]

        # All sources stale — return None instead of old data
        return None

    def get_fastest_price(self, asset: str) -> Tuple[Optional[float], str]:
        now = time.time()
        sources = [
            (self.binance_direct.get(asset), self.binance_direct_ts.get(asset, 0), "binance_direct"),
            (self.chainlink.get(asset), self.chainlink_ts.get(asset, 0), "chainlink"),
            (self.binance.get(asset), self.binance_ts.get(asset, 0), "binance_rtds"),
        ]
        valid = [(p, ts, src) for p, ts, src in sources if p is not None and ts > 0]
        if not valid:
            return None, "none"
        best = max(valid, key=lambda x: x[1])
        return best[0], best[2]

    def get_lot_price(self, token_id: str) -> Optional[float]:
        return self.lot_prices.get(token_id)

    def get_book(self, token_id: str) -> Optional[OrderBook]:
        b = self.books.get(token_id)
        if b and b.ts > 0:
            b.stale = (time.time() - b.ts) >= self.book_stale_secs
            return b
        return None

    def get_book_with_max_age(self, token_id: str, max_age: float) -> Optional[OrderBook]:
        """Return book if it exists. If older than max_age, mark as stale.

        Instead of returning None (which blocks the strategy from evaluating
        the token at all), we return the stale book and set b.stale = True.
        The strategy can then fall back to last_trade_price if needed.
        """
        b = self.books.get(token_id)
        if b and b.ts > 0:
            age = time.time() - b.ts
            b.stale = age >= max_age
            return b
        return None

    def get_book_imbalance(self, token_id: str) -> float:
        b = self.get_book(token_id)
        if b is None:
            return 0.5
        bv, av = b.bid_volume, b.ask_volume
        t = bv + av
        return bv / t if t > 0 else 0.5

    def ask_size_at(self, token_id: str, price: float, tol: float = 0.0) -> Optional[float]:
        """Size offered at ask levels matching `price` (within tol).
        tol=0 -> exact-level size. None if no book/levels. Unlike ask_volume
        (whole-book depth), this is the offer AT a specific price."""
        b = self.books.get(token_id)
        if not b or not b.asks:
            return None
        total = 0.0
        for a in b.asks:
            try:
                if abs(float(a.get("price", 0)) - price) <= tol + 1e-9:
                    total += float(a.get("size", 0))
            except (TypeError, ValueError):
                continue
        return total

    def ask_volume_up_to(self, token_id: str, max_price: float) -> Optional[float]:
        """Cumulative ask size at levels with price <= max_price (buyable volume
        if you sweep the book up to that price)."""
        b = self.books.get(token_id)
        if not b or not b.asks:
            return None
        total = 0.0
        for a in b.asks:
            try:
                if float(a.get("price", 0)) <= max_price + 1e-9:
                    total += float(a.get("size", 0))
            except (TypeError, ValueError):
                continue
        return total

    def get_chainlink_age(self, asset: str) -> float:
        ts = self.chainlink_ts.get(asset, 0)
        return time.time() - ts if ts > 0 else float("inf")

    def get_micro_trend_data(self, asset: str, window: float, min_points: int) -> List[Tuple[float, float]]:
        cutoff = time.time() - window
        history = self.binance_direct_history.get(asset)
        if history and len(history) >= min_points:
            data = [(ts, p) for ts, p in history if ts >= cutoff]
            if len(data) >= min_points:
                return data
        return [(ts, p) for ts, p in self.binance_history.get(asset, []) if ts >= cutoff]

    def get_volatility(self, asset: str, window_secs: float) -> Optional[float]:
        now = time.time()
        cached = self._volatility_cache.get(asset)
        if cached and (now - cached[1]) < self._vol_cache_ttl:
            return cached[0]

        cutoff = now - window_secs
        history = self.binance_direct_history.get(asset)
        if not history or len(history) < 2:
            history = self.chainlink_history.get(asset)
        if not history or len(history) < 2:
            history = self.binance_history.get(asset)
        if not history or len(history) < 2:
            return None

        min_p, max_p = float("inf"), float("-inf")
        count = 0
        for item in history:
            if len(item) == 2:
                ts, p = item
            elif len(item) == 3:
                _, ts, p = item
            else:
                continue
            if ts >= cutoff:
                min_p = min(min_p, p)
                max_p = max(max_p, p)
                count += 1
        if count < 2 or min_p <= 0:
            return None
        volatility = (max_p - min_p) / min_p
        self._volatility_cache[asset] = (volatility, now)
        return volatility

    def get_range_ratio(self, asset: str, window_secs: float) -> Optional[float]:
        """Relative price range (max-min)/MEAN over the window.

        Used by the range5 filter (5-min oracle range). Normalises by mean
        (unlike get_volatility which uses min). Reads primarily from the
        downsampled range_history (covers 5+ min); falls back to the dense
        binance_direct deque only if range_history is empty (short coverage,
        but better than nothing). Returns None if not enough points.
        """
        now = time.time()
        cutoff = now - window_secs
        history = self.range_history.get(asset)
        if not history or len(history) < 2:
            # fallback: dense aggTrade deque (covers only ~30s, not 5 min)
            history = self.binance_direct_history.get(asset) or self.chainlink_history.get(asset)
        if not history or len(history) < 2:
            history = self.binance_history.get(asset)
        if not history or len(history) < 2:
            return None
        vals = []
        for item in history:
            if len(item) == 2:
                ts, p = item
            elif len(item) == 3:
                _, ts, p = item
            else:
                continue
            if ts >= cutoff:
                vals.append(p)
        if len(vals) < 2:
            return None
        mean = sum(vals) / len(vals)
        if mean <= 0:
            return None
        return (max(vals) - min(vals)) / mean

    def snapshot(self) -> dict:
        return {
            "oracle_prices": {a: self.get_oracle_price(a) for a in self.assets},
            "chainlink_ages": {a: (lambda v: None if v == float("inf") else round(v, 1))(self.get_chainlink_age(a)) for a in self.assets},
            "sources": {a: self.get_fastest_price(a)[1] for a in self.assets},
            "tracked_books": len(self.books),
        }


class FillStore:
    """Tracks order/trade WS events to compute fill state."""

    def __init__(self) -> None:
        self.orders: Dict[str, Dict[str, Any]] = {}

    def _ensure(self, order_id: str) -> Dict[str, Any]:
        if order_id not in self.orders:
            self.orders[order_id] = {
                "status": None, "side": None, "limit_price": 0.0,
                "original_size": 0.0, "size_matched": 0.0,
                "filled_size": 0.0, "filled_value": 0.0,
                "avg_fill_price": 0.0, "last_ts": 0.0,
            }
        return self.orders[order_id]

    def record_order_event(self, order_id, side, limit_price, original_size, size_matched, status):
        rec = self._ensure(order_id)
        rec["side"] = side or rec["side"]
        if limit_price and limit_price > 0:
            rec["limit_price"] = limit_price
        if original_size and original_size > 0:
            rec["original_size"] = original_size
        if size_matched is not None and size_matched >= 0:
            rec["size_matched"] = size_matched
        rec["status"] = status or rec["status"]
        rec["last_ts"] = time.time()

    def record_trade_event(self, order_id, side, price, size):
        rec = self._ensure(order_id)
        rec["side"] = side or rec["side"]
        rec["filled_size"] += size
        rec["filled_value"] += price * size
        if rec["filled_size"] > 0:
            rec["avg_fill_price"] = rec["filled_value"] / rec["filled_size"]
        rec["last_ts"] = time.time()

    def snapshot(self, order_id: str) -> Optional[Dict[str, Any]]:
        rec = self.orders.get(order_id)
        return dict(rec) if rec else None

    def is_filled(self, order_id: str) -> bool:
        rec = self.orders.get(order_id)
        if not rec:
            return False
        status = (rec.get("status") or "").upper()
        if status in ("FILLED", "MATCHED"):
            return True
        orig = rec.get("original_size", 0)
        matched = max(rec.get("filled_size", 0), rec.get("size_matched", 0))
        return orig > 0 and matched >= orig

    def get_filled_size(self, order_id: str) -> float:
        rec = self.orders.get(order_id)
        if not rec:
            return 0.0
        return max(rec.get("filled_size", 0), rec.get("size_matched", 0))
