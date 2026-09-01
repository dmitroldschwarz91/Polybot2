"""
WebSocket connection managers.

Four independent feeds, each with auto-reconnect, mirroring the original bot:
  * Binance direct aggTrade     → fastest crypto price
  * Polymarket RTDS             → Chainlink oracle + Binance via RTDS
  * Polymarket market channel   → order books / lot prices
  * Polymarket user channel     → order & trade fills

CRITICAL ARCHITECTURE FIX:
The market channel now maintains a TTL (Time-To-Live) for token subscriptions.
5-minute Polymarket markets expire instantly. If we accumulate old token IDs
forever (as in previous versions), the subscription set grows by 2 tokens every
5 minutes. After 10 hours, a single reconnect would try to subscribe to 240
dead tokens in one giant JSON payload, which Polymarket rejects, causing
infinite reconnect loops and 'socket.send() raised exception' spam.
Now, tokens are automatically evicted after their market closes.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Dict, List, Optional, Set, Tuple

import websockets

from ..config import Settings
from ..core.logging import StructuredLogger
from .stores import FillStore, LivePriceStore


# How long to keep a token in the subscription buffer after it was added.
# 5-minute markets die after 5-10 minutes, so 15 minutes is a safe TTL.
SUBSCRIPTION_TTL_SECS = 900

# Dead-stream detection: Polymarket's market WS sometimes silently stops
# sending updates while keeping the TCP connection (and ping/pong) alive.
# `ping_timeout` does NOT catch this, so the recv loop would block forever
# and bid_volume/ask_volume would silently drop to 0. When no message arrives
# for this many seconds, we force a reconnect (which re-subscribes to active
# tokens and restores the order-book feed).
DEAD_STREAM_SECS = 10.0


def _ws_is_open(ws) -> bool:
    """Check if a WebSocket connection is still usable before sending."""
    if ws is None:
        return False
    state = getattr(ws, "state", None)
    if state is not None:
        return int(state) == 1
    transport = getattr(ws, "transport", None)
    if transport is not None and getattr(transport, "is_closing", lambda: True)():
        return False
    return True


class WebSocketManager:
    """Owns and supervises all four WS feeds as background tasks."""

    def __init__(self, settings: Settings, prices: LivePriceStore, fills: FillStore,
                 log: StructuredLogger) -> None:
        self.s = settings
        self.prices = prices
        self.fills = fills
        self.log = log
        self.api_creds: Optional[dict] = None
        self._market_ws = None
        self._market_ws_lock: Optional[asyncio.Lock] = None
        self._tasks: List[asyncio.Task] = []
        # ── TTL-based subscription buffer ──
        # Maps token_id -> timestamp when it was added.
        # Old tokens are purged automatically to prevent unbounded growth.
        self._subscribed_tokens: Dict[str, float] = {}
        # count of reconnects triggered by dead-stream detection (monitoring)
        self.dead_stream_reconnects = 0

    def _prune_subscriptions(self) -> Set[str]:
        """Remove expired tokens and return the set of active ones."""
        now = time.time()
        active = set()
        expired = 0
        for tid, ts in list(self._subscribed_tokens.items()):
            if now - ts < SUBSCRIPTION_TTL_SECS:
                active.add(tid)
            else:
                del self._subscribed_tokens[tid]
                expired += 1
        if expired > 0:
            self.log.debug(f"[WS-MARKET] Pruned {expired} expired tokens "
                           f"({len(active)} active)")
        return active

    def _get_active_tokens(self) -> Set[str]:
        """Return currently valid subscribed tokens (prunes expired first)."""
        return self._prune_subscriptions()

    # ── lifecycle ────────────────────────────────────────────────────────

    def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._run_binance_direct(), name="ws-binance-direct"),
            asyncio.create_task(self._run_rtds(), name="ws-rtds"),
            asyncio.create_task(self._run_market(), name="ws-market"),
            asyncio.create_task(self._run_user(), name="ws-user"),
        ]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()

    # ── Binance direct ───────────────────────────────────────────────────

    async def _run_binance_direct(self) -> None:
        streams = "/".join(self.s.binance_streams)
        url = f"{self.s.binance_ws_direct}/{streams}"
        symbol_map = {"BTCUSDT": "BTC", "ETHUSDT": "ETH"}
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    self.log.info("[WS-BINANCE-DIRECT] Connected")
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                            asset = symbol_map.get(msg.get("s", "").upper())
                            price = float(msg.get("p", 0))
                            qty = float(msg.get("q", 0))
                            if asset and price:
                                self.prices.update_binance_direct(asset, price, qty)
                        except Exception:
                            continue
            except Exception as e:
                self.log.warning("[WS-BINANCE-DIRECT] Reconnecting", error=str(e))
                await asyncio.sleep(1)

    # ── Polymarket RTDS (Chainlink + Binance) ────────────────────────────

    async def _run_rtds(self) -> None:
        bn_symbols = list(self.s.binance_symbols_ws.values())
        subs = [
            {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""},
            {"topic": "crypto_prices", "type": "update", "filters": json.dumps(bn_symbols)},
        ]
        # Official Chainlink TWAP (authoritative resolution feed). filters=""
        # subscribes to every symbol; we filter by payload.symbol below.
        if self.s.chainlink_twap_enabled:
            subs.append({"topic": "crypto_prices_twap_thirty", "type": "*", "filters": ""})
        sub = json.dumps({"action": "subscribe", "subscriptions": subs})
        while True:
            try:
                async with websockets.connect(
                    self.s.ws_rtds_url, ping_interval=self.s.ws_heartbeat_interval, ping_timeout=10
                ) as ws:
                    await ws.send(sub)
                    self.log.info("[WS-RTDS] Connected")
                    # Dead-stream detection (same pattern as the market WS):
                    # RTDS can go silent while keeping TCP+pings alive, stalling
                    # the Chainlink/TWAP/Binance feeds. Poll recv() with a short
                    # timeout and force a reconnect when silent > DEAD_STREAM_SECS.
                    last_msg = time.time()
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            silent = time.time() - last_msg
                            if silent >= DEAD_STREAM_SECS:
                                self.log.warning(
                                    f"[WS-RTDS] silent for {silent:.0f}s "
                                    f"(oracle/TWAP feed stalled) — forcing reconnect")
                                break  # → outer loop reconnects + re-subscribes
                            continue
                        last_msg = time.time()
                        if raw == "PONG":
                            continue
                        await self._handle_rtds(raw)
            except (websockets.exceptions.ConnectionClosed, ConnectionError, OSError) as e:
                self.log.warning("[WS-RTDS] Reconnecting", error=repr(e))
                await asyncio.sleep(self.s.ws_reconnect_delay)
            except Exception as e:
                self.log.error("[WS-RTDS] Fatal", error=repr(e))
                await asyncio.sleep(self.s.ws_reconnect_delay)

    async def _handle_rtds(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(msg, dict):
            return
        topic = msg.get("topic", "")
        payload = msg.get("payload")
        if not isinstance(payload, dict):
            return

        if topic == "crypto_prices_chainlink":
            sym = (payload.get("symbol") or "").lower()
            val = payload.get("value")
            oracle_ts = payload.get("timestamp")
            if val is None:
                return
            for asset, csym in self.s.chainlink_symbols.items():
                if sym == csym:
                    try:
                        ots = int(float(oracle_ts)) if oracle_ts is not None else None
                        self.prices.update_chainlink(asset, float(val), ots)
                    except Exception as e:
                        self.log.error("[CHAINLINK] update failed", asset=asset, error=repr(e))
                    break

        elif "twap" in topic:
            # Official Chainlink TWAP (30-sec lookback). The message topic string
            # varies by RTDS version ("crypto_prices_twap_thirty" on subscribe
            # echo vs "prices.crypto.chainlink.twap" in SDK docs), so match by
            # substring + payload.windowSeconds.
            sym = (payload.get("symbol") or "").lower()
            val = payload.get("value")
            oracle_ts = payload.get("timestamp")
            if val is None:
                return
            if not getattr(self, "_twap_logged", False):
                self._twap_logged = True
                # Dump the full first payload so the raw wire format (field names
                # for value/timestamp) can be verified — the SDK docs add
                # windowSeconds, but the raw RTDS payload may omit it.
                self.log.info("[WS-RTDS] first TWAP message received",
                              topic=topic, symbol=sym,
                              payload_keys=list(payload.keys()), payload=payload)
            for asset, csym in self.s.chainlink_symbols.items():
                if sym == csym:
                    try:
                        ots = int(float(oracle_ts)) if oracle_ts is not None else None
                        self.prices.update_chainlink_twap(asset, float(val), ots)
                    except Exception as e:
                        self.log.error("[TWAP] update failed", asset=asset, error=repr(e))
                    break

        elif topic == "crypto_prices":
            sym = (payload.get("symbol") or "").lower()
            val = payload.get("value")
            if val is None:
                return
            for asset, bsym in self.s.binance_symbols_ws.items():
                if sym == bsym:
                    try:
                        self.prices.update_binance(asset, float(val))
                    except Exception as e:
                        self.log.error("[BINANCE-RTDS] update failed", asset=asset, error=repr(e))
                    break

    # ── Polymarket market channel ────────────────────────────────────────

    async def _run_market(self) -> None:
        while True:
            try:
                async with websockets.connect(
                    self.s.ws_market_url, ping_interval=self.s.ws_heartbeat_interval, ping_timeout=10
                ) as ws:
                    self._market_ws = ws
                    self.log.info("[WS-MARKET] Connected")
                    # Re-subscribe to ACTIVE (non-expired) tokens after reconnect
                    active = self._get_active_tokens()
                    if active:
                        await self._send_subscription(ws, active)
                        self.log.info("[WS-MARKET] Re-subscribed to active tokens",
                                      tokens=len(active))
                    # ── Dead-stream detection ─────────────────────────────
                    # Polymarket market WS sometimes silently stops sending
                    # updates while keeping the connection (and ping) alive.
                    # The blocking `async for raw in ws:` would wait forever,
                    # leaving bid_volume/ask_volume stuck at 0. We poll recv()
                    # with a short timeout and force a reconnect when no data
                    # arrives for DEAD_STREAM_SECS — the outer loop then
                    # reconnects and re-subscribes to active tokens.
                    last_msg = time.time()
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            silent = time.time() - last_msg
                            if silent >= DEAD_STREAM_SECS:
                                self.dead_stream_reconnects += 1
                                self.log.warning(
                                    f"[WS-MARKET] silent for {silent:.0f}s — book "
                                    f"updates stopped, forcing reconnect"
                                )
                                break  # → outer while reconnects + re-subscribes
                            continue
                        last_msg = time.time()
                        if raw == "PONG":
                            continue
                        try:
                            msg = json.loads(raw)
                            if isinstance(msg, list):
                                for item in msg:
                                    if isinstance(item, dict):
                                        self._process_market_msg(item)
                            elif isinstance(msg, dict):
                                self._process_market_msg(msg)
                        except Exception:
                            continue
            except (websockets.exceptions.ConnectionClosed, ConnectionError, OSError):
                self._market_ws = None
                self.log.warning("[WS-MARKET] Reconnecting")
                await asyncio.sleep(self.s.ws_reconnect_delay)
            except Exception as e:
                self._market_ws = None
                self.log.error("[WS-MARKET] Error", error=str(e))
                await asyncio.sleep(self.s.ws_reconnect_delay)

    def _process_market_msg(self, msg: dict) -> None:
        et = msg.get("event_type", "")
        if et == "book":
            aid = msg.get("asset_id", "")
            bids = msg.get("bids", [])
            asks = msg.get("asks", [])
            if aid and isinstance(bids, list) and isinstance(asks, list):
                nb = [{"price": b.get("price", "0"), "size": b.get("size", "0")}
                      for b in bids if isinstance(b, dict)]
                na = [{"price": a.get("price", "0"), "size": a.get("size", "0")}
                      for a in asks if isinstance(a, dict)]
                self.prices.update_full_book(aid, nb, na)
        elif et == "price_change":
            self._apply_bba(msg)
            for pc in msg.get("price_changes", []):
                if isinstance(pc, dict):
                    self._apply_bba(pc)
        elif et in ("best_bid_ask", "last_trade_price"):
            self._apply_bba(msg)

    def _apply_bba(self, msg: dict) -> None:
        aid = msg.get("asset_id", "")
        if not aid:
            return
        ba = msg.get("best_ask")
        bb = msg.get("best_bid")
        p = msg.get("price")
        ask = ba if ba is not None else p
        try:
            self.prices.update_lot_price(
                aid, float(ask) if ask else None, float(bb) if bb else None
            )
        except (ValueError, TypeError):
            pass

    async def _send_subscription(self, ws, token_ids: Set[str]) -> bool:
        """Low-level send. Returns True on success, False on failure."""
        if not token_ids:
            return True
        try:
            payload = json.dumps({
                "assets_ids": list(token_ids), "type": "market",
                "custom_feature_enabled": True,
            })
            await ws.send(payload)
            return True
        except Exception:
            return False

    async def subscribe_market_tokens(self, token_ids: Set[str]) -> None:
        """Subscribe to new market tokens.

        Adds tokens to the TTL buffer (auto-expires after SUBSCRIPTION_TTL_SECS).
        Checks WebSocket state before sending to prevent exceptions.
        """
        if not token_ids:
            return

        # Add to buffer with current timestamp
        now = time.time()
        new_count = 0
        for tid in token_ids:
            if tid not in self._subscribed_tokens:
                self._subscribed_tokens[tid] = now
                new_count += 1
            else:
                self._subscribed_tokens[tid] = now  # refresh TTL

        # Prune expired tokens
        active = self._get_active_tokens()

        if self._market_ws is None or not _ws_is_open(self._market_ws):
            self.log.debug(f"[WS-MARKET] WS not ready, buffered {new_count} new tokens "
                           f"({len(active)} active total)")
            return

        if self._market_ws_lock is None:
            self._market_ws_lock = asyncio.Lock()
        try:
            if _ws_is_open(self._market_ws):
                # Only send the NEW tokens (not all active), to keep payload small
                new_tokens = token_ids & active
                if new_tokens:
                    success = await self._send_subscription(self._market_ws, new_tokens)
                    if success:
                        self.log.info("[WS-MARKET] Subscribed", tokens=len(new_tokens))
                    else:
                        self.log.debug("[WS-MARKET] Send failed (buffered for reconnect)")
        except Exception:
            pass

    # ── Polymarket user channel ──────────────────────────────────────────

    async def _run_user(self) -> None:
        while True:
            if not self.api_creds:
                await asyncio.sleep(1.0)
                continue
            try:
                async with websockets.connect(
                    self.s.ws_user_url, ping_interval=self.s.ws_heartbeat_interval, ping_timeout=10
                ) as ws:
                    await ws.send(json.dumps({"type": "user", "auth": self.api_creds}))
                    self.log.info("[WS-USER] Connected")
                    async for raw in ws:
                        if raw == "PONG":
                            continue
                        try:
                            msg = json.loads(raw)
                            if isinstance(msg, list):
                                for item in msg:
                                    if isinstance(item, dict):
                                        self._process_user_msg(item)
                            elif isinstance(msg, dict):
                                self._process_user_msg(msg)
                        except Exception:
                            continue
            except (websockets.exceptions.ConnectionClosed, ConnectionError, OSError):
                self.log.warning("[WS-USER] Reconnecting")
                await asyncio.sleep(self.s.ws_reconnect_delay)
            except Exception as e:
                self.log.error("[WS-USER] Error", error=str(e))
                await asyncio.sleep(self.s.ws_reconnect_delay)

    def _process_user_msg(self, msg: dict) -> None:
        event_type = (msg.get("event_type") or "").lower()
        if event_type == "order":
            oid = msg.get("id") or msg.get("order_id")
            if not oid:
                return
            try:
                self.fills.record_order_event(
                    order_id=oid, side=(msg.get("side") or ""),
                    limit_price=float(msg.get("price") or 0),
                    original_size=float(msg.get("original_size") or msg.get("size") or 0),
                    size_matched=float(msg.get("size_matched") or 0),
                    status=(msg.get("status") or msg.get("type") or ""),
                )
            except (ValueError, TypeError):
                pass
        elif event_type == "trade":
            self._process_trade(msg)

    def _process_trade(self, msg: dict) -> None:
        taker_id = msg.get("taker_order_id") or msg.get("order_id")
        price = msg.get("price")
        size = msg.get("size")
        side = msg.get("side") or ""
        try:
            if taker_id and price and size:
                self.fills.record_trade_event(taker_id, side, float(price), float(size))
        except (ValueError, TypeError):
            pass
        for mo in msg.get("maker_orders", []) or []:
            if not isinstance(mo, dict):
                continue
            try:
                oid = mo.get("id") or mo.get("order_id") or mo.get("maker_order_id")
                pr = mo.get("price") or price
                sz = mo.get("size") or mo.get("matched_size") or size
                sd = mo.get("side") or side
                if oid and pr and sz:
                    self.fills.record_trade_event(oid, sd, float(pr), float(sz))
            except (ValueError, TypeError):
                continue
