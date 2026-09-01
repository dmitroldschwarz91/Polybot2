"""
Execution layer — Polymarket CLOB client wrapper.

Isolates all `py_clob_client_v2` calls behind a single interface so the rest
of the app (and tests) never depend on the SDK directly. A paper-trading mode
lets the bot run end-to-end without real money.
"""

from __future__ import annotations

from functools import partial as fp
from typing import Optional

from ..config import Settings
from ..core.http import run_sync
from ..core.logging import StructuredLogger

try:
    from py_clob_client_v2 import (
        ClobClient, OrderArgs, OrderType, Side,
        PartialCreateOrderOptions, BalanceAllowanceParams, AssetType,
    )
    HAS_FAK = hasattr(OrderType, "FAK")
except ImportError:  # pragma: no cover — SDK not installed (e.g. CI / paper mode)
    ClobClient = None  # type: ignore
    OrderArgs = OrderType = Side = PartialCreateOrderOptions = None  # type: ignore
    BalanceAllowanceParams = AssetType = None  # type: ignore
    HAS_FAK = False


def normalize_api_creds(creds) -> Optional[dict]:
    if creds is None:
        return None
    if isinstance(creds, dict):
        api_key = creds.get("apiKey") or creds.get("key") or creds.get("api_key")
        secret = creds.get("secret") or creds.get("apiSecret") or creds.get("api_secret")
        passphrase = creds.get("passphrase") or creds.get("apiPassphrase") or creds.get("api_passphrase")
    else:
        api_key = getattr(creds, "apiKey", None) or getattr(creds, "key", None)
        secret = getattr(creds, "secret", None) or getattr(creds, "apiSecret", None)
        passphrase = getattr(creds, "passphrase", None) or getattr(creds, "apiPassphrase", None)
    if api_key and secret and passphrase:
        return {"apiKey": api_key, "secret": secret, "passphrase": passphrase}
    return None


class TradingClient:
    """Wraps ClobClient with async helpers + paper-trading support."""

    def __init__(self, settings: Settings, log: StructuredLogger) -> None:
        self.s = settings
        self.log = log
        self.api_creds: Optional[dict] = None
        self._client = None
        self.paper = settings.paper_trading
        self._tick_cache: dict = {}
        self._tick_ts: dict = {}

    async def init(self) -> None:
        if self.paper or ClobClient is None:
            self.log.warning("[CLIENT] Paper-trading mode — no real orders will be placed")
            return
        client = ClobClient(
            self.s.clob_api, key=self.s.private_key, chain_id=self.s.chain_id,
            signature_type=self.s.signature_type, funder=self.s.funder_address,
        )
        creds = await run_sync(client.derive_api_key)
        await run_sync(client.set_api_creds, creds)
        self.api_creds = normalize_api_creds(creds)
        self._client = client
        funder = self.s.funder_address
        self.log.info("Client initialized",
                      funder=f"{funder[:10]}...{funder[-6:]}" if len(funder) > 16 else funder)

    @property
    def ready(self) -> bool:
        return self.paper or self._client is not None

    # ── market params ────────────────────────────────────────────────────

    def get_market_params(self, token_id: str):
        """Returns (tick_size, neg_risk)."""
        import time as _t
        now = _t.time()
        cached = self._tick_cache.get(token_id)
        if cached and now - self._tick_ts.get(token_id, 0) < 300:
            return cached
        ts, nr = "0.01", False
        if self._client and not self.paper:
            try:
                r = self._client.get(f"{self.s.clob_api}/tick-size",
                                     params={"token_id": token_id}, timeout=10)
            except Exception:
                r = None
            if r and getattr(r, "status_code", 0) == 200:
                d = r.json()
                ts = str(d.get("minimum_tick_size") or d.get("tick_size") or "0.01")
        res = (ts, nr)
        self._tick_cache[token_id] = res
        self._tick_ts[token_id] = now
        return res

    # ── balance ──────────────────────────────────────────────────────────

    def fetch_wallet_usdc(self):
        if self.paper or not self._client:
            return 0.0, False
        try:
            r = self._client.get_balance_allowance(
                params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            return round(int(r.get("balance", 0)) / 1_000_000, 4), True
        except Exception as e:
            self.log.warning(f"[WALLET] {e}")
            return 0.0, False

    def get_real_balance(self):
        usdc, ok = self.fetch_wallet_usdc()
        if not ok:
            return None
        return round(usdc * self.s.balance_safety_margin, 4)

    def cancel_all(self):
        if self._client and not self.paper:
            try:
                self._client.cancel_all()
            except Exception:
                pass

    def cancel(self, order_id: str):
        if self._client and not self.paper:
            try:
                self._client.cancel(order_id)
            except Exception:
                pass

    # ── order placement ──────────────────────────────────────────────────

    def _post_order(self, token_id, price, size, side_str, order_type):
        """Synchronous SDK call. side_str ∈ {'BUY','SELL'}."""
        if self.paper:
            return {"orderID": f"paper-{abs(hash((token_id, price, size, side_str)))}"}
        opts = PartialCreateOrderOptions(tick_size="0.01", neg_risk=False)
        side = Side.BUY if side_str == "BUY" else Side.SELL
        fn = fp(
            self._client.create_and_post_order,
            order_args=OrderArgs(token_id=token_id, price=price, size=size, side=side),
            options=opts, order_type=order_type,
        )
        return fn()

    async def post_order_async(self, token_id, price, size, side_str,
                               tick_size="0.01", neg_risk=False, fak=False):
        if self.paper:
            return {"orderID": f"paper-{abs(hash((token_id, price, size, side_str, _t())))}"}
        opts = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)
        side = Side.BUY if side_str == "BUY" else Side.SELL
        ot = (OrderType.FAK if HAS_FAK else OrderType.FOK) if fak else OrderType.GTC
        fn = fp(
            self._client.create_and_post_order,
            order_args=OrderArgs(token_id=token_id, price=price, size=size, side=side),
            options=opts, order_type=ot,
        )
        return await run_sync(fn)


def extract_order_id(resp) -> Optional[str]:
    if resp is None:
        return None
    if isinstance(resp, dict):
        return resp.get("orderID") or resp.get("order_id") or resp.get("id")
    for a in ("orderID", "order_id", "id"):
        if hasattr(resp, a):
            return getattr(resp, a)
    return None


# keep _t imported lazily for paper ids
import time as _t  # noqa: E402
