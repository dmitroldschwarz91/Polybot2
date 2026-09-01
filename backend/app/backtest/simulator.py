"""
Simulated exchange + market model for backtesting.

Models the Polymarket UP/DOWN token prices and order books as a function of:
  * the real oracle price path (from Binance klines),
  * deviation from the interval start price,
  * time to close,
  * a configurable spread + liquidity.

We are explicit that token prices are MODELLED, not historical — only the
underlying oracle path is real. This is the standard limitation of any retail
Polymarket backtest: historical CLOB order books aren't available in bulk.

Execution charges the Polymarket taker fee on the profitable side and applies
slippage, so backtest P&L is pessimistic rather than optimistic.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Optional

from ..config import Settings
from .data import PriceTick


@dataclass
class MarketModel:
    """Models token (lot) prices from the oracle path.

    UP token fair value ~ P(oracle_end > start_price). We approximate this with
    a logistic of the z-score of current deviation scaled by time-to-close.
    """
    spread: float = 0.01          # half-spread in price units
    liquidity: float = 200.0      # assumed shares available per side
    noise: float = 0.01           # token-price noise (sentiment)

    def token_prices(self, oracle: float, start_price: float,
                     secs_to_close: float, interval_secs: int) -> tuple:
        """Return (up_mid, down_mid, best_ask_up, best_bid_up, ...).

        Returns a dict with full bid/ask for both sides.

        Calibrated so token ask prices pass THROUGH the 0.95–0.98 range that
        late-window scalpers target, rather than skipping over it.
        """
        if start_price <= 0:
            up_mid = 0.5
        else:
            dev = (oracle - start_price) / start_price
            time_weight = 1.0 - (secs_to_close / interval_secs)  # 0 → 1 over the window
            # Calibrated (D=0.0045, k=0.8+3.0·tw) so token ask prices DWELL in
            # the 0.95–0.98 band for moderate 0.4–0.8% deviations at various
            # times-to-close, instead of skipping straight to 0.99.
            z = dev / 0.0045
            k = 0.8 + 3.0 * time_weight
            up_mid = 1.0 / (1.0 + math.exp(-z * k))
        up_mid = max(0.02, min(0.982, up_mid))
        down_mid = 1.0 - up_mid
        return {
            "up_mid": up_mid, "down_mid": down_mid,
            "up_ask": min(0.999, up_mid + self.spread),
            "up_bid": max(0.001, up_mid - self.spread),
            "down_ask": min(0.999, down_mid + self.spread),
            "down_bid": max(0.001, down_mid - self.spread),
            "bid_volume": self.liquidity, "ask_volume": self.liquidity,
        }


@dataclass
class SimOrder:
    order_id: str
    side: str          # BUY / SELL
    token_id: str
    price: float
    size: int
    ts: int


@dataclass
class SimAccount:
    """Tracks cash + token holdings + realized P&L in the simulation."""
    cash: float = 0.0
    # token_id -> {size, avg_cost}
    holdings: dict = field(default_factory=dict)
    realized_pnl: float = 0.0
    fees_paid: float = 0.0
    trade_log: list = field(default_factory=list)

    def position_cost(self, token_id: str) -> float:
        h = self.holdings.get(token_id)
        return h["size"] * h["avg_cost"] if h else 0.0

    def holding_size(self, token_id: str) -> int:
        h = self.holdings.get(token_id)
        return h["size"] if h else 0


class SimulatedExchange:
    """Fills marketable orders instantly at the modelled book, with fees.

    BUY fills at best_ask, SELL fills at best_bid (the pessimistic case).
    Polymarket charges `taker_fee` on the WINNING (profitable) side at resolution;
    we approximate by charging it on every sell that closes at a gain, and also
    apply it to redemptions.
    """

    def __init__(self, settings: Settings, model: Optional[MarketModel] = None) -> None:
        self.s = settings
        self.model = model or MarketModel()
        self.account = SimAccount()
        self.taker_fee = settings.backtest_taker_fee
        self.slippage_ticks = settings.backtest_slippage_ticks
        self._oid = 0

    def fund(self, amount: float) -> None:
        self.account.cash = amount

    def _next_oid(self) -> str:
        self._oid += 1
        return f"sim-{self._oid}"

    def buy(self, token_id: str, ask_price: float, size: int, ts: int,
            side_label: str = "UP") -> Optional[SimOrder]:
        """Buy `size` at ask (with slippage). Returns None if not enough cash."""
        fill_price = min(0.999, ask_price + self.slippage_ticks * 0.01)
        cost = round(fill_price * size, 6)
        if cost > self.account.cash:
            # partial fill to available cash
            size = max(0, int(self.account.cash / fill_price))
            if size <= 0:
                return None
            cost = round(fill_price * size, 6)
        self.account.cash -= cost
        h = self.account.holdings.setdefault(token_id, {"size": 0, "avg_cost": 0.0})
        new_size = h["size"] + size
        h["avg_cost"] = ((h["avg_cost"] * h["size"]) + cost) / new_size if new_size else 0.0
        h["size"] = new_size
        order = SimOrder(self._next_oid(), "BUY", token_id, fill_price, size, ts)
        self.account.trade_log.append({"ts": ts, "action": "BUY", "side": side_label,
                                       "price": fill_price, "size": size})
        return order

    def sell(self, token_id: str, bid_price: float, size: int, ts: int,
             side_label: str = "UP") -> Optional[SimOrder]:
        h = self.account.holdings.get(token_id)
        if not h or h["size"] <= 0:
            return None
        size = min(size, h["size"])
        fill_price = max(0.001, bid_price - self.slippage_ticks * 0.01)
        proceeds = round(fill_price * size, 6)
        cost_basis = h["avg_cost"] * size
        gross_pnl = proceeds - cost_basis
        # Polymarket fee on profitable side
        fee = proceeds * self.taker_fee if gross_pnl > 0 else 0.0
        net_pnl = gross_pnl - fee
        self.account.cash += proceeds - fee
        self.account.realized_pnl += net_pnl
        self.account.fees_paid += fee
        h["size"] -= size
        if h["size"] <= 0:
            self.account.holdings.pop(token_id, None)
        order = SimOrder(self._next_oid(), "SELL", token_id, fill_price, size, ts)
        self.account.trade_log.append({"ts": ts, "action": "SELL", "side": side_label,
                                       "price": fill_price, "size": size,
                                       "pnl": net_pnl, "fee": fee})
        return order

    def redeem(self, token_id: str, won: bool, ts: int, side_label: str = "UP") -> float:
        """Resolve a token at market close: $1 if won, $0 if lost (minus fee on win)."""
        h = self.account.holdings.get(token_id)
        if not h or h["size"] <= 0:
            return 0.0
        size = h["size"]
        cost_basis = h["avg_cost"] * size
        if won:
            proceeds = size * 1.0
            fee = proceeds * self.taker_fee
            net_pnl = proceeds - cost_basis - fee
            self.account.fees_paid += fee
        else:
            net_pnl = -cost_basis
            proceeds = 0.0
        self.account.cash += (proceeds - (fee if won else 0.0))
        self.account.realized_pnl += net_pnl
        self.account.holdings.pop(token_id, None)
        self.account.trade_log.append({"ts": ts, "action": "REDEEM", "side": side_label,
                                       "won": won, "size": size, "pnl": net_pnl})
        return net_pnl

    def equity(self, book_prices: dict) -> float:
        """Mark-to-market equity = cash + holdings valued at mid."""
        val = self.account.cash
        for tid, h in self.account.holdings.items():
            mid = book_prices.get(tid, h["avg_cost"])
            val += h["size"] * mid
        return val
