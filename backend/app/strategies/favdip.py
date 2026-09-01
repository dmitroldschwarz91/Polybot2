"""
FavDip strategy — buy the VWAP-favorite when it dips cheap, confirmed by momentum revert.

Extracted from DemoEngine for clean separation:
  - This module: the DECISION logic (pure, no side effects).
  - DemoEngine: the EXECUTION (pending/fill/pair-complete/resolve).

Parameters (from walk-forward optimization, correct completion logic):
  cap=0.40, mom_K=30s, |mom|≥$5, target=0.40, window=[30,270]s.
Expected: OOS EV +$0.061/акц, WR 42%, Kelly 14%, maxDD 37%@5%.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# ── parameters ────────────────────────────────────────────────────────────

FAVDIP_CAP      = 0.40    # max favorite token price (dipped on counter-swing)
FAVDIP_MIN_LEG1 = 0.04    # min leg1 price (reject $0.02-artifact)
FAVDIP_MOM_K    = 30      # momentum window (seconds, ~points in zpair_prices)
FAVDIP_MOM_MIN  = 5.0     # min |momentum| ($) for confirmed revert


# ── result ────────────────────────────────────────────────────────────────

@dataclass
class FavDipSignal:
    """Entry signal from FavDip check."""
    token_id: str          # the favorite token to buy
    other_id: str          # the opposite token (for pair completion)
    direction: str         # "UP" or "DOWN"
    limit_price: float     # best_ask at signal (for limit order)
    shares: int            # computed position size
    momentum: float        # oracle change over MOM_K seconds ($)
    favorite_up: bool      # True if VWAP > open (UP is favorite)


# ── entry check ───────────────────────────────────────────────────────────

def check_entry(
    market: dict,
    asset: str,
    stc: float,
    prices,                    # LivePriceStore
    start_prices: dict,        # market_data.start_prices
    cur_interval: int,
    zpair_prices: dict,        # {asset: [oracle prices from interval start, 1/sec]}
    stake_ratio: float,
    virtual_capital: float,
    slippage: float = 0.01,
    risk=None,                 # RiskManager
    positions: Optional[dict] = None,
    min_shares: int = 5,
) -> Optional[FavDipSignal]:
    """Check FavDip entry conditions. Returns FavDipSignal or None.

    Logic:
      1. VWAP vs open → determine the favorite (UP if VWAP > open).
      2. Favorite token ask ≤ cap (it dipped on a counter-swing).
      3. Momentum (oracle change over MOM_K sec) already reverted toward the favorite
         AND |momentum| ≥ MOM_MIN (confirmed recovery, not noise).
      4. Risk gate passes.
    """
    op = prices.get_oracle_price(asset)
    if not op:
        return None
    vwap = prices.get_vwap(asset)
    if not vwap:
        return None

    # open/start price
    sp = start_prices.get(str(cur_interval), {}).get(asset)
    if sp is None or sp <= 0:
        return None

    # 1. favorite = UP if VWAP > open
    fav_up = vwap > sp
    token_id = market["up_token_id"] if fav_up else market["down_token_id"]
    other_id = market["down_token_id"] if fav_up else market["up_token_id"]
    if not token_id or not other_id:
        return None

    # 2. favorite dipped cheap?
    book = prices.get_book(token_id)
    if not book or book.best_ask is None:
        return None
    p1 = book.best_ask
    if p1 < FAVDIP_MIN_LEG1 or p1 > FAVDIP_CAP:
        return None

    # 3. momentum revert confirmed?
    ps = zpair_prices.get(asset) or []
    if len(ps) < FAVDIP_MOM_K + 5:
        return None
    mom = ps[-1] - ps[-FAVDIP_MOM_K]
    is_revert = (mom > 0) if fav_up else (mom < 0)
    if not is_revert or abs(mom) < FAVDIP_MOM_MIN:
        return None

    # 4. risk gate
    if risk is not None and positions is not None:
        open_positions = {s: p for s, p in positions.items() if not p.closed}
        ok, _ = risk.can_open_new(open_positions, virtual_capital)
        if not ok:
            return None

    # sizing
    fill = min(0.999, p1 + slippage)
    stake = virtual_capital * stake_ratio
    shares = int(stake / fill) if fill > 0 else 0
    if shares < min_shares:
        shares = min_shares if virtual_capital >= min_shares * fill else 0
    if shares == 0:
        return None

    return FavDipSignal(
        token_id=token_id,
        other_id=other_id,
        direction="UP" if fav_up else "DOWN",
        limit_price=p1,
        shares=shares,
        momentum=mom,
        favorite_up=fav_up,
    )
