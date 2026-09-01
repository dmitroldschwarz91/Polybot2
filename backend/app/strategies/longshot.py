"""
Longshot strategy — exploit the favorite-longshot bias (post-TWAP).

Empirically on 252 post-TWAP intervals: the UNDERDOG (cheap token) is under-
priced. Buying the cheaper token at stc~90-150 and holding to resolution:
  favorite (>=0.70) : WR ~78%, price ~0.90 -> -EV (over-priced)
  underdog (<=0.30) : WR ~21%, price ~0.10 -> +EV ~+0.10/sh (under-priced)
This is the classic favorite-longshot bias, now clean once manipulation was
removed by TWAP. It is the OPPOSITE of VacuumScalp/FavDip (which bought the
favorite and bled).

Design:
  * entry  : at stc in [WIN_LO, WIN_HI], buy the CHEAPER token if its ask is in
             [MIN, MAX]. Pure price signal — NO rolling-low / dip filter (that
             selected decisive losers, PairFirst's 0/125).
  * sizing : quarter-Kelly fraction of capital (default ~2.5%). High variance
             (WR ~20%, deep drawdowns) => keep it small; never compounds to ruin
             (P(ruin)=0% in MC), but expect 40-55% drawdowns.
  * exit   : HOLD to resolution. No TP (cuts the rare winners) / SL (cuts
             recoveries). The longshot wins at resolution (+0.88/share at 0.10).
  * guard  : optionally require a live TWAP stream (signal is price-based, so
             this only throttles frequency, not correctness).

PRELIMINARY (+0.10/sh on one day). Confirm on more data before real money.
Integration mirrors FavDip: check_entry() -> LongshotSignal -> engine buys at
ask -> single-leg HOLD -> existing cross-validation+cascade resolves.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

# ── parameters (mirror config longshot_*; small-sample — re-validate) ─────────
LONGSHOT_MIN   = 0.08    # price floor (avoid dust / thinnest-EV <0.05 band)
LONGSHOT_MAX   = 0.25    # price ceiling (above this variance drops but WR noisy)
LONGSHOT_WIN_LO = 90     # entry window start (secs to close) — late = firmer odds
LONGSHOT_WIN_HI = 150
LONGSHOT_KELLY = 0.025   # ~quarter-Kelly (deep drawdowns; keep small)


@dataclass
class LongshotSignal:
    token_id: str        # the underdog (cheaper token)
    direction: str
    limit_price: float   # best_ask at signal
    shares: int
    leader_ask: float    # the opposite (expensive) token — for logs/sanity


def check_entry(market: dict, asset: str, stc: float, prices,
                capital: float, kelly_frac: float = LONGSHOT_KELLY,
                price_min: float = LONGSHOT_MIN, price_max: float = LONGSHOT_MAX,
                win_lo: int = LONGSHOT_WIN_LO, win_hi: int = LONGSHOT_WIN_HI,
                risk=None, positions: Optional[dict] = None,
                require_twap_alive: bool = False, twap_max_age: float = 15.0,
                min_shares: int = 5, slippage: float = 0.01
                ) -> Optional[LongshotSignal]:
    """Buy the cheaper token (underdog) when its ask is in [min,max], stc in window."""
    if require_twap_alive:
        if prices.get_chainlink_twap(asset, max_age=twap_max_age) is None:
            return None
    if not (win_lo <= stc <= win_hi):
        return None
    up_id = market.get("up_token_id")
    dn_id = market.get("down_token_id")
    if not up_id or not dn_id:
        return None
    ub = prices.get_book(up_id)
    db = prices.get_book(dn_id)
    ua = ub.best_ask if (ub and ub.best_ask is not None) else None
    da = db.best_ask if (db and db.best_ask is not None) else None
    if ua is None or da is None:
        return None
    # the underdog = the cheaper token; require it in band AND the leader clearly expensive
    if ua <= da:
        under_id, under_ask, leader_ask, direction = up_id, ua, da, "UP"
    else:
        under_id, under_ask, leader_ask, direction = dn_id, da, ua, "DOWN"
    if not (price_min <= under_ask <= price_max):
        return None
    # sanity: leader should be the expensive side (avoids 50/50 ambiguity)
    if leader_ask < 0.60:
        return None
    if risk is not None and positions is not None:
        open_pos = {s: p for s, p in positions.items() if not p.closed}
        ok, _ = risk.can_open_new(open_pos, capital)
        if not ok:
            return None
    fill = min(0.999, under_ask + slippage)
    stake = capital * kelly_frac
    shares = int(stake / fill) if fill > 0 else 0
    if shares < min_shares:
        shares = min_shares if capital >= min_shares * fill else 0
    if shares == 0:
        return None
    return LongshotSignal(token_id=under_id, direction=direction,
                          limit_price=under_ask, shares=shares, leader_ask=leader_ask)
