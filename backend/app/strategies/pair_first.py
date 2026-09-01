"""
PairFirst strategy — async pair with a rolling-low leg1 entry (post-TWAP).

The only +EV surface found on post-TWAP Polymarket 5-min data (see
docs/post_twap_findings.md): buy a CHEAP leg1 when its ask makes a new N-second
low, then complete the pair (buy the opposite token) when entry + opposite <
target. If the pair never completes, hold leg1 to resolution. Deep locks on
completion (~+0.4/sh) outweigh the bounded cheap-leg loss on non-completion.

REFUTED alternatives (all -EV): directional cheap/expensive favorite, expensive
leg1 (thin locks + catastrophic reversal losses), exit-managed legs.

ENTRY GUARD (operator requirement): enter ONLY when the official TWAP stream is
ALIVE — prices.get_chainlink_twap(asset, max_age) must be fresh. A dead stream
means resolution is untrustworthy, so we sit out. (Resolution after entry still
falls back through cross-validation + cascade, so a stream death post-entry is
safe; the guard is about not entering blind.)

PRELIMINARY params (test_async_pairs.py, n=47 TWAP, hold-mode): EV ~+0.054/sh,
completion ~40%. CONFIRM on more data before risking real money.

Integration mirrors FavDip: the engine calls check_entry() -> PairFirstSignal,
places a GTC limit at ask (leg1) -> pending -> on fill a Position(is_pair,
leg2_token_id); each tick check_complete() decides the leg2 buy; resolution uses
the existing cross-validation + cascade (pair_locked when both legs filled).
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Optional, Tuple

# ── parameters (mirror config pair_*; tuned on a SMALL sample — re-validate) ──
PAIR_ENTRY_CAP      = 0.40   # max leg1 token price
PAIR_MIN_LEG1       = 0.04   # min leg1 price (reject dust / $0.02 artifacts)
PAIR_ROLLING_WINDOW = 30.0   # rolling-low lookback (seconds)
PAIR_TARGET         = 0.50   # complete pair when entry + opposite_ask < this
PAIR_WIN_LO         = 30     # entry window start (secs to close)
PAIR_WIN_HI         = 270    # entry window end
TWAP_ALIVE_MAX_AGE  = 15.0   # TWAP stream must be fresher than this to enter
ROLLING_MIN_POINTS  = 5      # past points needed before trusting a "new low"
_HIST_THROTTLE      = 0.5    # sample the rolling history every N seconds


@dataclass
class PairFirstSignal:
    """Leg1 entry signal from PairFirst.check_entry."""
    token_id: str        # leg1 — the cheap token at a new rolling low
    other_id: str        # leg2 — the opposite token
    direction: str       # "UP" / "DOWN" of leg1
    limit_price: float   # best_ask at signal (limit order price)
    shares: int
    rolling_min: float   # the rolling low that was broken
    twap_value: float    # live TWAP at entry (proven alive)


class PairFirstStrategy:
    """Stateful: keeps a rolling ask history per token to detect new lows.

    The rolling window is maintained internally (sampled from the live book each
    tick), so this needs no engine-side accumulator. Call reset() on a new
    interval to drop stale tokens (mirrors reset_vwap).
    """

    def __init__(self, cap: float = PAIR_ENTRY_CAP,
                 min_leg1: float = PAIR_MIN_LEG1,
                 window: float = PAIR_ROLLING_WINDOW,
                 target: float = PAIR_TARGET,
                 win_lo: int = PAIR_WIN_LO, win_hi: int = PAIR_WIN_HI) -> None:
        self.cap = cap
        self.min_leg1 = min_leg1
        self.window = window
        self.target = target
        self.win_lo = win_lo
        self.win_hi = win_hi
        self._ask_hist: dict = {}   # token_id -> deque[(ts, ask)]

    def reset(self) -> None:
        """Drop rolling history (call on new interval to evict dead tokens)."""
        self._ask_hist.clear()

    def _observe(self, token_id: str, ask: Optional[float], now: float) -> None:
        if ask is None:
            return
        h = self._ask_hist.setdefault(token_id, deque(maxlen=600))
        if not h or now - h[-1][0] >= _HIST_THROTTLE:
            h.append((now, ask))

    def _rolling_min(self, token_id: str, now: float) -> Optional[float]:
        """Min ask over (now-window, now), strictly past. None if too few points."""
        h = self._ask_hist.get(token_id)
        if not h:
            return None
        cutoff = now - self.window
        past = [a for ts, a in h if cutoff <= ts < now]
        return min(past) if len(past) >= ROLLING_MIN_POINTS else None

    def check_entry(self, market: dict, asset: str, stc: float, prices,
                    stake_ratio: float, capital: float,
                    risk=None, positions: Optional[dict] = None,
                    twap_max_age: float = TWAP_ALIVE_MAX_AGE,
                    slippage: float = 0.01, min_shares: int = 5
                    ) -> Optional[PairFirstSignal]:
        """Evaluate leg1 entry. Returns PairFirstSignal or None.

        1. TWAP-liveness guard — no entry on a dead stream.
        2. entry window.
        3. observe asks; find a token at a NEW rolling low within [min_leg1, cap].
        4. risk gate + sizing.
        """
        # 1. TWAP-liveness guard (operator requirement).
        twap = prices.get_chainlink_twap(asset, max_age=twap_max_age)
        if twap is None:
            return None
        # 2. entry window.
        if not (self.win_lo <= stc <= self.win_hi):
            return None
        up_id = market.get("up_token_id")
        dn_id = market.get("down_token_id")
        if not up_id or not dn_id:
            return None
        now = time.time()
        ub = prices.get_book(up_id)
        db = prices.get_book(dn_id)
        ua = ub.best_ask if (ub and ub.best_ask is not None) else None
        da = db.best_ask if (db and db.best_ask is not None) else None
        # 3. observe into rolling history, then look for new lows.
        self._observe(up_id, ua, now)
        self._observe(dn_id, da, now)
        cands = []
        for tok_id, tok_dir, ask in ((up_id, "UP", ua), (dn_id, "DOWN", da)):
            if ask is None or not (self.min_leg1 <= ask <= self.cap):
                continue
            rmin = self._rolling_min(tok_id, now)
            if rmin is not None and ask < rmin:      # new N-second low
                cands.append((ask, tok_id, tok_dir, rmin))
        if not cands:
            return None
        cands.sort()                                  # cheaper leg1 = more pairing room
        ask, tok_id, tok_dir, rmin = cands[0]
        other_id = dn_id if tok_dir == "UP" else up_id
        # 4. risk gate.
        if risk is not None and positions is not None:
            open_pos = {s: p for s, p in positions.items() if not p.closed}
            ok, _ = risk.can_open_new(open_pos, capital)
            if not ok:
                return None
        # 5. sizing.
        fill = min(0.999, ask + slippage)
        stake = capital * stake_ratio
        shares = int(stake / fill) if fill > 0 else 0
        if shares < min_shares:
            shares = min_shares if capital >= min_shares * fill else 0
        if shares == 0:
            return None
        return PairFirstSignal(
            token_id=tok_id, other_id=other_id, direction=tok_dir,
            limit_price=ask, shares=shares, rolling_min=rmin, twap_value=twap)


def check_complete(pos, prices, target: float = PAIR_TARGET) -> Optional[Tuple[str, float]]:
    """After leg1 is filled, signal a leg2 buy when entry + opposite_ask < target.

    Returns (leg2_token_id, leg2_limit_price=opposite best_ask) or None.
    `pos` must expose: leg2_token_id, leg2_filled, entry_price
    (same fields FavDip/ZPair positions already carry).
    """
    if getattr(pos, "leg2_filled", False):
        return None
    leg2_id = getattr(pos, "leg2_token_id", None)
    if not leg2_id:
        return None
    book = prices.get_book(leg2_id)
    if not book or book.best_ask is None:
        return None
    if pos.entry_price + book.best_ask >= target:
        return None
    return (leg2_id, book.best_ask)   # engine places the leg2 order at ask+slippage
