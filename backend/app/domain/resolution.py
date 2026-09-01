"""
Cross-validation of market resolution from two independent sources.

Polymarket 5-min UP/DOWN markets resolve to a winner. We can determine the
outcome from two INDEPENDENT signals:

  1. Chainlink/Binance oracle  — our 30-sec TWAP (close) vs the open price.
     The OFFICIAL resolution feed, but our local TWAP reconstruction can
     disagree with the official Chainlink stream in ~2-5% of borderline cases
     (TWAP window edge, missing ticks, etc.).

  2. Leader token price        — the market's OWN resolution signal: after
     close the winning token polarises to ~$1.0 and the loser to ~$0.0 within
     1-2 min. This is what ACTUALLY pays out.

Resolution order (operator-selected): CROSS-VALIDATION FIRST, then CASCADE.

  Step 1 — cross-validation (when BOTH sources are available):
      AGREE      → resolve with high confidence (cross_ok)
      DISAGREE   → UNRESOLVED, neutral (подстраховка: refuse to guess)
  Step 2 — cascade fallback (cross-validation off, OR only one / no source
           available — i.e. inconclusive):
      Chainlink TWAP → Gamma API → leader token price

A late snapshot (`late_truth_outcome`) re-reads the ground-truth token outcome
well after close and compares it to what we resolved, so the real Chainlink
error rate can be measured on live data — including trades the cascade closed
early before the token polarised.

Engines use it as:
    cl_won  = <chainlink TWAP method>(pos)                     # Optional[bool]
    tok_won = await leader_token_outcome(pos.token_id, ...)    # Optional[bool]
    xcc     = cross_validate(cl_won, tok_won)                  # CrossCheck
    # xcc.method in {cross_ok, chainlink_only, token_only, cross_disagree, no_source}
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

# Polarisation thresholds: a token priced above/below these is decisively the
# winner/loser. The open band (LOSE_LO .. WIN_HI) is "ambiguous, keep waiting".
TOKEN_WIN_HI: float = 0.95   # >= this → we bought the winner
TOKEN_LOSE_LO: float = 0.05  # <= this → we bought the loser


@dataclass
class CrossCheck:
    """Outcome of cross-validating the two resolution sources."""

    won: Optional[bool]            # final decision; None = unresolved / not-yet
    method: str                    # cross_ok | chainlink_only | token_only | cross_disagree | no_source
    chainlink_won: Optional[bool]  # raw Chainlink TWAP outcome
    token_won: Optional[bool]      # raw leader-token-price outcome
    agree: Optional[bool]          # True/False when both present; None otherwise


def cross_validate(chainlink_won: Optional[bool],
                   token_won: Optional[bool]) -> CrossCheck:
    """Combine the two sources.

    Returns (method, won):
      cross_ok        — both sources present and AGREE  → resolve (high conf.)
      cross_disagree  — both present but CONFLICT        → UNRESOLVED (neutral)
      chainlink_only  — only Chainlink available        → inconclusive → cascade
      token_only      — only token price available      → inconclusive → cascade
      no_source       — neither available yet           → inconclusive → cascade

    Only cross_ok / cross_disagree are CONCLUSIVE. The engines treat the other
    three as "cross-validation inconclusive" and fall through to the cascade.
    """
    if chainlink_won is not None and token_won is not None:
        if chainlink_won == token_won:
            return CrossCheck(chainlink_won, "cross_ok",
                              chainlink_won, token_won, True)
        return CrossCheck(None, "cross_disagree",
                          chainlink_won, token_won, False)
    if chainlink_won is not None:
        return CrossCheck(chainlink_won, "chainlink_only",
                          chainlink_won, None, None)
    if token_won is not None:
        return CrossCheck(token_won, "token_only",
                          None, token_won, None)
    return CrossCheck(None, "no_source", None, None, None)


async def leader_token_outcome(token_id: str, prices, http, settings) -> Optional[bool]:
    """At-resolution check: leader token price — the market's OWN signal.

    `token_id` is the token we BOUGHT. After close it polarises:
        >= TOKEN_WIN_HI  → we won  (return True)
        <= TOKEN_LOSE_LO → we lost (return False)
        otherwise        → ambiguous, return None (caller retries / cascades)

    Sources tried in order (first decisive one wins):
      1. local order book (WS market channel / BookPoller REST)  — fast, live
      2. CLOB /price REST (independent of the WS feed)

    Independent of the Chainlink/Binance oracle — that is the whole point.
    """
    # 1. local book
    book = prices.get_book(token_id)
    if book and getattr(book, "best_ask", None) is not None:
        p = book.best_ask
        if p >= TOKEN_WIN_HI:
            return True
        if p <= TOKEN_LOSE_LO:
            return False
    # 2. CLOB /price REST (direct, independent of WS)
    try:
        data = await http.get(
            f"{settings.clob_api}/price",
            params={"token_id": token_id, "side": "BUY"},
        )
        if data and "price" in data:
            p = float(data["price"])
            if p >= TOKEN_WIN_HI:
                return True
            if p <= TOKEN_LOSE_LO:
                return False
    except Exception:
        pass
    return None


async def gamma_outcome(slug: str, http, settings) -> Optional[bool]:
    """Gamma API resolution: returns True if UP won, False if DOWN won,
    None if the market is not yet decisively resolved.

    Requires closed=True AND definitive polarisation (one side >=0.98, the
    other <=0.02), or umaResolutionStatus == "resolved". Returns the ABSOLUTE
    outcome (UP won?) — callers map to their side via direction.
    """
    try:
        import json
        data = await http.get(f"{settings.gamma_api}/events/slug/{slug}")
        if not data or not data.get("markets"):
            return None
        m = data["markets"][0]
        if not m.get("closed"):
            return None
        prices = json.loads(m.get("outcomePrices", "[]"))
        if len(prices) < 2:
            return None
        up_price = float(prices[0])
        down_price = float(prices[1])
        is_definitive = (up_price >= 0.98 and down_price <= 0.02) or \
                        (down_price >= 0.98 and up_price <= 0.02)
        if not is_definitive and m.get("umaResolutionStatus") != "resolved":
            return None
        return up_price >= 0.5
    except Exception:
        return None


async def late_truth_outcome(token_id: str, slug: str, direction: str,
                             http, settings) -> Tuple[Optional[bool], str]:
    """Ground-truth recheck WELL after close (the market has settled).

    Deliberately skips the local order book — by the time this runs the next
    interval has started and the old book is stale/gone. Uses the sources that
    remain reliable for a CLOSED market:
      1. CLOB /price REST  (the resolved token price)
      2. Gamma API         (definitive outcomePrices)

    Returns (won-from-our-side, source). `won` is already mapped to the side we
    held via `direction`, so it is directly comparable to the resolved `won`.
    """
    # 1. CLOB /price REST
    try:
        data = await http.get(
            f"{settings.clob_api}/price",
            params={"token_id": token_id, "side": "BUY"},
        )
        if data and "price" in data:
            p = float(data["price"])
            if p >= TOKEN_WIN_HI:
                return True, "clob_price"
            if p <= TOKEN_LOSE_LO:
                return False, "clob_price"
    except Exception:
        pass
    # 2. Gamma (definitive for closed markets)
    up_won = await gamma_outcome(slug, http, settings)
    if up_won is not None:
        ours = up_won if direction == "UP" else (not up_won)
        return ours, "gamma"
    return None, "none"
