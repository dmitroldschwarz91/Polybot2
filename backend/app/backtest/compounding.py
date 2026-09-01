"""
Fast compounding simulator — the engine behind compounding-aware optimization.

Unlike BacktestEngine (which runs the full strategy loop per tick), this works
directly on the pre-computed trade-level entry opportunities and simulates
position sizing + P&L iteratively: each trade's size depends on CURRENT capital
(after all prior trades), not the starting capital.

This is the model the previous standalone analysis used, now productionised so
the walk-forward optimizer can search over threshold × stake ratio with it.

Why this matters (the core insight):
  * Fixed-capital models sum independent P&L → hide volatility drag.
  * Compounding models amplify losses: at 72% stake ratio, one loss at peak
    capital erases 8 wins. Monte Carlo over trade orderings captures this.
  * The min order (5 shares) interacts with small capital — at $15 and price
    0.80, min order = $4 = 27% of capital, so "10% stake" is impossible below
    a capital floor.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .poly_fetcher import BacktestDataset, IntervalMeta


# ── constants matching Polymarket ──
MIN_SHARES = 5      # limit order minimum (in shares)
MARKET_ORDER_MIN_USD = 1.0
FEE = 0.02           # Polymarket taker fee on winning side


@dataclass
class TradeOutcome:
    """One resolved trade in a compounding simulation."""
    interval_ts: int
    entry_price: float
    shares: int
    stake_ratio_actual: float     # actual fraction of capital used (may exceed target due to min order)
    pnl: float
    won: bool
    capital_after: float


@dataclass
class CompoundingResult:
    """Outcome of one compounding run over a sequence of trades."""
    final_capital: float
    peak_capital: float
    trough_capital: float
    max_drawdown_pct: float
    return_pct: float
    n_trades: int
    n_wins: int
    n_losses: int
    ruined: bool                  # capital fell below min-order threshold
    avg_stake_ratio: float
    win_rate: float
    trades: List[TradeOutcome] = field(default_factory=list)

    @property
    def sharpe(self) -> float:
        """Per-trade Sharpe (annualisation irrelevant for comparison)."""
        if self.n_trades < 2:
            return 0.0
        pnls = [t.pnl / max(0.01, t.capital_after - t.pnl) for t in self.trades]
        mean = sum(pnls) / len(pnls)
        var = sum((p - mean) ** 2 for p in pnls) / len(pnls)
        std = math.sqrt(var)
        return (mean / std * math.sqrt(self.n_trades)) if std > 0 else 0.0

    def to_metrics_dict(self) -> dict:
        return {
            "final_capital": round(self.final_capital, 2),
            "return_pct": round(self.return_pct, 1),
            "max_drawdown_pct": round(self.max_drawdown_pct, 1),
            "n_trades": self.n_trades,
            "win_rate": round(self.win_rate, 3),
            "sharpe": round(self.sharpe, 3),
            "ruined": self.ruined,
            "avg_stake_ratio": round(self.avg_stake_ratio, 3),
        }


@dataclass
class MonteCarloResult:
    """Aggregate over many shuffled compounding runs."""
    median_final: float
    p5_final: float
    p95_final: float
    median_return_pct: float
    median_max_dd: float
    ruin_probability: float
    n_runs: int

    def to_dict(self) -> dict:
        return {
            "median_final": round(self.median_final, 2),
            "p5_final": round(self.p5_final, 2),
            "p95_final": round(self.p95_final, 2),
            "median_return_pct": round(self.median_return_pct, 1),
            "median_max_dd": round(self.median_max_dd, 1),
            "ruin_probability": round(self.ruin_probability, 3),
            "n_runs": self.n_runs,
        }


# ── minimum-order constraints (critical for small capital) ────────────────

def min_achievable_stake_ratio(capital: float, price: float, min_shares: int = MIN_SHARES) -> float:
    """The LOWEST stake ratio actually achievable at this capital + price.

    Polymarket forces 5 shares min. At $15 and price 0.75, that's $3.75 = 25%
    of capital. You CANNOT stake less — any 'desired sr' below it is silently
    forced UP. This is why 'optimal sr=0.10' at $15 is impossible.
    """
    if capital <= 0 or price <= 0:
        return 1.0
    return (min_shares * price) / capital


def effective_stake_ratio(capital: float, price: float, desired_sr: float,
                          min_shares: int = MIN_SHARES) -> Tuple[float, bool]:
    """Returns (actual_sr, was_forced_up)."""
    floor = min_achievable_stake_ratio(capital, price, min_shares)
    if desired_sr < floor:
        return floor, True
    shares = int((capital * desired_sr) / price)
    actual = (shares * price) / capital
    return actual, False


def deduplicate_stake_ratios(
    capital: float, price: float, desired_srs: List[float], min_shares: int = MIN_SHARES,
) -> List[Tuple[float, float, bool]]:
    """Collapse a desired-sr grid into its DISTINCT actual behaviors.

    Returns list of (desired_sr, actual_sr, was_forced). Prevents the
    optimizer from 'choosing' between sr values that behave identically.
    """
    seen_actual = set()
    out = []
    for sr in sorted(desired_srs):
        actual, forced = effective_stake_ratio(capital, price, sr, min_shares)
        key = round(actual, 4)
        if key not in seen_actual:
            seen_actual.add(key)
            out.append((sr, actual, forced))
    return out


# ── entry opportunity extraction ──────────────────────────────────────────

def extract_entry_opportunities(
    dataset: BacktestDataset,
    threshold: float,
    entry_start_secs: int = 150,
    entry_end_secs: int = 30,
    hold_to_resolution: bool = True,
    tp_delta: float = 0.0,
    sl_pct: float = 0.0,
) -> List[dict]:
    """Extract the chronologically-ordered list of trade opportunities that
    fire for a given entry threshold.

    Each entry is a dict {ts, entry_price, won, pnl_per_share} ready for the
    compounding simulator. 'won' and 'pnl_per_share' reflect the chosen exit
    mode (HOLD = resolution; TP/SL = walk the price path).
    """
    entries = []
    for iv in sorted(dataset.intervals, key=lambda i: i.interval_ts):
        if iv.up_won is None:
            continue
        th = dataset.token_history.get(iv.interval_ts, {})
        up_trades = sorted(th.get("up", []))
        down_trades = sorted(th.get("down", []))
        end = iv.end_ts
        ws, we = end - entry_start_secs, end - entry_end_secs
        up_win = [(t, p) for t, p in up_trades if ws <= t <= we]
        dn_win = [(t, p) for t, p in down_trades if ws <= t <= we]
        if not up_win and not dn_win:
            continue
        up_avg = sum(p for _, p in up_win) / len(up_win) if up_win else 0
        dn_avg = sum(p for _, p in dn_win) / len(dn_win) if dn_win else 0
        leader_up = up_avg >= dn_avg
        leader_trades = up_win if leader_up else dn_win
        leader_won = iv.up_won if leader_up else (not iv.up_won)

        # find first trade reaching the threshold
        entry = None
        for t, p in leader_trades:
            if p >= threshold:
                entry = (t, p)
                break
        if entry is None:
            continue
        entry_t, entry_price = entry

        # compute pnl_per_share based on exit mode
        if hold_to_resolution or (tp_delta == 0 and sl_pct == 0):
            pnl = (1.0 - entry_price - FEE) if leader_won else (-entry_price)
        else:
            pnl = _walk_tpsl(
                up_trades if leader_up else down_trades,
                entry_t, entry_price, tp_delta, sl_pct, leader_won,
            )
        entries.append({
            "ts": iv.interval_ts, "entry_price": entry_price,
            "won": leader_won, "pnl_per_share": pnl,
        })
    return entries


def _walk_tpsl(price_path, entry_t, entry_price, tp_delta, sl_pct, leader_won):
    """Walk the price path after entry to find TP/SL hit or resolution."""
    tp_price = min(0.99, entry_price + tp_delta)
    sl_price = entry_price * (1 - sl_pct)
    for t, p in price_path:
        if t <= entry_t:
            continue
        if p >= tp_price:
            sell = p - 0.005  # bid approx
            pnl = sell - entry_price
            return pnl - (sell * FEE if pnl > 0 else 0)
        if p <= sl_price:
            sell = p - 0.005
            return sell - entry_price  # loss, no fee
    return (1.0 - entry_price - FEE) if leader_won else (-entry_price)


# ── compounding simulation ────────────────────────────────────────────────

def simulate_compounding(
    entries: List[dict],
    start_capital: float,
    stake_ratio: float,
    min_shares: int = MIN_SHARES,
) -> CompoundingResult:
    """Run ONE compounding pass over an ordered trade list.

    Each trade: size = max(min_shares, int(capital * stake_ratio / price)).
    Capital then += shares * pnl_per_share.
    """
    cap = start_capital
    peak = start_capital
    trough = start_capital
    ruined = False
    outcomes: List[TradeOutcome] = []
    actual_srs = []

    for e in entries:
        if cap <= 0:
            ruined = True
            break
        price = e["entry_price"]
        if price <= 0:
            continue
        # desired stake
        desired = cap * stake_ratio
        shares = int(desired / price)
        # enforce minimum order
        if shares < min_shares:
            if cap >= min_shares * price:
                shares = min_shares  # forced up by min order
            else:
                continue  # can't afford even the minimum
        cost = shares * price
        if cost > cap:
            shares = max(0, int(cap / price))
            if shares < min_shares:
                continue
            cost = shares * price
        actual_srs.append(cost / cap)
        # NOTE: pnl_per_share already includes -entry_price (it's the full net
        # cash flow: payout - entry - fee). So we do NOT subtract cost again —
        # that would double-count the entry. Just apply the net delta.
        pnl = shares * e["pnl_per_share"]
        cap += pnl
        peak = max(peak, cap)
        trough = min(trough, cap)
        cap_before = cap - pnl  # capital before this trade's net delta
        outcomes.append(TradeOutcome(
            interval_ts=e["ts"], entry_price=price, shares=shares,
            stake_ratio_actual=cost / cap_before if cap_before > 0 else 0,
            pnl=pnl, won=e["won"], capital_after=cap,
        ))

    max_dd = ((peak - trough) / peak * 100) if peak > 0 else 100.0
    wins = sum(1 for o in outcomes if o.won)
    return CompoundingResult(
        final_capital=round(cap, 4),
        peak_capital=round(peak, 4),
        trough_capital=round(trough, 4),
        max_drawdown_pct=max_dd,
        return_pct=round((cap / start_capital - 1) * 100, 2),
        n_trades=len(outcomes),
        n_wins=wins,
        n_losses=len(outcomes) - wins,
        ruined=ruined,
        avg_stake_ratio=round(sum(actual_srs) / len(actual_srs), 4) if actual_srs else 0.0,
        win_rate=round(wins / len(outcomes), 4) if outcomes else 0.0,
        trades=outcomes,
    )


def monte_carlo(
    entries: List[dict],
    start_capital: float,
    stake_ratio: float,
    n_runs: int = 1000,
    seed: int = 42,
    min_shares: int = MIN_SHARES,
) -> MonteCarloResult:
    """Shuffle the trade order n_runs times and run compounding on each.

    The chronological order is one draw from the distribution; Monte Carlo
    over orderings estimates the *distribution* of outcomes and the ruin prob.
    """
    rng = random.Random(seed)
    finals = []
    max_dds = []
    ruins = 0
    for _ in range(n_runs):
        shuffled = entries.copy()
        rng.shuffle(shuffled)
        r = simulate_compounding(shuffled, start_capital, stake_ratio, min_shares)
        finals.append(r.final_capital)
        max_dds.append(r.max_drawdown_pct)
        if r.ruined:
            ruins += 1
    finals.sort()
    max_dds.sort()
    n = len(finals)
    med = finals[n // 2]
    return MonteCarloResult(
        median_final=med,
        p5_final=finals[int(n * 0.05)],
        p95_final=finals[int(n * 0.95)],
        median_return_pct=round((med / start_capital - 1) * 100, 2),
        median_max_dd=max_dds[n // 2],
        ruin_probability=round(ruins / n_runs, 4),
        n_runs=n_runs,
    )


def score_result(r: CompoundingResult, ruin_penalty: float = 5.0) -> float:
    """Ranking score for a compounding result.

    Penalises ruin and deep drawdown; rewards return + enough trades.
    Higher = better.
    """
    if r.n_trades == 0:
        return -999.0
    score = r.return_pct - 0.3 * r.max_drawdown_pct
    score -= ruin_penalty * (1.0 if r.ruined else 0.0)
    score += min(r.n_trades, 15)  # prefer more trades (robustness)
    return score
