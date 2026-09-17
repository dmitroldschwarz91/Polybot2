#!/usr/bin/env python3
"""Walk-forward analysis for demo-mode Polymarket interval logs.

This is an offline research script. It parses logs/demo_interval_samples_part*.xml
(TSV chunks; later chunks may not include the header), validates outcomes by the
cross-validation cascade documented in the project, and runs a rolling
walk-forward grid search for TWAP Inertia parameters per asset.

Gamma API is intentionally optional: pass --gamma-cache reports/gamma_outcomes.json
with a JSON mapping {slug: true/false} for disputed intervals. The sandbox's
curl/Python TLS may be blocked for gamma-api.polymarket.com, so this script does
not fetch the network itself; disputed unresolved labels are excluded unless the
cache supplies them.
"""
from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ASSETS = ("BTC", "ETH", "SOL", "XRP")
STR_FIELDS = {"asset", "slug", "up_bq", "dn_bq"}
BOOL_FIELDS = {"book_alive"}
DEFAULT_HEADER = [
    "ts", "asset", "slug", "secs_to_close", "oracle_price", "oracle_age",
    "deviation_pct", "range5", "vwap", "twap", "twap_age", "twap_open",
    "deviation_twap_pct", "leader_token_price", "bid_volume", "ask_volume",
    "book_alive", "imbalance", "pair_ask_sum",
    "up_ask", "up_bid", "up_ask_vol", "up_ask_size", "up_ask_size_at", "up_bq",
    "dn_ask", "dn_bid", "dn_ask_vol", "dn_ask_size", "dn_ask_size_at", "dn_bq",
]


def _conv(field: str, val: str) -> Any:
    if val is None or val == "":
        return None
    if field in BOOL_FIELDS:
        return val in ("1", "true", "True")
    if field in STR_FIELDS:
        return val
    try:
        f = float(val)
        return int(f) if f.is_integer() and field in ("ts", "secs_to_close") else f
    except (TypeError, ValueError):
        return val


def parse_sample_parts(log_dir: Path) -> Dict[str, List[dict]]:
    header_path = log_dir / "Headers.txt"
    if header_path.exists() and header_path.read_text(encoding="utf-8").strip():
        fallback_header = header_path.read_text(encoding="utf-8").lstrip("# ").strip().split("\t")
    else:
        fallback_header = DEFAULT_HEADER

    by_slug: Dict[str, List[dict]] = defaultdict(list)
    for path in sorted(log_dir.glob("demo_interval_samples_part*.xml")):
        with path.open(encoding="utf-8") as f:
            first = f.readline().rstrip("\r\n")
            if not first:
                continue
            if first.startswith("#"):
                header = first.lstrip("# ").split("\t")
            else:
                # Chunks created by simple file split may begin mid-TSV without a header.
                header = fallback_header
                parts = first.split("\t")
                rec = {header[i]: _conv(header[i], parts[i]) for i in range(min(len(header), len(parts)))}
                if rec.get("slug"):
                    by_slug[rec["slug"]].append(rec)
            for line in f:
                line = line.rstrip("\r\n")
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                rec = {header[i]: _conv(header[i], parts[i]) for i in range(min(len(header), len(parts)))}
                if rec.get("slug"):
                    by_slug[rec["slug"]].append(rec)
    for slug in by_slug:
        by_slug[slug].sort(key=lambda r: (r.get("ts") or 0, -(r.get("secs_to_close") or 0)))
    return dict(by_slug)


def slug_epoch(slug: str) -> Optional[int]:
    m = re.search(r"-(\d{10})$", slug)
    return int(m.group(1)) if m else None


def dt(ts: Optional[int]) -> str:
    if not ts:
        return "n/a"
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def implied_spot_open(r: dict) -> Optional[float]:
    op = r.get("oracle_price")
    dev = r.get("deviation_pct")
    if op and dev is not None:
        denom = 1.0 + float(dev) / 100.0
        if denom > 0:
            return float(op) / denom
    return r.get("twap_open")


def sane_twap_row(r: dict, tol: float = 0.02) -> bool:
    """Reject the obvious Chainlink-TWAP scale/asset glitches in the demo log.

    In a 60s crypto TWAP window, an official TWAP can differ from spot, but a
    20-90% mismatch against the simultaneously logged oracle price/open is a
    feed/decoder issue, not market movement. The 2% tolerance is deliberately
    wide for 1-minute crypto moves and catches the pathologies seen in logs.
    """
    op = r.get("oracle_price")
    tw = r.get("twap")
    tpo = r.get("twap_open")
    dev = r.get("deviation_twap_pct")
    if not (op and tw and tpo and dev is not None and op > 0 and tw > 0 and tpo > 0):
        return False
    spot_open = implied_spot_open(r)
    if not (spot_open and spot_open > 0):
        return False
    return abs(float(tw) / float(op) - 1.0) <= tol and abs(float(tpo) / float(spot_open) - 1.0) <= tol


def token_outcome(rs: List[dict]) -> Tuple[Optional[bool], str, Optional[dict]]:
    """Resolve UP-won? from late token polarization near interval close."""
    for lim in (3, 5, 10, 20, 60):
        cand = [
            r for r in rs
            if r.get("secs_to_close") is not None
            and 0 <= r["secs_to_close"] <= lim
            and r.get("up_ask") is not None
            and r.get("dn_ask") is not None
        ]
        if cand:
            break
    else:
        return None, "no_token", None
    cand = sorted(cand, key=lambda r: r.get("ts") or 0)[-5:]
    votes: List[bool] = []
    for r in cand:
        ua, da, ub, db = r.get("up_ask"), r.get("dn_ask"), r.get("up_bid"), r.get("dn_bid")
        if ua is None or da is None:
            continue
        if ua >= 0.95 and da <= 0.10:
            votes.append(True)
        elif da >= 0.95 and ua <= 0.10:
            votes.append(False)
        elif ub is not None and db is not None:
            if ub >= 0.90 and da <= 0.15:
                votes.append(True)
            elif db >= 0.90 and ua <= 0.15:
                votes.append(False)
    if votes:
        # Majority over the last few samples; ties cannot happen with len>0 bool sum.
        return (sum(1 for v in votes if v) >= math.ceil(len(votes) / 2)), "token_polar", cand[-1]
    return None, "ambig_token", cand[-1]


def twap_outcome(rs: List[dict], tol: float = 0.02) -> Tuple[Optional[bool], str, Optional[dict]]:
    for lim in (3, 5, 10):
        cand = [
            r for r in rs
            if r.get("secs_to_close") is not None
            and 0 <= r["secs_to_close"] <= lim
            and sane_twap_row(r, tol=tol)
        ]
        if cand:
            break
    else:
        return None, "no_sane_twap", None
    r = max(cand, key=lambda r: r.get("ts") or 0)
    dev = r.get("deviation_twap_pct")
    if dev is None or abs(float(dev)) < 0.02:  # project fallback threshold
        return None, "small_twap", r
    return (float(dev) >= 0), "twap", r


@dataclass
class Label:
    slug: str
    asset: str
    interval_ts: int
    up_won: Optional[bool]
    method: str
    token_up_won: Optional[bool]
    twap_up_won: Optional[bool]
    needs_gamma: bool
    gamma_used: bool = False


def build_labels(by_slug: Dict[str, List[dict]], gamma_cache: Dict[str, bool], twap_tol: float,
                 cross_disagree_fallback: str = "exclude") -> Dict[str, Label]:
    labels: Dict[str, Label] = {}
    for slug, rs in by_slug.items():
        if not rs:
            continue
        asset = rs[0].get("asset") or "?"
        interval_ts = slug_epoch(slug) or int(min((r.get("ts") or 0) for r in rs) // 300 * 300)
        tok, tok_m, _ = token_outcome(rs)
        tw, tw_m, _ = twap_outcome(rs, tol=twap_tol)
        method = "unresolved"
        up: Optional[bool] = None
        needs_gamma = False
        gamma_used = False
        if tok is not None and tw is not None:
            if tok == tw:
                method = "cross_ok"
                up = tok
            else:
                method = "cross_disagree"
                needs_gamma = True
        elif tok is not None:
            method = "token_only"
            up = tok
        elif tw is not None:
            method = "twap_only"
            up = tw
            needs_gamma = True
        else:
            method = "unresolved"
            needs_gamma = True

        if needs_gamma and slug in gamma_cache:
            up = bool(gamma_cache[slug])
            method = f"gamma_from_{method}"
            gamma_used = True
            needs_gamma = False
        elif method == "cross_disagree" and cross_disagree_fallback == "token" and tok is not None:
            # Sensitivity mode: if Gamma cannot be fetched in this environment,
            # use the market's own post-close token polarization as the payout
            # proxy rather than optimistically dropping disputed intervals.
            up = tok
            method = "token_from_cross_disagree"
            needs_gamma = False
        elif method == "cross_disagree" and cross_disagree_fallback == "twap" and tw is not None:
            up = tw
            method = "twap_from_cross_disagree"
            needs_gamma = False

        labels[slug] = Label(slug, asset, interval_ts, up, method, tok, tw, needs_gamma, gamma_used)
    return labels


@dataclass(frozen=True)
class Params:
    stc_min: int
    stc_max: int
    min_dev_pct: float
    min_barrier_pct: float
    min_token_ask: float
    max_token_ask: float
    full_book: bool

    def short(self) -> str:
        return (f"T-{self.stc_min}..{self.stc_max} dev≥{self.min_dev_pct:.3f}% "
                f"B≥{self.min_barrier_pct:.3f}% ask∈[{self.min_token_ask:.2f},{self.max_token_ask:.2f}] "
                f"full={int(self.full_book)}")


@dataclass
class Entry:
    slug: str
    asset: str
    interval_ts: int
    ts: int
    stc: int
    direction: str
    ask: float
    fill_price: float
    depth: Optional[float]
    barrier_pct: float
    dev_twap_pct: float
    won: bool
    outcome_method: str


@dataclass
class SimTrade:
    slug: str
    interval_ts: int
    entry_price: float
    shares: int
    pnl: float
    won: bool
    capital_after: float
    params: str


@dataclass
class SimResult:
    final_capital: float
    return_pct: float
    max_drawdown_pct: float
    n_trades: int
    n_wins: int
    n_losses: int
    win_rate: float
    pnl: float
    trades: List[SimTrade]

    def score(self) -> float:
        if self.n_trades == 0:
            return -999.0
        return self.return_pct - 0.3 * self.max_drawdown_pct + min(self.n_trades, 15)


def row_depth(r: dict, direction: str, ask: float) -> Optional[float]:
    if direction == "UP":
        size = r.get("up_ask_size_at")
        av = r.get("up_ask_vol")
    else:
        size = r.get("dn_ask_size_at")
        av = r.get("dn_ask_vol")
    if size is not None:
        return float(size)
    if av is not None and ask > 0:
        return float(av) / ask
    return None


def precompute_rows(by_slug: Dict[str, List[dict]], labels: Dict[str, Label], twap_tol: float) -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = {}
    for slug, rs in by_slug.items():
        lab = labels.get(slug)
        if not lab or lab.up_won is None or lab.needs_gamma:
            continue
        rows = []
        for r in rs:
            stc = r.get("secs_to_close")
            if stc is None or stc <= 0 or stc > 60:
                continue
            if r.get("oracle_age") is not None and float(r["oracle_age"]) > 3.0:
                continue
            if not sane_twap_row(r, tol=twap_tol):
                continue
            dev = float(r.get("deviation_twap_pct"))
            if dev == 0:
                continue
            barrier = abs(dev) * (60.0 - float(stc)) / float(stc)
            direction = "UP" if dev > 0 else "DOWN"
            ask = r.get("up_ask") if direction == "UP" else r.get("dn_ask")
            opp = r.get("dn_ask") if direction == "UP" else r.get("up_ask")
            if ask is None or ask <= 0:
                continue
            ask = float(ask)
            if opp is not None and (0.49 <= ask <= 0.52) and (0.49 <= float(opp) <= 0.52):
                continue
            bq = r.get("up_bq") if direction == "UP" else r.get("dn_bq")
            depth = row_depth(r, direction, ask)
            if depth is not None and depth < 5.0:
                continue
            won = lab.up_won if direction == "UP" else (not lab.up_won)
            rows.append({
                "slug": slug,
                "asset": lab.asset,
                "interval_ts": lab.interval_ts,
                "ts": int(r.get("ts") or 0),
                "stc": int(stc),
                "direction": direction,
                "ask": ask,
                "fill_price": min(0.999, ask + 0.01),
                "depth": depth,
                "barrier_pct": barrier,
                "dev_twap_pct": dev,
                "bq": bq,
                "won": bool(won),
                "outcome_method": lab.method,
            })
        if rows:
            out[slug] = rows
    return out


def select_entry(rows: List[dict], p: Params) -> Optional[Entry]:
    cand = []
    for r in rows:
        if not (p.stc_min <= r["stc"] <= p.stc_max):
            continue
        if abs(r["dev_twap_pct"]) < p.min_dev_pct:
            continue
        if r["barrier_pct"] < p.min_barrier_pct:
            continue
        if r["ask"] < p.min_token_ask or r["ask"] > p.max_token_ask:
            continue
        if p.full_book and r["bq"] != "full":
            continue
        cand.append(r)
    if not cand:
        return None
    # Project-documented ranking: strongest mathematical barrier, then deviation;
    # use earlier timestamp as tie-breaker to avoid preferring later rows for free.
    cand.sort(key=lambda x: (-x["barrier_pct"], -abs(x["dev_twap_pct"]), x["ts"]))
    r = cand[0]
    return Entry(
        slug=r["slug"], asset=r["asset"], interval_ts=r["interval_ts"], ts=r["ts"], stc=r["stc"],
        direction=r["direction"], ask=r["ask"], fill_price=r["fill_price"], depth=r["depth"],
        barrier_pct=r["barrier_pct"], dev_twap_pct=r["dev_twap_pct"], won=r["won"], outcome_method=r["outcome_method"],
    )


def dynamic_fee_per_share(price: float) -> float:
    return 0.07 * price * (1.0 - price)


def simulate(entries: List[Entry], p: Params, start_capital: float = 100.0,
             stake_ratio: float = 0.20, min_shares: int = 5) -> SimResult:
    cap = start_capital
    peak = cap
    max_dd = 0.0
    trades: List[SimTrade] = []
    for e in sorted(entries, key=lambda x: (x.interval_ts, x.ts)):
        price = e.fill_price
        if cap < min_shares * price:
            continue
        desired_shares = int((cap * stake_ratio) / price)
        shares = max(min_shares, desired_shares)
        if e.depth is not None:
            shares = min(shares, int(e.depth))
        if shares < min_shares:
            continue
        # Fit to available capital including taker fee.
        while shares >= min_shares:
            fee_ps = dynamic_fee_per_share(price)
            cost = shares * (price + fee_ps)
            if cost <= cap + 1e-9:
                break
            shares -= 1
        if shares < min_shares:
            continue
        fee_ps = dynamic_fee_per_share(price)
        pnl_ps = (1.0 - price - fee_ps) if e.won else (-price - fee_ps)
        pnl = shares * pnl_ps
        cap += pnl
        peak = max(peak, cap)
        dd = (peak - cap) / peak * 100.0 if peak > 0 else 100.0
        max_dd = max(max_dd, dd)
        trades.append(SimTrade(e.slug, e.interval_ts, price, shares, round(pnl, 4), e.won, round(cap, 4), p.short()))
    wins = sum(1 for t in trades if t.won)
    losses = len(trades) - wins
    return SimResult(
        final_capital=round(cap, 4),
        return_pct=round((cap / start_capital - 1.0) * 100.0, 2),
        max_drawdown_pct=round(max_dd, 2),
        n_trades=len(trades),
        n_wins=wins,
        n_losses=losses,
        win_rate=round(wins / len(trades), 4) if trades else 0.0,
        pnl=round(cap - start_capital, 4),
        trades=trades,
    )


def param_grid() -> List[Params]:
    # Search space combines the project's default twap_inertia grid and the
    # strategy-methodology grids documented in KEY_FINDINGS_AND_METHODS.md.
    windows = [(10, 35), (2, 30), (2, 7), (2, 12), (2, 15), (2, 60)]
    min_devs = [0.015, 0.02, 0.03, 0.04, 0.05]
    min_bars = [0.05, 0.07, 0.08, 0.10]
    min_asks = [0.50, 0.52, 0.55, 0.60, 0.65]
    max_asks = [0.90, 0.92, 0.94, 0.96]
    fulls = [False, True]
    out = []
    for (lo, hi), md, mb, mn, mx, fb in itertools.product(windows, min_devs, min_bars, min_asks, max_asks, fulls):
        if mn <= mx:
            out.append(Params(lo, hi, md, mb, mn, mx, fb))
    return out


def entries_for_params(slugs: Iterable[str], pre_rows: Dict[str, List[dict]], p: Params) -> List[Entry]:
    out = []
    for slug in slugs:
        rows = pre_rows.get(slug)
        if not rows:
            continue
        e = select_entry(rows, p)
        if e is not None:
            out.append(e)
    return out


@dataclass
class FoldSummary:
    i: int
    train_start: int
    train_end: int
    test_start: int
    test_end: int
    best_params: Params
    train: Dict[str, Any]
    test: Dict[str, Any]


def run_wfo_for_asset(asset: str, by_slug: Dict[str, List[dict]], pre_rows: Dict[str, List[dict]],
                      labels: Dict[str, Label], train_intervals: int, test_intervals: int,
                      start_capital: float, stake_ratio: float) -> Dict[str, Any]:
    all_slugs = sorted([s for s, rs in by_slug.items() if rs and rs[0].get("asset") == asset], key=lambda s: slug_epoch(s) or 0)
    good_slugs = [s for s in all_slugs if s in pre_rows and labels.get(s) and labels[s].up_won is not None and not labels[s].needs_gamma]
    epochs = [slug_epoch(s) or labels[s].interval_ts for s in all_slugs]
    folds: List[FoldSummary] = []
    grid = param_grid()
    if len(all_slugs) < train_intervals + test_intervals:
        return {"asset": asset, "error": "not_enough_intervals", "n_intervals": len(all_slugs)}

    # Build contiguous folds by the chronological full interval list. Some labels
    # inside a fold may be excluded by quality/gamma filters; this mirrors reality.
    starts = range(0, len(all_slugs) - train_intervals - test_intervals + 1, test_intervals)
    oos_trades: List[SimTrade] = []
    for idx, start in enumerate(starts):
        tr_slugs = all_slugs[start:start + train_intervals]
        te_slugs = all_slugs[start + train_intervals:start + train_intervals + test_intervals]
        best_p: Optional[Params] = None
        best_train: Optional[SimResult] = None
        best_score = -1e18
        for p in grid:
            entries = entries_for_params(tr_slugs, pre_rows, p)
            r = simulate(entries, p, start_capital=start_capital, stake_ratio=stake_ratio)
            sc = r.score()
            # avoid one-trade overfit when possible
            if r.n_trades < 2:
                sc -= 10.0
            if sc > best_score:
                best_score, best_p, best_train = sc, p, r
        if best_p is None or best_train is None or best_train.n_trades == 0:
            # No train signal means there is no parameter estimate for this fold.
            # Do not count an arbitrary first-grid combo in parameter frequency.
            continue
        test_entries = entries_for_params(te_slugs, pre_rows, best_p)
        test_r = simulate(test_entries, best_p, start_capital=start_capital, stake_ratio=stake_ratio)
        oos_trades.extend(test_r.trades)
        folds.append(FoldSummary(
            i=idx,
            train_start=slug_epoch(tr_slugs[0]) or 0,
            train_end=(slug_epoch(tr_slugs[-1]) or 0) + 300,
            test_start=slug_epoch(te_slugs[0]) or 0,
            test_end=(slug_epoch(te_slugs[-1]) or 0) + 300,
            best_params=best_p,
            train={k: v for k, v in asdict(best_train).items() if k != "trades"},
            test={k: v for k, v in asdict(test_r).items() if k != "trades"},
        ))

    # Sequential OOS capital using each fold's selected trades (dedupe overlap not
    # needed: step == test length, test windows are non-overlapping).
    cap = start_capital
    peak = cap
    max_dd = 0.0
    seq_trades = []
    for t in sorted(oos_trades, key=lambda x: (x.interval_ts, x.slug)):
        cap += t.pnl
        peak = max(peak, cap)
        max_dd = max(max_dd, (peak - cap) / peak * 100.0 if peak else 100.0)
        t.capital_after = round(cap, 4)
        seq_trades.append(t)
    wins = sum(1 for t in seq_trades if t.won)
    freq: Dict[str, Counter] = defaultdict(Counter)
    for f in folds:
        p = f.best_params
        freq["stc_window"][f"{p.stc_min}-{p.stc_max}"] += 1
        freq["min_dev_pct"][p.min_dev_pct] += 1
        freq["min_barrier_pct"][p.min_barrier_pct] += 1
        freq["min_token_ask"][p.min_token_ask] += 1
        freq["max_token_ask"][p.max_token_ask] += 1
        freq["full_book"][p.full_book] += 1
    consensus = {k: c.most_common(1)[0][0] for k, c in freq.items() if c}
    return {
        "asset": asset,
        "n_intervals": len(all_slugs),
        "n_quality_labeled_intervals": len(good_slugs),
        "range": [dt(slug_epoch(all_slugs[0]) if all_slugs else None), dt((slug_epoch(all_slugs[-1]) or 0) + 300 if all_slugs else None)],
        "train_intervals": train_intervals,
        "test_intervals": test_intervals,
        "folds": [
            {
                "i": f.i,
                "train": [dt(f.train_start), dt(f.train_end)],
                "test": [dt(f.test_start), dt(f.test_end)],
                "best_params": asdict(f.best_params),
                "train_metrics": f.train,
                "test_metrics": f.test,
            } for f in folds
        ],
        "param_frequency": {k: {str(kk): vv for kk, vv in c.items()} for k, c in freq.items()},
        "consensus_params": {k: str(v) for k, v in consensus.items()},
        "oos": {
            "start_capital": start_capital,
            "final_capital": round(cap, 4),
            "return_pct": round((cap / start_capital - 1) * 100, 2),
            "max_drawdown_pct": round(max_dd, 2),
            "n_trades": len(seq_trades),
            "n_wins": wins,
            "n_losses": len(seq_trades) - wins,
            "win_rate": round(wins / len(seq_trades), 4) if seq_trades else 0.0,
            "pnl": round(cap - start_capital, 4),
        },
        "oos_trades": [asdict(t) for t in seq_trades],
    }


def quality_report(by_slug: Dict[str, List[dict]], labels: Dict[str, Label], pre_rows: Dict[str, List[dict]], twap_tol: float) -> Dict[str, Any]:
    rows = [r for rs in by_slug.values() for r in rs]
    by_asset_slugs: Dict[str, List[str]] = defaultdict(list)
    for slug, rs in by_slug.items():
        if rs:
            by_asset_slugs[rs[0].get("asset")].append(slug)
    asset_reports = {}
    for asset in ASSETS:
        slugs = sorted(by_asset_slugs.get(asset, []), key=lambda s: slug_epoch(s) or 0)
        ar = [r for s in slugs for r in by_slug[s]]
        entry_rows = [r for r in ar if r.get("secs_to_close") is not None and 10 <= r["secs_to_close"] <= 35]
        final60_ok = 0
        entry_ok = 0
        book_any = 0
        sane_entry_intervals = 0
        token_labels = 0
        for s in slugs:
            rs = by_slug[s]
            if sum(1 for r in rs if r.get("secs_to_close") is not None and 0 <= r["secs_to_close"] <= 60) >= 20:
                final60_ok += 1
            if sum(1 for r in rs if r.get("secs_to_close") is not None and 10 <= r["secs_to_close"] <= 35) >= 8:
                entry_ok += 1
            if any(r.get("book_alive") is True for r in rs):
                book_any += 1
            er = [r for r in rs if r.get("secs_to_close") is not None and 10 <= r["secs_to_close"] <= 35]
            if er and sum(1 for r in er if sane_twap_row(r, tol=twap_tol)) / len(er) >= 0.80:
                sane_entry_intervals += 1
            if labels.get(s) and labels[s].token_up_won is not None:
                token_labels += 1
        lab_counts = Counter(labels[s].method for s in slugs if s in labels)
        asset_reports[asset] = {
            "rows": len(ar),
            "intervals": len(slugs),
            "range": [dt(slug_epoch(slugs[0]) if slugs else None), dt((slug_epoch(slugs[-1]) or 0) + 300 if slugs else None)],
            "rows_per_interval_min_median_max": [
                min((len(by_slug[s]) for s in slugs), default=0),
                float(sorted([len(by_slug[s]) for s in slugs])[len(slugs)//2]) if slugs else 0,
                max((len(by_slug[s]) for s in slugs), default=0),
            ],
            "final60_coverage_ok": final60_ok,
            "entry_window_coverage_ok": entry_ok,
            "book_any_interval": book_any,
            "token_labeled": token_labels,
            "sane_entry_twap_intervals": sane_entry_intervals,
            "label_methods": dict(lab_counts),
            "needs_gamma_after_cache": sum(1 for s in slugs if labels.get(s) and labels[s].needs_gamma),
            "usable_for_wfo": sum(1 for s in slugs if s in pre_rows),
        }
    return {
        "total_rows": len(rows),
        "total_intervals": len(by_slug),
        "assets": asset_reports,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log-dir", type=Path, default=Path("logs"))
    ap.add_argument("--gamma-cache", type=Path, default=Path("reports/gamma_outcomes.json"))
    ap.add_argument("--out", type=Path, default=Path("reports/demo_wfo_results.json"))
    ap.add_argument("--train", type=int, default=144, help="train intervals per fold")
    ap.add_argument("--test", type=int, default=48, help="test intervals per fold")
    ap.add_argument("--capital", type=float, default=100.0)
    ap.add_argument("--stake", type=float, default=0.20)
    ap.add_argument("--twap-tol", type=float, default=0.02)
    ap.add_argument(
        "--cross-disagree-fallback",
        choices=["exclude", "token", "twap"],
        default="exclude",
        help="What to do when token and sane TWAP labels disagree and no Gamma cache value exists.",
    )
    args = ap.parse_args()

    by_slug = parse_sample_parts(args.log_dir)
    gamma_cache: Dict[str, bool] = {}
    if args.gamma_cache.exists():
        gamma_cache = json.loads(args.gamma_cache.read_text(encoding="utf-8"))
        gamma_cache = {str(k): bool(v) for k, v in gamma_cache.items()}
    labels = build_labels(
        by_slug, gamma_cache, twap_tol=args.twap_tol,
        cross_disagree_fallback=args.cross_disagree_fallback,
    )
    pre_rows = precompute_rows(by_slug, labels, twap_tol=args.twap_tol)
    q = quality_report(by_slug, labels, pre_rows, twap_tol=args.twap_tol)
    results = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "notes": {
            "selection": "per interval: max barrier_pct, then max abs(dev_twap_pct), then earliest timestamp",
            "fees": "Polymarket crypto dynamic taker fee per share = 0.07*p*(1-p), charged at entry in both win and loss cases",
            "fill": "best ask + 0.01 slippage, capped at 0.999; depth capped by logged best ask size/ask volume",
            "quality_filter": f"entry/resolution TWAP rows require twap and twap_open within {args.twap_tol:.0%} of concurrently logged oracle/open",
            "gamma_cache": str(args.gamma_cache),
            "gamma_cache_size": len(gamma_cache),
            "cross_disagree_fallback": args.cross_disagree_fallback,
        },
        "quality": q,
        "wfo": {},
        "gamma_needed_slugs": sorted([s for s, lab in labels.items() if lab.needs_gamma]),
    }
    for asset in ASSETS:
        results["wfo"][asset] = run_wfo_for_asset(
            asset, by_slug, pre_rows, labels,
            train_intervals=args.train, test_intervals=args.test,
            start_capital=args.capital, stake_ratio=args.stake,
        )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {args.out}")
    print(json.dumps({
        "quality": q,
        "wfo_oos": {a: results["wfo"][a].get("oos") for a in ASSETS},
        "gamma_needed": len(results["gamma_needed_slugs"]),
    }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
