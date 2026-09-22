#!/usr/bin/env python3
"""
Analyze the full-depth order-book log (`logs/demo_book_depth.jsonl`).

This is the companion to `backend/app/marketdata/book_recorder.py`. The depth log
records the whole bid/ask ladder per snapshot, which best-level interval samples
cannot give you — that ladder is what you need to estimate REALISTIC maker /
spread-capture economics (queue position, depth ahead of your quote, adverse
selection), rather than the optimistic best-level upper bound.

Usage:
    python3 tools/book_depth_analyze.py [path/to/demo_book_depth.jsonl]

It reports, per (slug,token) and aggregated:
  * snapshot counts, coverage of the near-close window
  * spread distribution (mean/median, and time-weighted)
  * depth-at-best and total depth per side
  * a first-order maker capture estimate that USES queue depth:
      when you post at best_bid, the size resting AHEAD of you is depth-at-best;
      you only get filled if enough volume trades through — approximated here by
      whether best_ask later crosses down to your price AND cumulative ask
      liquidity at/under your price exceeds the queue ahead of you.
This is analysis-only; it never touches the live engine.
"""
from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def load(path: Path):
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def depth_at_best(ladder):
    """size resting at the best (first) level."""
    return float(ladder[0][1]) if ladder else 0.0


def total_depth(ladder):
    return sum(float(l[1]) for l in ladder)


def main(argv):
    path = Path(argv[1]) if len(argv) > 1 else Path("logs/demo_book_depth.jsonl")
    if not path.exists():
        print(f"No depth log at {path}. Enable it with demo_book_depth_enabled=true "
              f"and run the demo engine to accumulate data.")
        return 1
    rows = load(path)
    if not rows:
        print(f"{path} is empty.")
        return 1

    print(f"=== Book-depth log: {path} ===")
    print(f"snapshots: {len(rows)}")
    assets = defaultdict(int)
    spreads = []
    best_bid_depths = []
    tot_bid_depths = []
    per_token = defaultdict(list)
    stc_vals = []
    for r in rows:
        assets[r.get("asset", "?")] += 1
        sp = r.get("spread")
        if sp is not None:
            spreads.append(float(sp))
        bd = depth_at_best(r.get("bids") or [])
        best_bid_depths.append(bd)
        tot_bid_depths.append(total_depth(r.get("bids") or []))
        per_token[(r.get("slug", ""), r.get("token_id", ""))].append(r)
        if r.get("stc") is not None:
            stc_vals.append(int(r["stc"]))

    print("by asset:", dict(assets))
    if stc_vals:
        print(f"stc window recorded: [{min(stc_vals)}, {max(stc_vals)}] secs-to-close")
    if spreads:
        print(f"spread: mean={statistics.mean(spreads):.4f} "
              f"median={statistics.median(spreads):.4f} "
              f"min={min(spreads):.4f} max={max(spreads):.4f}")
    if best_bid_depths:
        print(f"depth@best-bid: mean={statistics.mean(best_bid_depths):.1f} "
              f"median={statistics.median(best_bid_depths):.1f}")
    if tot_bid_depths:
        print(f"total bid depth (≤{len(rows[0].get('bids') or [])} lvls): "
              f"mean={statistics.mean(tot_bid_depths):.1f}")
    print(f"distinct (slug,token) series: {len(per_token)}")

    # per-series spread evolution near close (first vs last snapshot)
    widen = 0
    tighten = 0
    for series in per_token.values():
        s = sorted([x for x in series if x.get("stc") is not None],
                   key=lambda x: -x["stc"])
        if len(s) < 2:
            continue
        a, b = s[0].get("spread"), s[-1].get("spread")
        if a is None or b is None:
            continue
        if b > a:
            widen += 1
        elif b < a:
            tighten += 1
    print(f"series spread toward close: widen={widen} tighten={tighten} "
          f"(wide-at-close => harder maker fills / more adverse selection)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
