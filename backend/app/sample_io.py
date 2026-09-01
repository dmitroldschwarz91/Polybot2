"""
Shared interval-sample log I/O — compact TSV with automatic JSONL fallback.

The demo writes demo_interval_samples as TSV (one header line `# ...`, then
tab-separated rows) to slash the file size vs JSONL (no repeated keys per row).
This module is the single reader/writer so all analysis scripts read BOTH the
new TSV and any old JSONL files unchanged.

Field names are identical to the legacy JSONL, so scripts that key on
"secs_to_close" / "oracle_price" / "up_ask" / ... keep working.
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

# Canonical column order. Keep names == legacy JSONL keys for compatibility.
FIELDS: List[str] = [
    "ts", "asset", "slug", "secs_to_close", "oracle_price", "oracle_age",
    "deviation_pct", "range5", "vwap", "twap", "twap_age", "twap_open",
    "deviation_twap_pct", "leader_token_price", "bid_volume", "ask_volume",
    "book_alive", "imbalance", "pair_ask_sum",
    "up_ask", "up_bid", "up_ask_vol", "up_ask_size", "up_ask_size_at", "up_bq",
    "dn_ask", "dn_bid", "dn_ask_vol", "dn_ask_size", "dn_ask_size_at", "dn_bq",
]
_STR = {"asset", "slug", "up_bq", "dn_bq"}   # everything else numeric/bool
_BOOL = {"book_alive"}


def _conv(field: str, val: str):
    if val is None or val == "":
        return None
    if field in _BOOL:
        return val in ("1", "true", "True")
    if field in _STR:
        return val
    try:
        f = float(val)
        return int(f) if f.is_integer() and field in ("ts", "secs_to_close") else f
    except ValueError:
        return val


def load_samples(path) -> Dict[str, List[dict]]:
    """Load interval samples (TSV or JSONL, auto-detected) -> {slug: [sample,...]}
    sorted by ts. Drop-in replacement for the per-script `load()` functions."""
    path = Path(path)
    by: Dict[str, List[dict]] = defaultdict(list)
    if not path.exists():
        return by
    with path.open(encoding="utf-8") as f:
        first = f.readline()
        if not first:
            return by
        first = first.rstrip("\r\n")
        if first.startswith("#"):
            header = first.lstrip("# ").split("\t")
            for line in f:
                line = line.rstrip("\r\n")
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                rec = {header[i]: _conv(header[i], parts[i])
                       for i in range(min(len(header), len(parts)))}
                slug = rec.get("slug")
                if slug:
                    by[slug].append(rec)
        else:
            # legacy JSONL
            for raw in [first, *f]:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    rec = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                slug = rec.get("slug")
                if slug:
                    by[slug].append(rec)
    for s in by:
        by[s].sort(key=lambda x: (x.get("ts") or 0))
    return by


def append_sample(path, rec: dict) -> None:
    """Append one sample dict as a TSV row; writes the header if the file is new."""
    path = Path(path)
    new_file = (not path.exists()) or path.stat().st_size == 0
    try:
        with path.open("a", encoding="utf-8") as f:
            if new_file:
                f.write("# " + "\t".join(FIELDS) + "\n")
            row = []
            for k in FIELDS:
                v = rec.get(k)
                if v is None:
                    row.append("")
                elif isinstance(v, bool):
                    row.append("1" if v else "0")
                else:
                    row.append(str(v))
            f.write("\t".join(row) + "\n")
    except OSError:
        pass
