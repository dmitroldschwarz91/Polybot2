"""
Walk-forward optimization.

The gold standard for validating strategy parameters without overfitting:
split the real dataset into rolling windows, optimize on each TRAIN window,
then test those parameters on the following (unseen) TEST window. Aggregate
out-of-sample results across all test windows.

If in-sample (train) performance is great but out-of-sample (test) is poor,
the parameters are overfit and will not survive live trading.

We optimize by grid/random search over a small, strategy-relevant parameter
space (SL %, TP delta, stake ratio) — fast enough to run from the dashboard.
"""

from __future__ import annotations

import asyncio
import itertools
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from ..config import Settings
from ..core.logging import StructuredLogger, build_logger
from .data import PriceTick
from .engine import BacktestConfig, BacktestEngine
from .metrics import BacktestMetrics
from .poly_fetcher import BacktestDataset, PolymarketDataFetcher, IntervalMeta
from .compounding import (
    extract_entry_opportunities, simulate_compounding, monte_carlo,
    score_result, CompoundingResult, MonteCarloResult,
    deduplicate_stake_ratios, min_achievable_stake_ratio,
)


# ── parameter search space ────────────────────────────────────────────────

@dataclass
class ParamSpec:
    """One tunable knob and its candidate values."""
    name: str            # matches BacktestConfig / Settings field
    values: list

    def __iter__(self):
        return iter(self.values)


@dataclass
class ParamCombo:
    """A concrete parameter assignment + the backtest metrics it produced."""
    params: Dict[str, float]
    metrics: Optional[BacktestMetrics] = None

    @property
    def score(self) -> float:
        """Single scalar to rank combos. Penalises drawdown + low trade count."""
        m = self.metrics
        if m is None or m.num_trades == 0:
            return -999.0
        # prefer profit, but penalise deep drawdown and require enough trades
        return m.total_return_pct - 0.5 * m.max_drawdown_pct + min(m.num_trades, 10)


@dataclass
class WindowResult:
    """Outcome of one train→test fold."""
    window_index: int
    train_start: int
    train_end: int
    test_start: int
    test_end: int
    best_params: Dict[str, float]
    train_metrics: BacktestMetrics
    test_metrics: BacktestMetrics
    baseline_test_metrics: Optional[BacktestMetrics] = None  # default params

    @property
    def overfit_ratio(self) -> Optional[float]:
        """train_return / test_return. >>1 suggests overfitting."""
        if self.test_metrics.total_return_pct == 0:
            return None
        return self.train_metrics.total_return_pct / self.test_metrics.total_return_pct


@dataclass
class OptimizationResult:
    """Aggregate result across all walk-forward folds."""
    strategy: str
    folds: List[WindowResult] = field(default_factory=list)
    search_space: Dict[str, list] = field(default_factory=dict)
    elapsed_ms: float = 0.0

    def out_of_sample(self) -> BacktestMetrics:
        """Aggregate metrics across ALL test windows (the honest number)."""
        agg_curve = []
        agg_trades = []
        agg_fees = 0.0
        offset = 0.0  # equity offset to chain windows
        capital = 0.0
        for f in self.folds:
            m = f.test_metrics
            if m.num_trades == 0:
                continue
            if capital == 0.0:
                capital = m.final_equity - m.total_pnl if m.total_pnl != 0 else m.final_equity
            # shift this window's equity curve to continue from the offset
            for pt in m.equity_curve:
                agg_curve.append({"ts": pt["ts"],
                                  "equity": round(pt["equity"] + offset, 4)})
            agg_trades.extend(f.test_metrics.trade_pnls)
            agg_fees += m.total_fees
            offset += m.total_pnl
        from .metrics import compute_metrics
        cap = capital if capital > 0 else 7.0
        return compute_metrics(agg_curve, _pnl_to_trade_log(agg_trades), cap, agg_fees)

    def best_params_freq(self) -> Dict[str, Dict[float, int]]:
        """How often each param value was chosen as best across folds."""
        freq: Dict[str, Dict[float, int]] = {}
        for f in self.folds:
            for k, v in f.best_params.items():
                freq.setdefault(k, {})
                freq[k][v] = freq[k].get(v, 0) + 1
        return freq

    def summary(self) -> dict:
        oos = self.out_of_sample()
        is_total = sum(f.train_metrics.total_return_pct for f in self.folds)
        # out-of-sample return: prefer the reconstructed aggregate, but fall back
        # to the direct sum of per-fold test returns when equity curves are absent
        # (e.g. hand-built metrics in tests).
        os_total = oos.total_return_pct
        if os_total == 0:
            os_total = sum(f.test_metrics.total_return_pct for f in self.folds)
        freq = self.best_params_freq()
        # most frequently chosen value per param
        consensus = {}
        for k, counts in freq.items():
            consensus[k] = max(counts, key=counts.get)
        return {
            "strategy": self.strategy,
            "folds": len(self.folds),
            "elapsed_ms": round(self.elapsed_ms, 0),
            "search_space": self.search_space,
            "in_sample_total_return": round(is_total, 2),
            "out_of_sample_return": round(os_total, 2),
            "out_of_sample_sharpe": round(oos.sharpe, 3),
            "out_of_sample_max_dd": round(oos.max_drawdown_pct, 2),
            "out_of_sample_trades": oos.num_trades,
            "out_of_sample_win_rate": round(oos.win_rate, 3),
            "consensus_params": consensus,
            "param_frequency": {k: {str(vv): c for vv, c in v.items()} for k, v in freq.items()},
            "overfit_warning": os_total <= 0 and is_total > 0,
            "folds_detail": [{
                "i": f.window_index,
                "best_params": f.best_params,
                "train_return": round(f.train_metrics.total_return_pct, 2),
                "test_return": round(f.test_metrics.total_return_pct, 2),
                "test_trades": f.test_metrics.num_trades,
                "overfit_ratio": round(f.overfit_ratio, 2) if f.overfit_ratio else None,
            } for f in self.folds],
        }


def _pnl_to_trade_log(pnls: List[float]) -> List[dict]:
    return [{"action": "REDEEM", "pnl": p} for p in pnls]


# ── default search spaces ─────────────────────────────────────────────────

def default_search_space(strategy: str) -> List[ParamSpec]:
    """A small, meaningful grid per strategy — keeps optimization fast."""
    if strategy == "vacuum_scalp":
        return [
            ParamSpec("entry_threshold", [0.75, 0.80, 0.85, 0.90]),
            ParamSpec("stake_ratio", [0.10, 0.20, 0.30, 0.50]),
            ParamSpec("book_spread", [0.005, 0.01]),
        ]
    if strategy == "early_trend":
        return [
            ParamSpec("sl_pct_override", [0.06, 0.08, 0.10, 0.12]),
            ParamSpec("book_spread", [0.005, 0.01]),
        ]
    # standard / generic
    return [
        ParamSpec("sl_pct_override", [0.06, 0.08, 0.10, 0.12]),
        ParamSpec("book_spread", [0.005, 0.01]),
    ]


def _combo_to_overrides(combo: Tuple) -> Dict[str, float]:
    """Map a tuple of param values to the BacktestConfig field names."""
    # order must match default_search_space layout
    return {spec.name: val for spec, val in combo}


# ── compounding-aware search space (threshold + stake ratio) ──────────────

def compounding_search_space(strategy: str = "vacuum_scalp") -> List[ParamSpec]:
    """Search space for the compounding optimizer: threshold + stake ratio.

    These are the two knobs that the compounding analysis showed matter most:
    threshold (entry price) sets win/loss asymmetry, stake_ratio controls
    volatility drag. We HOLD to resolution (TP/SL loses per prior analysis).
    """
    if strategy == "vacuum_scalp":
        return [
            ParamSpec("entry_threshold", [0.75, 0.78, 0.80, 0.82, 0.85, 0.88, 0.90]),
            ParamSpec("stake_ratio", [0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50]),
        ]
    return [
        ParamSpec("entry_threshold", [0.75, 0.80, 0.85, 0.90]),
        ParamSpec("stake_ratio", [0.10, 0.20, 0.30, 0.50]),
    ]


@dataclass
class CompoundingWindowResult:
    """One train→test fold in the compounding walk-forward."""
    window_index: int
    train_start: int
    train_end: int
    test_start: int
    test_end: int
    best_params: Dict[str, float]
    train_result: CompoundingResult
    test_result: CompoundingResult
    test_mc: Optional[MonteCarloResult] = None
    baseline_test_result: Optional[CompoundingResult] = None


@dataclass
class CompoundingOptimizationResult:
    """Aggregate result across all compounding walk-forward folds."""
    strategy: str
    start_capital: float
    folds: List[CompoundingWindowResult] = field(default_factory=list)
    search_space: Dict[str, list] = field(default_factory=dict)
    elapsed_ms: float = 0.0

    def out_of_sample_mc(self, n_runs: int = 1000) -> Optional[MonteCarloResult]:
        """Aggregate ALL test-window trades + Monte Carlo on the combined set."""
        all_entries = []
        for f in self.folds:
            tr = f.test_result
            # reconstruct entries from the test trades
            for t in tr.trades:
                all_entries.append({
                    "ts": t.interval_ts,
                    "entry_price": t.entry_price,
                    "won": t.won,
                    "pnl_per_share": (1.0 - t.entry_price - 0.02) if t.won else (-t.entry_price),
                })
        if not all_entries:
            return None
        # run MC on combined out-of-sample trades at the consensus stake ratio
        freq = self.best_params_freq()
        consensus_sr = 0.25
        for v, cnt in freq.get("stake_ratio", {}).items():
            consensus_sr = v
        return monte_carlo(all_entries, self.start_capital, consensus_sr, n_runs=n_runs)

    def best_params_freq(self) -> Dict[str, Dict[float, int]]:
        freq: Dict[str, Dict[float, int]] = {}
        for f in self.folds:
            for k, v in f.best_params.items():
                freq.setdefault(k, {})
                freq[k][v] = freq[k].get(v, 0) + 1
        return freq

    def summary(self) -> dict:
        freq = self.best_params_freq()
        consensus = {}
        for k, counts in freq.items():
            consensus[k] = max(counts, key=counts.get)
        oos_mc = self.out_of_sample_mc()
        oos_returns = [f.test_result.return_pct for f in self.folds]
        is_returns = [f.train_result.return_pct for f in self.folds]
        is_total = sum(is_returns) / len(is_returns) if is_returns else 0
        os_total = sum(oos_returns) / len(oos_returns) if oos_returns else 0
        # min-order constraint analysis
        from .compounding import min_achievable_stake_ratio
        sr_floor = min_achievable_stake_ratio(self.start_capital, 0.80)
        # actual avg sr achieved in test folds (vs desired)
        actual_srs = [f.test_result.avg_stake_ratio for f in self.folds if f.test_result.n_trades > 0]
        avg_actual_sr = sum(actual_srs) / len(actual_srs) if actual_srs else 0
        return {
            "strategy": self.strategy,
            "start_capital": self.start_capital,
            "folds": len(self.folds),
            "elapsed_ms": round(self.elapsed_ms, 0),
            "search_space": self.search_space,
            "in_sample_avg_return": round(is_total, 2),
            "out_of_sample_avg_return": round(os_total, 2),
            "consensus_params": consensus,
            "param_frequency": {k: {str(vv): c for vv, c in v.items()} for k, v in freq.items()},
            "min_order_sr_floor": round(sr_floor, 3),
            "min_order_warning": (f"At ${self.start_capital}, min order (5 shares × $0.80) = "
                                  f"{sr_floor:.0%} of capital — cannot stake below this"
                                  if sr_floor > 0.20 else None),
            "actual_avg_stake_ratio": round(avg_actual_sr, 3),
            "overfit_warning": os_total <= 0 and is_total > 0,
            "out_of_sample_monte_carlo": oos_mc.to_dict() if oos_mc else None,
            "folds_detail": [{
                "i": f.window_index,
                "best_params": f.best_params,
                "train_return": round(f.train_result.return_pct, 2),
                "test_return": round(f.test_result.return_pct, 2),
                "test_trades": f.test_result.n_trades,
                "test_win_rate": round(f.test_result.win_rate, 3),
                "test_max_dd": round(f.test_result.max_drawdown_pct, 1),
                "test_mc": f.test_mc.to_dict() if f.test_mc else None,
            } for f in self.folds],
        }


# ── optimizer ─────────────────────────────────────────────────────────────

class WalkForwardOptimizer:
    def __init__(
        self,
        settings: Settings,
        strategy: str = "vacuum_scalp",
        capital: float = 15.0,  # default $15 per compounding analysis
        asset: str = "BTC",
        interval_minutes: int = 5,
        search_space: Optional[List[ParamSpec]] = None,
        max_combos: int = 60,
        log: Optional[StructuredLogger] = None,
    ) -> None:
        self.s = settings
        self.strategy = strategy
        self.capital = capital
        self.asset = asset
        self.interval_minutes = interval_minutes
        self.space = search_space or default_search_space(strategy)
        self.max_combos = max_combos
        self.log = log or build_logger("wfo")
        self.fetcher = PolymarketDataFetcher(settings, self.log)

    # ── dataset split ────────────────────────────────────────────────────

    def _split_dataset(
        self, dataset: BacktestDataset, window_intervals: int, step_intervals: int,
    ) -> List[Tuple[int, int, int, int]]:
        """Return list of (train_start, train_end, test_start, test_end) epochs."""
        epochs = sorted(i.interval_ts for i in dataset.intervals)
        if len(epochs) < window_intervals + step_intervals:
            return []
        fold_size = window_intervals + step_intervals
        sec = self.interval_minutes * 60
        folds = []
        start = 0
        while start + fold_size <= len(epochs):
            tr_start = epochs[start]
            tr_end = epochs[start + window_intervals - 1] + sec
            te_start = epochs[start + window_intervals]
            te_end = epochs[start + fold_size - 1] + sec
            folds.append((tr_start, tr_end, te_start, te_end))
            start += step_intervals
        return folds

    def _slice_dataset(self, dataset: BacktestDataset, start: int, end: int) -> Tuple[List[PriceTick], BacktestDataset]:
        """Slice ticks + build a sub-dataset covering [start, end]."""
        ticks = [t for t in dataset.oracle_ticks if start <= t.ts <= end]
        sub = BacktestDataset(asset=dataset.asset, interval_minutes=dataset.interval_minutes)
        for i in dataset.intervals:
            if start <= i.interval_ts < end:
                sub.intervals.append(i)
                if i.interval_ts in dataset.token_history:
                    sub.token_history[i.interval_ts] = dataset.token_history[i.interval_ts]
        sub.oracle_ticks = ticks
        return ticks, sub

    # ── single backtest with given params ────────────────────────────────

    def _run_single(self, ticks: List[PriceTick], sub: BacktestDataset,
                    overrides: Dict[str, float]) -> BacktestMetrics:
        cfg = BacktestConfig(
            strategy=self.strategy, capital=self.capital, asset=self.asset,
            interval_minutes=self.interval_minutes, data_mode="poly",
            use_fees=True,
            sl_pct_override=overrides.get("sl_pct_override"),
            tp_delta_override=overrides.get("tp_delta_override"),
            book_spread=overrides.get("book_spread", 0.005),
        )
        bt = BacktestEngine(self.s, ticks, cfg, dataset=sub)
        return bt.run()

    # ── train: find best combo ───────────────────────────────────────────

    def _optimize_window(self, ticks, sub) -> Tuple[ParamCombo, List[ParamCombo]]:
        combos = list(itertools.product(*[sp.values for sp in self.space]))
        # cap the search (random sample if too large)
        if len(combos) > self.max_combos:
            import random
            rng = random.Random(42)
            combos = rng.sample(combos, self.max_combos)
        results: List[ParamCombo] = []
        for combo in combos:
            overrides = _combo_to_overrides(zip(self.space, combo))
            metrics = self._run_single(ticks, sub, overrides)
            pc = ParamCombo(params=overrides, metrics=metrics)
            results.append(pc)
        best = max(results, key=lambda c: c.score)
        return best, results

    # ── public: run full walk-forward ────────────────────────────────────

    # ── compounding walk-forward (fast, threshold + stake ratio) ─────────

    def _slice_intervals(self, dataset: BacktestDataset, start: int, end: int) -> List[IntervalMeta]:
        """Return the IntervalMeta list covering [start, end)."""
        return [i for i in dataset.intervals if start <= i.interval_ts < end]

    def _optimize_window_compounding(
        self, entries: List[dict], space: List[ParamSpec],
    ) -> Tuple[Dict[str, float], CompoundingResult]:
        """Grid search on the compounding simulator for one window's entries."""
        import itertools
        best_score = -1e9
        best_params = {}
        best_result = None
        for combo in itertools.product(*[sp.values for sp in space]):
            params = {sp.name: val for sp, val in zip(space, combo)}
            threshold = params["entry_threshold"]
            sr = params["stake_ratio"]
            # filter entries that actually fire at this threshold
            fired = [e for e in entries if e["entry_price"] <= threshold + 0.001]
            # Actually we need to RE-EXTRACT: threshold determines which trades fire.
            # We approximate by pre-extracting at the lowest threshold and filtering,
            # but the correct way is to extract per threshold. Handled by caller.
            r = simulate_compounding(fired, self.capital, sr)
            sc = score_result(r)
            if sc > best_score:
                best_score = sc
                best_params = params
                best_result = r
        return best_params, best_result

    def _extract_for_threshold(self, dataset_slice: List[IntervalMeta],
                                dataset: BacktestDataset, threshold: float) -> List[dict]:
        """Extract entry opportunities at a given threshold (chronological)."""
        sub_ds = BacktestDataset(asset=dataset.asset, interval_minutes=dataset.interval_minutes)
        sub_ds.intervals = dataset_slice
        sub_ds.token_history = {i.interval_ts: dataset.token_history[i.interval_ts]
                                for i in dataset_slice if i.interval_ts in dataset.token_history}
        sub_ds.oracle_ticks = []
        return extract_entry_opportunities(sub_ds, threshold, hold_to_resolution=True)

    async def run_compounding(
        self,
        start_ts: int,
        end_ts: int,
        train_intervals: int = 36,    # 3h train
        test_intervals: int = 18,     # 1.5h test
        space: Optional[List[ParamSpec]] = None,
        monte_carlo_runs: int = 200,  # per test fold
        progress_cb=None,
    ) -> CompoundingOptimizationResult:
        """Compounding-aware walk-forward optimization.

        For each fold:
          * TRAIN: grid-search threshold × stake_ratio, rank by compounding score
          * TEST:  run best params (compounding) on unseen window + Monte Carlo
        """
        t0 = time.time()
        use_space = space or compounding_search_space(self.strategy)
        result = CompoundingOptimizationResult(
            strategy=self.strategy, start_capital=self.capital,
            search_space={sp.name: sp.values for sp in use_space},
        )

        self.log.info(f"[WFO-COMP] fetching dataset {start_ts}->{end_ts}, capital=${self.capital}")
        dataset = await self.fetcher.build_dataset(
            self.asset, start_ts, end_ts, self.interval_minutes, fidelity=1,
        )
        st = dataset.stats()
        need = train_intervals + test_intervals
        if st["intervals_with_token_data"] < need:
            self.log.warning(f"[WFO-COMP] need {need} intervals, have {st['intervals_with_token_data']}")
            result.elapsed_ms = (time.time() - t0) * 1000
            return result

        folds = self._split_dataset(dataset, train_intervals, test_intervals)
        # ── detect degenerate stake-ratio grid (min-order forcing) ──
        # At small capital, several desired sr values collapse to the SAME
        # actual behavior because the 5-share minimum forces a floor.
        ref_price = 0.80  # representative entry price for the floor calc
        sr_floor = min_achievable_stake_ratio(self.capital, ref_price)
        dedup = deduplicate_stake_ratios(self.capital, ref_price, use_space[1].values)
        n_collapse = len(use_space[1].values) - len(dedup)
        # use only the distinct desired-sr representatives
        distinct_srs = [d[0] for d in dedup]
        forced_note = ""
        if n_collapse > 0:
            forced_note = (f" ⚠ {n_collapse} sr values collapse to min-order floor "
                           f"({sr_floor:.0%} at ${self.capital}/p{ref_price}); "
                           f"using {len(distinct_srs)} distinct")
        self.log.info(f"[WFO-COMP] {len(folds)} folds, capital=${self.capital}, "
                      f"{len(use_space)} params{forced_note}")
        if sr_floor > 0.20:
            self.log.warning(f"[WFO-COMP] min-order floor = {sr_floor:.0%} — cannot "
                             f"trade below this stake ratio at ${self.capital}")
        if not folds:
            result.elapsed_ms = (time.time() - t0) * 1000
            return result

        for idx, (tr_s, tr_e, te_s, te_e) in enumerate(folds):
            tr_ivs = self._slice_intervals(dataset, tr_s, tr_e)
            te_ivs = self._slice_intervals(dataset, te_s, te_e)
            if not tr_ivs or not te_ivs:
                continue

            # TRAIN: extract entries at each threshold, grid search over DISTINCT sr
            import itertools
            best_score = -1e9
            best_params = None
            best_train = None
            threshold_entries = {}
            for thr in use_space[0].values:
                threshold_entries[thr] = self._extract_for_threshold(tr_ivs, dataset, thr)
            for thr in use_space[0].values:
                fired = threshold_entries[thr]
                if not fired:
                    continue
                for sr in distinct_srs:
                    r = simulate_compounding(fired, self.capital, sr)
                    sc = score_result(r)
                    if sc > best_score:
                        best_score = sc
                        best_params = {"entry_threshold": thr, "stake_ratio": sr}
                        best_train = r

            if best_params is None:
                continue

            # compute ACTUAL avg sr achieved (may differ from desired due to floor)
            actual_sr_label = best_train.avg_stake_ratio

            # TEST: run best threshold+sizing on unseen window (compounding)
            te_entries = self._extract_for_threshold(te_ivs, dataset, best_params["entry_threshold"])
            test_result = simulate_compounding(te_entries, self.capital, best_params["stake_ratio"])
            test_mc = monte_carlo(te_entries, self.capital, best_params["stake_ratio"],
                                  n_runs=monte_carlo_runs) if te_entries else None

            # BASELINE: default (threshold 0.95, sr 0.50)
            bl_entries = self._extract_for_threshold(te_ivs, dataset, 0.95)
            baseline = simulate_compounding(bl_entries, self.capital, 0.50)

            fold = CompoundingWindowResult(
                window_index=idx, train_start=tr_s, train_end=tr_e,
                test_start=te_s, test_end=te_e,
                best_params=best_params, train_result=best_train,
                test_result=test_result, test_mc=test_mc,
                baseline_test_result=baseline,
            )
            result.folds.append(fold)
            self.log.info(
                f"[WFO-COMP] fold {idx}: best thr={best_params['entry_threshold']:.2f} "
                f"sr={best_params['stake_ratio']:.0%} (actual avg {actual_sr_label:.0%}) | "
                f"train={best_train.return_pct:+.1f}% "
                f"test={test_result.return_pct:+.1f}% (MC ruin={test_mc.ruin_probability:.0%}) "
                f"baseline(0.95/sr0.5)={baseline.return_pct:+.1f}%"
            )
            if progress_cb:
                progress_cb(idx + 1, len(folds))

        result.elapsed_ms = (time.time() - t0) * 1000
        summ = result.summary()
        self.log.info(
            f"[WFO-COMP] done in {result.elapsed_ms/1000:.1f}s | "
            f"IS avg {summ['in_sample_avg_return']:+.1f}% "
            f"OOS avg {summ['out_of_sample_avg_return']:+.1f}% "
            f"consensus={summ['consensus_params']} "
            f"{'⚠ OVERFIT' if summ['overfit_warning'] else '✓ robust'}"
        )
        return result

    async def run(
        self,
        start_ts: int,
        end_ts: int,
        train_intervals: int = 24,     # e.g. 24 × 5min = 2h train
        test_intervals: int = 12,      # 12 × 5min = 1h test
        progress_cb=None,
    ) -> OptimizationResult:
        """Download data once, then walk forward over rolling windows."""
        t0 = time.time()
        result = OptimizationResult(strategy=self.strategy)
        result.search_space = {sp.name: sp.values for sp in self.space}

        self.log.info(f"[WFO] fetching dataset {start_ts}->{end_ts}")
        dataset = await self.fetcher.build_dataset(
            self.asset, start_ts, end_ts, self.interval_minutes, fidelity=1,
        )
        st = dataset.stats()
        if st["intervals_with_token_data"] < train_intervals + test_intervals:
            self.log.warning(
                f"[WFO] not enough data ({st['intervals_with_token_data']} intervals with "
                f"token prices, need ≥{train_intervals + test_intervals})")
            result.elapsed_ms = (time.time() - t0) * 1000
            return result

        folds = self._split_dataset(dataset, train_intervals, test_intervals)
        self.log.info(f"[WFO] {len(folds)} folds, "
                      f"{len(list(itertools.product(*[sp.values for sp in self.space])))} combos each")
        if not folds:
            result.elapsed_ms = (time.time() - t0) * 1000
            return result

        default_overrides: Dict[str, float] = {}  # baseline = strategy defaults

        for idx, (tr_s, tr_e, te_s, te_e) in enumerate(folds):
            tr_ticks, tr_sub = self._slice_dataset(dataset, tr_s, tr_e)
            te_ticks, te_sub = self._slice_dataset(dataset, te_s, te_e)
            if not tr_ticks or not te_ticks:
                continue

            # TRAIN: grid search
            best, _ = self._optimize_window(tr_ticks, tr_sub)
            train_metrics = best.metrics

            # TEST: run best params on unseen window
            test_metrics = self._run_single(te_ticks, te_sub, best.params)

            # BASELINE: default params on same test window (for comparison)
            baseline = self._run_single(te_ticks, te_sub, default_overrides)

            fold = WindowResult(
                window_index=idx, train_start=tr_s, train_end=tr_e,
                test_start=te_s, test_end=te_e,
                best_params=best.params, train_metrics=train_metrics,
                test_metrics=test_metrics, baseline_test_metrics=baseline,
            )
            result.folds.append(fold)
            self.log.info(
                f"[WFO] fold {idx}: best={best.params} "
                f"train={train_metrics.total_return_pct:+.2f}% "
                f"test={test_metrics.total_return_pct:+.2f}% "
                f"baseline={baseline.total_return_pct:+.2f}% "
                f"({test_metrics.num_trades} trades)"
            )
            if progress_cb:
                progress_cb(idx + 1, len(folds))

        result.elapsed_ms = (time.time() - t0) * 1000
        summary = result.summary()
        self.log.info(
            f"[WFO] done in {result.elapsed_ms/1000:.1f}s | "
            f"in-sample {summary['in_sample_total_return']:+.2f}% "
            f"vs out-of-sample {summary['out_of_sample_return']:+.2f}% "
            f"{'⚠ OVERFIT' if summary['overfit_warning'] else '✓ robust'}"
        )
        return result
