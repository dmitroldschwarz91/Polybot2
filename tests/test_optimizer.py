"""
Tests for the walk-forward optimizer.

Uses a synthetic dataset (no network) to verify window splitting, the grid
search ranking, and overfit detection.
"""

from backend.app.backtest.data import PriceTick
from backend.app.backtest.optimizer import (
    WalkForwardOptimizer, ParamSpec, ParamCombo, default_search_space,
    OptimizationResult, WindowResult, _combo_to_overrides,
)
from backend.app.backtest.metrics import BacktestMetrics
from backend.app.backtest.poly_fetcher import BacktestDataset, IntervalMeta
from backend.app.config import Settings


def make_settings(**kw):
    base = dict(private_key="0xK", funder_address="0xA", initial_balance=7.0,
                assets=["BTC"])
    base.update(kw)
    return Settings(**base)


def make_dataset(n_intervals=20, interval_minutes=5, n_ticks_per=6):
    """Synthetic dataset with real-ish structure (no network)."""
    ds = BacktestDataset(asset="BTC", interval_minutes=interval_minutes)
    step = interval_minutes * 60
    base = 1771168800
    for i in range(n_intervals):
        epoch = base + i * step
        end = epoch + step
        winner = "Up" if i % 2 == 0 else "Down"
        ds.intervals.append(IntervalMeta(
            interval_ts=epoch, asset="BTC",
            up_token_id=f"up_{i}", down_token_id=f"down_{i}",
            winner=winner, volume=1000.0 * (i + 1), end_ts=end,
        ))
        # token history: a few points per interval
        up_pts = [(epoch + 30 + k * 60, 0.6 if winner == "Up" else 0.4)
                  for k in range(n_ticks_per)]
        down_pts = [(t, round(1.0 - p, 4)) for t, p in up_pts]
        ds.token_history[epoch] = {"up": up_pts, "down": down_pts}
        # oracle ticks within the interval
        for k in range(n_ticks_per):
            p = 69000 + (10 if winner == "Up" else -10) * (k / n_ticks_per)
            ds.oracle_ticks.append(PriceTick(ts=epoch + 30 + k * 60, price=p))
    return ds


class TestSearchSpace:
    def test_vacuum_scalp_space(self):
        space = default_search_space("vacuum_scalp")
        names = [s.name for s in space]
        assert "entry_threshold" in names
        assert "stake_ratio" in names
        assert all(len(s.values) >= 2 for s in space)

    def test_compounding_search_space(self):
        from backend.app.backtest.optimizer import compounding_search_space
        space = compounding_search_space("vacuum_scalp")
        names = [s.name for s in space]
        assert "entry_threshold" in names
        assert "stake_ratio" in names
        assert len(space[0].values) >= 5  # threshold has many candidates

    def test_early_trend_space(self):
        space = default_search_space("early_trend")
        assert all(s.name in ("sl_pct_override", "book_spread") for s in space)

    def test_generic_space(self):
        space = default_search_space("standard")
        assert len(space) >= 1


class TestComboOverrides:
    def test_mapping(self):
        space = [ParamSpec("sl_pct_override", [0.06, 0.08]),
                 ParamSpec("book_spread", [0.005])]
        combo = (0.08, 0.005)
        overrides = _combo_to_overrides(zip(space, combo))
        assert overrides == {"sl_pct_override": 0.08, "book_spread": 0.005}


class TestParamComboScoring:
    def test_no_trades_low_score(self):
        pc = ParamCombo(params={}, metrics=BacktestMetrics(num_trades=0))
        assert pc.score == -999.0

    def test_profit_beats_loss(self):
        good = ParamCombo(params={},
                          metrics=BacktestMetrics(num_trades=5, total_return_pct=10,
                                                  max_drawdown_pct=2))
        bad = ParamCombo(params={},
                         metrics=BacktestMetrics(num_trades=5, total_return_pct=-5,
                                                 max_drawdown_pct=3))
        assert good.score > bad.score

    def test_high_drawdown_penalised(self):
        low_dd = ParamCombo(params={},
                            metrics=BacktestMetrics(num_trades=5, total_return_pct=10,
                                                    max_drawdown_pct=1))
        high_dd = ParamCombo(params={},
                             metrics=BacktestMetrics(num_trades=5, total_return_pct=10,
                                                     max_drawdown_pct=20))
        assert low_dd.score > high_dd.score


class TestDatasetSplit:
    def test_split_produces_folds(self):
        s = make_settings()
        opt = WalkForwardOptimizer(s, max_combos=4)
        ds = make_dataset(n_intervals=40)
        folds = opt._split_dataset(ds, window_intervals=20, step_intervals=10)
        # 40 intervals, 20 train + 10 test, step 10 → folds starting at 0,10
        assert len(folds) >= 2
        # train_end > train_start, test follows train
        for tr_s, tr_e, te_s, te_e in folds:
            assert tr_e > tr_s
            assert te_s >= tr_e - s.interval_minutes * 60  # contiguous-ish
            assert te_e > te_s

    def test_split_too_few_intervals(self):
        s = make_settings()
        opt = WalkForwardOptimizer(s, max_combos=4)
        ds = make_dataset(n_intervals=5)
        folds = opt._split_dataset(ds, window_intervals=24, step_intervals=12)
        assert folds == []

    def test_slice_dataset(self):
        s = make_settings()
        opt = WalkForwardOptimizer(s, max_combos=4)
        ds = make_dataset(n_intervals=20)
        epochs = sorted(i.interval_ts for i in ds.intervals)
        mid = epochs[10]
        ticks, sub = opt._slice_dataset(ds, epochs[5], mid)
        # sub-intervals should be those with interval_ts in [epochs[5], mid)
        assert all(epochs[5] <= i.interval_ts < mid for i in sub.intervals)
        assert all(epochs[5] <= t.ts <= mid for t in ticks)


class TestOptimizeWindow:
    def test_returns_best_combo(self):
        s = make_settings()
        space = [ParamSpec("sl_pct_override", [0.06, 0.10])]
        opt = WalkForwardOptimizer(s, strategy="vacuum_scalp",
                                   search_space=space, max_combos=4)
        ds = make_dataset(n_intervals=12)
        ticks, sub = opt._slice_dataset(ds, ds.intervals[0].interval_ts,
                                        ds.intervals[-1].end_ts)
        best, all_results = opt._optimize_window(ticks, sub)
        assert isinstance(best, ParamCombo)
        assert best.params["sl_pct_override"] in [0.06, 0.10]
        assert len(all_results) == 2


class TestOverfitDetection:
    def test_overfit_ratio(self):
        # train +50%, test +10% → ratio 5
        train = BacktestMetrics(total_return_pct=50, num_trades=3)
        test = BacktestMetrics(total_return_pct=10, num_trades=2)
        wr = WindowResult(0, 0, 0, 0, 0, {"sl_pct_override": 0.06}, train, test)
        assert wr.overfit_ratio == pytest.approx(5.0)

    def test_summary_flags_overfit(self):
        result = OptimizationResult(strategy="vacuum_scalp")
        result.folds.append(WindowResult(
            0, 0, 0, 0, 0, {"sl_pct_override": 0.06},
            BacktestMetrics(total_return_pct=20, num_trades=3),
            BacktestMetrics(total_return_pct=-5, num_trades=2),
        ))
        summary = result.summary()
        assert summary["overfit_warning"] is True  # +IS, -OOS

    def test_summary_no_overfit(self):
        result = OptimizationResult(strategy="vacuum_scalp")
        result.folds.append(WindowResult(
            0, 0, 0, 0, 0, {"sl_pct_override": 0.06},
            BacktestMetrics(total_return_pct=20, num_trades=3),
            BacktestMetrics(total_return_pct=8, num_trades=2),
        ))
        summary = result.summary()
        assert summary["overfit_warning"] is False


class TestBestParamsFreq:
    def test_frequency_counting(self):
        result = OptimizationResult(strategy="vacuum_scalp")
        for sl in [0.06, 0.06, 0.10]:
            result.folds.append(WindowResult(
                0, 0, 0, 0, 0, {"sl_pct_override": sl},
                BacktestMetrics(num_trades=1, total_return_pct=1),
                BacktestMetrics(num_trades=1, total_return_pct=1),
            ))
        freq = result.best_params_freq()
        assert freq["sl_pct_override"][0.06] == 2
        assert freq["sl_pct_override"][0.10] == 1
        consensus = result.summary()["consensus_params"]
        assert consensus["sl_pct_override"] == 0.06


import pytest  # noqa: E402
