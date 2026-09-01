"""Tests for the fast compounding simulator."""

import pytest

from backend.app.backtest.compounding import (
    simulate_compounding, monte_carlo, extract_entry_opportunities,
    score_result, CompoundingResult, FEE,
)
from backend.app.backtest.poly_fetcher import BacktestDataset, IntervalMeta


def make_entries(prices_and_outcomes):
    """Build entry list from [(price, won), ...]."""
    return [
        {"ts": i * 300, "entry_price": p, "won": w,
         "pnl_per_share": (1.0 - p - FEE) if w else (-p)}
        for i, (p, w) in enumerate(prices_and_outcomes)
    ]


class TestCompoundingBasics:
    def test_single_win_grows_capital(self):
        entries = make_entries([(0.80, True)])
        r = simulate_compounding(entries, start_capital=15.0, stake_ratio=0.25)
        assert r.n_trades == 1
        # 5 shares × 0.80 = 4.00 cost, +5 × 0.18 = 0.90 → 15.90
        assert r.final_capital == pytest.approx(15.90, abs=0.01)
        assert r.win_rate == 1.0
        assert not r.ruined

    def test_single_loss_shrinks_capital(self):
        entries = make_entries([(0.80, False)])
        r = simulate_compounding(entries, start_capital=15.0, stake_ratio=0.25)
        assert r.final_capital == pytest.approx(11.0, abs=0.01)

    def test_min_order_forces_higher_stake(self):
        """At $15, desired 10% = $1.50, but min order = 5 × 0.80 = $4.00 = 27%."""
        entries = make_entries([(0.80, True)])
        r = simulate_compounding(entries, start_capital=15.0, stake_ratio=0.10)
        assert r.avg_stake_ratio > 0.20  # forced above desired 0.10

    def test_compounding_amplifies_series(self):
        """Win-then-loss ≠ loss-then-win (order matters under compounding).
        Needs enough capital that stake_ratio creates >min_shares."""
        r1 = simulate_compounding(make_entries([(0.80, True), (0.80, False)]), 50.0, 0.50)
        r2 = simulate_compounding(make_entries([(0.80, False), (0.80, True)]), 50.0, 0.50)
        assert r1.final_capital != r2.final_capital


class TestVolatilityDrag:
    def test_high_stake_single_loss_devastating(self):
        """9 wins then 1 loss at 72% stake — the loss hits peak capital."""
        entries = make_entries([(0.80, True)] * 9 + [(0.80, False)])
        r = simulate_compounding(entries, start_capital=15.0, stake_ratio=0.72)
        assert r.n_trades == 10
        # After 9 wins capital is large; the 10th trade (loss) at 72% is brutal

    def test_low_stake_survives_same_sequence(self):
        entries = make_entries([(0.80, True)] * 9 + [(0.80, False)])
        r = simulate_compounding(entries, start_capital=15.0, stake_ratio=0.15)
        assert r.return_pct > 0


class TestMonteCarlo:
    def test_returns_distribution(self):
        entries = make_entries([(0.80, True)] * 8 + [(0.80, False)] * 2)
        mc = monte_carlo(entries, start_capital=15.0, stake_ratio=0.25, n_runs=100)
        assert mc.n_runs == 100
        assert mc.p5_final <= mc.median_final <= mc.p95_final
        assert 0 <= mc.ruin_probability <= 1

    def test_reproducible_with_seed(self):
        entries = make_entries([(0.80, True)] * 5 + [(0.80, False)] * 5)
        mc1 = monte_carlo(entries, 15.0, 0.25, n_runs=50, seed=123)
        mc2 = monte_carlo(entries, 15.0, 0.25, n_runs=50, seed=123)
        assert mc1.median_final == mc2.median_final

    def test_all_wins_no_ruin(self):
        entries = make_entries([(0.80, True)] * 10)
        mc = monte_carlo(entries, 15.0, 0.25, n_runs=100)
        assert mc.ruin_probability == 0.0


class TestExtractOpportunities:
    def _make_dataset(self):
        ds = BacktestDataset(asset="BTC", interval_minutes=5)
        for i in range(5):
            epoch = 1771168800 + i * 300
            up_won = i % 2 == 0
            ds.intervals.append(IntervalMeta(
                interval_ts=epoch, asset="BTC",
                up_token_id=f"up_{i}", down_token_id=f"down_{i}",
                winner="Up" if up_won else "Down", volume=1000.0, end_ts=epoch + 300,
            ))
            pts = [(epoch + 30 + k * 60, 0.80) for k in range(5)]
            ds.token_history[epoch] = {"up": pts, "down": [(t, 0.20) for t, p in pts]}
        return ds

    def test_extracts_at_threshold(self):
        entries = extract_entry_opportunities(self._make_dataset(), threshold=0.75)
        assert len(entries) == 5
        assert all(e["entry_price"] >= 0.75 for e in entries)

    def test_high_threshold_fewer_entries(self):
        ds = self._make_dataset()
        assert len(extract_entry_opportunities(ds, 0.75)) >= len(extract_entry_opportunities(ds, 0.95))

    def test_chronological_order(self):
        entries = extract_entry_opportunities(self._make_dataset(), threshold=0.75)
        tss = [e["ts"] for e in entries]
        assert tss == sorted(tss)


class TestScoreFunction:
    def _make_result(self, ret, dd, trades, ruined=False, wins=0):
        return CompoundingResult(
            final_capital=15 * (1 + ret / 100), peak_capital=20, trough_capital=10,
            max_drawdown_pct=dd, return_pct=ret, n_trades=trades,
            n_wins=wins, n_losses=trades - wins, ruined=ruined,
            avg_stake_ratio=0.25, win_rate=wins / trades if trades else 0,
        )

    def test_no_trades_low_score(self):
        assert score_result(self._make_result(0, 0, 0)) == -999.0

    def test_profit_beats_loss(self):
        assert score_result(self._make_result(30, 10, 5, wins=5)) > score_result(self._make_result(-30, 30, 5, wins=0))

    def test_ruin_penalised(self):
        ok = self._make_result(10, 10, 10, ruined=False, wins=8)
        ruined = self._make_result(-50, 100, 10, ruined=True, wins=7)
        assert score_result(ok) > score_result(ruined)
