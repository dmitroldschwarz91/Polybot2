"""Tests for demo vs backtest comparison.

Uses a synthetic dataset to verify the comparator correctly measures the gap
between live demo trades and backtest predictions.
"""

import json
import asyncio
import pytest
from pathlib import Path

from backend.app.config import Settings
from backend.app.demo.engine import DemoEngine, DEMO_START_CAPITAL
from backend.app.backtest.poly_fetcher import BacktestDataset, IntervalMeta


def make_settings(tmp_path):
    return Settings(
        private_key="0xK", funder_address="0xA", initial_balance=15.0,
        assets=["BTC"],
        backtest_data_dir=str(tmp_path),
    )


def make_demo_engine(settings):
    eng = DemoEngine(settings, start_capital=15.0, threshold=0.75, stake_ratio=0.30)
    return eng


class TestHistoryPersistence:
    def test_save_and_load_history(self, tmp_path):
        s = make_settings(tmp_path)
        eng = make_demo_engine(s)
        eng.closed_history = [
            {"interval_ts": 100, "asset": "BTC", "won": True, "pnl": 0.5},
            {"interval_ts": 200, "asset": "BTC", "won": False, "pnl": -1.0},
        ]
        eng._save_history()
        assert eng.history_path.exists()
        # reload
        eng2 = make_demo_engine(s)
        assert len(eng2.closed_history) == 2
        assert eng2.closed_history[0]["interval_ts"] == 100

    def test_clear_history(self, tmp_path):
        s = make_settings(tmp_path)
        eng = make_demo_engine(s)
        eng.closed_history = [{"interval_ts": 100, "won": True}]
        eng._save_history()
        eng.clear_history()
        assert eng.closed_history == []
        eng2 = make_demo_engine(s)
        assert eng2.closed_history == []

    def test_missing_history_file(self, tmp_path):
        s = make_settings(tmp_path)
        eng = make_demo_engine(s)
        # no history yet
        assert eng.closed_history == []

    def test_corrupt_history_file(self, tmp_path):
        s = make_settings(tmp_path)
        Path(s.backtest_data_dir).mkdir(parents=True, exist_ok=True)
        bad = Path(s.backtest_data_dir) / "demo_history.json"
        bad.write_text("not json{{{")
        eng = make_demo_engine(s)
        assert eng.closed_history == []  # graceful degradation


class TestCompareWithBacktest:
    def test_no_history_returns_error(self, tmp_path):
        s = make_settings(tmp_path)
        eng = make_demo_engine(s)
        eng.closed_history = []
        result = asyncio.run(eng.compare_with_backtest())
        assert "error" in result
        assert "no closed" in result["error"].lower()

    def test_compare_with_synthetic_data(self, tmp_path, monkeypatch):
        """Compare demo (3 trades) vs backtest on the same intervals."""
        s = make_settings(tmp_path)
        eng = make_demo_engine(s)
        # simulate 3 closed demo trades
        eng.closed_history = [
            {"interval_ts": 1000, "asset": "BTC", "direction": "UP",
             "entry_price": 0.76, "shares": 5, "cost": 3.8,
             "won": True, "pnl": 1.1, "entry_ts": 950, "end_ts": 1300},
            {"interval_ts": 1300, "asset": "BTC", "direction": "UP",
             "entry_price": 0.78, "shares": 5, "cost": 3.9,
             "won": False, "pnl": -3.9, "entry_ts": 1250, "end_ts": 1600},
            {"interval_ts": 1600, "asset": "BTC", "direction": "DOWN",
             "entry_price": 0.77, "shares": 5, "cost": 3.85,
             "won": True, "pnl": 1.05, "entry_ts": 1550, "end_ts": 1900},
        ]

        # mock the fetcher to return a synthetic dataset
        async def fake_build_dataset(*a, **kw):
            ds = BacktestDataset(asset="BTC", interval_minutes=5)
            for i, ts in enumerate([1000, 1300, 1600]):
                up_won = i != 1  # second trade lost
                ds.intervals.append(IntervalMeta(
                    interval_ts=ts, asset="BTC",
                    up_token_id=f"up_{i}", down_token_id=f"down_{i}",
                    winner="Up" if up_won else "Down", volume=1000.0, end_ts=ts + 300,
                ))
                # backtest assumes slightly lower entry price (last-trade, no slippage)
                leader_price = 0.75 if i != 1 else 0.76
                pts = [(ts + 30 + k * 60, leader_price) for k in range(5)]
                ds.token_history[ts] = {"up": pts if up_won else [],
                                        "down": pts if not up_won else []}
            return ds

        from backend.app.backtest.poly_fetcher import PolymarketDataFetcher
        monkeypatch.setattr(PolymarketDataFetcher, "build_dataset", fake_build_dataset)

        result = asyncio.run(eng.compare_with_backtest())
        assert "error" not in result
        assert "demo" in result
        assert "backtest" in result
        assert "comparison" in result
        # demo had 3 trades
        assert result["demo"]["trades"] == 3
        # verdict should be present
        assert "verdict" in result["comparison"]
        # there should be a gap analysis
        assert "avg_entry_price_gap" in result["comparison"]

    def test_compare_handles_empty_dataset(self, tmp_path, monkeypatch):
        s = make_settings(tmp_path)
        eng = make_demo_engine(s)
        eng.closed_history = [
            {"interval_ts": 1000, "asset": "BTC", "direction": "UP",
             "entry_price": 0.76, "shares": 5, "cost": 3.8,
             "won": True, "pnl": 1.1, "entry_ts": 950, "end_ts": 1300},
        ]

        async def fake_empty_dataset(*a, **kw):
            ds = BacktestDataset(asset="BTC", interval_minutes=5)
            return ds  # empty

        from backend.app.backtest.poly_fetcher import PolymarketDataFetcher
        monkeypatch.setattr(PolymarketDataFetcher, "build_dataset", fake_empty_dataset)

        result = asyncio.run(eng.compare_with_backtest())
        assert "error" in result
