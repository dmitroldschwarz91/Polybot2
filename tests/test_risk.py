"""
Tests for the RiskManager — the module that previously had its logic
duplicated across 4 functions and was therefore untestable.

Run:  cd polymarket_bot && python -m pytest tests/test_risk.py -v
"""

import time
from unittest.mock import patch

from backend.app.config import Settings
from backend.app.domain.enums import CloseReason, EntryType
from backend.app.domain.models import Position
from backend.app.risk.manager import RiskManager


def make_settings(**overrides) -> Settings:
    base = dict(
        private_key="0xKEY", funder_address="0xADDR",
        initial_balance=100.0, assets=["BTC", "ETH"],
    )
    base.update(overrides)
    return Settings(**base)


def make_position(entry_price=0.96, entry_type=EntryType.VACUUM_SCALP,
                  size=100, closed=False, sl_in_progress=False) -> Position:
    p = Position(
        slug="test", asset="BTC", token_id="tok", direction="UP",
        entry_price=entry_price, entry_size=size, entry_cost=entry_price * size,
        entry_type=entry_type, end_ts=int(time.time()) + 300,
    )
    p.closed = closed
    p.sl_in_progress = sl_in_progress
    # bypass grace period
    p.entry_timestamp = time.time() - 10
    return p


class TestStopLossThreshold:
    def test_vacuum_sl(self):
        rm = RiskManager(make_settings(vacuum_scalp_sl_pct=0.10))
        p = make_position(entry_price=0.96, entry_type=EntryType.VACUUM_SCALP)
        assert rm.stop_loss_threshold(p) == round(0.96 * 0.90, 4)  # 0.864

    def test_standard_sl(self):
        rm = RiskManager(make_settings(standard_sl_pct=0.10))
        p = make_position(entry_price=0.95, entry_type=EntryType.STANDARD)
        assert rm.stop_loss_threshold(p) == round(0.95 * 0.90, 4)  # 0.855

    def test_early_trend_trailing_uses_trailing_price(self):
        rm = RiskManager(make_settings())
        p = make_position(entry_price=0.80, entry_type=EntryType.EARLY_TREND)
        p.partial_tp_taken = True
        p.trailing_stop_price = 0.82
        assert rm.stop_loss_threshold(p) == 0.82


class TestEvaluate:
    def test_no_breach_above_sl(self):
        rm = RiskManager(make_settings(vacuum_scalp_sl_pct=0.10))
        p = make_position(entry_price=0.96)
        d = rm.evaluate(p, current_price=0.95)
        assert not d.breached

    def test_breach_at_sl(self):
        rm = RiskManager(make_settings(vacuum_scalp_sl_pct=0.10))
        p = make_position(entry_price=0.96)
        d = rm.evaluate(p, current_price=0.86)  # below 0.864
        assert d.breached
        assert d.reason == CloseReason.STOP_LOSS
        assert not d.nuclear

    def test_nuclear_crash(self):
        rm = RiskManager(make_settings(vacuum_scalp_sl_pct=0.10, nuclear_crash_pct=0.15))
        p = make_position(entry_price=0.96)
        sl = rm.stop_loss_threshold(p)  # 0.864
        nuclear_below = sl * (1 - 0.15)  # 0.7344
        d = rm.evaluate(p, current_price=nuclear_below - 0.01)
        assert d.breached and d.nuclear

    def test_grace_period_skipped(self):
        rm = RiskManager(make_settings())
        p = make_position(entry_price=0.96)
        p.entry_timestamp = time.time()  # just opened
        d = rm.evaluate(p, current_price=0.01)
        assert not d.breached

    def test_none_price_no_crash(self):
        rm = RiskManager(make_settings())
        p = make_position(entry_price=0.96)
        assert not rm.evaluate(p, None).breached


class TestFillAnomaly:
    def test_anomaly_detected(self):
        rm = RiskManager(make_settings(fill_anomaly_pct=0.20))
        assert rm.is_fill_anomaly(0.96, 0.70)  # 27% below expected

    def test_normal_fill_ok(self):
        rm = RiskManager(make_settings(fill_anomaly_pct=0.20))
        assert not rm.is_fill_anomaly(0.96, 0.95)


class TestPortfolioGuards:
    def test_blocks_on_max_positions(self):
        rm = RiskManager(make_settings(max_concurrent_positions=2, initial_balance=100))
        positions = {"a": make_position(closed=False), "b": make_position(closed=False)}
        ok, reason = rm.can_open_new(positions, bot_balance=100)
        assert not ok
        assert "max_concurrent" in reason

    def test_halt_on_drawdown(self):
        rm = RiskManager(make_settings(max_drawdown_pct=0.35, initial_balance=100))
        ok, reason = rm.can_open_new({}, bot_balance=60)  # 40% drawdown
        assert not ok
        assert rm.state.halted
        assert "max_drawdown" in reason

    def test_halt_persists(self):
        rm = RiskManager(make_settings())
        rm.state.halted = True
        rm.state.halt_reason = "manual"
        ok, reason = rm.can_open_new({}, bot_balance=1000)
        assert not ok
        assert "manual" in reason

    def test_daily_loss_limit(self):
        rm = RiskManager(make_settings(max_daily_loss_pct=0.20, initial_balance=100))
        rm.state.day_start_balance = 100
        ok, reason = rm.can_open_new({}, bot_balance=78)  # 22% day loss
        assert not ok
        assert "max_daily_loss" in reason

    def test_healthy_allows_entry(self):
        rm = RiskManager(make_settings(max_concurrent_positions=2, initial_balance=100))
        ok, reason = rm.can_open_new({"a": make_position(closed=False)}, bot_balance=90)
        assert ok and reason == "ok"


class TestPositionSizing:
    def test_base_stake_half_balance(self):
        rm = RiskManager(make_settings())
        assert rm.base_stake(100.0) == 50.0

    def test_imbalance_boosts_stake(self):
        rm = RiskManager(make_settings(max_stake_ratio=0.75))
        stake = rm.stake_with_imbalance(100.0, imbalance=0.95)
        base = 50.0
        assert stake == min(base * 1.3, 100 * 0.75)  # capped at 75

    def test_zero_balance_zero_stake(self):
        rm = RiskManager(make_settings())
        assert rm.base_stake(0) == 0.0
        assert rm.vacuum_scalp_stake(0, 0.9) == 0.0
