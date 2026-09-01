"""
Tests for the backtest framework + spread-capture strategy.

Run:  python -m pytest tests/test_backtest.py -v
"""

import math

from backend.app.backtest import BacktestConfig, BacktestEngine, HistoricalData
from backend.app.backtest.data import PriceTick
from backend.app.backtest.simulator import SimulatedExchange, MarketModel
from backend.app.config import Settings
from backend.app.domain.enums import EntryType
from backend.app.strategies.spread_capture import SpreadCaptureStrategy
from backend.app.strategies.vacuum_scalp import VacuumScalpStrategy


def make_settings(**kw):
    base = dict(private_key="0xK", funder_address="0xA", initial_balance=7.0,
                assets=["BTC"])
    base.update(kw)
    return Settings(**base)


def make_ticks(n=600, start=100000.0, vol=0.0003, seed=7):
    return HistoricalData.synthetic_random_walk(start, n, step_secs=1,
                                                volatility=vol, seed=seed)


class TestSimulatedExchange:
    def test_buy_reduces_cash(self):
        s = make_settings()
        ex = SimulatedExchange(s)
        ex.fund(10.0)
        ex.buy("T1", ask_price=0.96, size=5, ts=0, side_label="UP")
        assert ex.account.cash < 10.0

    def test_sell_charges_fee_on_profit(self):
        s = make_settings(backtest_taker_fee=0.02)
        ex = SimulatedExchange(s)
        ex.fund(10.0)
        ex.buy("T1", ask_price=0.50, size=10, ts=0)        # cost 5.0
        ex.sell("T1", bid_price=0.90, size=10, ts=1)       # proceeds 9.0, profit
        assert ex.account.fees_paid > 0
        assert ex.account.realized_pnl > 0

    def test_no_fee_on_loss(self):
        s = make_settings(backtest_taker_fee=0.02)
        ex = SimulatedExchange(s)
        ex.fund(10.0)
        ex.buy("T1", ask_price=0.90, size=10, ts=0)
        ex.sell("T1", bid_price=0.50, size=10, ts=1)       # loss
        assert ex.account.fees_paid == 0.0

    def test_redeem_winner(self):
        s = make_settings(backtest_taker_fee=0.02)
        ex = SimulatedExchange(s)
        ex.fund(10.0)
        ex.buy("T1", ask_price=0.90, size=10, ts=0)        # cost 9
        pnl = ex.redeem("T1", won=True, ts=1)              # pays $10 minus fee
        assert pnl > 0
        assert ex.account.cash > 9

    def test_redeem_loser(self):
        s = make_settings()
        ex = SimulatedExchange(s)
        ex.fund(10.0)
        ex.buy("T1", ask_price=0.90, size=10, ts=0)
        # buy fills at ask + slippage (2 ticks → 0.92), so cost = 9.2
        pnl = ex.redeem("T1", won=False, ts=1)
        assert pnl == -9.2

    def test_insufficient_cash_partial(self):
        s = make_settings()
        ex = SimulatedExchange(s)
        ex.fund(1.0)
        order = ex.buy("T1", ask_price=0.96, size=100, ts=0)
        assert order is None or order.size < 100


class TestMarketModel:
    def test_pair_costs_sum_near_one(self):
        m = MarketModel(spread=0.01)
        bp = m.token_prices(oracle=100100, start_price=100000,
                            secs_to_close=60, interval_secs=300)
        # up_ask + down_ask should be ~1 + 2*spread (bid-ask bounce)
        assert abs((bp["up_mid"] + bp["down_mid"]) - 1.0) < 0.01

    def test_deviation_pushes_up_price_up(self):
        m = MarketModel()
        low = m.token_prices(100000, 100000, 10, 300)["up_mid"]
        high = m.token_prices(100500, 100000, 10, 300)["up_mid"]
        assert high > low


class TestBacktestEngine:
    def test_runs_and_returns_metrics(self):
        s = make_settings(vacuum_scalp_enabled=True)
        ticks = make_ticks(900)
        cfg = BacktestConfig(strategy="vacuum_scalp", capital=7.0, asset="BTC")
        bt = BacktestEngine(s, ticks, cfg)
        m = bt.run()
        assert m.final_equity >= 0
        assert len(m.equity_curve) > 0
        assert m.total_fees >= 0

    def test_fees_disabled_increases_or_equal_equity(self):
        s = make_settings()
        ticks = make_ticks(900, seed=3)
        with_fee = BacktestEngine(s, ticks, BacktestConfig(strategy="vacuum_scalp",
                                  capital=7.0, use_fees=True)).run()
        no_fee = BacktestEngine(s, ticks, BacktestConfig(strategy="vacuum_scalp",
                                capital=7.0, use_fees=False)).run()
        # fees only reduce P&L; final equity without fees >= with fees
        assert no_fee.final_equity >= with_fee.final_equity - 0.001

    def test_sl_override_tightens(self):
        s = make_settings()
        ticks = make_ticks(900)
        cfg = BacktestConfig(strategy="vacuum_scalp", capital=7.0, sl_pct_override=0.03)
        bt = BacktestEngine(s, ticks, cfg)
        bt.run()
        assert bt.s_local.vacuum_scalp_sl_pct == 0.03

    def test_metrics_contain_sharpe_and_drawdown(self):
        s = make_settings()
        ticks = make_ticks(1200)
        m = BacktestEngine(s, ticks, BacktestConfig(strategy="vacuum_scalp",
                           capital=10.0)).run()
        assert isinstance(m.max_drawdown_pct, float)
        assert m.max_drawdown_pct >= 0
        assert isinstance(m.sharpe, float)


class TestSpreadCaptureStrategy:
    def test_inherits_vacuum_scalp_entry(self):
        s = make_settings(spread_capture_enabled=True, vacuum_scalp_enabled=True)
        strat = SpreadCaptureStrategy(s)
        assert strat.enabled()
        assert strat.entry_type == EntryType.VACUUM_SCALP

    def test_hedge_evaluated_only_when_pair_below_threshold(self):
        s = make_settings(spread_capture_enabled=True,
                          spread_capture_pair_threshold=0.97,
                          spread_capture_min_edge=0.03,
                          backtest_taker_fee=0.02)
        strat = SpreadCaptureStrategy(s)

        class FakeBook:
            def __init__(self, ba, bb, bv=100):
                self.best_ask, self.best_bid, self.bid_volume = ba, bb, bv
                self.asks = [{"price": ba}]

        class FakePrices:
            def __init__(self, prim_ask, opp_ask, prim_bb):
                self._books = {"UP": FakeBook(prim_ask, prim_bb), "DOWN": FakeBook(opp_ask, 0.3)}
            def get_book_with_max_age(self, tid, age):
                return self._books.get(tid)
            def get_book(self, tid):
                return self._books.get(tid)

        class FakePos:
            direction = "UP"
            entry_price = 0.95
            current_size = 50
            closed = False
            token_id = "UP"

        class FakeRisk:
            pass

        market = {"up_token_id": "UP", "down_token_id": "DOWN"}

        # cheap opposite side (0.01): pair = 0.96 < 0.97, gross edge 0.04 > 0.03
        prices = FakePrices(prim_ask=0.95, opp_ask=0.01, prim_bb=0.94)
        r = strat.evaluate_hedge(FakePos(), prices, market, bot_balance=10.0,
                                 risk=FakeRisk())
        assert r["should_hedge"]
        assert r["size"] > 0

        # expensive opposite (0.10): pair = 1.05 > threshold → no hedge
        prices2 = FakePrices(prim_ask=0.95, opp_ask=0.10, prim_bb=0.94)
        r2 = strat.evaluate_hedge(FakePos(), prices2, market, bot_balance=10.0,
                                  risk=FakeRisk())
        assert not r2["should_hedge"]
