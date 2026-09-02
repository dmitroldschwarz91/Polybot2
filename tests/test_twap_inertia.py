import time
import pytest
from backend.app.config import Settings
from backend.app.strategies.twap_inertia import TWAPInertiaStrategy
from backend.app.marketdata.stores import LivePriceStore, OrderBook
from backend.app.marketdata.markets import MarketData
from backend.app.risk.manager import RiskManager
from backend.app.sample_io import AsyncSampleBuffer, format_sample_row, FIELDS


class DummyHTTP:
    pass


@pytest.fixture
def settings():
    s = Settings()
    s.twap_inertia_enabled = True
    s.twap_min_dev_pct = 0.015
    s.twap_min_barrier_pct = 0.070
    s.twap_min_token_ask = 0.55
    s.twap_max_token_ask = 0.92
    s.twap_stc_min = 10.0
    s.twap_stc_max = 35.0
    s.twap_max_feed_age = 3.0
    s.twap_min_level_depth = 5
    s.assets = ["BTC", "ETH", "SOL", "XRP"]
    return s


@pytest.fixture
def env(settings):
    prices = LivePriceStore(settings.assets, 30.0)
    risk = RiskManager(settings)
    market_data = MarketData(settings, prices, DummyHTTP(), None)
    strategy = TWAPInertiaStrategy(settings)
    return strategy, prices, market_data, risk


def test_twap_inertia_valid_entry(env):
    strategy, prices, market_data, risk = env
    now = time.time()

    # Setup market data
    asset = "BTC"
    cur_interval = int(now // 300) * 300
    market_data.start_prices[str(cur_interval)] = {asset: 80000.0}

    # Setup live TWAP & oracle (accumulated +0.04% above open)
    prices.update_chainlink_twap(asset, 80032.0, now)
    prices.update_binance_direct(asset, 80035.0, 1.0)
    prices.chainlink_ts[asset] = now

    # Setup token books: UP token at $0.85, DOWN token at $0.15
    up_token = "tok_up_123"
    down_token = "tok_down_123"
    prices.update_full_book(
        up_token,
        [{"price": "0.84", "size": "100"}],
        [{"price": "0.85", "size": "50"}],
    )
    prices.update_full_book(
        down_token,
        [{"price": "0.14", "size": "100"}],
        [{"price": "0.15", "size": "50"}],
    )

    market = {
        "slug": "btc-5m-test",
        "end_ts": now + 20.0, # STC = 20s (elapsed = 40s)
        "up_token_id": up_token,
        "down_token_id": down_token,
        "target_price": 80000.0,
    }

    # Barrier = 0.04% * (40 / 20) = 0.08% >= 0.07% -> Valid entry
    opp = strategy.check(market, asset, set(), 100.0, prices, market_data, risk)

    assert opp.can_enter is True
    assert opp.direction == "UP"
    assert opp.entry_price == 0.85
    assert opp.reason == "twap_barrier_locked"
    assert opp.extra["barrier_pct"] == 0.08


def test_twap_inertia_rejects_frozen_book(env):
    strategy, prices, market_data, risk = env
    now = time.time()

    asset = "BTC"
    cur_interval = int(now // 300) * 300
    market_data.start_prices[str(cur_interval)] = {asset: 80000.0}

    prices.update_chainlink_twap(asset, 80040.0, now)
    prices.update_binance_direct(asset, 80040.0, 1.0)
    prices.chainlink_ts[asset] = now

    # Frozen book: both up and down tokens stuck at 0.51 / 0.50
    up_token = "tok_up_frozen"
    down_token = "tok_down_frozen"
    prices.update_full_book(
        up_token,
        [{"price": "0.49", "size": "100"}],
        [{"price": "0.51", "size": "100"}],
    )
    prices.update_full_book(
        down_token,
        [{"price": "0.49", "size": "100"}],
        [{"price": "0.50", "size": "100"}],
    )

    market = {
        "slug": "btc-5m-frozen",
        "end_ts": now + 20.0,
        "up_token_id": up_token,
        "down_token_id": down_token,
        "target_price": 80000.0,
    }

    opp = strategy.check(market, asset, set(), 100.0, prices, market_data, risk)
    assert opp.can_enter is False
    assert opp.reason == "frozen_market_50_50"


def test_twap_inertia_rejects_stale_feed(env):
    strategy, prices, market_data, risk = env
    now = time.time()

    asset = "BTC"
    cur_interval = int(now // 300) * 300
    market_data.start_prices[str(cur_interval)] = {asset: 80000.0}

    # Stale TWAP feed (> 3.0 seconds old)
    prices.update_chainlink_twap(asset, 80050.0, now - 5.0)
    prices.chainlink_ts[asset] = now - 5.0

    up_token = "tok_up_123"
    down_token = "tok_down_123"
    prices.update_full_book(up_token, [], [{"price": "0.85", "size": "50"}])

    market = {
        "slug": "btc-5m-stale",
        "end_ts": now + 20.0,
        "up_token_id": up_token,
        "down_token_id": down_token,
        "target_price": 80000.0,
    }

    opp = strategy.check(market, asset, set(), 100.0, prices, market_data, risk)
    assert opp.can_enter is False
    assert opp.reason == "stale_twap_or_oracle_feed"


def test_twap_inertia_window_timing(env):
    strategy, prices, market_data, risk = env
    now = time.time()

    asset = "BTC"
    cur_interval = int(now // 300) * 300
    market_data.start_prices[str(cur_interval)] = {asset: 80000.0}
    prices.update_chainlink_twap(asset, 80050.0, now)
    prices.update_binance_direct(asset, 80050.0, 1.0)
    prices.chainlink_ts[asset] = now

    up_token = "tok_up_123"
    prices.update_full_book(up_token, [], [{"price": "0.85", "size": "50"}])

    # STC = 5s (too late, < 10s)
    market_late = {"slug": "btc-5m-late", "end_ts": now + 5.0, "up_token_id": up_token}
    assert strategy.check(market_late, asset, set(), 100.0, prices, market_data, risk).reason == "too_late"

    # STC = 50s (too early, > 35s)
    market_early = {"slug": "btc-5m-early", "end_ts": now + 50.0, "up_token_id": up_token}
    assert strategy.check(market_early, asset, set(), 100.0, prices, market_data, risk).reason == "too_early"


@pytest.mark.asyncio
async def test_async_sample_buffer(tmp_path):
    log_file = tmp_path / "test_samples.tsv"
    buf = AsyncSampleBuffer(log_file, flush_interval=0.1, max_batch=2)
    buf.start()

    rec1 = {"ts": 12345, "asset": "BTC", "slug": "btc-5m-1", "secs_to_close": 20, "twap": 80000.0}
    rec2 = {"ts": 12346, "asset": "ETH", "slug": "eth-5m-1", "secs_to_close": 20, "twap": 3000.0}

    buf.push(rec1)
    buf.push(rec2)

    await buf.stop()

    assert log_file.exists()
    content = log_file.read_text()
    assert content.startswith("# ts\tasset")
    assert "btc-5m-1" in content
    assert "eth-5m-1" in content
