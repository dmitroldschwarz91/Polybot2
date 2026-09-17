import asyncio
import pytest
from pathlib import Path
from backend.app.config import Settings
from backend.app.marketdata.stores import LivePriceStore
from backend.app.demo.engine import DemoEngine
from backend.app.api.demo_routes import DemoConfigRequest, create_demo_router
from backend.app.api.routes import BotStartRequest, create_router
from backend.app.domain.models import polymarket_dynamic_taker_fee
from backend.app.engine.balance import BalanceManager


def test_live_price_store_set_assets():
    store = LivePriceStore(["BTC", "ETH"])
    assert store.assets == ["BTC", "ETH"]
    assert "BTC" in store.chainlink_history
    assert "ETH" in store.chainlink_history

    store.set_assets(["SOL", "XRP"])
    assert store.assets == ["SOL", "XRP"]
    assert "SOL" in store.chainlink_history
    assert "XRP" in store.chainlink_history
    assert "SOL" in store.range_history
    assert "SOL" in store.twap_reconstruction_history
    assert "XRP" in store.vwap_num


def test_demo_engine_custom_assets():
    s = Settings()
    demo = DemoEngine(s, assets=["SOL", "XRP"], strategy="twap_inertia")
    assert demo.s_demo.assets == ["SOL", "XRP"]
    assert demo.prices.assets == ["SOL", "XRP"]
    assert demo.s_demo.max_concurrent_positions == 2


def test_demo_twap_ignores_legacy_stake_override():
    s = Settings()
    s.max_stake_ratio = 0.20
    demo = DemoEngine(s, assets=["BTC"], strategy="twap_inertia", stake_ratio=0.05)

    assert demo.requested_stake_ratio == 0.05
    assert demo.stake_ratio == 0.20
    assert demo.s_demo.max_stake_ratio == 0.20


def test_demo_non_twap_keeps_explicit_stake_override():
    s = Settings()
    demo = DemoEngine(s, assets=["BTC"], strategy="vacuum_scalp", stake_ratio=0.05)

    assert demo.stake_ratio == 0.05
    assert demo.s_demo.max_stake_ratio == 0.05


def test_demo_config_request_with_assets():
    req = DemoConfigRequest(strategy="twap_inertia", assets=["BTC", "ETH", "SOL"])
    assert req.assets == ["BTC", "ETH", "SOL"]
    assert req.strategy == "twap_inertia"


def test_bot_start_request_with_assets():
    req = BotStartRequest(strategy="twap_inertia", assets=["ETH", "XRP"])
    assert req.assets == ["ETH", "XRP"]
    assert req.strategy == "twap_inertia"


def test_polymarket_dynamic_taker_fee():
    # 100 shares @ $0.50 -> 100 * 0.07 * 0.50 * 0.50 = $1.75 (peak fee 1.75%)
    fee_50 = polymarket_dynamic_taker_fee(100, 0.50)
    assert fee_50 == 1.75

    # 100 shares @ $0.70 -> 100 * 0.07 * 0.70 * 0.30 = $1.47
    fee_70 = polymarket_dynamic_taker_fee(100, 0.70)
    assert fee_70 == 1.47

    # 100 shares @ $0.90 -> 100 * 0.07 * 0.90 * 0.10 = $0.63
    fee_90 = polymarket_dynamic_taker_fee(100, 0.90)
    assert fee_90 == 0.63


def test_balance_audit_logging(tmp_path):
    s = Settings()
    s.log_dir = str(tmp_path)
    
    class DummyClient:
        def __init__(self):
            self.wallet = 100.0
        def fetch_wallet_usdc(self):
            return self.wallet, True
    
    client = DummyClient()
    bm = BalanceManager(s, client, None)
    
    # Initial snapshot
    r1 = bm.process_interval_snapshot(1, is_first=True)
    assert r1["success"]
    assert bm.state.prev_wallet_usdc == 100.0

    # Trade completed: wallet grew to $105.00, expected PnL was +$5.00
    client.wallet = 105.0
    r2 = bm.process_interval_snapshot(2, is_first=False, expected_pnl=5.0)
    assert r2["success"]
    assert r2["actual_delta"] == 5.0
    assert r2["discrepancy"] == 0.0

    # Audit file exists and contains 2 records
    audit_file = tmp_path / "balance_audits.jsonl"
    assert audit_file.exists()
    lines = audit_file.read_text().strip().split("\n")
    assert len(lines) == 2


def test_balance_reconcile_with_platform_delayed(tmp_path):
    s = Settings()
    s.log_dir = str(tmp_path)

    class DummyClient:
        def __init__(self):
            self.wallet = 100.0
        def fetch_wallet_usdc(self):
            return self.wallet, True

    client = DummyClient()
    bm = BalanceManager(s, client, None)
    bm.state.prev_wallet_usdc = 100.0
    bm.state.prev_bot_snap = 100.0

    # On-chain wallet actual balance is $107.50, but bot expected $108.00 (diff -$0.50 due to slippage/gas)
    client.wallet = 107.50
    res = bm.reconcile_with_platform(interval_num=5, expected_pnl=8.00, interval_ts=1787306000, audit_delay_secs=60.0)
    
    assert res["success"]
    assert res["reconciled"]
    assert res["actual_delta"] == 7.50
    assert res["discrepancy"] == -0.50
    # Platform ground truth took precedence:
    assert res["bot_snap"] == 107.50
    assert bm.state.prev_bot_snap == 107.50
    assert bm.state.prev_wallet_usdc == 107.50



def test_normalize_asset():
    from backend.app.marketdata.websockets import normalize_asset
    assert normalize_asset("btc/usd") == "BTC"
    assert normalize_asset("BTC-USD") == "BTC"
    assert normalize_asset("btcusdt") == "BTC"
    assert normalize_asset("ETH/USD") == "ETH"
    assert normalize_asset("ethusdt") == "ETH"
    assert normalize_asset("sol/usd") == "SOL"
    assert normalize_asset("solusdt") == "SOL"
    assert normalize_asset("xrp/usd") == "XRP"
    assert normalize_asset("xrpusdt") == "XRP"
    assert normalize_asset("DOGE") is None


def test_rtds_subscriptions_are_asset_filtered_and_use_configured_twap_window():
    from backend.app.marketdata.websockets import WebSocketManager
    from backend.app.marketdata.stores import FillStore
    from backend.app.core.logging import build_logger

    s = Settings()
    s.assets = ["BTC", "ETH"]
    s.chainlink_twap_window = 60
    store = LivePriceStore(s.assets)
    ws = WebSocketManager(s, store, FillStore(), build_logger("test-rtds"))

    subs = ws._build_rtds_subscriptions()

    assert {sub["topic"] for sub in subs} == {
        "crypto_prices", "crypto_prices_chainlink", "crypto_prices_twap_sixty"
    }
    assert {sub["filters"] for sub in subs if sub["topic"] == "crypto_prices_chainlink"} == {
        '{"symbol":"btc/usd"}', '{"symbol":"eth/usd"}'
    }
    assert {sub["filters"] for sub in subs if sub["topic"] == "crypto_prices_twap_sixty"} == {
        '{"symbol":"btc/usd"}', '{"symbol":"eth/usd"}'
    }
    assert [sub for sub in subs if sub["topic"] == "crypto_prices"] == [
        {"topic": "crypto_prices", "type": "update", "filters": "btcusdt,ethusdt"}
    ]
    assert "crypto_prices_twap_thirty" not in {sub["topic"] for sub in subs}
    assert "crypto_prices_twap" not in {sub["topic"] for sub in subs}


def test_rtds_ignores_wrong_twap_window():
    from backend.app.marketdata.websockets import WebSocketManager
    from backend.app.marketdata.stores import FillStore
    from backend.app.core.logging import build_logger

    s = Settings()
    s.assets = ["BTC"]
    s.chainlink_twap_window = 60
    store = LivePriceStore(["BTC"])
    ws = WebSocketManager(s, store, FillStore(), build_logger("test-rtds-window"))

    ws._process_single_rtds({
        "topic": "crypto_prices_twap_thirty",
        "type": "update",
        "payload": {"symbol": "btc/usd", "value": 80010, "window_s": 30, "timestamp": 123000},
    })
    assert "BTC" not in store.chainlink_twap

    ws._process_single_rtds({
        "topic": "crypto_prices_twap_sixty",
        "type": "update",
        "payload": {"symbol": "btc/usd", "value": 80020, "window_s": 60, "timestamp": 123001},
    })
    assert store.chainlink_twap["BTC"] == 80020.0


def test_market_ws_last_trade_does_not_overwrite_best_ask():
    from backend.app.marketdata.websockets import WebSocketManager
    from backend.app.marketdata.stores import FillStore
    from backend.app.core.logging import build_logger

    s = Settings()
    store = LivePriceStore(["BTC"])
    ws = WebSocketManager(s, store, FillStore(), build_logger("test-ws"))
    store.update_lot_price("tok", 0.52, 0.50)

    ws._process_market_msg({"event_type": "last_trade_price", "asset_id": "tok", "price": "0.80"})

    assert store.get_book("tok").best_ask == 0.52
    assert store.get_lot_price("tok") == 0.52


def test_market_ws_accepts_payload_camelcase_market_events():
    from backend.app.marketdata.websockets import WebSocketManager
    from backend.app.marketdata.stores import FillStore
    from backend.app.core.logging import build_logger

    s = Settings()
    store = LivePriceStore(["BTC"])
    ws = WebSocketManager(s, store, FillStore(), build_logger("test-ws-camel"))

    ws._process_market_msg({
        "topic": "market",
        "type": "book",
        "payload": {
            "tokenId": "tok",
            "bids": [{"price": "0.40", "size": "100"}],
            "asks": [{"price": "0.42", "size": "7"}, {"price": "0.43", "size": "20"}],
        },
    })
    assert store.get_book("tok").best_ask == 0.42
    assert store.ask_volume_up_to("tok", 0.43) == 27.0

    ws._process_market_msg({
        "topic": "market",
        "type": "price_change",
        "payload": {"priceChanges": [{"tokenId": "tok", "bestAsk": "0.41", "bestBid": "0.40"}]},
    })
    assert store.get_book("tok").best_ask == 0.41
    assert store.get_book("tok").best_bid == 0.40


def test_market_ws_dynamic_subscribe_uses_operation():
    from backend.app.marketdata.websockets import WebSocketManager
    from backend.app.marketdata.stores import FillStore
    from backend.app.core.logging import build_logger
    import json

    class FakeWS:
        def __init__(self):
            self.sent = []
        async def send(self, payload):
            self.sent.append(json.loads(payload))

    async def go():
        s = Settings()
        store = LivePriceStore(["BTC"])
        ws = WebSocketManager(s, store, FillStore(), build_logger("test-ws"))
        fake = FakeWS()
        assert await ws._send_subscription(fake, {"tok1"}, operation="subscribe") is True
        assert fake.sent[0]["operation"] == "subscribe"
        assert fake.sent[0]["custom_feature_enabled"] is True

    asyncio.run(go())


def test_book_poller_price_side_mapping():
    from backend.app.marketdata.book_poller import BookPoller

    async def go():
        s = Settings()
        store = LivePriceStore(["BTC"])
        poller = BookPoller(s, store)

        async def no_book(token_id):
            return None

        async def fake_price(token_id, side):
            # CLOB semantics: BUY is executable ask, SELL is executable bid.
            return {"price": "0.41" if side == "BUY" else "0.39"}

        poller._fetch_book = no_book
        poller._fetch_price = fake_price
        await poller._poll_token("tok")

        book = store.get_book("tok")
        assert book.best_ask == 0.41
        assert book.best_bid == 0.39

    asyncio.run(go())


def test_book_poller_prefers_full_book_depth():
    from backend.app.marketdata.book_poller import BookPoller

    async def go():
        s = Settings()
        store = LivePriceStore(["BTC"])
        poller = BookPoller(s, store)

        async def fake_book(token_id):
            return {
                "bids": [{"price": "0.38", "size": "10"}],
                "asks": [{"price": "0.42", "size": "7"}],
            }

        poller._fetch_book = fake_book
        await poller._poll_token("tok")

        book = store.get_book("tok")
        assert book.best_ask == 0.42
        assert book.best_ask_size == 7.0
        assert store.ask_size_at("tok", 0.42) == 7.0

    asyncio.run(go())


@pytest.mark.asyncio
async def test_live_twap_entry_uses_slippage_band_for_limit_and_depth(tmp_path):
    from backend.app.engine.bot import TradingEngine
    from backend.app.strategies.twap_inertia import TWAPInertiaStrategy
    from backend.app.strategies.base import Opportunity

    s = Settings()
    s.log_dir = str(tmp_path)
    s.assets = ["BTC"]
    s.paper_trading = True
    s.max_stake_ratio = 0.20
    s.twap_slippage_tol = 0.01
    engine = TradingEngine(s)
    engine.status.bot_balance = 100.0
    engine.status.initial_balance = 100.0

    token_id = "tok_up"
    engine.prices.update_full_book(
        token_id,
        bids=[{"price": "0.54", "size": "100"}],
        asks=[{"price": "0.55", "size": "5"}, {"price": "0.56", "size": "50"}],
    )

    class FakeClient:
        paper = True
        def get_real_balance(self):
            return 100.0

    class FakeExecutor:
        def __init__(self):
            self.calls = []
        async def execute_buy(self, token_id_arg, price, size, asset, max_budget):
            self.calls.append((token_id_arg, price, size, asset, max_budget))
            return {"success": True, "price": price, "size": size, "cost": round(price * size, 4), "order_id": "ord1"}

    engine.client = FakeClient()
    engine.executor = FakeExecutor()
    strat = TWAPInertiaStrategy(s)
    opp = Opportunity(
        can_enter=True, direction="UP", token_id=token_id, entry_price=0.55,
        target_price=80000.0, oracle_price=80050.0, deviation=0.0005,
        imbalance=0.5, confidence=0.9,
    )
    market = {"slug": "btc-updown-test", "end_ts": 9999999999}

    ok = await engine._enter(strat, "BTC", market, opp)

    assert ok is True
    assert engine.executor.calls[0][1] == 0.56  # best ask + twap_slippage_tol
    assert engine.executor.calls[0][2] == 35    # $20 stake / $0.56, not capped by best-ask-only size=5


def test_live_interval_samples_writing(tmp_path):
    from backend.app.sample_io import load_samples
    from backend.app.engine.bot import TradingEngine
    
    s = Settings()
    s.log_dir = str(tmp_path)
    s.assets = ["BTC"]
    bot = TradingEngine(s)
    
    market = {
        "slug": "btc-updown-5m-1757300000",
        "up_token_id": "tok_up_123",
        "down_token_id": "tok_dn_123",
        "end_ts": 1757300300,
        "target_price": 95000.0,
    }
    
    bot._sample_interval(market, "BTC", stc=45.0)
    samples_file = tmp_path / "live_interval_samples.jsonl"
    assert samples_file.exists()
    
    samples = load_samples(samples_file)
    assert "btc-updown-5m-1757300000" in samples
    rec = samples["btc-updown-5m-1757300000"][0]
    assert rec["asset"] == "BTC"
    assert rec["secs_to_close"] == 45
