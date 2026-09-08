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
    assert "XRP" in store.vwap_num


def test_demo_engine_custom_assets():
    s = Settings()
    demo = DemoEngine(s, assets=["SOL", "XRP"], strategy="twap_inertia")
    assert demo.s_demo.assets == ["SOL", "XRP"]
    assert demo.prices.assets == ["SOL", "XRP"]
    assert demo.s_demo.max_concurrent_positions == 2


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
