"""
Tests for the real Polymarket data fetcher.

Mocks reproduce the EXACT JSON shapes returned by Gamma and CLOB APIs (captured
from live requests during development). No network needed.
"""

import asyncio
import json

import pytest

from backend.app.backtest.poly_fetcher import (
    BacktestDataset, IntervalMeta, PolymarketDataFetcher,
    _resolve_winner, _iso_to_epoch, _step_value,
)
from backend.app.backtest.data import PriceTick
from backend.app.config import Settings


def make_settings(**kw):
    base = dict(private_key="0xK", funder_address="0xA", initial_balance=7.0,
                assets=["BTC"])
    base.update(kw)
    return Settings(**base)


# ── Real Gamma response shape (from btc-updown-5m-1771168800) ─────────────
GAMMA_RESPONSE = {
    "markets": [{
        "clobTokenIds": '["UP_TOKEN_123", "DOWN_TOKEN_456"]',
        "outcomes": '["Up", "Down"]',
        "outcomePrices": '["0", "1"]',   # Down won
        "volume": "110092.73",
        "endDate": "2026-02-15T15:25:00Z",
    }],
    "startDate": "2026-02-15T15:20:00Z",
}

# ── Real CLOB prices-history shape ─────────────────────────────────────────
PRICES_HISTORY_UP = {"history": [
    {"t": 1771168820, "p": 0.515},
    {"t": 1771168879, "p": 0.385},
    {"t": 1771168953, "p": 0.615},
    {"t": 1771169077, "p": 0.4},
]}
PRICES_HISTORY_DOWN = {"history": [
    {"t": 1771168820, "p": 0.485},
    {"t": 1771168879, "p": 0.615},
    {"t": 1771168953, "p": 0.385},
    {"t": 1771169077, "p": 0.6},
]}


class _FakeResp:
    def __init__(self, status, payload):
        self.status = status
        self._payload = payload

    async def json(self):
        return self._payload

    def raise_for_status(self):
        pass


class _FakeSession:
    """Routes GET by URL substring to canned responses."""
    def __init__(self):
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, params))
        if "events/slug" in url:
            return _FakeCtxMGR(_FakeResp(200, GAMMA_RESPONSE))
        if "prices-history" in url:
            token = (params or {}).get("market", "")
            hist = PRICES_HISTORY_UP if token == "UP_TOKEN_123" else PRICES_HISTORY_DOWN
            return _FakeCtxMGR(_FakeResp(200, hist))
        return _FakeCtxMGR(_FakeResp(404, {}))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        pass


class _FakeCtxMGR:
    def __init__(self, resp):
        self._resp = resp

    async def __aenter__(self):
        return self._resp

    async def __aexit__(self, *a):
        pass


class TestResolveWinner:
    def test_down_won(self):
        assert _resolve_winner(["Up", "Down"], ["0", "1"]) == "Down"

    def test_up_won(self):
        assert _resolve_winner(["Up", "Down"], ["1", "0"]) == "Up"

    def test_unresolved(self):
        # Live trading prices (not finalized) → should return None
        assert _resolve_winner(["Up", "Down"], ["0.52", "0.48"]) is None

    def test_empty(self):
        assert _resolve_winner([], []) is None


class TestIntervalEpochs:
    def test_aligned(self):
        epochs = PolymarketDataFetcher.interval_epochs(1771168830, 1771169100, 5)
        assert epochs == [1771168800]  # end excluded

    def test_full_range(self):
        epochs = PolymarketDataFetcher.interval_epochs(1771168800, 1771170000, 5)
        assert len(epochs) == 4
        assert all(e % 300 == 0 for e in epochs)


class TestStepValue:
    def test_most_recent_before(self):
        pts = [(100, 0.5), (200, 0.6), (300, 0.7)]
        assert _step_value(pts, 250) == 0.6

    def test_exact(self):
        assert _step_value([(100, 0.5)], 100) == 0.5

    def test_before_first(self):
        assert _step_value([(100, 0.5)], 50) is None


class TestIsoToEpoch:
    def test_zulu(self):
        assert _iso_to_epoch("2026-02-15T15:25:00Z") == 1771169100

    def test_none(self):
        assert _iso_to_epoch(None) == 0


class TestFetchInterval:
    def test_gamma_parse_and_winner(self):
        s = make_settings()
        f = PolymarketDataFetcher(s)
        meta = asyncio.run(f._fetch_gamma(_FakeSession(), "BTC", 1771168800, 5))
        assert meta is not None
        assert meta.up_token_id == "UP_TOKEN_123"
        assert meta.down_token_id == "DOWN_TOKEN_456"
        assert meta.winner == "Down"
        assert meta.up_won is False
        assert meta.volume == pytest.approx(110092.73)
        assert meta.end_ts == 1771169100

    def test_token_history_fetch(self):
        s = make_settings()
        f = PolymarketDataFetcher(s)
        meta = IntervalMeta(
            interval_ts=1771168800, asset="BTC",
            up_token_id="UP_TOKEN_123", down_token_id="DOWN_TOKEN_456",
            winner="Down", volume=110092.0, end_ts=1771171500)
        hist = asyncio.run(f._fetch_token_history(_FakeSession(), meta, fidelity=1))
        assert hist is not None
        assert len(hist["up"]) == 4
        assert hist["up"][0] == (1771168820, 0.515)
        assert len(hist["down"]) == 4

    def test_gamma_missing_market(self):
        s = make_settings()
        f = PolymarketDataFetcher(s)

        class Empty(_FakeSession):
            def get(self, url, params=None):
                return _FakeCtxMGR(_FakeResp(200, {"markets": []}))
        meta = asyncio.run(f._fetch_gamma(Empty(), "BTC", 999999, 5))
        assert meta is None


class TestDataset:
    def test_token_lookup_step_interpolation(self):
        ds = BacktestDataset(asset="BTC", interval_minutes=5)
        ds.oracle_ticks = [PriceTick(ts=1771168820, price=69000),
                           PriceTick(ts=1771168850, price=69100),
                           PriceTick(ts=1771168900, price=69050)]
        ds.token_history = {1771168800: {
            "up": [(1771168820, 0.515), (1771168879, 0.385)],
            "down": [(1771168820, 0.485)],
        }}
        lookup = ds.token_lookup()
        # ts=1771168820 → up 0.515, ts=1771168850 → still 0.515 (before 0.385),
        # ts=1771168900 → 0.385 (after 0.385)
        assert lookup[1771168820][0] == 0.515
        assert lookup[1771168850][0] == 0.515
        assert lookup[1771168900][0] == 0.385

    def test_token_lookup_complements_missing_side(self):
        ds = BacktestDataset(asset="BTC", interval_minutes=5)
        ds.oracle_ticks = [PriceTick(ts=100, price=1.0)]
        ds.token_history = {60: {"up": [(100, 0.7)], "down": []}}
        lookup = ds.token_lookup()
        # down missing → derived as 1 - 0.7 = 0.3
        assert lookup[100] == (0.7, 0.3)

    def test_stats(self):
        ds = BacktestDataset(asset="BTC", interval_minutes=5)
        ds.intervals = [
            IntervalMeta(100, "BTC", "u", "d", "Up", 5000, 400),
            IntervalMeta(400, "BTC", "u", "d", "Down", 3000, 700),
            IntervalMeta(700, "BTC", "u", "d", None, 1000, 1000),
        ]
        st = ds.stats()
        assert st["intervals_total"] == 3
        assert st["intervals_resolved"] == 2
        assert st["up_wins"] == 1
        assert st["down_wins"] == 1
        assert st["total_volume"] == 9000
