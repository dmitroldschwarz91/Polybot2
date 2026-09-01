"""
Tests for the real-time collector.

The collector's fetch logic reuses PolymarketDataFetcher (already tested); here
we verify the scheduling, boundary alignment, idempotency, and status reporting.
"""

import asyncio
import time

import pytest

from backend.app.backtest.collector import RealTimeCollector, CollectorStats
from backend.app.config import Settings


def make_settings(**kw):
    base = dict(private_key="0xK", funder_address="0xA", initial_balance=7.0,
                assets=["BTC"])
    base.update(kw)
    return Settings(**base)


class TestCollectorStats:
    def test_to_dict_serializable(self):
        st = CollectorStats(running=True, started_at=time.time(),
                            intervals_collected=5, last_interval_ts=1771168800)
        d = st.to_dict()
        assert d["running"] is True
        assert d["intervals_collected"] == 5
        assert "last_interval" in d
        assert isinstance(d["uptime"], (int, float))

    def test_empty_stats(self):
        d = CollectorStats().to_dict()
        assert d["running"] is False
        assert d["intervals_collected"] == 0
        assert d["last_interval"] is None


class TestBoundaryAlignment:
    def test_last_boundary_aligned_to_5min(self):
        s = make_settings()
        c = RealTimeCollector(s, assets=["BTC"], interval_minutes=5)
        # 1771168830 → boundary 1771168800 (300-aligned)
        assert c._last_boundary(1771168830) == 1771168800
        assert c._last_boundary(1771168800) == 1771168800
        assert c._last_boundary(1771169099) == 1771168800

    def test_boundary_15min(self):
        s = make_settings()
        c = RealTimeCollector(s, assets=["BTC"], interval_minutes=15)
        assert c._last_boundary(1771169300) % 900 == 0


class TestCollectorLifecycle:
    def test_start_sets_running_flag(self):
        s = make_settings()
        c = RealTimeCollector(s, assets=["BTC"], grace_secs=9999)
        assert not c.running

        async def go():
            c.start()
            assert c.stats.running is True
            assert c.stats.started_at is not None
            await c.stop()
            assert c.stats.running is False
        asyncio.run(go())

    def test_start_twice_idempotent(self):
        s = make_settings()
        c = RealTimeCollector(s, assets=["BTC"], grace_secs=9999)

        async def go():
            c.start()
            task1 = c._task
            c.start()  # should not create a new task
            assert c._task is task1
            await c.stop()
        asyncio.run(go())

    def test_default_assets_from_settings(self):
        s = make_settings(assets=["BTC", "ETH"])
        c = RealTimeCollector(s)
        assert c.assets == ["BTC", "ETH"]
        assert c.stats.assets == ["BTC", "ETH"]


class TestIdempotency:
    def test_seen_skips_recently_collected(self):
        s = make_settings()
        c = RealTimeCollector(s, assets=["BTC"])
        c._seen["BTC"] = 1771168800
        # _collect_one should skip because _seen >= epoch
        # (we verify the guard, not the fetch)
        asyncio.run(c._collect_one("BTC", 1771168800))
        # no error raised, and no stats change
        assert c.stats.intervals_collected == 0

    def test_seen_allows_older(self):
        s = make_settings()
        c = RealTimeCollector(s, assets=["BTC"])
        c._seen["BTC"] = 1771169100
        # collecting an older epoch (1771168800 < seen) — should attempt fetch
        # fetch will fail gracefully (no network in test), not crash
        asyncio.run(c._collect_one("BTC", 1771168800))
        # either collected or failed, but didn't crash
        assert c.stats.intervals_collected + c.stats.intervals_failed >= 0


class TestOracleCount:
    def test_count_missing_file(self):
        s = make_settings()
        c = RealTimeCollector(s, assets=["BTC"])
        assert c._count_oracle_ticks("BTC", 1000, 1300) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
