"""Balance reconciliation — mirrors BalanceState logic from the original."""

from __future__ import annotations

from ..config import Settings
from ..core.logging import StructuredLogger
from ..domain.models import BalanceState
from ..execution.client import TradingClient


class BalanceManager:
    def __init__(self, settings: Settings, client: TradingClient, log: StructuredLogger) -> None:
        self.s = settings
        self.client = client
        self.log = log
        self.state = BalanceState(prev_bot_snap=settings.initial_balance)

    def fetch_wallet_usdc(self):
        return self.client.fetch_wallet_usdc()

    def refresh_for_early_entry(self) -> dict:
        wu, ok = self.fetch_wallet_usdc()
        if not ok:
            return {"updated": False, "bot_balance": self.state.prev_bot_snap}
        if self.state.prev_wallet_usdc is None:
            self.state.prev_wallet_usdc = wu
            self.state.prev_bot_snap = self.s.initial_balance
            return {"updated": True, "bot_balance": self.s.initial_balance}
        pa = round(wu - self.state.prev_wallet_usdc, 4)
        nb = round(self.state.prev_bot_snap + pa, 4)
        self.state.prev_wallet_usdc = wu
        self.state.prev_bot_snap = nb
        self.state.total_profit += pa
        return {"updated": True, "bot_balance": nb}

    def process_interval_snapshot(self, interval_num: int, is_first: bool) -> dict:
        wu, ok = self.fetch_wallet_usdc()
        if not ok:
            return {"success": False, "bot_snap": self.state.prev_bot_snap}
        if is_first:
            self.log.info(f"INITIAL SNAPSHOT #{interval_num}", wallet=f"${wu:.4f}")
            self.state.prev_wallet_usdc = wu
            self.state.prev_bot_snap = self.s.initial_balance
            return {"success": True, "bot_snap": self.s.initial_balance}
        pa = round(wu - self.state.prev_wallet_usdc, 4)
        nb = round(self.state.prev_bot_snap + pa, 4)
        self.log.info(f"SNAPSHOT #{interval_num}",
                      usdc=f"${self.state.prev_wallet_usdc:.4f} -> ${wu:.4f}", bot=f"${nb:.2f}")
        self.state.prev_wallet_usdc = wu
        self.state.prev_bot_snap = nb
        self.state.total_profit += pa
        return {"success": True, "bot_snap": nb}
