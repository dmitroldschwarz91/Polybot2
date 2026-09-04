"""Balance reconciliation & on-chain platform snapshot post-audit."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

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
        self.audit_log_path = Path(settings.log_dir) / "balance_audits.jsonl"
        self.audit_log_path.parent.mkdir(parents=True, exist_ok=True)

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

    def process_interval_snapshot(self, interval_num: int, is_first: bool,
                                  expected_pnl: float = 0.0,
                                  interval_ts: Optional[int] = None) -> dict:
        wu, ok = self.fetch_wallet_usdc()
        if not ok:
            return {"success": False, "bot_snap": self.state.prev_bot_snap}
        
        now = time.time()
        prev_wallet = self.state.prev_wallet_usdc
        if is_first or prev_wallet is None:
            if self.log:
                self.log.info(f"INITIAL SNAPSHOT #{interval_num}", wallet=f"${wu:.4f}")
            self.state.prev_wallet_usdc = wu
            self.state.prev_bot_snap = self.s.initial_balance
            self._log_audit(
                interval_num=interval_num,
                interval_ts=interval_ts or int(now),
                wallet_before=wu,
                wallet_after=wu,
                actual_delta=0.0,
                expected_pnl=0.0,
                discrepancy=0.0,
                reconciled_bot_balance=self.s.initial_balance,
                is_initial=True,
            )
            return {"success": True, "bot_snap": self.s.initial_balance}

        actual_delta = round(wu - prev_wallet, 4)
        discrepancy = round(actual_delta - expected_pnl, 4)
        nb = round(self.state.prev_bot_snap + actual_delta, 4)
        
        if self.log:
            self.log.info(
                f"SNAPSHOT #{interval_num}",
                usdc=f"${prev_wallet:.4f} -> ${wu:.4f} (delta ${actual_delta:+.4f})",
                expected_pnl=f"${expected_pnl:+.4f}",
                discrepancy=f"${discrepancy:+.4f}",
                bot=f"${nb:.2f}",
            )
            if abs(discrepancy) > 0.05 and not self.s.paper_trading:
                self.log.warning(
                    f"[BALANCE AUDIT] Discrepancy detected: actual on-chain ${actual_delta:+.4f} vs expected ${expected_pnl:+.4f}",
                    diff=f"${discrepancy:+.4f}"
                )
                
        self.state.prev_wallet_usdc = wu
        self.state.prev_bot_snap = nb
        self.state.total_profit += actual_delta

        self._log_audit(
            interval_num=interval_num,
            interval_ts=interval_ts or int(now),
            wallet_before=prev_wallet,
            wallet_after=wu,
            actual_delta=actual_delta,
            expected_pnl=expected_pnl,
            discrepancy=discrepancy,
            reconciled_bot_balance=nb,
            is_initial=False,
        )

        return {
            "success": True,
            "bot_snap": nb,
            "actual_delta": actual_delta,
            "expected_pnl": expected_pnl,
            "discrepancy": discrepancy,
        }

    def reconcile_with_platform(self, interval_num: int, expected_pnl: float = 0.0,
                                interval_ts: Optional[int] = None,
                                audit_delay_secs: float = 60.0) -> dict:
        """Post-audit balance snapshot 60s after interval close.
        Uses platform / on-chain USDC balance as absolute ground truth.
        Overwrites bot's internal tracking balance with the platform balance."""
        wu, ok = self.fetch_wallet_usdc()
        if not ok or wu is None:
            # Fallback for offline/unfunded/paper mode
            return {"success": False, "bot_snap": self.state.prev_bot_snap}

        now = time.time()
        prev_wallet = self.state.prev_wallet_usdc
        if prev_wallet is None:
            prev_wallet = self.s.initial_balance

        actual_delta = round(wu - prev_wallet, 4)
        discrepancy = round(actual_delta - expected_pnl, 4)
        platform_balance = round(wu, 4)
        
        if self.log:
            self.log.info(
                f"[POST-AUDIT +60s] Interval #{interval_num} platform USDC: ${prev_wallet:.4f} -> ${wu:.4f} "
                f"(actual delta ${actual_delta:+.4f}, expected ${expected_pnl:+.4f}, diff ${discrepancy:+.4f})",
                platform_balance=f"${platform_balance:.2f}"
            )
            if abs(discrepancy) > 0.01 and not self.s.paper_trading:
                self.log.warning(
                    f"[BALANCE DISCREPANCY] Discrepancy ${discrepancy:+.4f} detected. "
                    f"Overriding bot balance with platform ground truth: ${platform_balance:.2f}",
                    platform_usdc=f"${wu:.4f}", expected_pnl=f"${expected_pnl:+.4f}"
                )

        self.state.prev_wallet_usdc = wu
        self.state.prev_bot_snap = platform_balance
        self.state.total_profit += actual_delta

        self._log_audit(
            interval_num=interval_num,
            interval_ts=interval_ts or int(now),
            wallet_before=prev_wallet,
            wallet_after=wu,
            actual_delta=actual_delta,
            expected_pnl=expected_pnl,
            discrepancy=discrepancy,
            reconciled_bot_balance=platform_balance,
            is_initial=False,
            audit_delay_secs=audit_delay_secs,
        )

        return {
            "success": True,
            "bot_snap": platform_balance,
            "actual_delta": actual_delta,
            "expected_pnl": expected_pnl,
            "discrepancy": discrepancy,
            "reconciled": True,
        }

    def _log_audit(self, interval_num: int, interval_ts: int,
                   wallet_before: float, wallet_after: float,
                   actual_delta: float, expected_pnl: float,
                   discrepancy: float, reconciled_bot_balance: float,
                   is_initial: bool = False,
                   audit_delay_secs: float = 60.0) -> None:
        now = time.time()
        rec = {
            "ts": int(now),
            "iso_ts": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
            "event": "BALANCE_AUDIT",
            "interval_num": interval_num,
            "interval_ts": interval_ts,
            "is_initial": is_initial,
            "audit_delay_secs": audit_delay_secs,
            "ground_truth_source": "polymarket_platform_wallet",
            "wallet_before": round(wallet_before, 4),
            "wallet_after": round(wallet_after, 4),
            "actual_wallet_delta": round(actual_delta, 4),
            "expected_pnl": round(expected_pnl, 4),
            "discrepancy": round(discrepancy, 4),
            "reconciled": True,
            "reconciled_bot_balance": round(reconciled_bot_balance, 4),
        }
        try:
            with open(self.audit_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except OSError:
            pass
