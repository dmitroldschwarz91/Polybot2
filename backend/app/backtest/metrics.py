"""
Backtest performance metrics.

Standard quant metrics computed from the equity curve + trade log, plus a few
Polymarket-specific ones (fee drag, fee-adjusted vs gross edge).
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import List


@dataclass
class BacktestMetrics:
    # headline
    final_equity: float = 0.0
    total_return_pct: float = 0.0
    total_pnl: float = 0.0
    # trades
    num_trades: int = 0
    num_wins: int = 0
    num_losses: int = 0
    win_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    profit_factor: float = 0.0
    expectancy: float = 0.0
    # risk
    max_drawdown_pct: float = 0.0
    sharpe: float = 0.0
    sortino: float = 0.0
    # costs
    total_fees: float = 0.0
    fee_drag_pct: float = 0.0       # fees as % of gross profit
    gross_pnl: float = 0.0
    # series
    equity_curve: List[dict] = field(default_factory=list)
    trade_pnls: List[float] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "final_equity": round(self.final_equity, 4),
            "total_return_pct": round(self.total_return_pct, 2),
            "total_pnl": round(self.total_pnl, 4),
            "num_trades": self.num_trades,
            "win_rate": round(self.win_rate, 4),
            "avg_win": round(self.avg_win, 4),
            "avg_loss": round(self.avg_loss, 4),
            "profit_factor": round(self.profit_factor, 4),
            "expectancy": round(self.expectancy, 4),
            "max_drawdown_pct": round(self.max_drawdown_pct, 2),
            "sharpe": round(self.sharpe, 4),
            "sortino": round(self.sortino, 4),
            "total_fees": round(self.total_fees, 4),
            "fee_drag_pct": round(self.fee_drag_pct, 2),
            "gross_pnl": round(self.gross_pnl, 4),
            "equity_curve": self.equity_curve,
        }


def compute_metrics(
    equity_curve: List[dict],
    trade_log: List[dict],
    initial_capital: float,
    total_fees: float,
) -> BacktestMetrics:
    """Compute metrics from equity samples (ts, equity) and closed-trade log."""
    m = BacktestMetrics()
    m.equity_curve = equity_curve
    m.total_fees = total_fees

    # closed-trade PnLs (REDEEM + SELL with pnl)
    pnls = [t["pnl"] for t in trade_log
            if t.get("action") in ("REDEEM", "SELL") and "pnl" in t]
    m.trade_pnls = pnls
    m.num_trades = len(pnls)
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    m.num_wins = len(wins)
    m.num_losses = len(losses)
    m.win_rate = (len(wins) / len(pnls)) if pnls else 0.0
    m.avg_win = statistics.mean(wins) if wins else 0.0
    m.avg_loss = statistics.mean(losses) if losses else 0.0
    gp = sum(wins)
    gl = abs(sum(losses))
    m.profit_factor = (gp / gl) if gl > 0 else float("inf") if gp > 0 else 0.0
    m.expectancy = statistics.mean(pnls) if pnls else 0.0
    m.gross_pnl = sum(pnls)  # net of fees already in sim, but gross_pnl here = net realized
    m.total_pnl = sum(pnls)

    # equity / drawdown / sharpe
    if equity_curve:
        m.final_equity = equity_curve[-1]["equity"]
        m.total_return_pct = ((m.final_equity / initial_capital) - 1) * 100 if initial_capital > 0 else 0.0

        # max drawdown
        peak = equity_curve[0]["equity"]
        max_dd = 0.0
        for pt in equity_curve:
            eq = pt["equity"]
            if eq > peak:
                peak = eq
            dd = (peak - eq) / peak if peak > 0 else 0.0
            if dd > max_dd:
                max_dd = dd
        m.max_drawdown_pct = max_dd * 100

        # per-step returns for Sharpe/Sortino
        rets = []
        prev = equity_curve[0]["equity"]
        for pt in equity_curve[1:]:
            if prev > 0:
                rets.append((pt["equity"] / prev) - 1.0)
            prev = pt["equity"]
        if len(rets) >= 2:
            mean_r = statistics.mean(rets)
            std_r = statistics.pstdev(rets)
            # annualisation factor depends on sampling; we report raw (per-step)
            m.sharpe = (mean_r / std_r * math.sqrt(len(rets))) if std_r > 0 else 0.0
            downside = [r for r in rets if r < 0]
            dstd = math.sqrt(sum(r * r for r in downside) / len(downside)) if downside else 0.0
            m.sortino = (mean_r / dstd * math.sqrt(len(rets))) if dstd > 0 else 0.0

    # fee drag
    if gp > 0:
        m.fee_drag_pct = (total_fees / (gp + total_fees)) * 100
    return m
