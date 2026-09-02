"""
Central configuration.

Replaces ~120 module-level constants from the original script with a single
validated Pydantic Settings object that loads from environment / .env.

A strategy or risk parameter changed here takes effect on the next bot restart
(or, where supported, live via the dashboard's hot-reload endpoint).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Dict, List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_project_root() -> Path:
    """Locate the project root (dir containing 'backend/' and 'frontend/').

    Robust to WHERE the project sits on disk — walks up from this file until
    it finds a 'frontend/static/index.html'. Falls back to parents[2]
    (polymarket_bot/), which is correct for the standard layout:
        polymarket_bot/backend/app/config.py  →  parents[2] = polymarket_bot/
    """
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        if (parent / "frontend" / "static" / "index.html").exists():
            return parent
    return here.parents[2]


PROJECT_ROOT = _find_project_root()


class Settings(BaseSettings):
    """All tunable knobs live here. Secrets come from .env."""

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / "config" / ".env"),
        env_file_encoding="utf-8",
        env_prefix="POLY_",
        extra="ignore",
        case_sensitive=False,
    )

    # ── Secrets ───────────────────────────────────────────────────────────
    private_key: str = Field("YOUR_KEY", validation_alias="POLYMARKET_PRIVATE_KEY")
    funder_address: str = Field("YOUR_ADDRESS", validation_alias="POLYMARKET_FUNDER_ADDRESS")
    signature_type: int = 1
    chain_id: int = 137

    # ── General ───────────────────────────────────────────────────────────
    initial_balance: float = 100.0
    assets: List[str] = Field(default_factory=lambda: ["BTC", "ETH", "SOL", "XRP"])
    interval_minutes: int = 5
    poll_interval: float = 0.1
    quiet_mode: bool = True

    # ── Strategy toggles ──────────────────────────────────────────────────
    twap_inertia_enabled: bool = True     # ★ Main TWAP barrier & inertia strategy
    standard_entry_enabled: bool = False
    early_trend_enabled: bool = False
    vacuum_scalp_enabled: bool = False
    spread_capture_enabled: bool = False  # gabagool-inspired hedge-lite

    # ── TWAP Inertia Strategy (★ Post-TWAP 60s Reversal Barrier) ──────────
    twap_min_dev_pct: float = 0.015       # min TWAP deviation from open (+-0.015%)
    twap_min_barrier_pct: float = 0.070   # min irreversibility barrier B(t) >= 0.070%
    twap_min_token_ask: float = 0.55      # rejects undecided / frozen 0.50/0.51 books
    twap_max_token_ask: float = 0.92      # preserves attractive EV (> +10% net)
    twap_stc_min: float = 10.0            # no entries after 10s to close
    twap_stc_max: float = 35.0            # window for accumulated TWAP mass (10-35s)
    twap_max_feed_age: float = 3.0        # max age for TWAP/oracle feeds (outage watchdog)
    twap_min_level_depth: int = 5         # min shares available at best_ask level

    # ── Spread capture / hedge-lite ───────────────────────────────────────
    spread_capture_pair_threshold: float = 0.97   # buy opposite only if up+down below this
    spread_capture_min_edge: float = 0.03         # gross edge needed to cover 2-3% fees
    spread_capture_hedge_ratio: float = 0.60      # fraction of primary position to hedge
    spread_capture_max_stake_ratio: float = 0.50  # keep capital reserve on small budgets

    # ── Standard entry ────────────────────────────────────────────────────
    entry_window_secs: int = 11
    min_lot_price: float = 0.89
    high_price_threshold: float = 0.97
    min_order_size: int = 1
    min_order_value: float = 1.0
    standard_buy_price: float = 0.99

    # ── Early trend ───────────────────────────────────────────────────────
    early_trend_cutoff_secs: int = 120
    early_trend_min_price: float = 0.75
    early_trend_max_price: float = 0.90
    early_trend_max_spread: float = 0.03
    early_trend_max_stake_ratio: float = 0.30
    early_trend_min_deviation: float = 0.001
    early_trend_tp_pct: float = 0.05
    early_trend_partial_tp_ratio: float = 0.5
    early_trend_sl_pct: float = 0.10
    early_trend_micro_window: float = 10.0
    early_trend_micro_min_points: int = 3
    early_trend_micro_min_change_pct: float = 0.0001

    # ── Vacuum scalp ──────────────────────────────────────────────────────
    vacuum_scalp_entry_start_secs: int = 150
    vacuum_scalp_entry_end_secs: int = 90
    vacuum_scalp_min_deviation: float = 0.0005
    vacuum_scalp_min_token_price: float = 0.75
    vacuum_scalp_max_token_price: float = 0.92
    vacuum_scalp_max_volatility: float = 0.001
    vacuum_scalp_range5_max: float = 0.0030
    vacuum_scalp_range5_window: float = 300.0
    vacuum_scalp_volatility_window: float = 10.0
    vacuum_scalp_liquidity_ratio: float = 5.0
    vacuum_scalp_tp_delta: float = 0.02
    vacuum_scalp_sl_pct: float = 0.10
    vacuum_scalp_max_stake_ratio: float = 0.30
    vacuum_scalp_book_max_age: float = 30.0
    vacuum_scalp_confirmation_secs: float = 60.0
    vacuum_scalp_tp_timeout_secs: float = 60.0

    # ── Risk management ───────────────────────────────────────────────────
    standard_sl_pct: float = 0.10
    nuclear_crash_pct: float = 0.15
    nuclear_sell_price: float = 0.01
    fill_anomaly_pct: float = 0.20
    sl_dynamic_slippage_pct: float = 0.03
    sl_chase_timeout: float = 2.0
    sl_chase_step_pct: float = 0.02
    sl_max_chase_rounds: int = 3
    trailing_stop_distance_pct: float = 0.03
    trailing_stop_min_profit_pct: float = 0.02
    max_stake_ratio: float = 0.20                # 20% fractional Kelly compounding
    balance_safety_margin: float = 0.98

    max_concurrent_positions: int = 4             # multi-market concurrency
    max_daily_loss_pct: float = 0.20   # halt trading if bot balance drops 20%/day
    max_drawdown_pct: float = 0.35     # hard kill switch vs. initial balance

    # ── Imbalance ─────────────────────────────────────────────────────────
    imbalance_enabled: bool = True
    vacuum_imbalance_threshold: float = 0.95
    high_imbalance_threshold: float = 0.85
    moderate_imbalance_threshold: float = 0.70
    imbalance_stake_multipliers: Dict[float, float] = Field(
        default_factory=lambda: {0.95: 1.3, 0.90: 1.25, 0.85: 1.2, 0.80: 1.1}
    )
    imbalance_confidence_boost: Dict[float, float] = Field(
        default_factory=lambda: {0.95: 0.20, 0.90: 0.15, 0.85: 0.10, 0.80: 0.05}
    )

    # ── Early exit ────────────────────────────────────────────────────────
    early_exit_enabled: bool = True
    early_exit_window: float = 5
    early_exit_min_profit: float = 0.01
    early_exit_skip_above_price: float = 0.98

    # ── Confidence / trend ────────────────────────────────────────────────
    min_confidence: float = 0.3
    min_trend_diff: float = 0.005
    price_hist_window: int = 10
    max_cv: float = 0.1
    max_direction_changes: int = 4
    min_price_points: int = 3
    deque_maxlen: int = 300
    fallback_confidence_multiplier: float = 0.7

    # ── Monitoring / timing ───────────────────────────────────────────────
    monitor_interval: float = 0.1
    monitor_grace_period: float = 5.0
    snapshot_before_end: int = 15
    balance_log_interval: int = 60
    start_price_chainlink_grace_secs: float = 1.5
    start_price_chainlink_poll_secs: float = 0.05

    # ── Buy fill timeouts (per strategy) ──────────────────────────────────
    buy_fill_timeout_early: float = 3.0
    buy_fill_timeout_standard: float = 1.0
    buy_fill_timeout_vacuum: float = 2.0
    fill_wait_interval: float = 0.05

    # ── WebSocket / HTTP ──────────────────────────────────────────────────
    ws_rtds_url: str = "wss://ws-live-data.polymarket.com"
    ws_market_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    ws_user_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    ws_heartbeat_interval: int = 5
    ws_reconnect_delay: int = 2
    ws_book_stale_secs: int = 30
    binance_ws_direct: str = "wss://stream.binance.com:9443/ws"
    binance_streams: List[str] = Field(default_factory=lambda: [
        "btcusdt@aggTrade", "ethusdt@aggTrade", "solusdt@aggTrade", "xrpusdt@aggTrade"
    ])
    binance_api: str = "https://api.binance.com/api/v3"
    gamma_api: str = "https://gamma-api.polymarket.com"
    clob_api: str = "https://clob.polymarket.com"
    http_timeout: float = 10.0
    http_retries: int = 3
    gamma_cache_ttl: float = 2.0

    chainlink_symbols: Dict[str, str] = Field(
        default_factory=lambda: {
            "BTC": "btc/usd", "ETH": "eth/usd", "SOL": "sol/usd", "XRP": "xrp/usd"
        }
    )
    binance_symbols_ws: Dict[str, str] = Field(
        default_factory=lambda: {
            "BTC": "btcusdt", "ETH": "ethusdt", "SOL": "solusdt", "XRP": "xrpusdt"
        }
    )

    # ── Persistence / web / Adaptive logging ──────────────────────────────
    database_url: str = f"sqlite:///{PROJECT_ROOT / 'storage' / 'bot.db'}"
    log_dir: str = str(PROJECT_ROOT / "logs")
    demo_decision_log_interval_secs: float = 1.0

    # Phase-aware interval sampler:
    # 5.0s during idle phase (STC > 60s) -> saves 65-70% disk and I/O
    # 1.0s during active TWAP phase (STC <= 60s) -> full resolution accuracy
    demo_sample_interval_secs_idle: float = 5.0
    demo_sample_interval_secs_active: float = 1.0
    demo_sample_interval_secs: float = 1.0

    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 8000
    dashboard_password: str = ""
    paper_trading: bool = False

    # ── FavDip strategy (live) ──
    trading_strategy: str = "twap_inertia"   # "twap_inertia" (recommended) | "standard" | "favdip"
    favdip_cap: float = 0.40
    favdip_min_leg1: float = 0.04
    favdip_mom_k: int = 30
    favdip_mom_min: float = 5.0
    favdip_target_sum: float = 0.40
    favdip_win_lo: int = 30
    favdip_win_hi: int = 270
    hold_to_resolution: bool = True

    # ── Resolution cross-validation ──
    cross_validate_resolution: bool = True
    chainlink_twap_enabled: bool = True
    chainlink_twap_window: int = 60   # 60s for modern post-Aug24 Polymarket 5m markets
    cross_late_snapshot: bool = True
    cross_late_snapshot_secs: float = 150.0

    # ── PairFirst strategy ──
    pair_entry_cap: float = 0.40
    pair_min_leg1: float = 0.04
    pair_rolling_window: float = 30.0
    pair_target: float = 0.50
    pair_win_lo: int = 30
    pair_win_hi: int = 270
    pair_twap_alive_max_age: float = 15.0

    # ── Longshot strategy ──
    longshot_min: float = 0.08
    longshot_max: float = 0.35
    longshot_win_lo: int = 45
    longshot_win_hi: int = 75
    longshot_kelly: float = 0.025

    require_fill_liquidity: bool = True

    # ── Backtesting ───────────────────────────────────────────────────────
    backtest_data_dir: str = str(PROJECT_ROOT / "storage" / "backtest_data")
    backtest_default_capital: float = 100.0
    backtest_taker_fee: float = 0.02
    backtest_slippage_ticks: float = 2.0
    backtest_fill_latency_ms: int = 100
    backtest_token_source: str = "trades"

    # ── Real-time collector ───────────────────────────────────────────────
    collector_grace_secs: float = 150.0
    collector_retry_secs: float = 20.0
    collector_autostart: bool = False

    @field_validator("private_key", "funder_address")
    @classmethod
    def _no_real_key_default(cls, v: str) -> str:
        return v

    @property
    def safe_view(self) -> Dict:
        d = self.model_dump()
        for k in ("private_key", "funder_address"):
            d.pop(k, None)
        return d


@lru_cache
def get_settings() -> Settings:
    return Settings()
