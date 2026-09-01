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
    initial_balance: float = 7.0
    assets: List[str] = Field(default_factory=lambda: ["BTC", "ETH"])
    interval_minutes: int = 5
    poll_interval: float = 0.1
    quiet_mode: bool = True

    # ── Strategy toggles ──────────────────────────────────────────────────
    standard_entry_enabled: bool = False
    early_trend_enabled: bool = False
    vacuum_scalp_enabled: bool = True
    spread_capture_enabled: bool = False  # gabagool-inspired hedge-lite

    # ── Spread capture / hedge-lite (NEW) ────────────────────────────────
    # Opportunistically buys the opposite side to lock a pair below $1.
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
    vacuum_scalp_entry_end_secs: int = 90         # failure analysis: no entries after 90s (was 30)
    vacuum_scalp_min_deviation: float = 0.0005    # walk-forward optimized (was 0.001)
    vacuum_scalp_min_token_price: float = 0.75    # walk-forward optimized (was 0.95)
    vacuum_scalp_max_token_price: float = 0.92    # failure analysis + live data: 0.88 too tight on volatile market (47% skipped) → 0.92
    vacuum_scalp_max_volatility: float = 0.001    # walk-forward optimized (was 0.0002)
    vacuum_scalp_range5_max: float = 0.0030       # range5 filter: 5-min oracle (max-min)/mean < this. 0.0015 was overfit to a calmer week → 0.003 (current market median ~0.0018)
    vacuum_scalp_range5_window: float = 300.0     # range5 window in seconds (5 min)
    vacuum_scalp_volatility_window: float = 10.0
    vacuum_scalp_liquidity_ratio: float = 5.0     # relaxed for 5m markets (was 10.0)
    vacuum_scalp_tp_delta: float = 0.02
    vacuum_scalp_sl_pct: float = 0.10
    vacuum_scalp_max_stake_ratio: float = 0.30    # walk-forward optimized (was 0.72)
    vacuum_scalp_book_max_age: float = 30.0       # 5m markets update slowly (was 5.0)
    vacuum_scalp_confirmation_secs: float = 60.0  # multi-confirmation: deviation must persist this long (WFO: 87%→92% WR)
    vacuum_scalp_tp_timeout_secs: float = 60.0

    # ── Risk management (★ the heart of the refactored app) ──────────────
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
    max_stake_ratio: float = 0.30                # optimized for compounding (was 0.75)
    balance_safety_margin: float = 0.98

    # NEW — portfolio-level risk guards (did not exist in the original script)
    max_concurrent_positions: int = 1             # optimized for small capital (was 2)
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
    binance_streams: List[str] = Field(default_factory=lambda: ["btcusdt@aggTrade", "ethusdt@aggTrade"])
    binance_api: str = "https://api.binance.com/api/v3"
    gamma_api: str = "https://gamma-api.polymarket.com"
    clob_api: str = "https://clob.polymarket.com"
    http_timeout: float = 10.0
    http_retries: int = 3
    gamma_cache_ttl: float = 2.0

    chainlink_symbols: Dict[str, str] = Field(
        default_factory=lambda: {"BTC": "btc/usd", "ETH": "eth/usd"}
    )
    binance_symbols_ws: Dict[str, str] = Field(
        default_factory=lambda: {"BTC": "btcusdt", "ETH": "ethusdt"}
    )

    # ── Persistence / web ─────────────────────────────────────────────────
    database_url: str = f"sqlite:///{PROJECT_ROOT / 'storage' / 'bot.db'}"
    log_dir: str = str(PROJECT_ROOT / "logs")
    # Demo decision-log throttle in the entry window. 1s = dense second-by-second
    # oracle/token path → later analysis WITHOUT interpolation (no look-ahead).
    demo_decision_log_interval_secs: float = 1.0
    # Demo interval sampler: log market state (oracle/deviation/range5/book) every
    # N seconds across the WHOLE interval (not just the entry window) — needed
    # because range5 looks 5 min back and confirmation 60s back, outside the
    # entry window. Enables full reconstruction of filters on real data.
    demo_sample_interval_secs: float = 1.0
    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 8000
    dashboard_password: str = ""  # if set, requires Basic Auth on all endpoints
    paper_trading: bool = False  # when True, simulate fills instead of real orders

    # ── FavDip strategy (live) ──
    trading_strategy: str = "standard"   # "standard" = all_strategies | "favdip"
    favdip_cap: float = 0.40             # max favorite token price (dipped)
    favdip_min_leg1: float = 0.04        # min leg1 price
    favdip_mom_k: int = 30              # momentum window (seconds)
    favdip_mom_min: float = 5.0         # min |momentum| ($) for confirmed revert
    favdip_target_sum: float = 0.40     # pair completion: p1 + p2 < this
    favdip_win_lo: int = 30            # entry window start (secs to close)
    favdip_win_hi: int = 270           # entry window end
    hold_to_resolution: bool = True   # HOLD beats TP/SL per walk-forward analysis

    # ── Resolution cross-validation (подстраховка) ──
    # Determine each market's outcome from TWO independent sources (Chainlink
    # TWAP + leader token price) and commit when they agree. On conflict the
    # position is marked unresolved (neutral) rather than guessed. When False,
    # the engines fall back to the legacy single-source cascade.
    cross_validate_resolution: bool = True

    # ── Official Chainlink TWAP stream (Polymarket RTDS) ──
    # Subscribe to RTDS topic crypto_prices_twap_thirty and resolve against the
    # AUTHORITATIVE Chainlink-computed TWAP (open & close). Per Chainlink docs
    # the TWAP is a signed black box — consume it, do NOT recompute it. Our local
    # tick-average reconstruction is kept only as a fallback when this stream is
    # unavailable.
    chainlink_twap_enabled: bool = True
    chainlink_twap_window: int = 30   # 30s for 5-min markets; 60s for 15-min / 4-hour

    # Late snapshot: re-read the ground-truth token outcome this many seconds
    # AFTER market close and compare to what we resolved. Measures the real
    # Chainlink-vs-token agreement rate on live data (incl. trades the cascade
    # closed early before the token polarised). Logged to cross_snapshots.jsonl.
    cross_late_snapshot: bool = True
    cross_late_snapshot_secs: float = 150.0

    # ── PairFirst strategy (post-TWAP async pair, rolling-low leg1 entry) ──
    # Buy a cheap leg1 at a new N-second low, complete the pair when
    # entry+opposite < target, else hold to resolution. Entry is GUARDED on a
    # live TWAP stream (pair_twap_alive_max_age) — no entry when the stream is
    # dead. PRELIMINARY params (small sample, EV ~+0.054/sh) — re-validate.
    pair_entry_cap: float = 0.40
    pair_min_leg1: float = 0.04
    pair_rolling_window: float = 30.0
    pair_target: float = 0.50
    pair_win_lo: int = 30
    pair_win_hi: int = 270
    pair_twap_alive_max_age: float = 15.0   # no entry when TWAP older than this

    # ── Longshot strategy (favorite-longshot bias, post-TWAP) ──
    # Buy the UNDERDOG (cheap token) at stc~90-150, HOLD to resolution. The
    # cheap token is under-priced (+EV ~+0.10/sh); the favorite is over-priced.
    # OPPOSITE of VacuumScalp/FavDip. High variance (WR ~20%) => quarter-Kelly.
    longshot_min: float = 0.08
    longshot_max: float = 0.35
    longshot_win_lo: int = 45
    longshot_win_hi: int = 75
    longshot_kelly: float = 0.025

    # ── Execution: liquidity gating on resting-limit fills ──
    # When False (default), pending limit orders fill on price touch alone,
    # ignoring ask_volume. Set True to require ask_volume >= min before filling
    # (the old behaviour; was blocking entries on thin books).
    require_fill_liquidity: bool = False

    # ── Backtesting ───────────────────────────────────────────────────────
    backtest_data_dir: str = str(PROJECT_ROOT / "storage" / "backtest_data")
    backtest_default_capital: float = 7.0
    backtest_taker_fee: float = 0.02       # Polymarket fee on profitable side
    backtest_slippage_ticks: float = 2.0   # assumed slippage in ticks
    backtest_fill_latency_ms: int = 200    # modeled execution latency
    backtest_token_source: str = "trades"  # "trades" (per-trade, dense) or "prices" (prices-history, ~5pts/5min)

    # ── Real-time collector ───────────────────────────────────────────────
    collector_grace_secs: float = 150.0    # wait after close (UMA needs 2-3 min)
    collector_retry_secs: float = 20.0
    collector_autostart: bool = False      # start recording on app launch

    @field_validator("private_key", "funder_address")
    @classmethod
    def _no_real_key_default(cls, v: str) -> str:
        # only guards against the placeholder leaking; real validation happens at runtime
        return v

    # convenience: the non-secret subset exposed to the dashboard
    @property
    def safe_view(self) -> Dict:
        d = self.model_dump()
        for k in ("private_key", "funder_address"):
            d.pop(k, None)
        return d


@lru_cache
def get_settings() -> Settings:
    return Settings()
