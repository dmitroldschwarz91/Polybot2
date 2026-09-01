"""Backtest package."""

from .data import HistoricalData, PriceTick
from .engine import BacktestConfig, BacktestEngine, STRATEGY_REGISTRY
from .metrics import BacktestMetrics, compute_metrics
from .poly_fetcher import BacktestDataset, IntervalMeta, PolymarketDataFetcher
from .simulator import MarketModel, SimulatedExchange
from .collector import RealTimeCollector, CollectorStats
from .compounding import (
    CompoundingResult, MonteCarloResult, simulate_compounding,
    monte_carlo, extract_entry_opportunities, score_result,
)
from .optimizer import (
    WalkForwardOptimizer, OptimizationResult, WindowResult,
    ParamSpec, ParamCombo, default_search_space,
)

__all__ = [
    "HistoricalData", "PriceTick", "BacktestConfig", "BacktestEngine",
    "STRATEGY_REGISTRY", "BacktestMetrics", "compute_metrics",
    "MarketModel", "SimulatedExchange",
    "BacktestDataset", "IntervalMeta", "PolymarketDataFetcher",
    "RealTimeCollector", "CollectorStats",
    "WalkForwardOptimizer", "OptimizationResult", "WindowResult",
    "ParamSpec", "ParamCombo", "default_search_space",
    "compounding_search_space", "CompoundingOptimizationResult",
    "CompoundingResult", "MonteCarloResult", "simulate_compounding",
]
