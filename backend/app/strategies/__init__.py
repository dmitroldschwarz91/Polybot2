"""Strategies package."""

from .base import BaseStrategy, Opportunity
from .early_trend import EarlyTrendStrategy
from .standard import StandardEntryStrategy
from .vacuum_scalp import VacuumScalpStrategy
from .spread_capture import SpreadCaptureStrategy
from .twap_inertia import TWAPInertiaStrategy


def all_strategies(settings):
    """Instantiates strategies in their evaluation priority order."""
    return [
        TWAPInertiaStrategy(settings),
        EarlyTrendStrategy(settings),
        VacuumScalpStrategy(settings),
        SpreadCaptureStrategy(settings),
        StandardEntryStrategy(settings),
    ]


__all__ = [
    "BaseStrategy", "Opportunity", "TWAPInertiaStrategy", "EarlyTrendStrategy",
    "StandardEntryStrategy", "VacuumScalpStrategy", "SpreadCaptureStrategy",
    "all_strategies",
]
