"""Domain enums — entry types and close reasons."""

from enum import Enum


class EntryType(str, Enum):
    STANDARD = "STANDARD"
    EARLY_TREND = "EARLY_TREND"
    HIGH_PRICE = "HIGH_PRICE"
    IMBALANCE = "IMBALANCE"
    VACUUM_SCALP = "VACUUM_SCALP"


class CloseReason(str, Enum):
    TAKE_PROFIT = "take_profit"
    PARTIAL_TP = "partial_tp"
    TRAILING_STOP = "trailing_stop"
    STOP_LOSS = "stop_loss"
    EARLY_EXIT = "early_exit"
    EXPIRED = "expired"
    VACUUM_TP = "vacuum_tp"
    NUCLEAR_CRASH = "nuclear_crash"
