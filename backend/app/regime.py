"""Per-asset TWAP regime state machine.

The controller is deliberately strategy-agnostic and deterministic.  Engines
submit resolved real or shadow trades; the controller decides whether the next
TWAP opportunity is shadow-only, probe-sized, or fully enabled.  No market IO
or order execution lives here, which keeps the safety policy unit-testable.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Deque, Dict, Iterable, Optional


class RegimeState(str, Enum):
    OFF = "OFF"
    PROBE = "PROBE"
    ON = "ON"


@dataclass(frozen=True)
class RegimeDecision:
    state: RegimeState
    execute: bool
    stake_multiplier: float
    reason: str


@dataclass
class RegimeTrade:
    ts: float
    won: bool
    pnl_return: float  # PnL / entry cost; capital-independent EV observation
    reversal: bool
    shadow: bool


@dataclass
class AssetRegime:
    state: RegimeState = RegimeState.OFF
    changed_at: float = field(default_factory=time.time)
    reason: str = "initial"
    trades: Deque[RegimeTrade] = field(default_factory=deque)


class RegimeController:
    """Auditable OFF -> PROBE -> ON controller, independently per asset."""

    def __init__(self, assets: Iterable[str], *, enabled: bool = False,
                 probe_execute: bool = False, probe_order_size: int = 5,
                 window: int = 15, probe_min_trades: int = 10,
                 on_min_trades: int = 15, probe_min_wr: float = 0.80,
                 on_min_wr: float = 0.85, probe_max_reversal: float = 0.20,
                 on_max_reversal: float = 0.15, posterior_threshold: float = 0.90,
                 demote_wr: float = 0.72, demote_ev: float = 0.0,
                 state_path: Optional[Path] = None,
                 events_path: Optional[Path] = None,
                 observations_path: Optional[Path] = None,
                 run_id: Optional[str] = None, config_hash: str = "", logger=None) -> None:
        self.enabled = enabled
        self.probe_execute = probe_execute
        self.probe_order_size = max(1, int(probe_order_size))
        self.window = max(5, window)
        self.probe_min_trades = max(1, probe_min_trades)
        self.on_min_trades = max(self.probe_min_trades, on_min_trades)
        self.probe_min_wr = probe_min_wr
        self.on_min_wr = on_min_wr
        self.probe_max_reversal = probe_max_reversal
        self.on_max_reversal = on_max_reversal
        self.posterior_threshold = posterior_threshold
        self.demote_wr = demote_wr
        self.demote_ev = demote_ev
        self.state_path = Path(state_path) if state_path else None
        self.events_path = Path(events_path) if events_path else None
        self.observations_path = Path(observations_path) if observations_path else None
        self.run_id = run_id or f"{int(time.time())}-{uuid.uuid4().hex[:8]}"
        self.config_hash = config_hash
        self.log = logger
        self.assets: Dict[str, AssetRegime] = {
            str(a).upper(): AssetRegime(trades=deque(maxlen=self.window)) for a in assets
        }
        self._load()
        if self.enabled:
            self.audit_event("REGIME_CONTROLLER_STARTED", states={
                a: r.state.value for a, r in self.assets.items()
            }, restored=bool(self.state_path and self.state_path.exists()))

    @classmethod
    def from_settings(cls, settings, logger=None) -> "RegimeController":
        path = getattr(settings, "regime_state_path", "") or None
        raw = settings.model_dump() if hasattr(settings, "model_dump") else vars(settings)
        relevant = {k: raw[k] for k in sorted(raw) if k.startswith(("regime_", "twap_"))
                    or k in ("max_stake_ratio", "min_order_size", "min_order_value")}
        config_hash = hashlib.sha256(
            json.dumps(relevant, sort_keys=True, default=str).encode()
        ).hexdigest()[:16]
        return cls(
            settings.assets, enabled=getattr(settings, "regime_enabled", False),
            probe_execute=getattr(settings, "regime_probe_execute", False),
            probe_order_size=getattr(settings, "regime_probe_order_size", 5),
            window=getattr(settings, "regime_window", 15),
            probe_min_trades=getattr(settings, "regime_probe_min_trades", 10),
            on_min_trades=getattr(settings, "regime_on_min_trades", 15),
            probe_min_wr=getattr(settings, "regime_probe_min_wr", 0.80),
            on_min_wr=getattr(settings, "regime_on_min_wr", 0.85),
            probe_max_reversal=getattr(settings, "regime_probe_max_reversal", 0.20),
            on_max_reversal=getattr(settings, "regime_on_max_reversal", 0.15),
            posterior_threshold=getattr(settings, "regime_posterior_threshold", 0.90),
            demote_wr=getattr(settings, "regime_demote_wr", 0.72),
            demote_ev=getattr(settings, "regime_demote_ev", 0.0),
            state_path=Path(path) if path else None,
            events_path=(Path(settings.regime_events_path)
                         if getattr(settings, "regime_events_path", "") else None),
            observations_path=(Path(settings.regime_observations_path)
                               if getattr(settings, "regime_observations_path", "") else None),
            config_hash=config_hash, logger=logger,
        )

    def decision(self, asset: str) -> RegimeDecision:
        if not self.enabled:
            return RegimeDecision(RegimeState.ON, True, 1.0, "disabled")
        ar = self._asset(asset)
        if ar.state is RegimeState.ON:
            return RegimeDecision(ar.state, True, 1.0, ar.reason)
        if ar.state is RegimeState.PROBE and self.probe_execute:
            # Engines size this as exactly max(exchange minimum, probe_order_size).
            return RegimeDecision(ar.state, True, 1.0, ar.reason)
        return RegimeDecision(ar.state, False, 0.0, "shadow_only")

    def record(self, asset: str, *, won: bool, pnl: float, cost: float,
               shadow: bool, ts: Optional[float] = None,
               context: Optional[dict] = None) -> RegimeState:
        ar = self._asset(asset)
        observed_at = ts or time.time()
        old = ar.state
        metrics_before = self.metrics(asset)
        pnl_return = float(pnl) / float(cost) if cost > 0 else 0.0
        ar.trades.append(RegimeTrade(observed_at, bool(won), pnl_return,
                                     reversal=not bool(won), shadow=bool(shadow)))
        self._transition(asset, ar)
        metrics_after = self.metrics(asset)
        if ar.state is not old:
            metrics = self.metrics(asset)
            if self.log:
                self.log.warning("REGIME transition", asset=asset, old=old.value,
                                 new=ar.state.value, reason=ar.reason,
                                 metrics=metrics)
            self._audit({
                "event": "REGIME_TRANSITION", "ts": ts or time.time(),
                "asset": asset.upper(), "old_state": old.value,
                "new_state": ar.state.value, "reason": ar.reason,
                "trigger": {"won": bool(won), "pnl": float(pnl),
                            "cost": float(cost), "shadow": bool(shadow)},
                "metrics": metrics,
            })
        observation = {
            "schema_version": 1, "event": "REGIME_OBSERVATION",
            "ts": observed_at, "run_id": self.run_id,
            "config_hash": self.config_hash, "asset": asset.upper(),
            "state_before": old.value, "state_after": ar.state.value,
            "transitioned": ar.state is not old, "transition_reason": ar.reason,
            "execution_mode": "shadow" if shadow else ("probe" if old is RegimeState.PROBE else "on"),
            "won": bool(won), "pnl": float(pnl), "cost": float(cost),
            "pnl_return": pnl_return, "reversal": not bool(won),
            "metrics_before": metrics_before, "metrics_after": metrics_after,
        }
        if context:
            observation["context"] = context
        self._append_jsonl(self.observations_path, observation, "observation")
        self._save()
        return ar.state

    def metrics(self, asset: str) -> dict:
        ar = self._asset(asset)
        xs = list(ar.trades)
        n = len(xs)
        wins = sum(t.won for t in xs)
        # Beta(1,1) posterior probability that p exceeds observed break-even.
        # EV itself remains the primary gate because entry prices vary.
        posterior = self._beta_prob_gt_half(wins + 1, n - wins + 1)
        return {
            "state": ar.state.value, "reason": ar.reason, "changed_at": ar.changed_at,
            "n": n, "wins": wins, "win_rate": wins / n if n else 0.0,
            "mean_return": sum(t.pnl_return for t in xs) / n if n else 0.0,
            "cumulative_return": sum(t.pnl_return for t in xs),
            "reversal_rate": (n - wins) / n if n else 0.0,
            "posterior_p_gt_half": posterior,
            "shadow_count": sum(t.shadow for t in xs),
        }

    def snapshot(self) -> dict:
        return {asset: self.metrics(asset) for asset in self.assets}

    def _transition(self, asset: str, ar: AssetRegime) -> None:
        m = self.metrics(asset)
        n, wr, ev, rev, post = (m["n"], m["win_rate"], m["mean_return"],
                                m["reversal_rate"], m["posterior_p_gt_half"])
        new, reason = ar.state, ar.reason
        if ar.state is RegimeState.OFF:
            if n >= self.probe_min_trades and wr >= self.probe_min_wr and ev > 0 and rev <= self.probe_max_reversal:
                new, reason = RegimeState.PROBE, "positive_shadow_window"
        elif ar.state is RegimeState.PROBE:
            if n >= self.on_min_trades and wr >= self.on_min_wr and ev > 0 and rev <= self.on_max_reversal and post >= self.posterior_threshold:
                new, reason = RegimeState.ON, "confirmed_positive_ev"
            elif n >= self.probe_min_trades and (wr < self.demote_wr or ev <= self.demote_ev):
                new, reason = RegimeState.OFF, "probe_degraded"
        else:  # ON
            if n >= self.probe_min_trades and (wr < self.demote_wr or ev <= self.demote_ev):
                new, reason = RegimeState.OFF, "edge_lost"
            elif rev > self.probe_max_reversal:
                new, reason = RegimeState.PROBE, "reversal_rate_warning"
        if new is not ar.state:
            ar.state, ar.reason, ar.changed_at = new, reason, time.time()

    def _asset(self, asset: str) -> AssetRegime:
        key = asset.upper()
        if key not in self.assets:
            self.assets[key] = AssetRegime(trades=deque(maxlen=self.window))
        return self.assets[key]

    @staticmethod
    def _beta_prob_gt_half(alpha: int, beta: int) -> float:
        # For integer alpha/beta: P(Beta(a,b)>0.5) = sum_{j=0}^{a-1} C(n,j)/2^n.
        n = alpha + beta - 1
        return sum(math.comb(n, j) for j in range(alpha)) / (2 ** n)

    def audit_event(self, event: str, **fields) -> None:
        """Write a lifecycle/block/manual event with run/config correlation."""
        self._audit({"schema_version": 1, "event": event, "ts": time.time(),
                     "run_id": self.run_id, "config_hash": self.config_hash, **fields})

    def _audit(self, record: dict) -> None:
        """Append a durable, machine-readable regime event."""
        record.setdefault("schema_version", 1)
        record.setdefault("run_id", self.run_id)
        record.setdefault("config_hash", self.config_hash)
        self._append_jsonl(self.events_path, record, "audit")

    def _append_jsonl(self, path: Optional[Path], record: dict, kind: str) -> None:
        if not path:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str) + "\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception as exc:
            if self.log:
                self.log.error(f"REGIME {kind} write failed", error=str(exc))

    def _save(self) -> None:
        if not self.state_path:
            return
        payload = {"version": 1, "assets": {a: {
            "state": r.state.value, "changed_at": r.changed_at, "reason": r.reason,
            "trades": [asdict(t) for t in r.trades],
        } for a, r in self.assets.items()}}
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=self.state_path.name, dir=self.state_path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, separators=(",", ":"))
                f.flush(); os.fsync(f.fileno())
            os.replace(tmp, self.state_path)
        finally:
            if os.path.exists(tmp): os.unlink(tmp)

    def _load(self) -> None:
        if not self.state_path or not self.state_path.exists():
            return
        try:
            raw = json.loads(self.state_path.read_text(encoding="utf-8"))
            for asset, rec in raw.get("assets", {}).items():
                ar = self._asset(asset)
                ar.state = RegimeState(rec.get("state", "OFF"))
                ar.changed_at = float(rec.get("changed_at", time.time()))
                ar.reason = str(rec.get("reason", "restored"))
                ar.trades.clear()
                for t in rec.get("trades", [])[-self.window:]:
                    ar.trades.append(RegimeTrade(**t))
        except Exception as exc:
            if self.log:
                self.log.error("REGIME state restore failed; fail-closed to OFF", error=str(exc))
