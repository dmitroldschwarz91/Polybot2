from pathlib import Path

from backend.app.regime import RegimeController, RegimeState


def make(**kw):
    defaults = dict(enabled=True, window=15, probe_min_trades=5,
                    on_min_trades=8, probe_min_wr=.8, on_min_wr=.85,
                    probe_max_reversal=.2, on_max_reversal=.15,
                    posterior_threshold=.8, demote_wr=.6)
    defaults.update(kw)
    return RegimeController(["XRP", "SOL"], **defaults)


def record(c, asset, won, pnl=None, shadow=True):
    c.record(asset, won=won, pnl=(1.0 if won else -1.0) if pnl is None else pnl,
             cost=1.0, shadow=shadow)


def test_disabled_is_backward_compatible():
    c = RegimeController(["XRP"], enabled=False)
    d = c.decision("XRP")
    assert d.execute and d.stake_multiplier == 1 and d.state is RegimeState.ON


def test_off_is_shadow_and_assets_are_isolated():
    c = make()
    assert not c.decision("XRP").execute
    for _ in range(5): record(c, "XRP", True)
    assert c.metrics("XRP")["state"] == "PROBE"
    assert c.metrics("SOL")["state"] == "OFF"


def test_probe_shadow_default_and_configurable_execution():
    c = make(probe_execute=False)
    for _ in range(5): record(c, "XRP", True)
    assert not c.decision("XRP").execute

    c2 = make(probe_execute=True, probe_order_size=5)
    for _ in range(5): record(c2, "XRP", True)
    d = c2.decision("XRP")
    assert d.execute and d.state is RegimeState.PROBE
    assert d.stake_multiplier == 1.0
    assert c2.probe_order_size == 5


def test_promotes_to_on_and_demotes_when_edge_is_lost():
    c = make()
    for _ in range(8): record(c, "XRP", True)
    assert c.metrics("XRP")["state"] == "ON"
    assert c.decision("XRP").execute
    # Rolling window eventually contains enough losses to fail closed.
    for _ in range(10): record(c, "XRP", False)
    assert c.metrics("XRP")["state"] == "OFF"
    assert not c.decision("XRP").execute


def test_negative_ev_prevents_promotion_despite_high_wr():
    c = make()
    for _ in range(4): record(c, "XRP", True, pnl=.01)
    record(c, "XRP", False, pnl=-1.0)
    assert c.metrics("XRP")["win_rate"] == .8
    assert c.metrics("XRP")["mean_return"] < 0
    assert c.metrics("XRP")["state"] == "OFF"


def test_state_is_persisted_atomically(tmp_path: Path):
    path = tmp_path / "state.json"
    c = make(state_path=path)
    for _ in range(5): record(c, "XRP", True)
    restored = make(state_path=path)
    assert restored.metrics("XRP")["state"] == "PROBE"
    assert restored.metrics("XRP")["n"] == 5


def test_every_transition_has_durable_audit_event(tmp_path: Path):
    import json
    events = tmp_path / "events.jsonl"
    c = make(events_path=events)
    for _ in range(5): record(c, "XRP", True)
    rows = [json.loads(line) for line in events.read_text().splitlines()]
    transitions = [r for r in rows if r["event"] == "REGIME_TRANSITION"]
    assert len(transitions) == 1
    assert transitions[0]["old_state"] == "OFF"
    assert transitions[0]["new_state"] == "PROBE"
    assert transitions[0]["reason"] == "positive_shadow_window"
    assert transitions[0]["metrics"]["n"] == 5


def test_every_resolved_trade_has_reproducible_observation(tmp_path: Path):
    import json
    observations = tmp_path / "observations.jsonl"
    c = make(observations_path=observations, run_id="run-test", config_hash="cfg-test")
    c.record("XRP", won=True, pnl=1.0, cost=4.0, shadow=True,
             context={"slug": "xrp-1", "barrier_pct": 0.08})
    row = json.loads(observations.read_text().strip())
    assert row["run_id"] == "run-test"
    assert row["config_hash"] == "cfg-test"
    assert row["state_before"] == "OFF"
    assert row["metrics_before"]["n"] == 0
    assert row["metrics_after"]["n"] == 1
    assert row["context"]["slug"] == "xrp-1"
