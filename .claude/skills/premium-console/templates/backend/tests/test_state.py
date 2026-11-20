"""Pure state derivation: explicit states instead of 'red' while busy, with hysteresis on degraded."""
from __future__ import annotations

from server.state import STICKY_S, component_states, fmt_progress
from server.tasks import rank_insights

UP = {"id": "store", "status": "up", "latency_ms": 3.0, "details": {}}


def test_down_starting_and_ready():
    comps = [{"id": "a", "status": "down", "latency_ms": 2500.0, "details": {"error": "ConnectError: refused"}},
             {"id": "b", "status": "starting", "latency_ms": 1.0, "details": {"reason": "worker just started"}}, UP]
    s = component_states(comps, [], now=100.0, sticky={})
    assert s["a"] == {"state": "down", "reason": "ConnectError: refused", "progress": None}
    assert s["b"]["state"] == "starting" and s["store"]["state"] == "ready"


def test_busy_component_is_warming_with_progress_not_degraded():
    steps = [{"id": "load", "label": "Load inputs", "status": "running", "component": "store",
              "progress": {"done": 412_330, "total": 583_063, "unit": "items", "rate": 9_800, "eta_s": 17.4}}]
    slow = {**UP, "latency_ms": 900.0}   # slow probe, but it is busy with a step: warming wins
    s = component_states([slow], steps, now=100.0, sticky={})
    assert s["store"]["state"] == "warming" and "71% (412,330/583,063 items)" in s["store"]["reason"]
    assert abs(s["store"]["progress"] - 412_330 / 583_063) < 1e-9


def test_degraded_is_sticky_then_clears():
    sticky: dict[str, float] = {}
    assert component_states([{**UP, "latency_ms": 400.0}], [], 100.0, sticky)["store"]["state"] == "degraded"
    held = component_states([UP], [], 100.0 + STICKY_S / 2, sticky)["store"]
    assert held["state"] == "degraded" and held["reason"].startswith("recovering")
    assert component_states([UP], [], 100.0 + STICKY_S + 1, sticky)["store"]["state"] == "ready"


def test_progress_wording_matches_the_ui():
    assert fmt_progress({"done": 412330, "total": 583063, "unit": "items", "rate": 9800, "eta_s": 17.4}) == \
        "71% (412,330/583,063 items) · 9,800/s · ETA 17 s"
    assert fmt_progress(None) == ""


def test_insights_are_ranked_with_evidence():
    res = {"within_budget": 0.8, "budget_ms": 100, "p99_ms": 180, "p99_max_ms": 240, "failed": 3, "processed": 997}
    steps = [{"label": "Load", "duration_s": 2.0}, {"label": "Process", "duration_s": 8.0}]
    out = rank_insights(res, steps)
    assert [i["severity"] for i in out] == ["critical", "warning", "info"]
    assert "20% of seconds" in out[0]["title"] and out[0]["anchor"] == "timeline"
    assert out[2]["title"].startswith("Slowest step: Process")
