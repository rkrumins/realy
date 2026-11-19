"""Live guardrails abort an overloaded / failing run instead of letting it queue for minutes."""
from __future__ import annotations

import threading
from types import SimpleNamespace

from realy.runner.run import _guardrails
from realy.scenario import load_scenario


def _handle(live: dict) -> SimpleNamespace:
    return SimpleNamespace(stop=threading.Event(), phase="MEASURE", abort_reason=None, live=live, service_errors=0.0)


def _emit(n: int, errors: int = 0) -> dict:
    return {"emitter:shard-0": {"counters": {"emitted_total": n, "errors_total": errors}}}


def test_healthy_run_continues():
    h = _handle({**_emit(10_000, 5), "collector:c0": {"counters": {"decided_total": 9_990}}})
    _guardrails(h, load_scenario("smoke"), {"source": "collector", "stages": []}, lambda _: None)
    assert h.abort_reason is None and not h.stop.is_set()


def test_backlog_aborts():
    sc = load_scenario("smoke", {"guardrails": {"max_consumer_lag": 1_000}})
    h = _handle({**_emit(10_000), "collector:c0": {"counters": {"decided_total": 5_000}}})
    _guardrails(h, sc, {"source": "collector", "stages": []}, lambda _: None)
    assert h.stop.is_set() and "backlog" in h.abort_reason


def test_schedule_lag_aborts():
    h = _handle(_emit(100))
    snap = {"source": "emitter", "stages": [{"name": "schedule_lag", "p99_ms": 5_000.0}]}
    _guardrails(h, load_scenario("smoke"), snap, lambda _: None)
    assert h.stop.is_set() and "schedule lag" in h.abort_reason


def test_error_rate_aborts_on_this_runs_windows_only():
    h = _handle({**_emit(10_000, 10), "collector:c0": {"counters": {"decided_total": 9_990}}})
    sc = load_scenario("smoke")
    # a gateway that failed 50k events in an EARLIER run: lifetime total is huge, this window is clean
    _guardrails(h, sc, {"source": "gateway", "counters": {"pipeline_errors": 0, "pipeline_errors_total": 50_000}}, lambda _: None)
    assert h.abort_reason is None
    for _ in range(7):
        _guardrails(h, sc, {"source": "gateway", "counters": {"pipeline_errors": 100}}, lambda _: None)
    assert h.stop.is_set() and "error rate" in h.abort_reason


def test_guardrails_ignore_warm_down_phases():
    h = _handle(_emit(100))
    h.phase = "DRAIN"
    snap = {"source": "emitter", "stages": [{"name": "schedule_lag", "p99_ms": 5_000.0}]}
    _guardrails(h, load_scenario("smoke"), snap, lambda _: None)
    assert h.abort_reason is None


def test_auto_prime_only_for_cold_async_warm_state(monkeypatch):
    from realy.runner import run as run_mod
    from realy.world import load_or_build  # noqa: F401  (world only needed for its natural rate)

    class W:
        n_customers = 1000
    monkeypatch.setattr(run_mod, "natural_rate_total", lambda w: 6.7)
    ep = run_mod.Endpoints(gateway_admin="http://gw", flink_rest="http://flink")
    monkeypatch.setattr(run_mod, "_gateway_decisions", lambda admin: 0)
    monkeypatch.setattr(run_mod, "_flink_job_uptime_s", lambda rest: 5.0)
    days = run_mod.auto_prime_days(load_scenario("smoke"), ep, W(), 500, lambda _: None)
    assert days > 0 and abs(days * 6.7 * 86_400 - 15_000) < 1   # 30 s at 500 TPS
    assert run_mod.auto_prime_days(load_scenario("flink-inline-smoke"), ep, W(), 500, lambda _: None) == 0  # fresh state
    assert run_mod.auto_prime_days(load_scenario("sync-smoke"), ep, W(), 500, lambda _: None) == 0         # sync rail
    monkeypatch.setattr(run_mod, "_gateway_decisions", lambda admin: 1_000_000)
    monkeypatch.setattr(run_mod, "_flink_job_uptime_s", lambda rest: 3_600.0)
    assert run_mod.auto_prime_days(load_scenario("smoke"), ep, W(), 500, lambda _: None) == 0             # warm
