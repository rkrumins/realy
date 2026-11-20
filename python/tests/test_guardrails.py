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


def test_history_plan_primes_a_cold_scoring_path_after_the_bulk_replay():
    from realy.runner.run import describe_history, history_days, plan_history
    rate = 6.7                                     # natural events per sim-second
    prime_days = 15_000 / (rate * 86_400)          # 30 s at 500 TPS
    # warm pipeline, 1-day backfill: one unpaced bulk segment (unchanged behaviour)
    assert plan_history(1.0, [], 500, rate) == [{"mode": "bulk", "days": 1.0}]
    # cold scoring path after a world switch: bulk replay, then the last 30 s of history paced + scored
    segs = plan_history(1.0, ["gateway has scored 0 events"], 500, rate)
    assert [s["mode"] for s in segs] == ["bulk", "prime"]
    assert abs(segs[1]["days"] - prime_days) < 1e-12 and abs(history_days(segs) - 1.0) < 1e-12
    assert "scored" in describe_history(segs, 500, rate)
    # cold, no backfill (first run after `make up`): prime only (the former auto-prime)
    assert plan_history(None, ["Flink job started 5 s ago"], 500, rate) == [{"mode": "prime", "days": prime_days}]
    # a backfill shorter than one prime: all of it scored
    assert plan_history(prime_days / 2, ["cold"], 500, rate) == [{"mode": "prime", "days": prime_days / 2}]
    # priming disabled / warm: nothing extra
    assert plan_history(None, ["cold"], 500, rate, auto_prime=False) == []
    assert plan_history(None, [], 500, rate) == []


def test_emitter_reads_legacy_and_segmented_history_plans():
    from realy.emit.driver import ShardPlan, history_segments
    base = dict(shard=0, n_shards=1, run_id="r", run_dir="/tmp", scenario={}, world_dir="/w", t_start_us=0,
                time_scale=1.0, sim_start_us=0, duration_s=1.0)
    assert history_segments(ShardPlan(**base), 1.0) == [{"mode": "bulk", "days": 1.0}]
    assert history_segments(ShardPlan(**base, prime=True), 0.1) == [{"mode": "prime", "days": 0.1}]
    segs = [{"mode": "bulk", "days": 0.9}, {"mode": "prime", "days": 0.1}]
    assert history_segments(ShardPlan(**base, extra={"history": segs}), 1.0) == segs
    assert history_segments(ShardPlan(**base, extra={"history": []}), 1.0) == []


def test_prime_wait_uses_group_lag_then_falls_back(monkeypatch):
    from realy.common import kafka as kafka_mod
    from realy.runner import run as run_mod
    ep = run_mod.Endpoints(gateway_admin="http://gw", flink_rest=None)
    calls = []
    # multi-replica case: the group lag answers, the per-replica counter is never consulted
    monkeypatch.setattr(kafka_mod, "wait_group_caught_up", lambda *a, **k: calls.append(k) or 12)
    monkeypatch.setattr(run_mod, "_wait_gateway_scored", lambda *a: (_ for _ in ()).throw(AssertionError("unused")))
    assert run_mod.wait_gateway_primed(ep, 0, 15_000, 900, lambda _: None)
    assert calls[0]["no_offsets_grace_s"] == 20.0 and calls[0]["timeout_s"] == 300.0
    # group never committed offsets: fall back to the decision counter
    monkeypatch.setattr(kafka_mod, "wait_group_caught_up", lambda *a, **k: -1)
    seen = {}
    monkeypatch.setattr(run_mod, "_wait_gateway_scored", lambda admin, target, t, stop=None: seen.setdefault("target", target) > 0)
    assert run_mod.wait_gateway_primed(ep, 1_000, 15_000, 900, lambda _: None)
    assert seen["target"] == 1_000 + int(15_000 * 0.99)
    assert not run_mod.wait_gateway_primed(ep, None, 15_000, 900, lambda _: None)


def test_wait_group_grace_returns_when_group_has_no_offsets(monkeypatch):
    from realy.common import kafka as kafka_mod
    monkeypatch.setattr(kafka_mod, "group_lag", lambda g, t: None)
    lines = []
    assert kafka_mod.wait_group_caught_up("g", "t", 10, timeout_s=30, log=lines.append,
                                          no_offsets_grace_s=0.0, poll_s=0.01) == -1
    assert any("no committed offsets" in ln for ln in lines)
