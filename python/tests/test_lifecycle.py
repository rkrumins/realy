"""Run lifecycle: pipeline lease, aborts from every source, instrument failures, partial reports,
bulk control in the console, and recovery after a console restart. No infrastructure: Redis is a
small in-memory fake that implements the lease scripts' semantics."""
from __future__ import annotations

import json
import shutil
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from realy.runner import lifecycle
from realy.runner.run import RunHandle, _Run, _Stop, request_abort_handle
from realy.scenario import SCENARIO_DIR, load_scenario


class FakeRedis:
    """Enough of redis-py for the lease: SET NX PX, GET, DEL, PTTL and the two EVAL scripts."""

    def __init__(self) -> None:
        self.kv: dict[str, tuple[bytes, float | None]] = {}

    def _live(self, k: str):
        v = self.kv.get(k)
        if v and v[1] is not None and time.time() > v[1]:
            del self.kv[k]
            return None
        return v

    def set(self, k, v, nx=False, px=None, ex=None):
        if nx and self._live(k):
            return None
        exp = time.time() + px / 1000 if px else (time.time() + ex if ex else None)
        self.kv[k] = (v.encode() if isinstance(v, str) else v, exp)
        return True

    def get(self, k):
        v = self._live(k)
        return v[0] if v else None

    def delete(self, *ks):
        return sum(1 for k in ks if self.kv.pop(k, None) is not None)

    def pttl(self, k):
        v = self._live(k)
        return -2 if not v else (-1 if v[1] is None else int((v[1] - time.time()) * 1000))

    def eval(self, script, nkeys, key, run_id, *args):
        cur = self.get(key)
        mine = cur is not None and cur.decode().startswith(run_id + "\n")
        if script == lifecycle._RELEASE:
            return self.delete(key) if mine else 0
        value, ttl = args
        if mine:
            self.set(key, value, px=int(ttl))
            return 1
        if cur is None:
            self.set(key, value, nx=True, px=int(ttl))
            return 2
        return 0


@pytest.fixture()
def redis(monkeypatch):
    r = FakeRedis()
    monkeypatch.setattr(lifecycle, "_redis", lambda: r)
    monkeypatch.setattr(lifecycle, "HEARTBEAT_S", 0.05)
    return r


# ------------------------------------------------------------------------------------------ lease
def test_lease_excludes_a_second_run_and_frees_on_release(redis):
    a = lifecycle.RunLease("run-a", {"scenario": "smoke"}).acquire(log=lambda m: None)
    assert a.held and lifecycle.holder()["run_id"] == "run-a"
    with pytest.raises(lifecycle.PipelineBusy) as e:
        lifecycle.RunLease("run-b", {"scenario": "smoke"}).acquire(log=lambda m: None)
    assert e.value.holder["run_id"] == "run-a" and "busy" in str(e.value)
    a.release()
    assert lifecycle.holder() is None
    b = lifecycle.RunLease("run-b", {}).acquire(log=lambda m: None)
    assert b.held


def test_lease_wait_acquires_when_the_holder_finishes(redis):
    a = lifecycle.RunLease("run-a", {}).acquire(log=lambda m: None)
    threading.Timer(0.3, a.release).start()
    t0 = time.time()
    b = lifecycle.RunLease("run-b", {}).acquire(wait_s=5, log=lambda m: None)
    assert b.held and 0.2 < time.time() - t0 < 3


def test_crashed_owner_lease_expires(redis, monkeypatch):
    monkeypatch.setattr(lifecycle, "LEASE_TTL_S", 0.2)
    lifecycle.RunLease("dead", {}).acquire(log=lambda m: None)     # never renewed, never released
    time.sleep(0.3)
    assert lifecycle.RunLease("next", {}).acquire(log=lambda m: None).held


def test_redis_down_does_not_block_runs(monkeypatch):
    def boom():
        raise ConnectionError("no redis")
    monkeypatch.setattr(lifecycle, "_redis", boom)
    lines: list[str] = []
    lease = lifecycle.RunLease("x", {}).acquire(log=lines.append)
    assert not lease.held and lease.unavailable and "not prevented" in lines[0]


def test_heartbeat_delivers_a_remote_abort(redis, tmp_path):
    h = RunHandle("run-a", tmp_path / "run-a")
    lease = lifecycle.RunLease("run-a", {}).acquire(log=lambda m: None).start(h, log=lambda m: None)
    try:
        assert lifecycle.request_abort("run-a", "operator says stop", publish=False)["redis"]
        for _ in range(100):
            if h.abort_reason:
                break
            time.sleep(0.02)
        assert h.abort_reason == "operator says stop" and h.abort_kind == "operator" and h.stop.is_set()
        hb = json.loads((tmp_path / "run-a" / "heartbeat").read_text())
        assert hb["phase"] == "PREPARE"
    finally:
        lease.release()


def test_lease_stolen_aborts_the_run(redis, tmp_path):
    h = RunHandle("run-a", tmp_path / "run-a")
    lease = lifecycle.RunLease("run-a", {}).acquire(log=lambda m: None)
    redis.set(lifecycle.lease_key(), "intruder\n{}")          # e.g. forced by an operator elsewhere
    lease.start(h, log=lambda m: None)
    try:
        for _ in range(100):
            if h.abort_reason:
                break
            time.sleep(0.02)
        assert h.abort_kind == "lease" and "intruder" in h.abort_reason
    finally:
        lease.release()
    assert lifecycle.holder()["run_id"] == "intruder"           # release never deletes someone else's lease


# ------------------------------------------------------------------------------------------ aborts
def test_first_abort_reason_wins():
    h = RunHandle("r", Path("/tmp/r"))
    h.phase = "MEASURE"
    assert h.request_abort("guardrail: backlog", "guardrail")
    assert not h.request_abort("operator", "operator")
    assert (h.abort_reason, h.abort_kind, h.abort_phase) == ("guardrail: backlog", "guardrail", "MEASURE")
    assert h.stop.is_set()
    ns = SimpleNamespace(stop=threading.Event(), phase="WARMUP", abort_reason=None)   # duck-typed handles work too
    assert request_abort_handle(ns, "x", "signal") and ns.abort_kind == "signal"


def _run(tmp_path: Path, **kw) -> _Run:
    sc = load_scenario("smoke")
    h = RunHandle("r", tmp_path)
    lease = SimpleNamespace(info={}, held=False, disabled=True, unavailable=None)
    r = _Run(sc, None, "r", tmp_path, h, lease, lambda m: None, None)
    for k, v in kw.items():
        setattr(r, k, v)
    return r


def test_dead_child_process_aborts_as_instrument_failure(tmp_path):
    r = _run(tmp_path, drain=threading.Event())
    r.procs = [("emitter", 0, SimpleNamespace(exitcode=None)), ("collector", 0, SimpleNamespace(exitcode=-9))]
    with pytest.raises(_Stop):
        r.check("measure")
    assert r.h.abort_kind == "instrument" and "collector 0 exited with code -9" in r.h.abort_reason


def test_collector_exiting_early_is_a_failure_but_not_after_drain(tmp_path):
    drain = threading.Event()
    r = _run(tmp_path, drain=drain)
    r.procs = [("collector", 0, SimpleNamespace(exitcode=0))]
    drain.set()
    r.check("drain")                      # a collector that finished its drain is fine
    drain.clear()
    with pytest.raises(_Stop):
        r.check("measure")


def test_watchdog(tmp_path):
    r = _run(tmp_path, watchdog_s=0.0)
    time.sleep(0.01)
    with pytest.raises(_Stop):
        r.check("backfill")
    assert r.h.abort_kind == "watchdog"


def test_emitter_error_result_aborts(tmp_path):
    r = _run(tmp_path)
    r.results.put(("emitter", 1, {"error": "Traceback...\nKafkaException: broker down"}))
    time.sleep(0.2)
    assert r.pump(2.0)
    assert r.h.abort_kind == "instrument" and "broker down" in r.h.abort_reason


# ------------------------------------------------------------------------------------------ partial reports
def _run_dir(tmp_path: Path, n: int = 2000, decided: int | None = None, abort: dict | None = None,
             started: bool = True) -> Path:
    d = tmp_path / "run"
    d.mkdir()
    sc = load_scenario("smoke")
    (d / "scenario.json").write_text(json.dumps(sc.model_dump(mode="json")))
    t0 = 1_700_000_000_000_000
    (d / "manifest.json").write_text(json.dumps({"run_id": "run", "scenario": "smoke", "topology": sc.topology,
                                                 "t_start_us": t0 if started else None, "warmup_s": 1.0,
                                                 "duration_s": sc.traffic.duration_s}))
    if started:
        ids = np.arange(1, n + 1, dtype=np.uint64)
        intended = t0 + np.arange(n, dtype=np.int64) * 1000      # 1k events/s
        pq.write_table(pa.table({"event_id": ids, "t_intended_us": intended, "t_sent_us": intended + 100,
                                 "ack_us": np.full(n, 500, np.int64)}), d / "emit-0.parquet")
        k = n if decided is None else decided
        z = np.zeros(k, np.int64)
        pq.write_table(pa.table({"event_id": ids[:k], "revision": np.zeros(k, np.uint8),
                                 "t_ingest_us": intended[:k] + 2000, "t_decision_us": intended[:k] + 12000,
                                 "t_collected_us": intended[:k] + 15000, "outcome": np.ones(k, np.int8),
                                 "degraded": np.zeros(k, np.bool_), "feature_staleness_ms": z.astype(np.float32)}),
                       d / "decisions-0.parquet")
    run = {"aborted": (abort or {}).get("reason"), "abort": abort,
           "emission_end_us": t0 + n * 1000 if started else None, "measured_s": (n / 1000 - 1.0) if started else 0}
    (d / "run.json").write_text(json.dumps(run))
    return d


def test_operator_abort_report_is_aborted_with_actual_window(tmp_path):
    from realy.report.compare import run_summary
    from realy.report.report import build_report
    d = _run_dir(tmp_path, abort={"reason": "aborted by operator", "kind": "operator", "phase": "MEASURE", "after_s": 3})
    s = build_report(d)["summary"]
    assert s["verdict"] == "ABORTED" and s["reasons"][0].startswith("aborted (operator, in MEASURE)")
    assert s["measured_s"] == pytest.approx(1.0)
    assert s["achieved_emit_tps"] == pytest.approx(1000, rel=0.01)      # not diluted by the unreached window
    assert run_summary(d)["status"] == "aborted" and (d / "report.html").exists()


def test_guardrail_abort_is_a_fail(tmp_path):
    from realy.report.report import build_report
    d = _run_dir(tmp_path, abort={"reason": "pipeline backlog 90000 > 50000", "kind": "guardrail", "phase": "MEASURE"})
    assert build_report(d)["summary"]["verdict"] == "FAIL"


def test_abort_before_emission_has_a_minimal_report(tmp_path):
    from realy.report.report import build_report
    d = _run_dir(tmp_path, started=False, abort={"reason": "SIGTERM received", "kind": "signal", "phase": "BACKFILL"})
    rep = build_report(d)
    assert rep["summary"]["verdict"] == "ABORTED" and "BACKFILL" in rep["summary"]["reasons"][0]
    assert (d / "report.html").exists() and (d / "report.md").exists()


def test_in_flight_events_are_labelled_on_an_aborted_run(tmp_path):
    from realy.report.report import build_report
    d = _run_dir(tmp_path, decided=1500, abort={"reason": "x", "kind": "operator", "phase": "MEASURE"})
    s = build_report(d)["summary"]
    assert s["missing"] == 500 and any("in flight" in r for r in s["reasons"])


def test_status_running_needs_a_fresh_heartbeat(tmp_path):
    from realy.report.compare import run_summary
    d = tmp_path / "r"
    d.mkdir()
    (d / "manifest.json").write_text(json.dumps({"run_id": "r", "scenario": "smoke"}))
    (d / "heartbeat").write_text(json.dumps({"ts": time.time(), "phase": "BACKFILL"}))
    s = run_summary(d)
    assert (s["status"], s["phase"]) == ("running", "BACKFILL")
    (d / "heartbeat").write_text(json.dumps({"ts": time.time() - 120, "phase": "MEASURE"}))
    assert run_summary(d)["status"] == "incomplete"           # its owner died: not "running" forever


# ------------------------------------------------------------------------------------------ console
@pytest.fixture()
def console(tmp_path: Path):
    from fastapi.testclient import TestClient

    from realy.control.app import create_app
    runs, scen = tmp_path / "runs", tmp_path / "scenarios"
    scen.mkdir()
    shutil.copy(SCENARIO_DIR / "smoke.yaml", scen / "smoke.yaml")
    published: list = []
    gate = threading.Event()
    calls = {"busy": 0}

    def run(sc, handle=None, log=print, run_id=None):
        if calls["busy"] > 0:
            calls["busy"] -= 1
            raise lifecycle.PipelineBusy({"run_id": "cli-run", "owner": "laptop:1", "started_at": time.time()})
        handle.phase = "MEASURE"
        while not handle.stop.is_set() and not gate.is_set():
            time.sleep(0.01)
        return {"run_dir": str(runs / run_id), "report": {"verdict": "ABORTED" if handle.abort_reason else "PASS"},
                "aborted": handle.abort_reason}

    app = create_app(runs_dir=runs, scenario_dir=scen, run_fn=run, verify_fn=lambda d: {"passed": True},
                     kafka=False, background=False, publish_fn=published.extend)
    with TestClient(app) as c:
        yield c, app, published, gate, calls, runs


def _wait(pred, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.02)
    return False


def test_bulk_abort_running_and_queued(console):
    c, app, published, gate, _, _ = console
    w = app.state.worker
    jobs = [c.post("/api/runs", json={"scenario": "smoke"}).json() for _ in range(3)]
    t = threading.Thread(target=w.execute, args=(app.state.store.next_queued(),))
    t.start()
    assert _wait(lambda: w.current is not None)
    r = c.post("/api/runs/abort", json={"all": True, "reason": "bulk"}).json()
    assert r["count"] == 3
    t.join(5)
    st = {j["id"]: app.state.store.job(j["id"])["status"] for j in jobs}
    assert sorted(st.values()) == ["aborted", "cancelled", "cancelled"]
    assert c.post("/api/runs/abort", json={}).status_code == 400


def test_abort_selected_ids_and_foreign_run(console):
    c, app, published, _, _, _ = console
    a = c.post("/api/runs", json={"scenario": "smoke"}).json()
    b = c.post("/api/runs", json={"scenario": "smoke"}).json()
    r = c.post("/api/runs/abort", json={"ids": [b["id"], "someone-elses-run"]}).json()["results"]
    assert r[b["id"]]["result"] == "cancelled" and r["someone-elses-run"]["result"] == "not-running-here"
    assert r["someone-elses-run"]["channels"]["kafka"] and published[-1].phase == 6
    assert app.state.store.job(a["id"])["status"] == "queued"
    assert c.post(f"/api/runs/{b['id']}/abort").json()["result"] == "already-cancelled"


def test_stop_all_pauses_schedules_and_publishes_wildcard(console):
    c, app, published, _, _, _ = console
    s = c.post("/api/schedules", json={"scenario": "smoke", "every_minutes": 5}).json()
    c.post("/api/runs", json={"scenario": "smoke"})
    r = c.post("/api/stop-all", json={}).json()
    assert r["schedules_paused"] == [s["id"]] and r["wildcard_published"]
    assert published[-1].run_id == "" and published[-1].phase == 6
    assert not app.state.store.queued()
    assert c.post(f"/api/schedules/{s['id']}/resume").json()["resumed"] == [s["id"]]
    assert c.post("/api/schedules/pause-all").json()["paused"] == [s["id"]]


def test_busy_pipeline_keeps_the_job_queued(console):
    c, app, _, gate, calls, _ = console
    w = app.state.worker
    calls["busy"] = 1
    j = c.post("/api/runs", json={"scenario": "smoke"}).json()
    w.execute(app.state.store.next_queued())
    job = app.state.store.job(j["id"])
    assert job["status"] == "queued" and job["run_id"] is None
    assert w.status()["blocked_by"]["run_id"] == "cli-run"
    gate.set()
    w.execute(app.state.store.next_queued())          # pipeline free now: runs
    assert app.state.store.job(j["id"])["status"] == "done" and w.blocked_by is None


def test_shutdown_aborts_the_running_job(console):
    c, app, _, _, _, _ = console
    w = app.state.worker
    c.post("/api/runs", json={"scenario": "smoke"})
    t = threading.Thread(target=w.execute, args=(app.state.store.next_queued(),))
    t.start()
    assert _wait(lambda: w.current is not None)
    w.shutdown(grace_s=5)
    t.join(5)
    assert app.state.store.jobs()[0]["status"] == "aborted"
    assert app.state.store.jobs()[0]["abort_reason"] == "console shutting down"


def test_restart_marks_running_jobs_interrupted_and_finishes_them(tmp_path, monkeypatch):
    from realy.control.store import Store
    from realy.control.worker import RunWorker
    monkeypatch.setenv("REALY_RECOVER_DELAY_S", "0")
    runs = tmp_path / "runs"
    s1 = Store(runs / "console.db")
    j = s1.enqueue("smoke")
    s1.update(j["id"], status="running", run_id="run")
    d = _run_dir(runs, started=True)                     # what the dead process left on disk (no report)
    assert d.name == "run"
    (d / "run.json").unlink()
    s2 = Store(runs / "console.db")                      # the console starts again
    assert s2.job(j["id"])["status"] == "interrupted" and [x["id"] for x in s2.interrupted] == [j["id"]]
    aborted: list = []
    w = RunWorker(s2, runs, SCENARIO_DIR, remote_abort=lambda rid, why: aborted.append((rid, why)))
    assert w.recover() == ["run"] and aborted == [("run", "console restarted during run")]
    assert _wait(lambda: (d / "report.json").exists())
    rep = json.loads((d / "report.json").read_text())
    assert rep["summary"]["verdict"] == "ABORTED" and rep["abort"]["kind"] == "interrupted"
