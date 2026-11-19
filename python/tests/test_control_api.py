"""Operator console API tests (no Kafka, no real runs: the runner is replaced by a fake)."""
from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from realy.control.app import create_app
from realy.scenario import SCENARIO_DIR


def _fake_run(runs_dir: Path):
    def run(sc, handle=None, log=print, run_id=None):
        d = runs_dir / run_id
        d.mkdir(parents=True)
        handle.phase = "MEASURE"
        log(f"fake run of {sc.name} at {sc.traffic.profile.tps} TPS")
        verdict = "PASS" if sc.traffic.profile.tps <= 1000 else "FAIL"
        (d / "manifest.json").write_text(json.dumps({"run_id": run_id, "scenario": sc.name, "topology": sc.topology,
                                                     "t_start_us": int(time.time() * 1e6), "duration_s": 1}))
        stages = {"e2e": {"count": 10, "p50": 10.0, "p90": 20.0, "p99": 30.0 * (2 if verdict == "FAIL" else 1),
                          "p999": 40.0, "max": 50.0, "mean": 12.0}}
        (d / "report.json").write_text(json.dumps({
            "run_id": run_id, "scenario": sc.name, "topology": sc.topology, "budget": sc.budget.model_dump(),
            "summary": {"verdict": verdict, "reasons": [], "emitted": 10, "decided": 10, "missing": 0, "duplicates": 0,
                        "achieved_emit_tps": sc.traffic.profile.tps, "e2e_p50_ms": 10.0, "e2e_p99_ms": stages["e2e"]["p99"]},
            "stages": stages, "levels": []}))
        (d / "run.json").write_text(json.dumps({"aborted": None}))
        (d / "report.html").write_text("<html>ok</html>")
        handle.phase = "COMPLETE"
        return {"run_dir": str(d), "report": {"verdict": verdict}, "aborted": None}
    return run


@pytest.fixture()
def env(tmp_path: Path):
    runs = tmp_path / "runs"
    scen = tmp_path / "scenarios"
    scen.mkdir()
    shutil.copy(SCENARIO_DIR / "smoke.yaml", scen / "smoke.yaml")
    published: list = []
    verified: list = []

    def verify(run_dir):
        verified.append(run_dir)
        return {"passed": True, "run_id": Path(run_dir).name, "features": {"compared_events": 0, "exact_set_agreement": 1.0,
                "note": "", "worst": []}, "rules": {"parity": 1.0, "checked": 0}, "score": {"parity": 1.0, "checked": 0},
                "outcome": {"parity": 1.0}}

    app = create_app(runs_dir=runs, scenario_dir=scen, run_fn=_fake_run(runs), verify_fn=verify, kafka=False,
                     background=False, publish_fn=published.extend)
    with TestClient(app) as client:
        yield client, app, published, verified, scen


def test_scenarios_list_validate_save_budget(env):
    c, _, _, _, scen = env
    names = [s["name"] for s in c.get("/api/scenarios").json()]
    assert names == ["smoke"]
    text = c.get("/api/scenarios/smoke").json()["yaml"]
    assert c.post("/api/scenarios/validate", json={"yaml": text}).json()["ok"]
    bad = c.post("/api/scenarios/validate", json={"yaml": text.replace("gateway-kafka", "carrier-pigeon")})
    assert bad.status_code == 400
    assert c.put("/api/scenarios/other", json={"yaml": text}).status_code == 400  # name mismatch
    new = text.replace("name: smoke", "name: smoke2")
    assert c.put("/api/scenarios/smoke2", json={"yaml": new}).status_code == 200
    assert (scen / "smoke2.yaml").exists()
    b = c.post("/api/scenarios/smoke/budget", json={"samples": 5000}).json()
    assert b["verdict"] in ("FEASIBLE", "MARGINAL", "INFEASIBLE") and "p99" in b["e2e"]
    b2 = c.post("/api/scenarios/smoke/budget", json={"samples": 5000, "overrides": {
        "scoring": {"feature_lookup_delay": {"fixed": {"ms": 100}}, "feature_deadline_ms": 140, "model_deadline_ms": 140},
        "model_server": {"service_time": {"uniform": {"min_ms": 30, "max_ms": 100}}}}}).json()
    assert b2["verdict"] != "FEASIBLE" and b2["fallback_rate"] > 0.5


def test_run_lifecycle_history_detail_compare(env):
    c, app, _, verified, _ = env
    assert c.post("/api/runs", json={"scenario": "nope"}).status_code == 404
    assert c.post("/api/runs", json={"scenario": "smoke", "overrides": {"topology": "bogus"}}).status_code == 400
    j1 = c.post("/api/runs", json={"scenario": "smoke", "overrides": {"traffic": {"duration_s": 5}}, "verify": True}).json()
    assert j1["status"] == "queued"
    w = app.state.worker
    done = w.execute(app.state.store.next_queued())
    assert done["status"] == "done" and done["verdict"] == "PASS" and done["verify_passed"] == 1
    assert verified
    j2 = c.post("/api/runs", json={"scenario": "smoke", "overrides": {"traffic": {"profile": {"tps": 5000}}}}).json()
    done2 = w.execute(app.state.store.job(j2["id"]))
    assert done2["verdict"] == "FAIL"
    runs = c.get("/api/runs").json()
    ids = [h["run_id"] for h in runs["history"]]
    assert done["run_id"] in ids and done2["run_id"] in ids
    d = c.get(f"/api/runs/{done['run_id']}").json()
    assert d["report"]["summary"]["verdict"] == "PASS"
    assert any("fake run" in line for line in d["logs"])
    assert c.get(f"/api/runs/{done['run_id']}/report.html").text == "<html>ok</html>"
    cmp_ = c.get(f"/api/compare?a={done['run_id']}&b={done2['run_id']}").json()
    assert cmp_["stages"]["e2e"]["d_p99"] == pytest.approx(30.0)
    assert c.get("/api/runs/..%2Fetc").status_code in (400, 404)


def test_abort_queued_and_running(env):
    c, app, published, _, _ = env
    j = c.post("/api/runs", json={"scenario": "smoke"}).json()
    assert c.post(f"/api/runs/{j['id']}/abort").json()["result"] == "cancelled"
    assert app.state.store.job(j["id"])["status"] == "cancelled"
    # a run in another process: abort is published on bench.control
    r = c.post("/api/runs/someone-elses-run/abort").json()
    assert r["result"] == "not-running-here"
    assert published and published[-1].phase == 6


def test_live_controls_publish(env):
    c, _, published, _, _ = env
    assert c.post("/api/live/rate", json={"tps": 750}).json()["published"]
    m = published[-1]
    assert m.target == "emitter" and m.WhichOneof("cmd") == "rate" and m.rate.tps == 750
    r = c.post("/api/live/model", json={"service_time": {"lognormal": {"median_ms": 5, "p99_ms": 20}}, "workers": 8}).json()
    assert r["published"] and published[-1].target == "model"
    assert c.post("/api/live/scoring", json={"no_such_field": 1}).status_code == 400
    assert c.post("/api/live/rate", json={"tps": 0}).status_code == 422


def test_schedules(env):
    c, app, _, _, _ = env
    s = c.post("/api/schedules", json={"scenario": "smoke", "every_minutes": 10, "count": 2}).json()
    assert s["remaining"] == 2
    sch = app.state.scheduler
    now = time.time() + 1
    assert len(sch.tick(now)) == 1
    assert sch.tick(now) == []                      # not due again yet
    assert len(sch.tick(now + 601)) == 1            # second (last) slot
    assert sch.tick(now + 1300) == []               # exhausted
    assert not c.get("/api/schedules").json()[0]["enabled"]
    assert len(app.state.store.queued()) == 2
    once = c.post("/api/schedules", json={"scenario": "smoke", "at": "2000-01-01T00:00:00Z"}).json()
    assert len(sch.tick()) == 1
    assert c.delete(f"/api/schedules/{once['id']}").json()["deleted"] == once["id"]
    assert c.delete("/api/schedules/nope").status_code == 404


def test_websocket_and_history(env):
    c, app, _, _, _ = env
    app.state.hub.ingest({"run_id": "r1", "source": "emitter", "instance": "shard-0", "ts_us": str(int(time.time() * 1e6)),
                          "counters": {"emitted": "500"}, "stages": [{"name": "schedule_lag", "count": "3", "p99_ms": 0.5}]})
    h = c.get("/api/live/history").json()
    snap = h["metrics"]["emitter:shard-0"][0]
    assert snap["counters"]["emitted"] == 500.0 and snap["stages"][0]["p50_ms"] == 0.0
    with c.websocket_connect("/api/live") as ws:
        first = ws.receive_json()
        assert first["type"] == "status" and first["current"] is None
