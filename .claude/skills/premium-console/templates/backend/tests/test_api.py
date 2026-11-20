"""API tests: no threads, no network. The job function is a fake and jobs run synchronously via worker.execute."""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server import app as app_mod
from server.health import Probe, http_probe


def fake_run(job, h):
    h.define_steps([("load", "Load", "store"), ("work", "Work", "worker")])
    h.step("load", "running")
    h.step("load", "done")
    h.step("work", "running")
    for i in range(1, 4):
        h.metric(throughput=100.0, p50_ms=10.0, p99_ms=20.0 * i, errors=0)
        h.step("work", done=i, total=3, unit="items")
    h.step("work", "done")
    h.log("fake work complete")
    return {"processed": 3, "failed": 0, "throughput": 100.0, "p50_ms": 10.0, "p99_ms": 60.0, "budget_ms": 100,
            "within_budget": 1.0, "insights": [], "series": []}


PROBES = [Probe("store", "Job store", "database", lambda: {"version": "test"}),
          Probe("worker", "Worker", "job runner", lambda: {"status": "degraded", "details": {"reason": "test"}})]


@pytest.fixture()
def client(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("APP_TOKEN", raising=False)
    app = app_mod.create_app(data_dir=tmp_path, run_fn=fake_run, probes=PROBES, background=False,
                             static_dir=tmp_path / "no-ui")
    with TestClient(app) as c:
        yield c, app


def test_health_kinds_and_env(client):
    c, _ = client
    assert c.get("/api/health").json()["ok"] is True
    kinds = {k["kind"]: k for k in c.get("/api/kinds").json()}
    assert "demo" in kinds and kinds["demo"]["params"][0]["name"] == "items"
    comps = {x["id"]: x for x in c.get("/api/env").json()["components"]}
    assert comps["store"]["status"] == "up" and comps["worker"]["status"] == "degraded"
    assert isinstance(comps["store"]["latency_ms"], float)


def test_job_lifecycle(client):
    c, app = client
    assert c.post("/api/jobs", json={"kind": "nope"}).status_code == 404
    bad = c.post("/api/jobs", json={"kind": "demo", "params": {"items": -5}})
    assert bad.status_code == 400 and "items" in bad.json()["detail"]
    assert c.post("/api/jobs", json={"kind": "demo", "params": {"bogus": 1}}).status_code == 400
    j = c.post("/api/jobs", json={"kind": "demo", "params": {"items": 50}}).json()
    assert j["status"] == "queued" and j["params"]["items"] == 50 and j["params"]["rate"] == 120   # defaults filled in
    assert [q["id"] for q in c.get("/api/status").json()["queue"]] == [j["id"]]

    done = app.state.worker.execute(app.state.store.next_queued())
    assert done["status"] == "done" and done["result"]["processed"] == 3
    assert [s["status"] for s in done["steps"]] == ["done", "done"] and done["steps"][1]["progress"]["done"] == 3

    d = c.get(f"/api/jobs/{j['id']}").json()
    assert d["result"]["p99_ms"] == 60.0 and any("fake work complete" in line for line in d["logs"])
    listed = c.get("/api/jobs").json()
    assert listed["jobs"][0]["id"] == j["id"] and "series" not in listed["jobs"][0]["result"]
    s = c.get("/api/summary").json()
    assert s["jobs_total"] == 1 and s["jobs_done"] == 1 and s["items_total"] == 3
    hist = c.get("/api/live/history?seconds=60").json()
    assert len(hist["metrics"]) == 3 and hist["metrics"][-1]["p99_ms"] == 60.0
    assert c.get("/api/jobs/..%2Fetc").status_code in (400, 404)
    assert c.get("/api/jobs/missing").status_code == 404


def test_abort_queued_and_failure(client):
    c, app = client
    j = c.post("/api/jobs", json={"kind": "smoke"}).json()
    assert c.post(f"/api/jobs/{j['id']}/abort").json()["result"] == "cancelled"
    assert c.post(f"/api/jobs/{j['id']}/abort").json()["result"] == "already-cancelled"

    def boom(job, h):
        raise RuntimeError("disk full")
    app.state.worker.run_fn = boom
    j2 = c.post("/api/jobs", json={"kind": "smoke"}).json()
    failed = app.state.worker.execute(app.state.store.job(j2["id"]))
    assert failed["status"] == "failed" and "disk full" in failed["error"]


def test_demo_job_runs_end_to_end_fast(client):
    """The real demo task with a 10 ms tick: steps, metrics, result and ranked insights."""
    c, app = client
    from server import tasks
    app.state.worker.run_fn = tasks.run
    j = c.post("/api/jobs", json={"kind": "demo", "params": {"items": 400, "rate": 100, "tick_s": 0.01}}).json()
    done = app.state.worker.execute(app.state.store.job(j["id"]))
    r = done["result"]
    assert done["status"] == "done" and r["processed"] + r["failed"] == 400
    assert [s["status"] for s in done["steps"]] == ["done"] * 4
    sev = [i["severity"] for i in r["insights"]]
    assert sev == sorted(sev, key=tasks.SEVERITY_ORDER.get) and sev   # ranked, most severe first


def test_websocket_sends_status_then_broadcasts(client):
    c, app = client
    with c.websocket_connect("/api/live") as ws:
        first = ws.receive_json()
        assert first["type"] == "status" and first["current"] is None
        app.state.hub.add_log("j1", "hello from a worker thread")
        msg = ws.receive_json()
        assert msg["type"] == "log" and msg["line"] == "hello from a worker thread"


def test_auth_token_cookie_bearer_and_ws(tmp_path, monkeypatch):
    monkeypatch.setenv("APP_TOKEN", "s3cret-token")
    c = TestClient(app_mod.create_app(data_dir=tmp_path, run_fn=fake_run, probes=PROBES, background=False))
    assert c.get("/api/jobs").status_code == 401
    assert c.get("/api/health").status_code == 200
    assert c.get("/api/auth/me").json() == {"auth_required": True, "authenticated": False}
    assert c.post("/api/auth/login", json={"token": "wrong"}).status_code == 401
    r = c.post("/api/auth/login", json={"token": "s3cret-token"})
    assert r.status_code == 200 and "s3cret-token" not in r.headers.get("set-cookie", "")
    assert c.get("/api/jobs").status_code == 200
    c.post("/api/auth/logout")
    c.cookies.clear()
    assert c.get("/api/jobs").status_code == 401
    assert c.get("/api/jobs", headers={"Authorization": "Bearer s3cret-token"}).status_code == 200
    with pytest.raises(Exception):
        with c.websocket_connect("/api/live") as ws:
            ws.receive_json()


def test_spa_fallback(tmp_path):
    static = tmp_path / "static"
    (static / "assets").mkdir(parents=True)
    (static / "index.html").write_text("<html>console</html>")
    (static / "assets" / "app.js").write_text("console.log(1)")
    c = TestClient(app_mod.create_app(data_dir=tmp_path, run_fn=fake_run, probes=PROBES, background=False, static_dir=static))
    assert "console" in c.get("/jobs/some-job").text         # deep link -> index.html
    assert c.get("/assets/app.js").text == "console.log(1)"
    assert c.get("/api/nope").status_code == 404              # unknown API paths stay 404s


def test_unreachable_dependency_is_down_quickly(tmp_path):
    probes = [Probe("search", "Search", "http dependency", http_probe("http://127.0.0.1:1/health"))]
    c = TestClient(app_mod.create_app(data_dir=tmp_path, run_fn=fake_run, probes=probes, background=False))
    comp = c.get("/api/env").json()["components"][0]
    assert comp["status"] == "down" and "error" in comp["details"] and comp["latency_ms"] < 3000
