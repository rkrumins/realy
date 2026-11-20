"""A run in progress is visible with its live stats whoever started it: the console describes a run it
did not start (CLI, a Kubernetes Job, another console) from the pipeline lease and its run directory,
and serves its persisted run.log."""
from __future__ import annotations

import json
import time

from fastapi.testclient import TestClient

from realy.control import app as app_mod
from realy.runner.run import RunLog, read_run_log


def test_run_log_round_trip_and_partial_lines(tmp_path):
    seen: list[str] = []
    log = RunLog(tmp_path / "run.log", seen.append)
    log("[startup 1/13] world loaded")
    log("two\nlines")
    log.close()
    assert seen == ["[startup 1/13] world loaded", "two\nlines"]
    lines, off = read_run_log(tmp_path)
    assert [x["line"] for x in lines] == ["[startup 1/13] world loaded", "two", "lines"]
    assert all(x["t"].endswith("Z") and abs(x["ts"] - time.time()) < 60 for x in lines)
    with open(tmp_path / "run.log", "a") as f:
        f.write("2026-01-01T12:00:00.000Z half a li")            # still being written: not handed out yet
    more, off2 = read_run_log(tmp_path, off)
    assert more == [] and off2 == off


def test_status_describes_a_run_started_elsewhere(tmp_path, monkeypatch):
    runs = tmp_path / "runs"
    rid = "step-ramp-20991231-000000"
    d = runs / rid
    d.mkdir(parents=True)
    (d / "manifest.json").write_text(json.dumps({"run_id": rid, "scenario": "step-ramp", "topology": "gateway-kafka",
                                                 "warmup_s": 10, "duration_s": 300}))
    (d / "scenario.json").write_text(json.dumps({"name": "step-ramp", "topology": "gateway-kafka",
                                                 "traffic": {"profile": {"kind": "step", "start_tps": 1000, "tps": 1000},
                                                             "duration_s": 300, "warmup_s": 10},
                                                 "budget": {"e2e_p99_ms": 150}}))
    (d / "heartbeat").write_text(json.dumps({"ts": time.time(), "phase": "BACKFILL", "owner": "cli:host:42"}))
    log = RunLog(d / "run.log", lambda _l: None)
    log("[startup 9/13] feature catch-up: 77%")
    log.close()
    lease = {"run_id": rid, "scenario": "step-ramp", "owner": "cli:host:42", "phase": "PREPARE",
             "started_at": time.time() - 600}
    import realy.runner.lifecycle as lifecycle
    monkeypatch.setattr(lifecycle, "holder", lambda r=None: dict(lease))
    app = app_mod.create_app(runs_dir=runs, kafka=True, background=False, run_fn=lambda *a, **k: {},
                             publish_fn=lambda msgs: None)
    c = TestClient(app)
    st = c.get("/api/status").json()
    assert st["current"] is None
    a = st["active"]
    assert a["run_id"] == rid and a["local"] is False and a["owner"] == "cli:host:42"
    assert a["phase"] == "BACKFILL"                      # the run's own heartbeat, fresher than the lease
    assert a["base_tps"] == 1000 and a["duration_s"] == 300 and a["warmup_s"] == 10
    assert a["topology"] == "gateway-kafka" and a["budget"]["e2e_p99_ms"] == 150
    # the report page reads the persisted log of a run this console never ran
    assert c.get(f"/api/runs/{rid}").json()["logs"] == ["[startup 9/13] feature catch-up: 77%"]
