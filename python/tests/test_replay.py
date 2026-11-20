"""Live replay: a run's recorded telemetry served in the live hub's shape (snapshots per instance,
log, startup, markers, a decision sample)."""
from __future__ import annotations

import json
import time

from fastapi.testclient import TestClient

from realy.control import app as app_mod
from realy.runner.run import RunLog
from tests.test_explorer_entities import _run


def test_telemetry_of_a_run(tmp_path):
    runs = tmp_path / "runs"
    runs.mkdir()
    rid = _run(runs)
    d = runs / rid
    t0 = 1_900_000_000.0
    with open(d / "metrics.jsonl", "w") as f:
        for i in range(30):
            for src, inst in (("emitter", "shard-0"), ("collector", "c0"), ("gateway", "gw1")):
                f.write(json.dumps({"source": src, "instance": inst, "run_id": rid if src != "gateway" else "",
                                    "ts_us": int((t0 + 20 + i) * 1e6), "window_ms": 1000,
                                    "counters": {"emitted": 100.0} if src == "emitter" else {"decided": 100.0},
                                    "stages": []}) + "\n")
    (d / "manifest.json").write_text(json.dumps({"run_id": rid, "scenario": "x", "topology": "gateway-kafka",
                                                 "created_at": t0, "t_start_us": int((t0 + 20) * 1e6),
                                                 "warmup_s": 5, "duration_s": 30}))
    (d / "run.json").write_text(json.dumps({"phases": {"PREPARE": 0.0, "BACKFILL": 5.0, "WARMUP": 20.0, "MEASURE": 25.0,
                                                       "DRAIN": 50.0}, "rate_changes": [], "emission_end_us": int((t0 + 50) * 1e6)}))
    (d / "startup.json").write_text(json.dumps({"started_at": t0, "steps": [
        {"id": "world", "label": "World loaded", "index": 1, "status": "done", "detail": "", "started_at": t0, "ended_at": t0 + 1}]}))
    log = RunLog(d / "run.log", lambda _l: None)
    log("[startup 1/13] world loaded done")
    log.close()
    c = TestClient(app_mod.create_app(runs_dir=runs, kafka=False, background=False, run_fn=lambda *a, **k: {},
                                      publish_fn=lambda m: None))
    t = c.get(f"/api/runs/{rid}/telemetry").json()
    assert {k: len(v) for k, v in t["metrics"].items()} == {"emitter:shard-0": 30, "collector:c0": 30, "gateway:gw1": 30}
    run = t["run"]
    assert run["measure_started_at"] == t0 + 20 and run["warmup_s"] == 5 and run["complete"] is True
    labels = [m["label"] for m in t["markers"]]
    assert labels == ["prepare", "backfill", "warmup", "measure", "drain"]
    assert [m["t"] for m in t["markers"]][2] == t0 + 20
    assert t["logs"][0]["line"] == "[startup 1/13] world loaded done" and abs(t["logs"][0]["ts"] - time.time()) < 60
    assert t["startup"]["steps"][0]["id"] == "world"
    # a decision sample: the fixture's decisions all share one second, so at most 3 come back, with event fields
    assert 1 <= len(t["decisions"]) <= 3 and t["decisions"][0]["payment_type"] in ("CARD_PURCHASE", "TRANSFER")
    assert all(x["score"] is None or x["path"] == "FULL" for x in t["decisions"])


def test_run_without_telemetry_says_so(tmp_path):
    runs = tmp_path / "runs"
    (runs / "old-run").mkdir(parents=True)
    (runs / "old-run" / "manifest.json").write_text(json.dumps({"run_id": "old-run", "scenario": "x"}))
    c = TestClient(app_mod.create_app(runs_dir=runs, kafka=False, background=False, run_fn=lambda *a, **k: {},
                                      publish_fn=lambda m: None))
    t = c.get("/api/runs/old-run/telemetry").json()
    assert t["metrics"] == {} and "no recorded telemetry" in t["note"]
    assert c.get("/api/runs/nope/telemetry").status_code == 404
