"""A run's timeline (runner/timeline.py), its ETA (report/eta.py) and how the console serves them: the
times every page shows come from one place, and preparation, warm-up, drain, report and verify are each
accounted for (warm-up is inside the traffic window, not added to it)."""
from __future__ import annotations

import json
import time

from realy.report.eta import estimate, scenario_estimate
from realy.runner.startup import Startup, timed_step
from realy.runner.timeline import timeline

T0 = float(int(time.time()) - 5_000)   # recent past: a real verification (now) comes after it


def _run(tmp_path, name="r1", prepare=100.0, warmup=10.0, duration=60.0, abort_in=None, steps_until="report",
         scenario="smoke"):
    """A synthetic run directory: launched at T0, `prepare` s of preparation (of which only 70% in steps),
    then traffic for `duration` s (warm-up inside), a 5 s drain and a 3 s report."""
    d = tmp_path / name
    d.mkdir()
    traffic = T0 + prepare
    man = {"run_id": name, "scenario": scenario, "topology": "gateway-kafka", "created_at": T0,
           "warmup_s": warmup, "duration_s": duration, "t_start_us": None}
    st = Startup(d, lambda _m: None, write_every_s=0)
    st.t0 = T0

    def step(sid, a, b, status="done"):
        st.steps[sid].update(status=status, started_at=a, ended_at=b)

    step("world", T0, T0 + 0.7 * prepare)            # 30% of the preparation is outside any step
    run = {}
    if abort_in == "BACKFILL":
        step("history", T0 + 0.7 * prepare, None, "failed")
        st.steps["history"]["ended_at"] = T0 + 0.8 * prepare
        run = {"aborted": "operator stop", "abort": {"reason": "operator stop", "kind": "operator",
                                                     "phase": "BACKFILL", "after_s": 0.8 * prepare},
               "transitions": [["PREPARE", 0.0], ["BACKFILL", 1.0], ["ABORTING", 0.8 * prepare]], "wall_s": prepare}
        step("drain", T0 + 0.8 * prepare, T0 + 0.8 * prepare + 2)
        step("report", T0 + 0.8 * prepare + 2, T0 + 0.8 * prepare + 3)
    else:
        man["t_start_us"] = int(traffic * 1e6)
        step("warmup", traffic, traffic + warmup)
        end = traffic + (duration if abort_in != "MEASURE" else warmup + 20)
        step("measure", traffic + warmup, end)
        if steps_until == "measure":       # still measuring
            st.steps["measure"].update(status="running", ended_at=None)
        if steps_until in ("drain", "report"):
            step("drain", end, end + 5)
            run = {"emission_end_us": int(end * 1e6), "transitions": [["PREPARE", 0.0]], "wall_s": end + 8 - T0}
            if abort_in == "MEASURE":
                run["aborted"] = "guardrail"
                run["abort"] = {"reason": "guardrail", "kind": "guardrail", "phase": "MEASURE", "after_s": end - T0}
        if steps_until == "report":
            step("report", end + 5, end + 8)
            (d / "report.json").write_text("{}")
    st.write()
    (d / "manifest.json").write_text(json.dumps(man))
    if run:
        (d / "run.json").write_text(json.dumps(run))
    return d


def test_a_finished_run_accounts_for_every_second(tmp_path):
    tl = timeline(_run(tmp_path), now=T0 + 10_000)
    d = tl["durations"]
    assert tl["stage"] == "done" and not tl["live"]
    assert d["prepare_s"] == 100.0           # includes the 30 s spent outside any step
    assert d["warmup_s"] == 10.0 and d["measured_s"] == 50.0   # warm-up inside the 60 s window
    assert d["drain_s"] == 5.0 and d["report_s"] == 3.0 and d["verify_s"] is None
    assert d["total_s"] == 168.0 and tl["finished_at"] == T0 + 168.0
    assert tl["configured"] == {"duration_s": 60.0, "warmup_s": 10.0, "measure_s": 50.0}


def test_verification_extends_the_timeline_from_another_process(tmp_path):
    d = _run(tmp_path)
    with timed_step(d, "verify", "recomputing") as t:
        st = json.loads((d / "startup.json").read_text())
        assert {s["id"]: s for s in st["steps"]}["verify"]["status"] == "running"
        assert timeline(d)["stage"] == "verify"
        t.detail = "PASS"
    tl = timeline(d)
    assert tl["stage"] == "done" and tl["durations"]["verify_s"] is not None
    assert tl["finished_at"] == tl["verify_end"] and tl["durations"]["total_s"] >= 168.0


def test_a_run_aborted_while_preparing_has_no_traffic(tmp_path):
    tl = timeline(_run(tmp_path, abort_in="BACKFILL"), now=T0 + 10_000)
    assert tl["traffic_start"] is None and tl["measure_start"] is None
    assert tl["durations"]["prepare_s"] == 80.0 and tl["durations"]["measured_s"] is None
    assert tl["aborted"] and tl["stage"] == "done"


def test_skipped_steps_take_no_time(tmp_path):
    d = _run(tmp_path)
    st = Startup.resume(d)
    st.skip("verify", "not requested")
    st.write()
    tl = timeline(d, now=T0 + 10_000)
    assert tl["verify_start"] is None and tl["durations"]["verify_s"] is None
    assert tl["finished_at"] == T0 + 168.0


def test_a_run_aborted_while_measuring(tmp_path):
    tl = timeline(_run(tmp_path, abort_in="MEASURE"), now=T0 + 10_000)
    assert tl["aborted"] and tl["durations"]["measured_s"] == 20.0


def test_live_stages_follow_the_run(tmp_path):
    d = _run(tmp_path, steps_until="measure")
    tl = timeline(d, now=T0 + 105, live=True)
    assert tl["stage"] == "measure" and tl["durations"]["total_s"] == 105.0
    (d / "manifest.json").write_text(json.dumps({**json.loads((d / "manifest.json").read_text()), "t_start_us": None}))
    st = json.loads((d / "startup.json").read_text())
    for s in st["steps"]:
        if s["id"] in ("warmup", "measure"):
            s.update(status="pending", started_at=None, ended_at=None)
    (d / "startup.json").write_text(json.dumps(st))
    tl = timeline(d, now=T0 + 40, live=True)
    assert tl["stage"] == "prepare" and tl["durations"]["prepare_s"] == 40.0


def test_a_legacy_run_without_finish_steps_still_has_honest_times(tmp_path):
    d = _run(tmp_path, steps_until="measure")
    end = T0 + 160
    (d / "run.json").write_text(json.dumps({"emission_end_us": int(end * 1e6), "wall_s": 166.0,
                                            "phases": {"PREPARE": 0.0}}))
    (d / "report.json").write_text("{}")
    tl = timeline(d, now=T0 + 10_000)
    assert tl["drain_end"] == T0 + 166.0 and tl["durations"]["drain_s"] == 6.0
    assert tl["report_end"] is not None and not tl["live"]


def test_the_eta_learns_from_earlier_runs_and_never_goes_negative(tmp_path):
    for i in range(3):
        _run(tmp_path, name=f"old{i}", prepare=100.0)
    cur = _run(tmp_path, name="cur", steps_until="measure")
    # the current run: traffic started at T0+100; at T0+130 the 60 s window has 30 s left
    e = estimate(cur, tmp_path, verify=False, now=T0 + 130)
    assert e["per_step"]["measure"] == 30.0
    assert 30 + 5 <= e["remaining_s"] <= 30 + 5 + 3 + 1   # + drain + report medians
    assert "3 earlier smoke run(s)" in e["basis"]
    # preparing, a step running past its usual time: remaining is never negative and it is flagged late
    st = json.loads((cur / "startup.json").read_text())
    for s in st["steps"]:
        if s["id"] == "world":
            s.update(status="running", started_at=T0, ended_at=None)
        elif s["id"] in ("warmup", "measure"):
            s.update(status="pending", started_at=None, ended_at=None)
    (cur / "startup.json").write_text(json.dumps(st))
    man = json.loads((cur / "manifest.json").read_text())
    man["t_start_us"] = None
    (cur / "manifest.json").write_text(json.dumps(man))
    from realy.report import eta as eta_mod
    eta_mod._CACHE["at"] = 0
    e = estimate(cur, tmp_path, verify=True, now=T0 + 500)
    assert e["per_step"].get("world", 0) == 0 and "world" in e["late"]
    assert e["per_step"]["warmup"] == 10.0 and e["per_step"]["measure"] == 50.0
    assert e["per_step"]["verify"] == 60.0   # no earlier verification: the default


def test_a_queued_job_is_estimated_from_its_scenario(tmp_path):
    from realy.report import eta as eta_mod
    eta_mod._CACHE["at"] = 0
    _run(tmp_path, name="old", prepare=100.0)
    est = scenario_estimate(tmp_path, "smoke", "gateway-kafka", 60.0, verify=False)
    assert est["total_s"] == 60.0 + 70.0 + 5.0 + 3.0    # traffic + the steps' medians (30 s outside steps unknown)


def test_run_lists_order_by_launch_and_show_wall_times(tmp_path):
    from realy.report.compare import run_summary
    a = _run(tmp_path, name="a")
    b = _run(tmp_path, name="b")
    man = json.loads((b / "manifest.json").read_text())
    man["created_at"] = T0 + 1000
    (b / "manifest.json").write_text(json.dumps(man))
    time.sleep(0.01)
    (a / "report.json").write_text("{}")   # touched later: must not move it ahead
    sa = run_summary(a)
    assert sa["started_at"] == T0 and sa["launched_at"] == T0 and sa["traffic_started_at"] == T0 + 100
    assert sa["prepare_s"] == 100.0 and sa["total_s"] == 168.0 and sa["measured_s"] == 50.0


def test_the_console_serves_launch_order_timelines_and_verify_state(tmp_path):
    """Runs list in launch order whatever was touched last; a run's page carries its timeline; a verified
    job ends after its verification (finished_at), with the verify step timed."""
    import shutil

    from fastapi.testclient import TestClient

    from realy.control.app import create_app
    from realy.scenario import SCENARIO_DIR
    runs = tmp_path / "runs"
    runs.mkdir()
    scen = tmp_path / "scenarios"
    scen.mkdir()
    shutil.copy(SCENARIO_DIR / "smoke.yaml", scen / "smoke.yaml")
    old, new = _run(runs, name="old"), _run(runs, name="new")
    man = json.loads((new / "manifest.json").read_text())
    man["created_at"] = T0 + 1000
    (new / "manifest.json").write_text(json.dumps(man))
    time.sleep(0.01)
    (old / "verify.json").write_text(json.dumps({"passed": True}))   # touched last

    def run_fn(sc, handle=None, log=print, run_id=None):
        d = runs / run_id
        d.mkdir()
        st = Startup(d, lambda _m: None, write_every_s=0)
        st.start("world")
        st.finish("world")
        (d / "manifest.json").write_text(json.dumps({"run_id": run_id, "scenario": sc.name, "topology": sc.topology,
                                                     "created_at": time.time() - 2, "duration_s": 1, "warmup_s": 0,
                                                     "t_start_us": int((time.time() - 1) * 1e6)}))
        (d / "run.json").write_text(json.dumps({"aborted": None, "transitions": [], "wall_s": 2}))
        st.start("report")
        (d / "report.json").write_text(json.dumps({"summary": {"verdict": "PASS"}}))
        st.finish("report")
        return {"run_dir": str(d), "report": {"verdict": "PASS"}, "aborted": None}

    def verify_fn(run_dir):
        time.sleep(0.05)
        return {"passed": True}

    app = create_app(runs_dir=runs, scenario_dir=scen, run_fn=run_fn, verify_fn=verify_fn, kafka=False, background=False)
    with TestClient(app) as c:
        ids = [h["run_id"] for h in c.get("/api/runs").json()["history"]]
        assert ids[:2] == ["new", "old"]
        r = c.get("/api/runs/old").json()
        assert r["timeline"]["durations"]["prepare_s"] == 100.0 and r["server_now"] > 0
        job = c.post("/api/runs", json={"scenario": "smoke", "verify": True}).json()
        q = c.get("/api/status").json()["queue"]
        assert q and q[0]["expected_total_s"] > 0 and q[0]["expected_start"] > 0
        done = app.state.worker.execute(app.state.store.next_queued())
        assert done["status"] == "done" and done["verify_passed"] == 1
        rid = done["run_id"]
        tl = c.get(f"/api/runs/{rid}").json()["timeline"]
        assert tl["stage"] == "done" and tl["durations"]["verify_s"] >= 0.04
        assert done["finished_at"] >= tl["verify_end"] - 0.01      # the job ends after its verification
        assert job["id"] == done["id"]
