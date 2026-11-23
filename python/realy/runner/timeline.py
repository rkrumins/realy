"""A run's timeline: when it was launched, prepared, sent traffic, measured, drained, reported and
verified. One function derives it from what the run directory records, so the console, the report, the
CLI and the run history can never disagree about how long anything took.

  launched_at     the run was started (manifest `created_at`)
  traffic_start   live traffic started (the manifest's final `t_start_us`; set when the gates pass)
  measure_start   the warm-up ended and measurement began (the `measure` step)
  emission_end    the last event was sent
  drain_end       the collectors drained the decisions in flight (the `drain` step)
  report_end      the report was built (the `report` step)
  verify_start / verify_end
  finished_at     the last of these

Durations: `prepare_s` = traffic_start - launched_at (everything before traffic, inside a step or not),
`warmup_s`, `measured_s` = emission_end - measure_start, `drain_s`, `report_s`, `verify_s`, `total_s`.
A scenario's `traffic.duration_s` is the whole traffic window: warm-up is inside it, not added to it.

Runs written before the drain / report / verify steps existed fall back to what they did record: the
run's wall time (run.json `wall_s`, the end of its drain) and the files' modification times (report.json,
verify.json), so their times stay honest too.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

STAGES = ("queued", "prepare", "warmup", "measure", "drain", "report", "verify", "done")


def _json(p: Path) -> dict[str, Any]:
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return {}


def _mtime(p: Path) -> float | None:
    try:
        return p.stat().st_mtime
    except OSError:
        return None


def timeline(run_dir: Path, job: dict[str, Any] | None = None, now: float | None = None,
             live: bool | None = None) -> dict[str, Any]:
    """The timeline of the run in `run_dir`. `job` (the console's queue row) adds the queue time and,
    for a job still verifying, that it is. `live` says whether the run is still running (None: inferred
    from run.json / the steps)."""
    run_dir = Path(run_dir)
    now = time.time() if now is None else now
    man = _json(run_dir / "manifest.json")
    run = _json(run_dir / "run.json")
    st = _json(run_dir / "startup.json")
    steps = {s.get("id"): s for s in st.get("steps") or []}

    def at(sid: str, key: str) -> float | None:
        s = steps.get(sid) or {}
        if s.get("status") == "skipped":     # a skipped step took no time and marks no instant
            return None
        v = s.get(key)
        return float(v) if v else None

    def status(sid: str) -> str | None:
        return (steps.get(sid) or {}).get("status")

    launched = man.get("created_at") or st.get("started_at") or (job or {}).get("started_at")
    launched = float(launched) if launched else None
    queued = (job or {}).get("created_at")
    traffic = man["t_start_us"] / 1e6 if man.get("t_start_us") else None
    warmup_cfg = float(man.get("warmup_s") or 0.0)
    duration_cfg = float(man.get("duration_s") or 0.0)
    measure = at("measure", "started_at")
    if measure is None and traffic is not None and run and run.get("emission_end_us"):
        measure = traffic + warmup_cfg       # an older run: no measure step, the configured warm-up
    emission_end = run.get("emission_end_us") / 1e6 if run.get("emission_end_us") else at("measure", "ended_at")
    run_end = launched + float(run["wall_s"]) if launched and run.get("wall_s") is not None and "transitions" not in run \
        else None   # older runs: wall_s was taken before the report, i.e. the end of the drain
    drain_start = at("drain", "started_at") or emission_end
    drain_end = at("drain", "ended_at") or run_end
    report_start = at("report", "started_at") or drain_end
    report_end = at("report", "ended_at")
    # a run recorded before these steps existed (no step, or still pending in its record): file times
    if report_end is None and status("report") in (None, "pending") and run and (run_dir / "report.json").exists():
        report_end = _mtime(run_dir / "report.json")
    verify_start = at("verify", "started_at")
    verify_end = at("verify", "ended_at")
    if verify_end is None and status("verify") in (None, "pending") and run and (run_dir / "verify.json").exists():
        verify_end = _mtime(run_dir / "verify.json")
        verify_start = verify_start or report_end
    report_over = report_end is not None or status("report") in ("done", "warn", "skipped", "failed")
    # verifying: its verify step runs, or a job that asked for it has ended its report and not verified yet
    verifying = status("verify") == "running" or ((job or {}).get("status") == "running" and bool(run) and report_over
                                                    and bool((job or {}).get("verify")) and verify_end is None)
    ended = run and (report_end is not None or status("report") in ("skipped", "failed")) and not verifying
    if live is None:
        live = not ended
    finished = None
    if ended:
        finished = max(x for x in (drain_end, report_end, verify_end, emission_end, launched) if x is not None)
    aborted = bool(run.get("aborted") or run.get("abort"))

    # the stage the run is in (or ended in)
    if not launched:
        stage = "queued"
    elif not live:
        stage = "done"
    elif verifying:
        stage = "verify"
    elif run:
        stage = "report"
    elif drain_start and (emission_end or status("drain") == "running"):
        stage = "drain"
    elif measure is not None:
        stage = "measure"
    elif traffic is not None and now >= traffic:
        stage = "warmup"
    else:
        stage = "prepare"

    def span(a: float | None, b: float | None, running: bool) -> float | None:
        if a is None:
            return None
        end = b if b is not None else (now if running else None)
        return None if end is None else round(max(0.0, end - a), 3)

    prep_end = traffic
    if prep_end is None and not live:
        # aborted before traffic: preparation lasted until the abort (run.json abort.after_s), else to the end
        ab = run.get("abort") or {}
        prep_end = launched + float(ab["after_s"]) if launched and ab.get("after_s") is not None else finished
    durations = {
        "prepare_s": span(launched, prep_end, live and stage == "prepare"),
        "warmup_s": span(traffic, measure if measure is not None else emission_end, live and stage == "warmup"),
        "measured_s": span(measure, emission_end, live and stage == "measure"),
        "drain_s": span(drain_start, drain_end, live and stage == "drain"),
        "report_s": span(report_start, report_end, live and stage == "report") if run else None,
        "verify_s": span(verify_start, verify_end, stage == "verify"),
        "total_s": span(launched, finished, live),
        "queued_s": span(queued, launched, False) if queued and launched else None,
    }
    return {
        "stage": stage, "live": bool(live), "aborted": aborted,
        "queued_at": queued, "launched_at": launched, "traffic_start": traffic, "measure_start": measure,
        "emission_end": emission_end, "drain_end": drain_end, "report_end": report_end,
        "verify_start": verify_start, "verify_end": verify_end, "finished_at": finished,
        "configured": {"duration_s": duration_cfg, "warmup_s": warmup_cfg,
                       "measure_s": max(0.0, duration_cfg - warmup_cfg)},
        "durations": durations,
    }
