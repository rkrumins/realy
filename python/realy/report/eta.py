"""When will a run (or a queued job) finish? An estimate from the runs that came before it.

Each step's duration is taken from recent finished runs of the same scenario and topology (else the
same scenario, else every run), as the median. The traffic window is known exactly from the scenario
(`traffic.duration_s`, warm-up included). A running step's remaining time is the larger of its own
progress estimate and (median - elapsed), never negative; a step past its usual p75 is reported as
`late` instead of pretending to know. The basis says how many runs the estimate rests on.
"""
from __future__ import annotations

import json
import statistics
import time
from pathlib import Path
from typing import Any

from ..runner.startup import FINISH_STEPS, STEPS

# steps whose time is not learned from history: the traffic window is configured exactly
TRAFFIC = ("warmup", "measure")
DEFAULTS = {"drain": 5.0, "report": 10.0, "verify": 60.0}
_CACHE: dict[str, Any] = {"at": 0.0, "key": None, "rows": []}


def _json(p: Path) -> dict[str, Any]:
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return {}


def _history(runs_dir: Path, limit: int = 60) -> list[dict[str, Any]]:
    """(scenario, topology, step durations) of the most recent finished runs, cached for 30 s."""
    now = time.time()
    if _CACHE["key"] == str(runs_dir) and now - _CACHE["at"] < 30:
        return _CACHE["rows"]
    rows = []
    try:
        dirs = sorted((d for d in runs_dir.iterdir() if (d / "report.json").exists()),
                      key=lambda d: (d / "report.json").stat().st_mtime, reverse=True)[:limit]
    except OSError:
        dirs = []
    for d in dirs:
        man, st = _json(d / "manifest.json"), _json(d / "startup.json")
        if not st:
            continue
        durs = {}
        for s in st.get("steps") or []:
            if s.get("status") in ("done", "warn", "skipped") and s.get("started_at") and s.get("ended_at"):
                durs[s["id"]] = max(0.0, float(s["ended_at"]) - float(s["started_at"]))
            elif s.get("status") == "skipped":
                durs[s["id"]] = 0.0
        rows.append({"scenario": man.get("scenario"), "topology": man.get("topology"), "steps": durs})
    _CACHE.update(at=now, key=str(runs_dir), rows=rows)
    return rows


def _pick(rows: list[dict[str, Any]], scenario: str | None, topology: str | None) -> tuple[list[dict[str, Any]], str]:
    same = [r for r in rows if r["scenario"] == scenario and r["topology"] == topology]
    if len(same) >= 1:
        return same[:5], f"{len(same[:5])} earlier {scenario} run(s)"
    scen = [r for r in rows if r["scenario"] == scenario]
    if scen:
        return scen[:5], f"{len(scen[:5])} earlier {scenario} run(s) on other topologies"
    return rows[:20], (f"the median of {len(rows[:20])} recent runs of other scenarios" if rows else "no earlier runs")


def _stats(rows: list[dict[str, Any]], sid: str) -> tuple[float | None, float | None]:
    vals = sorted(r["steps"][sid] for r in rows if sid in r["steps"])
    if not vals:
        return None, None
    p75 = vals[min(len(vals) - 1, int(round(0.75 * (len(vals) - 1))))]
    return statistics.median(vals), p75


def estimate(run_dir: Path, runs_dir: Path, verify: bool, now: float | None = None) -> dict[str, Any]:
    """Remaining seconds and finish time of the run in `run_dir`, per step."""
    now = time.time() if now is None else now
    man, st = _json(run_dir / "manifest.json"), _json(run_dir / "startup.json")
    rows, basis = _pick(_history(runs_dir), man.get("scenario"), man.get("topology"))
    duration = float(man.get("duration_s") or 0.0)
    warmup = float(man.get("warmup_s") or 0.0)
    traffic_start = man["t_start_us"] / 1e6 if man.get("t_start_us") else None
    steps = {s["id"]: s for s in st.get("steps") or []}
    per: dict[str, float] = {}
    late: list[str] = []
    order = [sid for sid, _ in STEPS] + [sid for sid, _ in FINISH_STEPS]
    for sid in order:
        if sid == "verify" and not verify:
            continue
        s = steps.get(sid) or {"status": "pending"}
        stt = s.get("status")
        if stt in ("done", "warn", "skipped", "failed"):
            continue
        if sid in TRAFFIC:
            if traffic_start is None:
                per[sid] = warmup if sid == "warmup" else max(0.0, duration - warmup)
            else:
                end = traffic_start + (warmup if sid == "warmup" else duration)
                per[sid] = max(0.0, end - now)
            continue
        med, p75 = _stats(rows, sid)
        if med is None:
            med = DEFAULTS.get(sid, 0.0)
        if stt == "running":
            el = now - float(s.get("started_at") or now)
            own = ((s.get("progress") or {}).get("eta_s"))
            rem = max(float(own) if own is not None else 0.0, med - el, 0.0)
            if p75 is not None and el > p75 and own is None:
                late.append(sid)
            per[sid] = rem
        else:
            per[sid] = med
    remaining = sum(per.values())
    return {"remaining_s": round(remaining, 1), "finish_at": now + remaining, "basis": basis,
            "per_step": {k: round(v, 1) for k, v in per.items() if v > 0.05}, "late": late}


def scenario_estimate(runs_dir: Path, scenario: str, topology: str | None, duration_s: float, verify: bool) -> dict[str, Any]:
    """Expected wall time of a run of `scenario` not started yet (a queued job, the Launch page)."""
    rows, basis = _pick(_history(runs_dir), scenario, topology)
    total = float(duration_s or 0.0)
    for sid, _ in STEPS + FINISH_STEPS:
        if sid in TRAFFIC or (sid == "verify" and not verify):
            continue
        med, _ = _stats(rows, sid)
        total += med if med is not None else DEFAULTS.get(sid, 0.0)
    return {"total_s": round(total, 1), "basis": basis}
