"""The startup sequence of a run, as structured steps.

Everything a run does before its measured window (world, topics, state provenance, the Flink job,
history replay, feature catch-up, scoring-path prime, warm-up) and after it (drain, report, verify)
is recorded here with a status, timings and progress. The same record feeds three consumers, so they can never disagree:

  * the run log: one consistent line per transition / progress update (`[startup 9/12] ...`);
  * `startup.json` in the run directory, rewritten atomically (throttled): the console reads it for
    any active run, including runs started by the CLI or a Kubernetes Job in another process;
  * the report: the final file becomes the report's "startup" section (time spent per step).

Steps are declared once (STEPS) and are data, not control flow: a runner that skips a step marks it
`skipped` with the reason, so the sequence is always complete and comparable across runs.
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable

STEPS: list[tuple[str, str]] = [
    ("infra", "Infrastructure checked"),
    ("world", "World loaded"),
    ("topics", "Kafka topics"),
    ("profiles", "Profiles seeded"),
    ("provenance", "State provenance"),
    ("config", "Configs applied"),
    ("store", "Online store"),
    ("flink", "Flink job"),
    ("history", "History replay"),
    ("catchup", "Feature catch-up"),
    ("prime", "Scoring-path prime"),
    ("sinks", "Audit sinks caught up"),
    ("warmup", "Warm-up"),
    ("measure", "Measure"),
]
# after the traffic: the same record times them, so a run's whole wall time is accounted for
FINISH_STEPS: list[tuple[str, str]] = [
    ("drain", "Drain"),
    ("report", "Report"),
    ("verify", "Verify"),
]
TERMINAL = ("done", "skipped", "failed", "warn")


def fmt_progress(p: dict[str, Any] | None) -> str:
    if not p or not p.get("total"):
        return ""
    done, total = p.get("done") or 0, p["total"]
    unit = f" {p['unit']}" if p.get("unit") else ""
    out = f"{min(100.0, 100.0 * done / max(total, 1)):.0f}% ({done:,.0f}/{total:,.0f}{unit})"
    if p.get("rate"):
        out += f", {p['rate']:,.0f}/s"
    if p.get("eta_s") is not None:
        out += f", ETA {p['eta_s']:.0f} s"
    return out


class Startup:
    def __init__(self, run_dir: Path | None, log: Callable[[str], None] = print, write_every_s: float = 0.5,
                 log_progress_every_s: float = 5.0) -> None:
        self.path = Path(run_dir) / "startup.json" if run_dir else None
        self.log = log
        self.write_every_s = write_every_s
        self.log_progress_every_s = log_progress_every_s
        self.t0 = time.time()
        self.steps: dict[str, dict[str, Any]] = {}
        for group, steps in (("startup", STEPS), ("finish", FINISH_STEPS)):
            for i, (sid, label) in enumerate(steps):
                self.steps[sid] = {"id": sid, "label": label, "group": group, "index": i + 1, "status": "pending",
                                   "detail": "", "started_at": None, "ended_at": None, "progress": None}
        self._lock = threading.Lock()
        self._last_write = 0.0
        self._last_progress_log: dict[str, float] = {}

    # ---------------------------------------------------------------- transitions
    def start(self, sid: str, detail: str = "") -> None:
        with self._lock:
            s = self.steps[sid]
            s.update(status="running", started_at=s["started_at"] or time.time(), ended_at=None)
            if detail:
                s["detail"] = detail
        self._emit(sid, force=True)

    def progress(self, sid: str, done: float, total: float, unit: str = "", rate: float | None = None,
                 eta_s: float | None = None, detail: str | None = None) -> None:
        with self._lock:
            s = self.steps[sid]
            if s["status"] == "pending":
                s.update(status="running", started_at=time.time())
            s["progress"] = {"done": done, "total": total, "unit": unit, "rate": rate, "eta_s": eta_s}
            if detail is not None:
                s["detail"] = detail
        now = time.time()
        if now - self._last_progress_log.get(sid, 0.0) >= self.log_progress_every_s:
            self._last_progress_log[sid] = now
            self._log_line(sid)
        self._write()

    def finish(self, sid: str, detail: str = "", status: str = "done") -> None:
        assert status in TERMINAL, status
        with self._lock:
            s = self.steps[sid]
            now = time.time()
            s.update(status=status, ended_at=now, started_at=s["started_at"] or now)
            if detail:
                s["detail"] = detail
        self._emit(sid, force=True)

    def skip(self, sid: str, why: str) -> None:
        self.finish(sid, why, "skipped")

    @classmethod
    def resume(cls, run_dir: Path, log: Callable[[str], None] = print) -> "Startup":
        """The record of a run that has already ended its traffic, reopened to time what comes after it (a
        verification run by the console or by `realy verify`, possibly in another process)."""
        st = cls(run_dir, log)
        old = read(run_dir) or {}
        if old.get("started_at"):
            st.t0 = float(old["started_at"])
        for s in old.get("steps") or []:
            if s.get("id") in st.steps:
                keep = {k: s.get(k) for k in ("status", "detail", "started_at", "ended_at", "progress")}
                st.steps[s["id"]].update(keep)
        return st

    def fail_running(self, why: str) -> None:
        """An aborted / failed run: whatever was running did not finish."""
        for sid, s in self.steps.items():
            if s["status"] == "running":
                self.finish(sid, why, "failed")

    # ---------------------------------------------------------------- reads
    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            steps = [dict(s, progress=dict(s["progress"]) if s["progress"] else None) for s in self.steps.values()]
        now = time.time()
        for s in steps:
            if s["started_at"]:
                s["duration_s"] = round((s["ended_at"] or now) - s["started_at"], 3)
        active = next((s for s in steps if s["status"] == "running"), None)
        return {"started_at": self.t0, "updated_at": now, "active": active["id"] if active else None, "steps": steps}

    def write(self) -> None:
        """Write the record now (a step without progress updates otherwise keeps its last written time)."""
        self._write(force=True)

    def summary(self) -> dict[str, float]:
        """Seconds per finished step (report)."""
        return {s["id"]: s["duration_s"] for s in self.snapshot()["steps"] if s.get("duration_s") is not None}

    # ---------------------------------------------------------------- output
    def line(self, sid: str) -> str:
        s = self.steps[sid]
        prog = fmt_progress(s["progress"]) if s["status"] == "running" else ""
        dur = ""
        if s["status"] in TERMINAL and s["started_at"] and s["ended_at"]:
            dur = f" in {s['ended_at'] - s['started_at']:.1f} s"
        state = "" if s["status"] == "running" else f" {s['status']}{dur}"
        text = " · ".join(x for x in (prog, s["detail"]) if x)
        group, n = (("startup", len(STEPS)) if s.get("group", "startup") == "startup" else ("finish", len(FINISH_STEPS)))
        return f"[{group} {s['index']}/{n}] {s['label'].lower()}{state}" + (f": {text}" if text else "")

    def _log_line(self, sid: str) -> None:
        try:
            self.log(self.line(sid))
        except Exception:  # noqa: BLE001 - logging never breaks a run
            pass

    def _emit(self, sid: str, force: bool = False) -> None:
        self._log_line(sid)
        self._write(force)

    def _write(self, force: bool = False) -> None:
        if self.path is None:
            return
        now = time.time()
        if not force and now - self._last_write < self.write_every_s:
            return
        self._last_write = now
        try:
            tmp = self.path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(self.snapshot(), separators=(",", ":")))
            os.replace(tmp, self.path)
        except OSError:
            pass


class timed_step:
    """Time a step of a run that has ended its traffic (verify), in any process: `with timed_step(run_dir,
    "verify") as t: ... t.detail = "PASS"`. A failure marks the step failed and propagates."""

    def __init__(self, run_dir: Path, sid: str, detail: str = "", log: Callable[[str], None] = lambda _l: None) -> None:
        self.st = Startup.resume(Path(run_dir), log)
        self.sid, self.detail, self.status = sid, detail, "done"

    def __enter__(self) -> "timed_step":
        s = self.st.steps[self.sid]
        s.update(started_at=None, ended_at=None)     # a repeated verification is timed afresh
        self.st.start(self.sid, self.detail)
        return self

    def __exit__(self, et, ev, tb) -> bool:
        if et is not None:
            self.st.finish(self.sid, f"failed: {ev}"[:300], "failed")
        else:
            self.st.finish(self.sid, self.detail, self.status)
        return False


def read(run_dir: Path) -> dict[str, Any] | None:
    try:
        return json.loads((Path(run_dir) / "startup.json").read_text())
    except (OSError, ValueError):
        return None
