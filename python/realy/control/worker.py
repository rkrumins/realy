"""Run worker (one run at a time, like a real perf lab) and the schedule ticker for soak tests."""
from __future__ import annotations

import collections
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Callable

from .store import Store

LogSink = Callable[[str, str], None]  # (run_id, line)


class RunWorker:
    """Pulls queued jobs from the store and executes them sequentially with the real runner.

    `run_fn(scenario, handle=..., log=..., run_id=...)` defaults to realy.runner.run.run_scenario and
    `verify_fn(run_dir)` to realy.verify.verify.verify_run; both are injectable for tests.

    One run at a time per pipeline is enforced across processes by the runner's pipeline lease: when
    another console / CLI / bench Job holds it, the next job simply stays queued (`blocked_by`).
    """

    def __init__(self, store: Store, runs_dir: Path, scenario_dir: Path, run_fn: Callable | None = None,
                 verify_fn: Callable | None = None, log_sink: LogSink | None = None,
                 remote_abort: Callable[[str, str], Any] | None = None) -> None:
        self.store = store
        self.runs_dir = runs_dir
        self.scenario_dir = scenario_dir
        self._run_fn = run_fn
        self._verify_fn = verify_fn
        self.log_sink = log_sink or (lambda run_id, line: None)
        self.remote_abort = remote_abort or (lambda run_id, reason: None)
        self.wake = threading.Event()
        self.current: dict[str, Any] | None = None      # job dict of the running job
        self.handle: Any = None                           # realy.runner.run.RunHandle
        self.scenario: Any = None                         # validated Scenario of the running job
        self.logs: dict[str, collections.deque] = {}
        self.blocked_by: dict[str, Any] | None = None     # lease holder keeping the next job queued
        self._blocked_job: str | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._idle = threading.Event()
        self._idle.set()

    # ------------------------------------------------------------------ plumbing
    @property
    def run_fn(self) -> Callable:
        if self._run_fn is None:
            from ..runner.run import run_scenario
            self._run_fn = run_scenario
        return self._run_fn

    @property
    def verify_fn(self) -> Callable:
        if self._verify_fn is None:
            from ..verify.verify import verify_run
            self._verify_fn = verify_run
        return self._verify_fn

    def start(self) -> "RunWorker":
        self._thread = threading.Thread(target=self._loop, name="console-run-worker", daemon=True)
        self._thread.start()
        return self

    def shutdown(self, grace_s: float | None = None) -> None:
        """Stop taking jobs; abort the running one and give it `grace_s` to drain and write its report
        (the console's stop grace period must cover it: compose stop_grace_period, k8s
        terminationGracePeriodSeconds)."""
        import os
        self._stop.set()
        self.wake.set()
        if self.current is not None and self.handle is not None:
            self.handle.request_abort("console shutting down", "interrupted")
            self._idle.wait(grace_s if grace_s is not None else float(os.environ.get("REALY_SHUTDOWN_GRACE_S", "45")))

    def _log(self, run_id: str, line: str) -> None:
        for part in str(line).splitlines() or [""]:
            self.logs.setdefault(run_id, collections.deque(maxlen=2000)).append(part)
            self.log_sink(run_id, part)

    def status(self) -> dict[str, Any]:
        cur = None
        if self.current is not None:
            h = self.handle
            sc = self.scenario
            cur = {"job_id": self.current["id"], "run_id": self.current.get("run_id"),
                   "scenario": self.current["scenario"], "phase": getattr(h, "phase", "PREPARE"),
                   "abort_reason": getattr(h, "abort_reason", None), "abort_kind": getattr(h, "abort_kind", None),
                   "started_at": self.current.get("started_at"),
                   "rate_multiplier": float(h.rate_mult.value) if h is not None and h.rate_mult is not None else 1.0,
                   "base_tps": sc.traffic.profile.tps if sc else None,
                   "topology": sc.topology if sc else None,
                   "budget": sc.budget.model_dump() if sc else None,
                   "duration_s": sc.traffic.duration_s if sc else None,
                   "warmup_s": sc.traffic.warmup_s if sc else None}
        return {"current": cur, "queue": self.store.queued(), "blocked_by": self.blocked_by}

    # ------------------------------------------------------------------ control
    def abort(self, ident: str, reason: str = "aborted by operator") -> str:
        """Abort the running job (by job id or run id) or cancel a queued one. Returns what happened."""
        cur = self.current
        if cur is not None and ident in (cur["id"], cur.get("run_id")):
            h = self.handle
            if h is not None:
                h.request_abort(reason, "operator")
            return "abort-signalled"
        job = self.store.job(ident)
        if job is not None and job["status"] == "queued":
            self.store.update(job["id"], status="cancelled", abort_reason=reason, finished_at=time.time())
            return "cancelled"
        if job is not None and job["status"] in ("done", "failed", "aborted", "cancelled", "interrupted"):
            return f"already-{job['status']}"
        return "not-running-here"

    def abort_many(self, ids: list[str] | None = None, all_: bool = False, include_queued: bool = True,
                   reason: str = "aborted by operator") -> dict[str, str]:
        """Bulk abort. `all_`: the running job plus (include_queued) every queued job. Queued jobs are
        cancelled first so the worker cannot start one of them in between."""
        out: dict[str, str] = {}
        if all_:
            if include_queued:
                for j in self.store.queued():
                    out[j["id"]] = self.abort(j["id"], reason)
            cur = self.current
            if cur is not None:
                out[cur.get("run_id") or cur["id"]] = self.abort(cur["id"], reason)
        for ident in ids or []:
            if ident not in out:
                out[ident] = self.abort(ident, reason)
        return out

    # ------------------------------------------------------------------ recovery
    def recover(self) -> list[str]:
        """Jobs that were running when a previous console process died: tell any surviving run process
        to stop (Redis flag + bench.control), then finish their run directories so history is accurate:
        run.json records the interruption and a partial report is built from whatever is on disk."""
        done = []
        for job in self.store.interrupted:
            run_id = job.get("run_id")
            if not run_id:
                continue
            try:
                self.remote_abort(run_id, "console restarted during run")
            except Exception:  # noqa: BLE001
                pass
            threading.Thread(target=self._finish_interrupted, args=(run_id,), daemon=True).start()
            done.append(run_id)
        return done

    def _finish_interrupted(self, run_id: str) -> None:
        import json
        d = self.runs_dir / run_id
        if not (d / "manifest.json").exists() or (d / "report.json").exists():
            return
        time.sleep(float(__import__("os").environ.get("REALY_RECOVER_DELAY_S", "20")))  # let a live owner finish first
        if (d / "report.json").exists():
            return
        from ..report.compare import run_status, read_json
        status, _ = run_status(d, read_json(d / "manifest.json") or {}, {}, read_json(d / "run.json") or {})
        if status == "running":       # another process still owns it (heartbeat fresh): not ours to finish
            return
        run = read_json(d / "run.json") or {}
        run.setdefault("run_id", run_id)
        run["aborted"] = run.get("aborted") or "console restarted during run"
        run["abort"] = run.get("abort") or {"reason": run["aborted"], "kind": "interrupted", "phase": None}
        (d / "run.json").write_text(json.dumps(run, indent=1, default=str))
        try:
            from ..report.report import build_report
            build_report(d)
            from ..report.history import store
            store(d)
        except Exception as e:  # noqa: BLE001
            self._log(run_id, f"[console] partial report for interrupted run failed: {e}")

    # ------------------------------------------------------------------ main loop
    def _loop(self) -> None:
        while not self._stop.is_set():
            job = self.store.next_queued()
            if job is None:
                self.blocked_by = None
                self.wake.wait(1.0)
                self.wake.clear()
                continue
            self.execute(job)
            if self.blocked_by is not None:
                self.wake.wait(2.0)    # pipeline busy elsewhere: retry the same job shortly
                self.wake.clear()

    def execute(self, job: dict[str, Any]) -> dict[str, Any]:
        from ..runner.run import RunHandle, new_run_id
        from ..scenario import load_scenario
        try:
            sc = load_scenario(self.scenario_dir / f"{job['scenario']}.yaml", job["overrides"] or None)
        except Exception as e:  # noqa: BLE001
            self.store.update(job["id"], status="failed", error=f"invalid scenario: {e}", finished_at=time.time())
            return self.store.job(job["id"])
        run_id = new_run_id(sc)
        n = 1
        while (self.runs_dir / run_id).exists():
            n += 1
            run_id = f"{new_run_id(sc)}-{n}"
        handle = RunHandle(run_id, self.runs_dir / run_id)
        handle.verifiable = bool(job.get("verify"))   # verified runs start from state only they build
        self._idle.clear()
        self.handle, self.scenario = handle, sc
        started = time.time()
        self.store.update(job["id"], status="running", run_id=run_id, started_at=started)
        self.current = {**job, "run_id": run_id, "started_at": started}
        busy = False
        try:
            from ..runner.lifecycle import PipelineBusy
        except Exception:  # noqa: BLE001
            PipelineBusy = ()  # type: ignore[assignment]
        try:
            self._log(run_id, f"[console] starting {sc.name} ({sc.topology}) as {run_id}")
            res = self.run_fn(sc, handle=handle, log=lambda line: self._log(run_id, line), run_id=run_id)
            self.blocked_by = None
            self._blocked_job = None
            rep = (res or {}).get("report") or {}
            aborted = (res or {}).get("aborted") or handle.abort_reason
            fields: dict[str, Any] = {"status": "aborted" if aborted else "done", "verdict": rep.get("verdict"),
                                      "abort_reason": aborted, "finished_at": time.time()}
            if (res or {}).get("exception") and not handle.abort_kind == "operator":
                fields.update(status="failed", error=str(res["exception"])[:2000])
            if job["verify"] and not aborted:
                self._log(run_id, "[console] verifying ...")
                try:
                    v = self.verify_fn(self.runs_dir / run_id)
                    fields["verify_passed"] = int(bool(v.get("passed")))
                    from ..verify.verify import render
                    self._log(run_id, render(v))
                    from ..report.history import store
                    store(self.runs_dir / run_id, log=lambda m: self._log(run_id, m))   # refresh with verify results
                except Exception as e:  # noqa: BLE001
                    self._log(run_id, f"[console] verify failed: {e}")
            self.store.update(job["id"], **fields)
        except PipelineBusy as e:  # type: ignore[misc]
            busy = True
            self.blocked_by = getattr(e, "holder", None) or {"detail": str(e)}
            if self._blocked_job != job["id"]:
                self._log(run_id, f"[console] {e}; job {job['id']} stays queued until the pipeline is free")
                self._blocked_job = job["id"]
            self.store.update(job["id"], status="queued", run_id=None, started_at=None)
        except Exception as e:  # noqa: BLE001
            self._log(run_id, f"[console] run failed: {e}\n{traceback.format_exc()}")
            self.store.update(job["id"], status="failed", error=str(e)[:2000], finished_at=time.time(),
                              abort_reason=handle.abort_reason)
        finally:
            if not busy:
                self._log(run_id, f"[console] finished {run_id}")
            self.current = None
            self.handle = None
            self.scenario = None
            self._idle.set()
        return self.store.job(job["id"])


class Scheduler:
    """Enqueues due schedules (one-shot `at`, or repeating `every_minutes` up to `count`)."""

    def __init__(self, store: Store, worker: RunWorker, interval_s: float = 5.0) -> None:
        self.store = store
        self.worker = worker
        self.interval_s = interval_s
        self._stop = threading.Event()

    def tick(self, now: float | None = None) -> list[str]:
        now = time.time() if now is None else now
        enq = []
        for s in self.store.due_schedules(now):
            job = self.store.enqueue(s["scenario"], s["overrides"], s["verify"], schedule_id=s["id"])
            enq.append(job["id"])
            remaining = s["remaining"] - 1 if s["remaining"] is not None else None
            if s["every_minutes"] and (remaining is None or remaining > 0):
                nxt = s["next_at"] + s["every_minutes"] * 60
                while nxt <= now:  # skip missed slots rather than bursting
                    nxt += s["every_minutes"] * 60
                self.store.update_schedule(s["id"], next_at=nxt, remaining=remaining, last_job=job["id"])
            else:
                self.store.update_schedule(s["id"], enabled=0, remaining=0 if remaining is not None else None,
                                           last_job=job["id"])
        if enq:
            self.worker.wake.set()
        return enq

    def start(self) -> "Scheduler":
        def loop() -> None:
            while not self._stop.is_set():
                try:
                    self.tick()
                except Exception:  # noqa: BLE001
                    traceback.print_exc()
                self._stop.wait(self.interval_s)
        threading.Thread(target=loop, name="console-scheduler", daemon=True).start()
        return self

    def shutdown(self) -> None:
        self._stop.set()
