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
    """

    def __init__(self, store: Store, runs_dir: Path, scenario_dir: Path, run_fn: Callable | None = None,
                 verify_fn: Callable | None = None, log_sink: LogSink | None = None) -> None:
        self.store = store
        self.runs_dir = runs_dir
        self.scenario_dir = scenario_dir
        self._run_fn = run_fn
        self._verify_fn = verify_fn
        self.log_sink = log_sink or (lambda run_id, line: None)
        self.wake = threading.Event()
        self.current: dict[str, Any] | None = None      # job dict of the running job
        self.handle: Any = None                           # realy.runner.run.RunHandle
        self.scenario: Any = None                         # validated Scenario of the running job
        self.logs: dict[str, collections.deque] = {}
        self._pending_abort: str | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

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

    def shutdown(self) -> None:
        self._stop.set()
        self.wake.set()

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
                   "abort_reason": getattr(h, "abort_reason", None),
                   "started_at": self.current.get("started_at"),
                   "rate_multiplier": float(h.rate_mult.value) if h is not None and h.rate_mult is not None else 1.0,
                   "base_tps": sc.traffic.profile.tps if sc else None,
                   "topology": sc.topology if sc else None,
                   "budget": sc.budget.model_dump() if sc else None,
                   "duration_s": sc.traffic.duration_s if sc else None,
                   "warmup_s": sc.traffic.warmup_s if sc else None}
        return {"current": cur, "queue": self.store.queued()}

    # ------------------------------------------------------------------ control
    def abort(self, ident: str, reason: str = "aborted by operator") -> str:
        """Abort the running job (by job id or run id) or cancel a queued one. Returns what happened."""
        cur = self.current
        if cur is not None and ident in (cur["id"], cur.get("run_id")):
            h = self.handle
            if h is not None:
                h.abort_reason = reason
                if h.stop is not None:
                    h.stop.set()
                else:  # runner has not created its stop event yet: set it as soon as it exists
                    self._pending_abort = reason
            return "abort-signalled"
        job = self.store.job(ident)
        if job is not None and job["status"] == "queued":
            self.store.update(job["id"], status="cancelled", abort_reason=reason, finished_at=time.time())
            return "cancelled"
        return "not-running-here"

    # ------------------------------------------------------------------ main loop
    def _loop(self) -> None:
        while not self._stop.is_set():
            job = self.store.next_queued()
            if job is None:
                self.wake.wait(1.0)
                self.wake.clear()
                continue
            self.execute(job)

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
        self.handle, self.scenario = handle, sc
        self._pending_abort = None
        started = time.time()
        self.store.update(job["id"], status="running", run_id=run_id, started_at=started)
        self.current = {**job, "run_id": run_id, "started_at": started}
        watcher = threading.Thread(target=self._abort_watcher, args=(handle,), daemon=True)
        watcher.start()
        self._log(run_id, f"[console] starting {sc.name} ({sc.topology}) as {run_id}")
        try:
            res = self.run_fn(sc, handle=handle, log=lambda line: self._log(run_id, line), run_id=run_id)
            rep = (res or {}).get("report") or {}
            aborted = (res or {}).get("aborted") or handle.abort_reason
            fields: dict[str, Any] = {"status": "aborted" if aborted else "done", "verdict": rep.get("verdict"),
                                      "abort_reason": aborted, "finished_at": time.time()}
            if job["verify"] and not aborted:
                self._log(run_id, "[console] verifying ...")
                try:
                    v = self.verify_fn(self.runs_dir / run_id)
                    fields["verify_passed"] = int(bool(v.get("passed")))
                    from ..verify.verify import render
                    self._log(run_id, render(v))
                except Exception as e:  # noqa: BLE001
                    self._log(run_id, f"[console] verify failed: {e}")
            self.store.update(job["id"], **fields)
        except Exception as e:  # noqa: BLE001
            self._log(run_id, f"[console] run failed: {e}\n{traceback.format_exc()}")
            self.store.update(job["id"], status="failed", error=str(e)[:2000], finished_at=time.time(),
                              abort_reason=handle.abort_reason)
        finally:
            self._log(run_id, f"[console] finished {run_id}")
            self.current = None
            self.handle = None
            self.scenario = None
        return self.store.job(job["id"])

    def _abort_watcher(self, handle: Any) -> None:
        while self.handle is handle:
            if self._pending_abort and handle.stop is not None:
                handle.abort_reason = self._pending_abort
                handle.stop.set()
                self._pending_abort = None
            time.sleep(0.2)


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
