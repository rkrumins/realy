"""Background job worker: pulls queued jobs from the store and runs them one at a time in a thread.

A job function has the signature `run_fn(job: dict, handle: JobHandle) -> dict | None` (the returned dict is
the job's result). It reports through the handle (steps with progress, per-second metrics, log lines) and
checks `handle.aborted` / `handle.wait()` so an abort stops it at the next checkpoint with a partial result.
`run_fn` is injectable, so tests run jobs synchronously with `worker.execute(job)` and no thread.
"""
from __future__ import annotations

import collections
import threading
import time
import traceback
from typing import Any, Callable

from .store import FINAL, Store

LogSink = Callable[[str, str], None]          # (job_id, line)
MetricSink = Callable[[dict[str, Any]], None]  # sample dict (job_id added)


class JobHandle:
    """What a running job sees and reports through. Thread-safe snapshots for the status ticker."""

    def __init__(self, job_id: str, log: Callable[[str], None], metric: Callable[[dict[str, Any]], None]) -> None:
        self.job_id = job_id
        self.phase = "PREPARE"
        self.planned_s: float | None = None
        self.budget_ms: float | None = None
        self.abort_reason: str | None = None
        self._abort = threading.Event()
        self._log, self._metric = log, metric
        self._lock = threading.Lock()
        self._steps: list[dict[str, Any]] = []

    # ------------------------------------------------------------------ steps
    def define_steps(self, steps: list[tuple[str, str] | tuple[str, str, str | None]]) -> None:
        """Declare the checklist up front (id, label[, component]) so the UI shows what is coming."""
        with self._lock:
            self._steps = [{"id": s[0], "index": i + 1, "label": s[1], "component": s[2] if len(s) > 2 else None,
                            "status": "pending", "detail": "", "duration_s": None, "progress": None, "_t0": None}
                           for i, s in enumerate(steps)]

    def step(self, sid: str, status: str | None = None, detail: str | None = None,
             done: float | None = None, total: float | None = None, unit: str | None = None) -> None:
        """Update a step. Passing done/total computes rate and ETA from the step's start time."""
        now = time.time()
        with self._lock:
            s = next((x for x in self._steps if x["id"] == sid), None)
            if s is None:
                return
            if status:
                if status == "running" and s["_t0"] is None:
                    s["_t0"] = now
                if status in ("done", "failed", "warn", "skipped") and s["_t0"] is not None:
                    s["duration_s"] = round(now - s["_t0"], 2)
                    if status == "done" and s["progress"]:
                        s["progress"] = {**s["progress"], "done": s["progress"]["total"], "eta_s": 0}
                s["status"] = status
            if detail is not None:
                s["detail"] = detail
            if done is not None and total:
                el = max(1e-6, now - (s["_t0"] or now))
                rate = done / el if done else None
                s["progress"] = {"done": done, "total": total, "unit": unit or "", "rate": round(rate, 1) if rate else None,
                                 "eta_s": round((total - done) / rate, 1) if rate else None}

    def steps(self) -> list[dict[str, Any]]:
        """Snapshot without internals; running steps report their duration so far."""
        now = time.time()
        with self._lock:
            out = []
            for s in self._steps:
                d = {k: v for k, v in s.items() if not k.startswith("_")}
                if s["status"] == "running" and s["_t0"] is not None:
                    d["duration_s"] = round(now - s["_t0"], 1)
                out.append(d)
            return out

    # ------------------------------------------------------------------ reporting + control
    def log(self, line: str) -> None:
        self._log(line)

    def metric(self, **values: Any) -> None:
        self._metric({"job_id": self.job_id, **values})

    def request_abort(self, reason: str) -> None:
        self.abort_reason = self.abort_reason or reason
        self._abort.set()

    @property
    def aborted(self) -> bool:
        return self._abort.is_set()

    def wait(self, seconds: float) -> bool:
        """Sleep that wakes up on abort. Returns True when the job should stop."""
        return self._abort.wait(max(0.0, seconds))


class JobWorker:
    def __init__(self, store: Store, run_fn: Callable[[dict[str, Any], JobHandle], dict[str, Any] | None],
                 log_sink: LogSink | None = None, metric_sink: MetricSink | None = None) -> None:
        self.store = store
        self.run_fn = run_fn
        self.log_sink = log_sink or (lambda job_id, line: None)
        self.metric_sink = metric_sink or (lambda sample: None)
        self.wake = threading.Event()
        self.current: dict[str, Any] | None = None
        self.handle: JobHandle | None = None
        self.logs: dict[str, collections.deque] = {}
        self.started_at = time.time()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._idle = threading.Event()
        self._idle.set()

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> "JobWorker":
        self._thread = threading.Thread(target=self._loop, name="job-worker", daemon=True)
        self._thread.start()
        return self

    @property
    def alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def shutdown(self, grace_s: float = 20.0) -> None:
        """Stop taking jobs; abort the running one and give it `grace_s` to stop and save a partial result."""
        self._stop.set()
        self.wake.set()
        if self.handle is not None:
            self.handle.request_abort("service shutting down")
            self._idle.wait(grace_s)

    def recover(self) -> list[str]:
        """Jobs the previous process left running (the store marked them interrupted at startup)."""
        for jid in self.store.interrupted:
            self._log(jid, "[console] marked interrupted: the service restarted while this job was running")
        return list(self.store.interrupted)

    def _log(self, job_id: str, line: str) -> None:
        for part in str(line).splitlines() or [""]:
            self.logs.setdefault(job_id, collections.deque(maxlen=2000)).append(part)
            self.log_sink(job_id, part)

    # ------------------------------------------------------------------ status + control
    def status(self) -> dict[str, Any]:
        cur = None
        if self.current is not None and self.handle is not None:
            h = self.handle
            cur = {"id": self.current["id"], "kind": self.current["kind"], "phase": h.phase,
                   "started_at": self.current.get("started_at"), "planned_s": h.planned_s, "budget_ms": h.budget_ms,
                   "abort_reason": h.abort_reason, "steps": h.steps()}
        return {"current": cur, "queue": self.store.queued()}

    def abort(self, jid: str, reason: str = "aborted by operator") -> str:
        """Abort the running job or cancel a queued one. Returns what happened, in words the UI can show."""
        if self.current is not None and self.current["id"] == jid and self.handle is not None:
            self.handle.request_abort(reason)
            return "abort-signalled"
        job = self.store.job(jid)
        if job is None:
            return "not-found"
        if job["status"] == "queued":
            self.store.update(jid, status="cancelled", abort_reason=reason, finished_at=time.time())
            return "cancelled"
        if job["status"] in FINAL:
            return f"already-{job['status']}"
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
        jid = job["id"]
        handle = JobHandle(jid, log=lambda line: self._log(jid, line), metric=self.metric_sink)
        self._idle.clear()
        started = time.time()
        self.store.update(jid, status="running", started_at=started)
        self.current, self.handle = {**job, "started_at": started}, handle
        try:
            self._log(jid, f"[console] starting {job['kind']} ({jid})")
            result = self.run_fn(job, handle)
            status = "aborted" if handle.aborted else "done"
            self.store.update(jid, status=status, result=result, abort_reason=handle.abort_reason,
                              finished_at=time.time(), steps=handle.steps())
        except Exception as e:  # noqa: BLE001 - a failing job must never kill the worker
            self._log(jid, f"[console] job failed: {e}\n{traceback.format_exc()}")
            self.store.update(jid, status="failed", error=str(e)[:2000], finished_at=time.time(),
                              abort_reason=handle.abort_reason, steps=handle.steps())
        finally:
            self._log(jid, f"[console] finished {jid}")
            self.current, self.handle = None, None
            self._idle.set()
        return self.store.job(jid)  # type: ignore[return-value]
