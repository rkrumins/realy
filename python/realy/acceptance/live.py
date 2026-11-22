"""Live acceptance suite: drives a running stack through the console API the way an operator would,
and checks that runs are functional, reported accurately, abortable from anywhere, and that the
pipeline is healthy again after every failure.

    pulsebench live-test                          # every in-process case (~15 min on a laptop)
    pulsebench live-test --case abort-measure     # one case (repeatable)
    pulsebench live-test fault-start / fault-check  # host-orchestrated faults (deploy/compose/live-test.sh)

It runs inside the console container (`make live-test`) or anywhere with network access to the
console, Redis, Kafka and ClickHouse. Cases that inspect or signal the driver's processes
(instrument failure, signals, orphan check) need the run directories and the process table of the
console, i.e. the console container; elsewhere they are SKIPped.

Every case ends with a cleanup that aborts whatever is left and waits until the pipeline is free, so
one failing case cannot cascade into the next.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import httpx

SHORT = {"traffic.duration_s": 40, "traffic.warmup_s": 5, "traffic.profile.tps": 300}
LONG = {"traffic.duration_s": 600, "traffic.warmup_s": 5, "traffic.profile.tps": 300}
ABORT_SLA_S = 60.0        # a requested abort must end the run (drain + partial report) within this


def _nest(flat: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in flat.items():
        cur = out
        parts = k.split(".")
        for p in parts[:-1]:
            cur = cur.setdefault(p, {})
        cur[parts[-1]] = v
    return out


@dataclass
class Row:
    case: str
    check: str
    status: str            # PASS | FAIL | WARN (environment-dependent, does not fail the suite) | SKIP
    detail: str = ""
    seconds: float = 0.0


@dataclass
class Suite:
    console: str = "http://localhost:8090"
    clickhouse: str = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")
    scenario: str = "smoke"
    runs_dir: Path = Path(os.environ.get("REALY_RUNS", "/runs"))
    log: Callable[[str], None] = print
    rows: list[Row] = field(default_factory=list)

    def __post_init__(self) -> None:
        tok = os.environ.get("REALY_CONSOLE_TOKEN")
        self.c = httpx.Client(base_url=self.console.rstrip("/"), timeout=30,
                              headers={"Authorization": f"Bearer {tok}"} if tok else {})
        self._case = ""
        self._t0 = time.time()

    # ------------------------------------------------------------------ recording
    def check(self, name: str, ok: bool | None, detail: str = "", soft: bool = False) -> bool:
        """soft: a property of the hardware rather than of the system's correctness (e.g. a latency
        budget on a CPU-starved laptop): WARN instead of FAIL. Budgets are gated by `pulsebench gate`."""
        st = "SKIP" if ok is None else ("PASS" if ok else ("WARN" if soft else "FAIL"))
        self.rows.append(Row(self._case, name, st, detail, round(time.time() - self._t0, 1)))
        self.log(f"  {st:4s} {self._case} / {name}  {detail}")
        return bool(ok)

    # ------------------------------------------------------------------ console helpers
    def get(self, path: str, **kw) -> Any:
        r = self.c.get(path, **kw)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, body: Any = None, **kw) -> Any:
        r = self.c.post(path, json=body, **kw)
        r.raise_for_status()
        return r.json()

    def launch(self, overrides: dict[str, Any], verify: bool = False, scenario: str | None = None) -> dict:
        return self.post("/api/runs", {"scenario": scenario or self.scenario, "overrides": _nest(overrides), "verify": verify})

    def job(self, jid: str) -> dict:
        for j in self.get("/api/runs", params={"limit": 50})["jobs"]:
            if j["id"] == jid:
                return j
        return {}

    def wait(self, pred: Callable[[], Any], timeout: float, every: float = 1.0) -> Any:
        end = time.time() + timeout
        while time.time() < end:
            try:
                v = pred()
            except httpx.HTTPError:
                v = None
            if v:
                return v
            time.sleep(every)
        return None

    def wait_phase(self, jid: str, phases: tuple[str, ...], timeout: float = 240) -> dict | None:
        """The job's status once it is in one of `phases`; None on timeout or when the job ends first
        (then `self.not_reached` says how it ended, instead of waiting out the timeout)."""
        self.not_reached = "timeout"
        ended: dict = {}

        def f():
            cur = self.get("/api/status")["current"]
            if cur and cur["job_id"] == jid:
                return cur if cur["phase"] in phases else None
            j = self.job(jid)
            if j.get("status") in ("done", "aborted", "failed", "cancelled", "interrupted"):
                ended.update(j)
                return True
            return None
        v = self.wait(f, timeout)
        if ended:
            self.not_reached = (f"job ended {ended.get('status')} first: "
                                f"{(ended.get('abort_reason') or ended.get('error') or '')[:200]}")
            return None
        return v

    def wait_job_end(self, jid: str, timeout: float) -> dict | None:
        return self.wait(lambda: (lambda j: j if j.get("status") in ("done", "aborted", "failed", "cancelled",
                                                                     "interrupted") else None)(self.job(jid)), timeout)

    def run(self, run_id: str) -> dict:
        return self.get(f"/api/runs/{run_id}")

    def lease(self) -> dict | None:
        return self.get("/api/active").get("lease")

    def idle(self, timeout: float = 120) -> bool:
        def f():
            a = self.get("/api/active")
            return not a["local"] and not a["queue"] and not a["lease"]
        return bool(self.wait(f, timeout))

    def cleanup(self) -> None:
        try:
            a = self.get("/api/active")
            if a["local"] or a["queue"] or a["lease"]:
                self.post("/api/stop-all", {"reason": "live-test cleanup", "pause_schedules": False})
            for s in self.get("/api/schedules"):
                if s.get("scenario") == self.scenario and "live-test" in json.dumps(s.get("overrides") or {}):
                    self.c.delete(f"/api/schedules/{s['id']}")
            if not self.idle(150):
                self.log("  warn: pipeline still busy after cleanup")
        except httpx.HTTPError as e:
            self.log(f"  warn: cleanup failed: {e}")

    # ------------------------------------------------------------------ evidence helpers
    def ch(self, sql: str) -> str | None:
        try:
            from ..common import clickhouse as chc
            r = chc.post("", params={"query": sql}, timeout=15, base=self.clickhouse)
            return r.text.strip() if r.status_code == 200 else None
        except httpx.HTTPError:
            return None

    def ch_decisions(self, run_id: str, expect: int, timeout: float = 60) -> int | None:
        """Distinct decisions ClickHouse holds for the run (ingest is asynchronous: wait for it)."""
        last = None
        end = time.time() + timeout
        while time.time() < end:
            v = self.ch(f"SELECT uniqExact(event_id) FROM realy.decisions WHERE run_id = '{run_id}' AND revision = 0")
            last = int(v) if v and v.isdigit() else last
            if last is not None and last >= expect:
                return last
            time.sleep(3)
        return last

    def children_alive(self, run_id: str) -> list[int] | None:
        p = self.runs_dir / run_id / "pids.json"
        if not p.exists():
            return None
        pids = [c["pid"] for c in json.loads(p.read_text())["children"]]
        return [pid for pid in pids if _alive(pid)]

    def report_summary(self, run_id: str) -> dict:
        return ((self.run(run_id).get("report") or {}).get("summary")) or {}

    def local(self) -> bool:
        return self.runs_dir.is_dir() and os.access(self.runs_dir, os.R_OK)

    # ================================================================== cases
    def case_preflight(self) -> None:
        h = self.get("/api/health")
        self.check("console up, Kafka connected", h.get("ok") and h.get("kafka"), json.dumps(h))
        a = self.get("/api/active")
        self.check("pipeline free", not a["local"] and not a["lease"] and not a["queue"],
                   "busy: " + json.dumps({k: a[k] for k in ("local", "lease")}, default=str)[:200] if a["lease"] or a["local"] else "")
        env = self.get("/api/env")
        down = [c.get("name") for c in env.get("components", []) if c.get("status") == "down"]
        degraded = [c.get("name") for c in env.get("components", []) if c.get("status") == "degraded"]
        self.check("components up", not down, f"down: {down}; degraded: {degraded}" if down or degraded else "")

    def case_full_run(self) -> None:
        """A complete run: live telemetry over the WebSocket, a live rate change that takes effect, and a
        report whose counts agree with the independent ClickHouse audit and the run-history table."""
        j = self.launch(SHORT, verify=True)
        ws_msgs: dict[str, int] = {}
        stop = threading.Event()
        t = threading.Thread(target=_ws_listen, args=(self.console, ws_msgs, stop), daemon=True)
        t.start()
        cur = self.wait_phase(j["id"], ("MEASURE",), 300)
        self.check("reaches MEASURE", bool(cur), "" if cur else self.not_reached)
        if not cur:
            stop.set()
            return
        run_id = cur["run_id"]
        base = SHORT["traffic.profile.tps"]
        self.post("/api/live/rate", {"tps": base * 2})
        rate = self.wait(lambda: (lambda v: v if v and v >= base * 1.7 else None)(_emitter_rate(self, run_id)), 20, 1)
        self.check("live rate change takes effect", bool(rate), f"emitter {rate or _emitter_rate(self, run_id) or 0:.0f}/s after asking for {base * 2}/s")
        end = self.wait_job_end(j["id"], 400)
        stop.set()
        self.check("WebSocket streams status, metrics and logs",
                   all(ws_msgs.get(k, 0) > 0 for k in ("status", "metrics", "log")), json.dumps(ws_msgs))
        if not end:
            self.check("run completes", False, "timeout")
            return
        s = self.report_summary(run_id)
        self.check("run completes with a report", end["status"] == "done" and s.get("verdict") in ("PASS", "FAIL"),
                   f"status {end['status']}, verdict {s.get('verdict')}: {'; '.join(s.get('reasons') or [])[:200]}")
        self.check("latency budget met on this host", s.get("verdict") == "PASS", "; ".join(s.get("reasons") or [])[:200],
                   soft=True)
        self.check("zero loss", s.get("missing") == 0 and s.get("decided") == s.get("emitted") and s.get("emitted", 0) > 0,
                   f"emitted {s.get('emitted')}, decided {s.get('decided')}, missing {s.get('missing')}")
        n = self.ch_decisions(run_id, int(s.get("decided") or 0))
        self.check("ClickHouse audit agrees with the report", n == s.get("decided"), f"ClickHouse {n}, report {s.get('decided')}")
        self.check("verification passed", end.get("verify_passed") == 1, f"verify_passed={end.get('verify_passed')}")
        hist = self.ch(f"SELECT verdict FROM realy.runs FINAL WHERE run_id = '{run_id}' FORMAT TSV")
        self.check("run history row in ClickHouse", hist == s.get("verdict"), f"realy.runs: {hist!r}")
        self.check("lease released", self.wait(lambda: self.lease() is None, 20) is not None)

    def case_abort_measure(self) -> None:
        self._abort_case(LONG, ("MEASURE",), "MEASURE")

    def case_abort_backfill(self) -> None:
        self._abort_case({**LONG, "traffic.backfill_days": 3}, ("BACKFILL",), "BACKFILL")

    def _abort_case(self, overrides: dict, phases: tuple[str, ...], expect_phase: str) -> None:
        j = self.launch(overrides)
        cur = self.wait_phase(j["id"], phases, 300)
        self.check(f"reaches {expect_phase}", bool(cur), "" if cur else self.not_reached)
        if not cur:
            return
        run_id = cur["run_id"]
        t0 = time.time()
        r = self.post(f"/api/runs/{j['id']}/abort")
        self.check("abort accepted", r.get("result") == "abort-signalled", json.dumps(r))
        end = self.wait_job_end(j["id"], ABORT_SLA_S + 30)
        took = time.time() - t0
        self.check(f"run ends within {ABORT_SLA_S:.0f} s", bool(end) and took <= ABORT_SLA_S, f"{took:.1f} s")
        if not end:
            return
        s = self.report_summary(run_id)
        ab = s.get("aborted") or {}
        self.check("verdict ABORTED with who/where", s.get("verdict") == "ABORTED" and ab.get("kind") == "operator"
                   and ab.get("phase") == expect_phase, json.dumps(ab))
        if expect_phase == "MEASURE":
            ms = s.get("measured_s") or 0
            self.check("report covers the window actually measured", 0 < ms < LONG["traffic.duration_s"],
                       f"measured {ms:.1f} s of {LONG['traffic.duration_s']} s; achieved {s.get('achieved_emit_tps')} TPS")
            n = self.ch_decisions(run_id, int(s.get("decided") or 0), 45)
            self.check("audit holds every reported decision", n is not None and n >= (s.get("decided") or 0),
                       f"ClickHouse {n} >= report {s.get('decided')} (late decisions of in-flight events land after the drain)")
        alive = self.children_alive(run_id)
        self.check("all driver processes reaped", None if alive is None else not alive, f"alive: {alive}")
        self.check("lease released", self.wait(lambda: self.lease() is None, 20) is not None)

    def case_guardrail(self) -> None:
        j = self.launch({**SHORT, "guardrails.max_schedule_lag_ms": 0.0001})
        end = self.wait_job_end(j["id"], 300)
        if not self.check("run ends by itself", bool(end)):
            return
        s = self.report_summary(end["run_id"])
        ab = s.get("aborted") or {}
        self.check("guardrail abort is a FAIL verdict", end["status"] == "aborted" and s.get("verdict") == "FAIL"
                   and ab.get("kind") == "guardrail", f"{s.get('verdict')}: {ab.get('reason')}")

    def case_instrument_failure(self) -> None:
        if not self.local():
            self.check("collector killed -> instrument abort", None, "needs the console's process table")
            return
        j = self.launch(LONG)
        cur = self.wait_phase(j["id"], ("MEASURE",), 300)
        if not self.check("reaches MEASURE", bool(cur), "" if cur else self.not_reached):
            return
        pids = json.loads((self.runs_dir / cur["run_id"] / "pids.json").read_text())["children"]
        coll = [p["pid"] for p in pids if p["role"] == "collector"][0]
        os.kill(coll, signal.SIGKILL)
        t0 = time.time()
        end = self.wait_job_end(j["id"], ABORT_SLA_S + 30)
        took = time.time() - t0
        s = self.report_summary(cur["run_id"]) if end else {}
        ab = s.get("aborted") or {}
        self.check("dead collector aborts the run", bool(end) and ab.get("kind") == "instrument"
                   and "collector 0" in (ab.get("reason") or ""), f"{took:.1f} s: {ab.get('reason')}")
        self.check("verdict ABORTED, never a silent PASS", s.get("verdict") == "ABORTED", str(s.get("verdict")))

    def case_queue_control(self) -> None:
        """Abort / cancel one, several, all: select-cancel a queued job, clear the queue, abort-all."""
        jobs = [self.launch(LONG) for _ in range(4)]
        first = self.wait_phase(jobs[0]["id"], ("PREPARE", "BACKFILL", "WARMUP", "MEASURE"), 120)
        self.check("first job running, others queued", bool(first))
        r = self.post("/api/runs/abort", {"ids": [jobs[3]["id"]], "reason": "live-test select"})["results"]
        self.check("cancel a selected queued job", r.get(jobs[3]["id"], {}).get("result") == "cancelled"
                   and self.job(jobs[1]["id"]).get("status") == "queued", json.dumps(r))
        r = self.post("/api/queue/clear")
        self.check("cancel all queued", sorted(r["cancelled"]) == sorted([jobs[1]["id"], jobs[2]["id"]]), json.dumps(r))
        self.wait_phase(jobs[0]["id"], ("WARMUP", "MEASURE"), 240)
        t0 = time.time()
        r = self.post("/api/runs/abort", {"all": True, "reason": "live-test abort all"})
        end = self.wait_job_end(jobs[0]["id"], ABORT_SLA_S + 30)
        self.check("abort all ends the running run", bool(end) and end["status"] == "aborted",
                   f"{time.time() - t0:.1f} s: {json.dumps(r)[:160]}")
        st = [self.job(x["id"]).get("status") for x in jobs]
        self.check("every job accounted for", st == ["aborted", "cancelled", "cancelled", "cancelled"], str(st))

    def case_stop_all(self) -> None:
        at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        s = self.post("/api/schedules", {"scenario": self.scenario, "overrides": _nest({**LONG, "description": "live-test"}),
                                         "every_minutes": 60, "at": at})
        j2 = self.launch(LONG)
        running = self.wait(lambda: (lambda c: c if c and c["phase"] in ("WARMUP", "MEASURE") else None)(
            self.get("/api/status")["current"]), 240)
        self.check("schedule fired and a run is live", bool(running))
        t0 = time.time()
        r = self.post("/api/stop-all", {"reason": "live-test stop all"})
        ok = self.wait(lambda: (lambda a: not a["local"] and not a["queue"] and not a["lease"])(self.get("/api/active")),
                       ABORT_SLA_S + 30)
        sched = [x for x in self.get("/api/schedules") if x["id"] == s["id"]]
        self.check("stop all: nothing running or queued", bool(ok), f"{time.time() - t0:.1f} s")
        self.check("stop all: schedules paused", bool(sched) and not sched[0]["enabled"], json.dumps(r)[:200])
        self.check("stop all: queued run cancelled", self.job(j2["id"]).get("status") in ("cancelled", "aborted"),
                   self.job(j2["id"]).get("status", "?"))
        self.c.delete(f"/api/schedules/{s['id']}")

    def case_cross_process(self) -> None:
        """The lease keeps two runs off one pipeline whoever started them, and an abort reaches a run in
        any process: CLI refused while the console runs; the console aborts a CLI run; the CLI aborts
        a console run; a console job waits while a CLI run holds the pipeline."""
        j = self.launch(LONG)
        cur = self.wait_phase(j["id"], ("WARMUP", "MEASURE"), 300)
        if not self.check("console run live", bool(cur)):
            return
        p = _cli(["run", self.scenario, "--set", "traffic.duration_s=10"], timeout=60)
        self.check("second run (CLI) refused: pipeline busy", p.returncode == 3 and "busy" in p.stderr,
                   f"exit {p.returncode}: {p.stderr.strip().splitlines()[-1:] or ''}")
        t0 = time.time()
        p = _cli(["abort", cur["run_id"], "--reason", "live-test cli abort"], timeout=30)
        end = self.wait_job_end(j["id"], ABORT_SLA_S + 30)
        self.check("CLI aborts the console's run", bool(end) and end["status"] == "aborted",
                   f"{time.time() - t0:.1f} s; {p.stdout.strip()[:160]}")
        self.idle(90)
        # a CLI run holds the pipeline; a console job waits for it; the console aborts the CLI run
        proc = subprocess.Popen([sys.executable, "-m", "realy.cli", "run", self.scenario,
                                 *sum((["--set", f"{k}={v}"] for k, v in LONG.items()), [])],
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        h = self.wait(lambda: (lambda x: x if x and x.get("phase") in ("WARMUP", "MEASURE") else None)(self.lease()), 300)
        if not self.check("CLI run holds the lease", bool(h)):
            proc.kill()
            return
        j2 = self.launch(SHORT)
        blocked = self.wait(lambda: (lambda r: r.get("blocked_by") if (r.get("blocked_by") or {}).get("run_id") == h["run_id"] else None)(
            self.get("/api/runs", params={"limit": 5})), 20)
        self.check("console job waits for the pipeline", bool(blocked) and self.job(j2["id"]).get("status") == "queued",
                   f"blocked_by {(blocked or {}).get('run_id')}")
        t0 = time.time()
        r = self.post(f"/api/runs/{h['run_id']}/abort")
        try:
            rc = proc.wait(timeout=ABORT_SLA_S + 30)
        except subprocess.TimeoutExpired:
            proc.kill()
            rc = None
        self.check("console aborts a CLI run", rc == 2, f"exit {rc} after {time.time() - t0:.1f} s; {json.dumps(r)[:160]}")
        st = self.wait(lambda: (lambda x: x if x.get("status") != "queued" else None)(self.job(j2["id"])), 60)
        self.check("queued job starts once the pipeline frees", bool(st), (st or {}).get("status", "still queued"))
        self.post(f"/api/runs/{j2['id']}/abort")

    def case_signal(self) -> None:
        if not self.local():
            self.check("SIGTERM -> graceful abort", None, "needs the run directories")
            return
        proc = subprocess.Popen([sys.executable, "-m", "realy.cli", "run", self.scenario,
                                 *sum((["--set", f"{k}={v}"] for k, v in LONG.items()), [])],
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        h = self.wait(lambda: (lambda x: x if x and x.get("phase") == "MEASURE" else None)(self.lease()), 300)
        if not self.check("CLI run reaches MEASURE", bool(h)):
            proc.kill()
            return
        t0 = time.time()
        proc.send_signal(signal.SIGTERM)
        try:
            rc = proc.wait(timeout=ABORT_SLA_S + 30)
        except subprocess.TimeoutExpired:
            proc.kill()
            rc = None
        rep = _read_json(self.runs_dir / h["run_id"] / "report.json") or {}
        ab = (rep.get("summary") or {}).get("aborted") or {}
        self.check("SIGTERM: graceful abort with a partial report", rc == 2 and ab.get("kind") == "signal",
                   f"exit {rc} after {time.time() - t0:.1f} s; {ab.get('reason')}")
        alive = self.children_alive(h["run_id"])
        self.check("SIGTERM: no orphaned children", not alive, f"alive: {alive}")

    def case_final_health(self) -> None:
        """After every failure above, the pipeline must still produce a clean PASS."""
        j = self.launch(SHORT)
        end = self.wait_job_end(j["id"], 400)
        s = self.report_summary(end["run_id"]) if end else {}
        self.check("pipeline healthy afterwards: complete report, zero loss",
                   s.get("verdict") in ("PASS", "FAIL") and s.get("missing") == 0 and (s.get("emitted") or 0) > 0,
                   f"emitted {s.get('emitted')}, missing {s.get('missing')}")
        self.check("latency budget met afterwards on this host", s.get("verdict") == "PASS",
                   f"{s.get('verdict')}: {'; '.join(s.get('reasons') or [])[:200]}", soft=True)
        orphans = _orphans() if self.local() else None
        self.check("no orphaned driver processes", None if orphans is None else not orphans, f"{orphans}")

    CASES = ("preflight", "full_run", "abort_measure", "abort_backfill", "guardrail", "instrument_failure",
             "queue_control", "stop_all", "cross_process", "signal", "final_health")

    def run_cases(self, names: list[str] | None = None) -> bool:
        names = [n.replace("-", "_") for n in (names or self.CASES)]
        for n in names:
            self._case = n
            self.log(f"== {n}")
            try:
                getattr(self, f"case_{n}")()
            except Exception as e:  # noqa: BLE001
                self.check("case raised", False, f"{type(e).__name__}: {e}")
            if n != "preflight":
                self.cleanup()
            if n == "preflight" and any(r.status == "FAIL" for r in self.rows):
                self.log("preflight failed: not starting runs")
                break
        return not any(r.status == "FAIL" for r in self.rows)

    # ------------------------------------------------------------------ host-orchestrated faults
    def fault_start(self, duration_s: float = 90) -> str:
        j = self.launch({**SHORT, "traffic.duration_s": duration_s, "guardrails.max_schedule_lag_ms": 60_000,
                         "guardrails.max_consumer_lag": 10_000_000, "guardrails.max_error_rate": 1.0})
        cur = self.wait_phase(j["id"], ("MEASURE",), 300)
        if not cur:
            raise SystemExit("run did not reach MEASURE")
        time.sleep(5)
        return f"{j['id']} {cur['run_id']}"

    def fault_check(self, job_id: str, run_id: str, expect: str) -> bool:
        """expect: zero-loss (a pipeline component restarted mid-run) | aborted (console stopped
        gracefully) | interrupted (console killed)."""
        self._case = f"fault:{expect}"
        end = self.wait(lambda: (lambda j: j if j.get("status") not in ("queued", "running", None) else None)(self.job(job_id)), 600, 2)
        if not self.check("job finished", bool(end)):
            return False
        if expect == "zero-loss":
            s = self.report_summary(run_id)
            self.check("zero loss across the restart", s.get("missing") == 0 and (s.get("emitted") or 0) > 0,
                       f"emitted {s.get('emitted')}, decided {s.get('decided')}, missing {s.get('missing')}, dups {s.get('duplicates')}")
            n = self.ch_decisions(run_id, int(s.get("decided") or 0))
            self.check("ClickHouse audit complete", n is not None and n >= (s.get("decided") or 0), f"ClickHouse {n}")
        else:
            want = {"aborted": ("aborted",), "interrupted": ("interrupted", "aborted")}[expect]
            self.check(f"job {'/'.join(want)}", end["status"] in want, f"{end['status']}: {end.get('abort_reason')}")
            rep = self.wait(lambda: (self.run(run_id).get("report") or None), 120, 3)
            s = (rep or {}).get("summary") or {}
            self.check("partial report written (ABORTED)", s.get("verdict") == "ABORTED", f"{s.get('verdict')}: {(s.get('aborted') or {}).get('reason')}")
        self.check("pipeline free again", self.idle(90))
        return not any(r.status == "FAIL" for r in self.rows)

    # ------------------------------------------------------------------ output
    def render(self) -> str:
        lines = ["", f"{'status':6s} {'case':20s} check", "-" * 100]
        for r in self.rows:
            lines.append(f"{r.status:6s} {r.case:20s} {r.check}" + (f"  [{r.detail}]" if r.detail else ""))
        n = {s: sum(r.status == s for r in self.rows) for s in ("PASS", "FAIL", "WARN", "SKIP")}
        lines.append(f"\n{n['PASS']} passed, {n['FAIL']} failed, {n['WARN']} warnings (host-dependent), "
                     f"{n['SKIP']} skipped in {time.time() - self._t0:.0f} s")
        return "\n".join(lines)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"console": self.console, "at": time.time(),
                                    "passed": not any(r.status == "FAIL" for r in self.rows),
                                    "rows": [r.__dict__ for r in self.rows]}, indent=1))


# ---------------------------------------------------------------------------------------- utilities
def _alive(pid: int) -> bool:
    try:
        st = Path(f"/proc/{pid}/stat").read_text()
        return st.split(") ", 1)[1][0] != "Z"
    except (OSError, IndexError):
        return False


def _orphans() -> list[int]:
    """Driver children (multiprocessing spawn workers) whose run is over: their parent is gone."""
    out = []
    for d in Path("/proc").iterdir():
        if not d.name.isdigit():
            continue
        try:
            cmd = (d / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
            ppid = int((d / "stat").read_text().split(") ", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            continue
        if "multiprocessing.spawn" in cmd and "resource_tracker" not in cmd and not _alive(ppid):
            out.append(int(d.name))
    return out


def _read_json(p: Path) -> dict | None:
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return None


def _cli(args: list[str], timeout: float) -> subprocess.CompletedProcess:
    try:
        return subprocess.run([sys.executable, "-m", "realy.cli", *args], capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        return subprocess.CompletedProcess(args, -1, e.stdout or "", f"timeout after {timeout} s")


def _emitter_rate(s: Suite, run_id: str) -> float | None:
    h = s.get("/api/live/history", params={"run_id": run_id, "seconds": 5})
    rates = []
    for k, snaps in (h.get("metrics") or {}).items():
        if k.startswith("emitter:") and snaps:
            rates.append(float((snaps[-1].get("counters") or {}).get("emitted", 0)))
    return sum(rates) if rates else None


def _ws_listen(console: str, counts: dict[str, int], stop: threading.Event) -> None:
    try:
        from websockets.sync.client import connect
    except ImportError:
        return
    url = console.replace("http", "ws", 1).rstrip("/") + "/api/live"
    tok = os.environ.get("REALY_CONSOLE_TOKEN")
    try:
        with connect(url, additional_headers={"Authorization": f"Bearer {tok}"} if tok else None, open_timeout=10) as ws:
            while not stop.is_set():
                try:
                    msg = json.loads(ws.recv(timeout=1.0))
                except TimeoutError:
                    continue
                counts[msg.get("type", "?")] = counts.get(msg.get("type", "?"), 0) + 1
    except Exception as e:  # noqa: BLE001
        counts["error"] = 1
        counts["error_detail"] = str(e)[:100]  # type: ignore[assignment]
