"""Benchmark run orchestration: one call runs a scenario end to end and writes runs/<run_id>/.

  1. world (build or load), Kafka topics, online-store profile seeding
  2. topology + config push (bench.control, admin HTTP for immediate effect)
  3. collector(s) subscribe, then emitter shards start on a common schedule origin
  4. live metrics (bench.metrics) -> metrics.jsonl, guardrails, live rate control
  5. drain, then report (report.json / report.md / report.html)
"""
from __future__ import annotations

import json
import multiprocessing as mp
import os
import queue as queue_mod
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import httpx

from ..collect.collector import collector_main
from ..common import control
from ..common.clock import now_us
from ..common.kafka import CONTROL, METRICS, ensure_topics, make_consumer
from ..emit.driver import ShardPlan, emitter_main, generator_main
from ..gen import auto_time_scale, natural_rate_total, sim_start_us
from ..scenario import REPO_ROOT, Scenario
from ..world import load_or_build

RUNS_DIR = Path(os.environ.get("REALY_RUNS", REPO_ROOT / "runs"))


@dataclass
class Endpoints:
    gateway_admin: str | None = os.environ.get("GATEWAY_ADMIN", "http://localhost:8080")
    model_admin: str | None = os.environ.get("MODEL_ADMIN", "http://localhost:8002")
    flink_rest: str | None = os.environ.get("FLINK_REST", "http://localhost:8081")


@dataclass
class RunHandle:
    """Shared between the run and whoever steers it (console worker, CLI signal handler, monitors).
    `stop` and `rate_mult` exist from construction, so an abort can land before the run starts."""
    run_id: str
    run_dir: Path
    rate_mult: Any = None
    stop: Any = None
    abort_reason: str | None = None
    abort_kind: str | None = None      # operator | guardrail | instrument | watchdog | signal | lease | error | interrupted
    abort_phase: str | None = None
    abort_at: float | None = None
    phase: str = "PREPARE"
    live: dict[str, Any] = field(default_factory=dict)
    service_errors: float = 0.0   # this run's gateway pipeline errors + Flink async timeouts (1 s windows summed)

    def __post_init__(self) -> None:
        ctx = mp.get_context("spawn")
        if self.rate_mult is None:
            self.rate_mult = ctx.Value("d", 1.0)
        if self.stop is None:
            self.stop = ctx.Event()

    def request_abort(self, reason: str, kind: str = "operator") -> bool:
        return request_abort_handle(self, reason, kind)


def request_abort_handle(h: Any, reason: str, kind: str = "operator") -> bool:
    """First reason wins (later ones only re-set the stop event). Returns True if this call aborted."""
    stop = getattr(h, "stop", None)
    if getattr(h, "abort_reason", None):
        if stop is not None:
            stop.set()
        return False
    h.abort_reason = reason
    h.abort_kind = kind
    h.abort_phase = getattr(h, "phase", None)
    h.abort_at = time.time()
    if stop is not None:
        stop.set()
    return True


# abort kinds whose verdict is FAIL (the system under test failed) rather than ABORTED (no verdict)
FAIL_KINDS = ("guardrail",)


def new_run_id(sc: Scenario) -> str:
    return f"{sc.name}-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"


def apply_topology(sc: Scenario, run_id: str, ep: Endpoints, log: Callable[[str], None], world: str | None = None) -> None:
    scoring = sc.scoring_config()
    model_cfg = sc.model_server_config()
    msgs = [
        control.control_message(run_id, "gateway", ingress={"kafka_enabled": sc.topology in ("gateway-kafka", "echo-kafka")}),
        control.control_message(run_id, "*", scoring=scoring),
    ]
    msgs.append(control.control_message(run_id, "model", model_server=model_cfg))
    control.publish(msgs)
    # Admin HTTP for immediate, verifiable effect (bench.control is the scalable path).
    with httpx.Client(timeout=5) as c:
        for url, body, name in ((ep.gateway_admin, scoring, "gateway"), (ep.model_admin, model_cfg, "model")):
            if not url or not body:
                continue
            try:
                r = c.put(f"{url}/admin/config", content=json.dumps(body))
                r.raise_for_status()
                log(f"{name} config applied ({len(body)} keys)")
            except Exception as e:  # noqa: BLE001
                log(f"warn: could not push {name} config via admin HTTP ({e}); relying on bench.control")
    if sc.flink_state == "fresh":
        try:
            from .seed import clear_stream_blobs
            log(f"fresh state: removed {clear_stream_blobs():,} online-store stream blobs")
        except Exception as e:  # noqa: BLE001
            log(f"warn: could not clear online-store stream blobs ({e})")
    if sc.topology == "flink-inline" or sc.topology.startswith("gateway"):
        try:
            from .flink import ensure_jobs
            ensure_jobs(sc, ep.flink_rest, log, run_id=run_id, world=world)
        except Exception as e:  # noqa: BLE001
            log(f"warn: flink job management skipped: {e}")


def _metrics_monitor(handle: RunHandle, sc: Scenario, stop_evt: threading.Event, log) -> None:
    cons = make_consumer(f"runner-{handle.run_id}", **{"auto.offset.reset": "latest"})
    cons.subscribe([METRICS, CONTROL])
    out = open(handle.run_dir / "metrics.jsonl", "a")
    try:
        while not stop_evt.is_set():
            for m in cons.consume(num_messages=500, timeout=0.2):
                if m.error() or m.value() is None:
                    continue
                try:
                    data = json.loads(m.value())
                except ValueError:
                    continue
                if m.topic() == METRICS:
                    data = normalize_snapshot(data)
                    if data.get("run_id") not in ("", handle.run_id):
                        continue
                    out.write(json.dumps(data) + "\n")
                    key = f"{data.get('source')}:{data.get('instance')}"
                    handle.live[key] = data
                    _guardrails(handle, sc, data, log)
                else:  # control: live rate changes / abort from the operator console
                    if data.get("run_id") not in (None, "", handle.run_id):
                        continue
                    if "rate" in data and data.get("target") in ("emitter", "*"):
                        r = data["rate"]
                        base = sc.traffic.profile.tps
                        if r.get("tps"):
                            handle.rate_mult.value = float(r["tps"]) / base
                        elif r.get("multiplier"):
                            handle.rate_mult.value = float(r["multiplier"])
                        log(f"live rate change -> x{handle.rate_mult.value:.2f}")
                    if data.get("phase") in ("ABORTED", 6):
                        if request_abort_handle(handle, "aborted by operator (bench.control)", "operator"):
                            log("abort requested on bench.control")
            out.flush()
    finally:
        out.close()
        cons.close()


def normalize_snapshot(s: dict) -> dict:
    """proto3 JSON (Java side) omits empty maps and zero-valued fields and prints int64 as strings."""
    s["counters"] = {k: float(v) for k, v in (s.get("counters") or {}).items()}
    s["stages"] = [{"name": st.get("name", "?"), "count": int(st.get("count", 0)),
                    **{f: float(st.get(f, 0.0)) for f in ("p50_ms", "p90_ms", "p99_ms", "p999_ms", "max_ms")}}
                   for st in (s.get("stages") or [])]
    s["ts_us"] = int(s.get("ts_us", 0))
    s.setdefault("run_id", "")
    return s


def _guardrails(handle: RunHandle, sc: Scenario, snap: dict, log) -> None:
    g = sc.guardrails
    if handle.stop.is_set() or handle.phase not in ("WARMUP", "MEASURE"):
        return
    reason = None
    if snap.get("source") == "emitter":
        for s in snap.get("stages", []):
            if s["name"] == "schedule_lag" and s["p99_ms"] > g.max_schedule_lag_ms:
                reason = f"emitter schedule lag p99 {s['p99_ms']:.0f} ms > {g.max_schedule_lag_ms:.0f} ms"
    if snap.get("source") == "emitter" or snap.get("source") == "collector":
        emitted = sum(v.get("counters", {}).get("emitted_total", 0) for k, v in handle.live.items() if k.startswith("emitter:"))
        decided = sum(v.get("counters", {}).get("decided_total", 0) for k, v in handle.live.items() if k.startswith("collector:"))
        if emitted - decided > g.max_consumer_lag:
            reason = f"pipeline backlog {emitted - decided:.0f} > {g.max_consumer_lag}"
    # failed produces (emitter, per-run process) + failed scoring pipelines (gateway) + async scoring
    # timeouts (Flink). Service `*_total` counters live as long as the process, so sum their 1 s windows.
    if snap.get("source") in ("gateway", "flink"):
        c = snap.get("counters", {})
        handle.service_errors += c.get("pipeline_errors", 0) + c.get("async_timeouts", 0)
    emitted = sum(v.get("counters", {}).get("emitted_total", 0) for k, v in handle.live.items() if k.startswith("emitter:"))
    errors = handle.service_errors + sum(v.get("counters", {}).get("errors_total", 0)
                                         for k, v in handle.live.items() if k.startswith("emitter:"))
    if emitted >= 1000 and errors / emitted > g.max_error_rate:
        reason = f"error rate {errors / emitted:.2%} > {g.max_error_rate:.0%} ({errors:.0f} errors)"
    if reason and request_abort_handle(handle, reason, "guardrail"):
        log(f"GUARDRAIL: {reason} -> aborting run")


WARM_GATEWAY_DECISIONS = 20_000   # a gateway that has scored fewer events than this is still JIT-compiling
WARM_FLINK_UPTIME_S = 90.0         # a Flink job younger than this is still restoring state / warming up


def _gateway_decisions(admin: str | None) -> int | None:
    if not admin:
        return None
    try:
        text = httpx.get(f"{admin}/metrics", timeout=3).text
    except Exception:  # noqa: BLE001
        return None
    for line in text.splitlines():
        if line.startswith("realy_events_total") and 'counter="decisions"' in line:
            return int(float(line.rsplit(" ", 1)[-1]))
    return 0


def _flink_job_uptime_s(rest: str | None) -> float | None:
    if not rest:
        return None
    try:
        jobs = httpx.get(f"{rest}/jobs/overview", timeout=3).json().get("jobs", [])
    except Exception:  # noqa: BLE001
        return None
    up = [j.get("duration", 0) / 1000 for j in jobs if j.get("state") == "RUNNING" and str(j.get("name", "")).startswith("realy-")]
    return min(up) if up else None


def _wait_gateway_scored(admin: str | None, target: int, timeout_s: float,
                         stop: Callable[[], bool] | None = None) -> bool:
    end = time.time() + min(timeout_s, 300.0)
    while time.time() < end:
        if stop is not None and stop():
            return False
        n = _gateway_decisions(admin)
        if n is None or n >= target:
            return True
        time.sleep(1.0)
    return False


def wait_gateway_primed(ep: Endpoints, decisions_before: int | None, primed: int, timeout_s: float,
                        log: Callable[[str], None], stop: Callable[[], bool] | None = None) -> bool:
    """Wait until the gateway has scored the priming events. Priming events (unlike regular `x-backfill`
    history, which the gateway skips) are scored, and the gateway's consumer group commits offsets as it
    goes: group lag is correct for any number of replicas. Fallback when the group has no offsets: the
    decision counter of the replica the Service routes to (exact only for a single replica)."""
    from ..common.kafka import wait_group_caught_up
    grp = os.environ.get("GATEWAY_GROUP", "realy-gateway")
    max_lag = max(500, primed // 200)
    lag = wait_group_caught_up(grp, "payments.events.raw", max_lag=max_lag,
                               timeout_s=min(timeout_s, 300.0), log=log, no_offsets_grace_s=20.0, stop=stop)
    if lag == -2:
        return False
    if 0 <= lag <= max_lag:   # a timeout returns the last (too high) lag: not caught up
        log(f"prime: gateway caught up (lag {lag:,})")
        return True
    if decisions_before is None:
        return False
    done = _wait_gateway_scored(ep.gateway_admin, decisions_before + int(primed * 0.99), timeout_s, stop)
    log(f"prime: gateway {'caught up' if done else 'still catching up (continuing)'}")
    return done


def cold_pipeline(sc: Scenario, ep: Endpoints) -> list[str]:
    """Why the pipeline under test is cold (empty list = warm). A cold JVM / freshly started Flink job
    adds seconds of latency for the first ~15 s, which would land inside a short scenario's measured
    window and fail it for reasons that have nothing to do with the pipeline's steady state."""
    reasons = []
    if sc.topology in ("gateway-kafka", "echo-kafka"):
        n = _gateway_decisions(ep.gateway_admin)
        if n is not None and n < WARM_GATEWAY_DECISIONS:
            reasons.append(f"gateway has scored {n:,} events since it started")
    if sc.topology in ("gateway-kafka", "gateway-grpc", "flink-inline"):
        up = _flink_job_uptime_s(ep.flink_rest)
        if up is not None and up < WARM_FLINK_UPTIME_S:
            reasons.append(f"Flink job started {up:.0f} s ago")
    return reasons


def plan_history(backfill_days: float | None, cold_reasons: list[str], base_tps: float, natural_rate: float,
                 prime_s: float = 30.0, auto_prime: bool = True) -> list[dict[str, Any]]:
    """The run's unmeasured history, as ordered segments (oldest first), each `{"mode", "days"}`:

      bulk   unpaced, header `x-backfill`: Flink builds feature state from it, the gateway skips it.
             Fastest way to warm state with days of history.
      prime  paced at the scenario's starting rate, scored by the pipeline (no `x-backfill`), never
             measured: warms the scoring path itself (gateway / Flink-inline JIT, connections, model
             server, ClickHouse ingest) at the steady state it will be measured in.

    A cold scoring path (`cold_reasons`) gets the last `prime_s` seconds' worth of history as a prime
    segment: inside the backfill when there is one (state is still built from all history, in event
    order), or on its own when there is none. History events of every segment are logged as
    `backfill` and replayed by `verify`, so priming never costs exactness, fresh state included."""
    bulk = float(backfill_days or 0.0)
    prime_days = 0.0
    if auto_prime and cold_reasons:
        events = int(min(600_000, max(5_000, prime_s * base_tps)))
        prime_days = events / max(1e-9, natural_rate * 86_400)
    if prime_days >= bulk > 0:            # the whole backfill is shorter than one prime: score all of it
        return [{"mode": "prime", "days": bulk}]
    segs = []
    if bulk > 0:
        segs.append({"mode": "bulk", "days": bulk - prime_days})
    if prime_days > 0:
        segs.append({"mode": "prime", "days": prime_days})
    return segs


def history_days(segs: list[dict[str, Any]], mode: str | None = None) -> float:
    return float(sum(s["days"] for s in segs if mode in (None, s["mode"])))


def describe_history(segs: list[dict[str, Any]], base_tps: float, natural_rate: float) -> str:
    parts = []
    for s in segs:
        n = s["days"] * natural_rate * 86_400
        if s["mode"] == "bulk":
            parts.append(f"{s['days']:.3g} sim days unpaced (~{n:,.0f} events, builds feature state; not scored)")
        else:
            parts.append(f"~{n:,.0f} events paced at {base_tps:,.0f}/s (~{n / max(base_tps, 1):.0f} s, scored: warms the scoring path)")
    return " + ".join(parts) if parts else "none"


class _Stop(Exception):
    """Unwinds run_scenario to its single teardown path (abort requested / instrument failure)."""


class _Run:
    """One run's state, so teardown can stop exactly what was started whatever the exit path:
    completion, operator abort, guardrail, a dead child process, the watchdog, a signal or an error."""

    def __init__(self, sc: Scenario, ep: Endpoints, run_id: str, run_dir: Path, handle: RunHandle,
                 lease: Any, log: Callable[[str], None], collectors: int | None) -> None:
        self.sc, self.ep, self.run_id, self.run_dir, self.h, self.lease, self.log = sc, ep, run_id, run_dir, handle, lease, log
        self.ctx = mp.get_context("spawn")
        self.n_coll_req = collectors
        self.results = self.ctx.Queue()
        self.procs: list[tuple[str, int, Any]] = []
        self.sync_thread: threading.Thread | None = None
        self.emit_summ: dict[int, dict] = {}
        self.coll_summ: dict[int, dict] = {}
        self.bf_got = 0
        self.bf_events = 0
        self.bf_primed = 0
        self.history: list[dict[str, Any]] = []
        self.primes = False
        self.n_ems = 0
        self.n_coll = 0
        self.drain = None
        self.expected = None
        self.drain_limit = None
        self.go = None
        self.stop_mon = threading.Event()
        self.mon: threading.Thread | None = None
        self.infra: Any = None
        self.manifest: dict[str, Any] = {}
        self.emission_end_us: int | None = None
        self.error: str | None = None
        self.t0 = time.time()
        self.phases: dict[str, float] = {}
        self.watchdog_s = float(os.environ.get("REALY_RUN_WATCHDOG_S") or (
            sc.traffic.duration_s + sc.traffic.warmup_s + float(os.environ.get("REALY_BACKFILL_TIMEOUT_S", "900")) + 900))

    # ------------------------------------------------------------------ helpers
    def phase(self, name: str) -> None:
        self.h.phase = name
        self.phases[name] = round(time.time() - self.t0, 2)

    def check(self, where: str) -> None:
        """Raise _Stop when the run must end now. Called from every wait loop."""
        h = self.h
        if h.abort_reason:
            raise _Stop()
        if time.time() - self.t0 > self.watchdog_s:
            request_abort_handle(h, f"watchdog: run exceeded {self.watchdog_s:.0f} s (in {where})", "watchdog")
            raise _Stop()
        for role, idx, p in self.procs:
            code = p.exitcode
            if code is None:
                continue
            if code != 0 or (role == "collector" and not (self.drain is not None and self.drain.is_set())):
                request_abort_handle(h, f"instrument failure: {role} {idx} exited with code {code} during {where}",
                                     "instrument")
                raise _Stop()

    def pump(self, timeout: float) -> bool:
        """Read one result from the child processes. Returns False when nothing arrived."""
        try:
            kind, idx, val = self.results.get(timeout=timeout)
        except queue_mod.Empty:
            return False
        except (EOFError, OSError):
            return False
        if kind in ("emitter", "collector") and isinstance(val, dict) and "error" in val:
            last = [x for x in str(val["error"]).strip().splitlines() if x.strip()][-1:] or ["?"]
            self.log(f"{kind} {idx} failed:\n{val['error']}")
            request_abort_handle(self.h, f"instrument failure: {kind} {idx}: {last[0][:200]}", "instrument")
        if kind == "emitter":
            self.emit_summ[idx] = val
        elif kind == "collector":
            self.coll_summ[idx] = val
        elif kind == "backfill":
            self.bf_got += 1
            if isinstance(val, dict):
                self.bf_events += int(val.get("events", 0))
                self.bf_primed += int(val.get("primed", 0))
            else:
                self.bf_events += int(val)
        return True

    def sleep(self, seconds: float, where: str) -> None:
        end = time.time() + seconds
        while time.time() < end:
            self.check(where)
            time.sleep(min(0.2, max(0.0, end - time.time())))

    def write_manifest(self) -> None:
        (self.run_dir / "manifest.json").write_text(json.dumps(self.manifest, indent=1))

    def write_pids(self) -> None:
        """Child process ids (role, index, pid): for operators and the live acceptance suite's fault
        injection. Removed implicitly by being stale once the run ends (processes are reaped)."""
        pids = [{"role": role, "index": idx, "pid": p.pid} for role, idx, p in self.procs]
        (self.run_dir / "pids.json").write_text(json.dumps({"driver": os.getpid(), "children": pids}, indent=1))

    # ------------------------------------------------------------------ phases
    def prepare(self) -> None:
        sc, ep, log = self.sc, self.ep, self.log
        self.phase("PREPARE")
        # a provisional manifest makes a preparing run visible (console history, `realy status`) at once
        self.manifest = {"run_id": self.run_id, "scenario": sc.name, "fingerprint": sc.fingerprint(),
                         "topology": sc.topology, "t_start_us": None, "warmup_s": sc.traffic.warmup_s,
                         "duration_s": sc.traffic.duration_s, "created_at": self.t0,
                         "owner": self.lease.info.get("owner"), "pipeline": self.lease.info.get("pipeline")}
        self.write_manifest()
        self.world = load_or_build(sc.world, sc.seed, sc.world_dir)
        log(f"world {sc.world_dir.name}: {self.world.n_customers:,} customers, hash {self.world.content_hash[:12]}")
        self.check("prepare")
        created = ensure_topics()
        if created:
            log(f"created topics: {', '.join(created)}")
        try:
            from .seed import seed_profiles
            n = seed_profiles(self.world)
            if n:
                log(f"seeded {n:,} profile blobs into the online store")
        except Exception as e:  # noqa: BLE001
            log(f"warn: profile seeding skipped ({e})")
        self.check("prepare")
        world_switch_state(sc, self.world.content_hash, log, ep.flink_rest)
        prof = sc.traffic.profile
        self.base_tps = prof.tps if prof.kind != "step" else (prof.start_tps or prof.tps)
        self.ts = auto_time_scale(self.world, self.base_tps) if sc.traffic.time_scale == "auto" else float(sc.traffic.time_scale)
        log(f"time scale {self.ts:.1f} sim-s per wall-s (natural rate {natural_rate_total(self.world):.1f} ev/sim-s)")
        apply_topology(sc, self.run_id, ep, log, world=self.world.content_hash)
        self.sleep(float(os.environ.get("REALY_CONFIG_SETTLE_S", "2")), "config settle")
        self.sync = sc.topology == "gateway-grpc"
        # the sync rail replays a compiled dataset (no history path); REALY_AUTO_PRIME=0 disables priming
        auto_prime = os.environ.get("REALY_AUTO_PRIME", "1") != "0" and not self.sync
        cold = cold_pipeline(sc, ep) if auto_prime else []
        rate = natural_rate_total(self.world)
        self.history = [] if self.sync else plan_history(sc.traffic.backfill_days, cold, self.base_tps, rate,
                                                          float(os.environ.get("REALY_PRIME_S", "30")), auto_prime)
        if self.history:
            sc.traffic.backfill_days = history_days(self.history)
            log(f"history: {describe_history(self.history, self.base_tps, rate)}"
                + (f" (cold scoring path: {'; '.join(cold)})" if history_days(self.history, "prime") else ""))
        self.primes = bool(history_days(self.history, "prime"))
        self.gw_before = _gateway_decisions(ep.gateway_admin) if self.primes else None
        self.sim0 = sim_start_us(sc)
        if self.sync:
            from .sync import compile_run_dataset
            t_c = time.time()
            meta = compile_run_dataset(self.world, sc, self.run_dir, self.ts, self.sim0)
            log(f"compiled {meta['events']:,} events for the sync rail in {time.time() - t_c:.1f}s (sha256 {meta['sha256'][:12]})")
        self.check("prepare")

    def start_collectors(self) -> None:
        sc, ctx = self.sc, self.ctx
        self.mon = threading.Thread(target=_metrics_monitor, args=(self.h, sc, self.stop_mon, self.log), daemon=True)
        self.mon.start()
        from .infra import InfraSampler
        self.infra = InfraSampler(self.run_dir, self.ep.flink_rest if sc.topology != "echo-kafka" else None, self.log,
                                  max_s=sc.traffic.duration_s + sc.traffic.warmup_s + 3600).start()
        tps = sc.traffic.profile.tps
        n_coll = self.n_coll_req or max(1, int(tps // 15000) + (1 if tps % 15000 else 0))
        self.n_coll = n_coll = min(n_coll, 6)
        self.ready = ready = [ctx.Event() for _ in range(n_coll)]   # referenced for the run (see start_emitters)
        self.drain = ctx.Event()
        self.expected = ctx.Value("q", -1)
        self.drain_limit = ctx.Value("d", 0.0)
        for i in range(n_coll):
            p = ctx.Process(target=collector_main, args=(self.run_id, str(self.run_dir), i, ready[i], self.drain,
                                                         self.expected, self.results),
                            kwargs={"read_committed": sc.exactly_once, "drain_limit": self.drain_limit}, daemon=True)
            p.start()
            self.procs.append(("collector", i, p))
        self.write_pids()
        end = time.time() + 60
        while not all(e.is_set() for e in ready):
            self.check("collector start")
            if time.time() > end:
                raise RuntimeError("collector did not get a partition assignment within 60 s")
            time.sleep(0.2)
        self.sleep(1.0, "collector start")

    def start_emitters(self) -> None:
        sc, ctx, h = self.sc, self.ctx, self.h
        n_shards = 1 if self.sync else max(1, sc.traffic.generators)
        t_start = now_us() + 3_000_000
        self.plans = [ShardPlan(shard=s, n_shards=n_shards, run_id=self.run_id, run_dir=str(self.run_dir),
                                scenario=sc.model_dump(mode="json"), world_dir=str(sc.world_dir), t_start_us=t_start,
                                time_scale=self.ts, sim_start_us=self.sim0, duration_s=sc.traffic.duration_s,
                                extra={"history": self.history, "prime_tps": self.base_tps / n_shards})
                      for s in range(n_shards)]
        self.manifest.update({"t_start_us": t_start, "time_scale": self.ts, "sim_start_us": self.sim0,
                              "shards": n_shards, "collectors": self.n_coll, "world_hash": self.world.content_hash,
                              "prepare_s": round(time.time() - self.t0, 2), "auto_prime": self.primes, "history": self.history})
        self.write_manifest()
        self.log(f"run {self.run_id}: {sc.topology}, {n_shards} emitter shard(s), {self.n_coll} collector(s), "
                 f"{sc.traffic.duration_s:.0f}s ({sc.traffic.profile.kind} {sc.traffic.profile.tps:.0f} TPS)")
        self.n_ems = n_shards
        if self.sync:
            from .sync import run_emitter
            target = sc.sync.target or os.environ.get("GATEWAY_GRPC", "localhost:8080")
            results, run_dir, run_id, ts, sim0, log = self.results, self.run_dir, self.run_id, self.ts, self.sim0, self.log

            def _sync_emit() -> None:
                try:
                    results.put(("emitter", 0, run_emitter(run_dir, run_id, t_start, ts, sim0, target, sc.sync.mode,
                                                           sc.sync.channels, sc.sync.deadline_ms, log, stop=h.stop)))
                except Exception as e:  # noqa: BLE001
                    results.put(("emitter", 0, {"error": str(e)}))

            self.sync_thread = threading.Thread(target=_sync_emit, daemon=True)
            self.sync_thread.start()
            return
        self.start_v = ctx.Value("q", 0)
        self.go = ctx.Event()
        # kept on self for the whole run: with the spawn start method children unpickle their arguments
        # after start() returns, and a Queue the parent no longer references is garbage-collected and
        # its semaphores unlinked (children then fail with SemLock FileNotFoundError)
        self.queues = queues = [ctx.Queue(maxsize=200) for _ in self.plans]
        for p_, q in zip(self.plans, queues):
            g = ctx.Process(target=generator_main, args=(p_, q, h.rate_mult, h.stop, self.start_v, self.go), daemon=True)
            e = ctx.Process(target=emitter_main, args=(p_, q, h.stop, self.results, "bench.metrics", self.start_v, self.go),
                            daemon=True)
            g.start()
            e.start()
            self.procs += [("generator", p_.shard, g), ("emitter", p_.shard, e)]
        self.write_pids()

    def backfill(self) -> None:
        if self.sync:
            return
        sc, ep, log, h = self.sc, self.ep, self.log, self.h
        self.phase("BACKFILL" if sc.traffic.backfill_days else "PREPARE")
        while self.bf_got < len(self.plans):
            self.check("backfill")
            self.pump(0.5)
        bf = self.bf_events
        if bf:
            log(f"history: {bf:,} events emitted ({sc.traffic.backfill_days:.3g} sim days"
                + (f"; {self.bf_primed:,} paced and scored" if self.bf_primed else "") + ")")
            from ..common.kafka import wait_group_caught_up
            timeout = float(os.environ.get("REALY_BACKFILL_TIMEOUT_S", "900"))
            stopped = lambda: bool(h.abort_reason)  # noqa: E731
            if sc.topology in ("flink-inline", "gateway-kafka", "gateway-grpc"):
                grp = feature_group(sc, self.run_id, log)
                max_lag = max(500, bf // 200)
                lag = wait_group_caught_up(grp, "payments.events.raw", max_lag=max_lag, timeout_s=timeout,
                                           log=log, stop=stopped)
                self.check("backfill catch-up")
                ok = 0 <= lag <= max_lag
                self.manifest["backfill_caught_up"] = ok
                self.manifest["backfill_lag"] = lag
                if ok:
                    log(f"backfill: feature job caught up (group {grp}, lag {lag:,})")
                else:
                    log(f"WARN backfill: feature job NOT caught up after {timeout:.0f} s (group {grp}, lag "
                        f"{lag if lag >= 0 else 'unknown'}): measured-window features are stale and the report will say so")
            if self.bf_primed and sc.topology in ("gateway-kafka", "echo-kafka"):
                wait_gateway_primed(ep, self.gw_before, self.bf_primed, timeout, log, stop=stopped)
                self.check("prime catch-up")
        t_start = now_us() + 3_000_000
        self.start_v.value = t_start
        self.manifest["t_start_us"] = t_start
        self.manifest["backfill_events"] = bf
        self.write_manifest()
        self.go.set()

    def measure(self) -> None:
        sc, h = self.sc, self.h
        self.phase("WARMUP")
        t_start = self.manifest["t_start_us"]
        last_print = 0.0
        while len(self.emit_summ) < self.n_ems:
            self.check("measure")
            self.pump(0.5)
            el = (now_us() - t_start) / 1e6
            if h.phase == "WARMUP" and el >= sc.traffic.warmup_s:
                self.phase("MEASURE")
            if time.time() - last_print > 5 and el > 0:
                last_print = time.time()
                self.log(_live_line(h, el))
            if not self._emitters_alive() and len(self.emit_summ) < self.n_ems:
                self._collect_pending(1.0)
                self.check("measure")
                break
        self.emission_end_us = now_us()

    def _emitters_alive(self) -> bool:
        if self.sync_thread is not None:
            return self.sync_thread.is_alive()
        return any(p.is_alive() for role, _, p in self.procs if role == "emitter")

    def _collect_pending(self, settle_s: float) -> None:
        time.sleep(settle_s)
        while self.pump(0.05):
            pass

    # ------------------------------------------------------------------ teardown (always runs)
    def teardown(self) -> None:
        h, log = self.h, self.log
        aborted_at_start = bool(h.abort_reason)
        if aborted_at_start:
            log(f"ABORTING ({h.abort_kind}): {h.abort_reason}")
            self.phase("ABORTING")
        h.stop.set()
        if self.go is not None:
            self.go.set()          # release children still waiting for the measured phase (they see stop)
        # 1. emitters flush, write emit-*.parquet and report a summary (needed for an accurate report)
        finish_s = float(os.environ.get("REALY_EMITTER_FINISH_S", "120"))
        end = time.time() + finish_s
        while len(self.emit_summ) < self.n_ems and time.time() < end:
            if not self._emitters_alive():
                self._collect_pending(0.5)
                break
            self.pump(0.5)
        if self.emission_end_us is None and self.manifest.get("t_start_us"):
            self.emission_end_us = now_us()
        # 2. collectors drain the decisions still in flight
        if self.drain is not None:
            total = sum(int(s.get("emitted", 0)) for s in self.emit_summ.values() if isinstance(s, dict))
            self.expected.value = total if self.n_coll == 1 else -1
            abort_drain = float(os.environ.get("REALY_ABORT_DRAIN_S", "20"))
            if h.abort_reason:
                self.drain_limit.value = abort_drain
            self.drain.set()
            if not aborted_at_start:
                self.phase("DRAIN")
            log(f"emission {'stopped' if h.abort_reason else 'complete'}: {total:,} events; draining")
            deadline = time.time() + (abort_drain + 60 if h.abort_reason else 180)
            shortened = bool(h.abort_reason)
            while len(self.coll_summ) < self.n_coll and time.time() < deadline:
                if h.abort_reason and not shortened:   # aborted during a normal drain: cut it short
                    shortened = True
                    self.drain_limit.value = abort_drain
                    deadline = min(deadline, time.time() + abort_drain + 60)
                if not any(p.is_alive() for role, _, p in self.procs if role == "collector"):
                    self._collect_pending(0.2)
                    break
                self.pump(0.5)
        # 3. live monitoring stops after the drain, so metrics cover it
        self.stop_mon.set()
        if self.infra is not None:
            try:
                self.infra.stop()
            except Exception:  # noqa: BLE001
                pass
        # 4. reap every child; terminate stragglers (a hung child must not outlive its run)
        for role, idx, p in self.procs:
            p.join(timeout=5)
            if p.is_alive():
                log(f"warn: {role} {idx} still running after teardown: terminating")
                p.terminate()
                p.join(timeout=3)
                if p.is_alive():
                    p.kill()
                    p.join(timeout=2)
        if self.sync_thread is not None:
            self.sync_thread.join(timeout=10)
        if self.mon is not None:
            self.mon.join(timeout=3)

    def finish(self, report: bool) -> dict[str, Any]:
        h = self.h
        self.phase("ABORTED" if h.abort_reason else "COMPLETE")
        man = self.manifest
        measured_s = None
        if man.get("t_start_us") and self.emission_end_us:
            measured_s = max(0.0, min(self.sc.traffic.duration_s, (self.emission_end_us - man["t_start_us"]) / 1e6)
                             - self.sc.traffic.warmup_s)
        summary: dict[str, Any] = {
            "run_id": self.run_id, "run_dir": str(self.run_dir), "emitters": self.emit_summ,
            "collectors": self.coll_summ, "aborted": h.abort_reason,
            "abort": ({"reason": h.abort_reason, "kind": h.abort_kind, "phase": h.abort_phase,
                       "after_s": round((h.abort_at or time.time()) - self.t0, 2)} if h.abort_reason else None),
            "error": self.error, "emission_end_us": self.emission_end_us, "measured_s": measured_s,
            "wall_s": round(time.time() - self.t0, 2), "phases": self.phases,
            "owner": self.lease.info.get("owner"), "lease": ("held" if self.lease.held else
                                                             "disabled" if self.lease.disabled else
                                                             f"unavailable: {self.lease.unavailable}"),
        }
        (self.run_dir / "run.json").write_text(json.dumps(summary, indent=1, default=str))
        if report:
            from ..report.report import build_report
            try:
                rep = build_report(self.run_dir)
                summary["report"] = rep["summary"]
                self.log(rep["text"])
            except Exception as e:  # noqa: BLE001 - a partial run must still end cleanly
                import traceback as tb
                self.log(f"report failed: {e}\n{tb.format_exc()}")
                summary["report"] = {"verdict": "ABORTED" if h.abort_reason else "ERROR", "reasons": [f"report: {e}"]}
            from ..report.history import store
            store(self.run_dir, log=self.log)
        return summary


def world_switch_decision(prev_world: str | None, world: str, flink_state: str, backfill_days: float | None,
                          default_backfill_days: float = 1.0, why_unknown: str = "") -> tuple[str, float | None, str | None]:
    """Entity ids are deterministic per world (customer = row number, cards and devices by shard
    counters), so Flink's keyed state and the online store built for one world are wrong for another:
    a card's last location, a customer's 30-day average and its novelty sets would belong to a
    different entity (measured: 78.8% feature agreement instead of 99.1%). Unless the state is known
    to belong to this world, start it fresh and warm it with history. `prev_world` None means the
    state's world is unknown, which is treated like another world: assuming it matches is how
    contaminated runs happen. Returns (flink_state, backfill_days, reason or None)."""
    if flink_state == "fresh" or prev_world == world:
        return flink_state, backfill_days, None
    if prev_world is None:
        reason = f"the pipeline state's world is unknown ({why_unknown or 'no provenance'})"
    else:
        reason = f"pipeline state belongs to world {prev_world[:12]}, this run uses {world[:12]}"
    return "fresh", backfill_days or default_backfill_days, reason


def world_switch_state(sc: Scenario, world_hash: str, log: Callable[[str], None], flink_rest: str | None) -> None:
    """Decide fresh vs warm from the provenance record of the job running now (flink.state_provenance).
    Nothing is recorded here: ensure_jobs records a job only once it is RUNNING, so a run that dies
    before its state was reset leaves no record claiming it was."""
    from .flink import desired_mode, state_provenance
    if desired_mode(sc) is None:
        return
    try:
        prov = state_provenance(flink_rest)
    except Exception as e:  # noqa: BLE001
        prov = {"world": None, "why": f"cannot read it: {e.__class__.__name__}"}
    state, bf, reason = world_switch_decision(prov.get("world"), world_hash, sc.flink_state, sc.traffic.backfill_days,
                                              float(os.environ.get("REALY_WORLD_SWITCH_BACKFILL_DAYS", "1")),
                                              prov.get("why", ""))
    if reason:
        log(f"world switch: {reason}: starting Flink state fresh and clearing online-store stream blobs, "
            f"{bf:g} sim day(s) of history warm it (REALY_WORLD_SWITCH_BACKFILL_DAYS)")
        sc.flink_state, sc.traffic.backfill_days = state, bf


def feature_group(sc: Scenario, run_id: str, log: Callable[[str], None] = print) -> str:
    """The consumer group the running feature job commits to: from its provenance record (a job kept
    across runs keeps the group it was started with), else the name ensure_jobs would have used."""
    if sc.flink_state == "fresh":
        return f"realy-flink-{run_id}"     # this run started the job itself (ensure_jobs)
    guess = "realy-flink"
    try:
        from .flink import job_record
        rec = job_record()
        if rec and rec.get("group"):
            if rec["group"] != guess:
                log(f"backfill: the running feature job commits as {rec['group']}")
            return rec["group"]
    except Exception:  # noqa: BLE001
        pass
    return guess


def run_scenario(sc: Scenario, ep: Endpoints | None = None, run_id: str | None = None,
                 log: Callable[[str], None] = print, handle: RunHandle | None = None,
                 collectors: int | None = None, report: bool = True, wait_lease_s: float = 0.0) -> dict[str, Any]:
    """Run a scenario end to end. Raises PipelineBusy (nothing touched) when another run holds the
    pipeline lease and it does not free up within `wait_lease_s`. Every other outcome, including an
    abort from any source and an unexpected error, ends in the same teardown: children stopped and
    reaped, decisions drained, run.json + report written, lease released."""
    from .lifecycle import RunLease
    ep = ep or Endpoints()
    run_id = run_id or new_run_id(sc)
    run_dir = RUNS_DIR / run_id
    handle = handle or RunHandle(run_id, run_dir)
    handle.run_id, handle.run_dir = run_id, run_dir
    lease = RunLease(run_id, {"scenario": sc.name, "topology": sc.topology})
    lease.acquire(wait_s=wait_lease_s, stop=lambda: bool(handle.abort_reason), log=log)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "scenario.json").write_text(json.dumps(sc.model_dump(mode="json"), indent=1))
    r = _Run(sc, ep, run_id, run_dir, handle, lease, log, collectors)
    lease.start(handle, log)
    err: BaseException | None = None
    try:
        try:
            r.prepare()
            r.start_collectors()
            r.start_emitters()
            r.backfill()
            r.measure()
        except _Stop:
            pass
        except BaseException as e:  # noqa: BLE001 - includes KeyboardInterrupt: always tear down
            import traceback as tb
            err = e
            r.error = f"{type(e).__name__}: {e}"
            log(f"run error: {r.error}\n{tb.format_exc()}")
            request_abort_handle(handle, f"error: {r.error}"[:500], "error")
        finally:
            r.teardown()
        summary = r.finish(report)
    finally:
        lease.release()
    if err is not None and not isinstance(err, Exception):
        raise err          # KeyboardInterrupt / SystemExit propagate after a clean teardown
    if err is not None:
        summary["exception"] = r.error
    return summary


def _live_line(h: RunHandle, el: float) -> str:
    em = [v for k, v in h.live.items() if k.startswith("emitter:")]
    co = [v for k, v in h.live.items() if k.startswith("collector:")]
    sent = sum(v["counters"].get("emitted", 0) for v in em)
    dec = sum(v["counters"].get("decided", 0) for v in co)
    e2e = [s for v in co for s in v.get("stages", []) if s["name"] == "e2e"]
    lag = [s for v in em for s in v.get("stages", []) if s["name"] == "schedule_lag"]
    p99 = max((s["p99_ms"] for s in e2e), default=float("nan"))
    p50 = max((s["p50_ms"] for s in e2e), default=float("nan"))
    sl = max((s["p99_ms"] for s in lag), default=float("nan"))
    return (f"[{h.phase:7s} t={el:6.1f}s] emitted {sent:7.0f}/s  decided {dec:7.0f}/s  "
            f"e2e p50 {p50:7.2f} ms  p99 {p99:7.2f} ms  sched-lag p99 {sl:6.2f} ms")
