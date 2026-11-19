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
    run_id: str
    run_dir: Path
    rate_mult: Any = None
    stop: Any = None
    abort_reason: str | None = None
    phase: str = "PREPARE"
    live: dict[str, Any] = field(default_factory=dict)


def new_run_id(sc: Scenario) -> str:
    return f"{sc.name}-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"


def apply_topology(sc: Scenario, run_id: str, ep: Endpoints, log: Callable[[str], None]) -> None:
    scoring = sc.scoring_config()
    model_cfg = dict(sc.model_server)
    msgs = [
        control.control_message(run_id, "gateway", ingress={"kafka_enabled": sc.topology in ("gateway-kafka", "echo-kafka")}),
        control.control_message(run_id, "*", scoring=scoring),
    ]
    if model_cfg:
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
            ensure_jobs(sc, ep.flink_rest, log, run_id=run_id)
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
                        handle.abort_reason = "aborted by operator"
                        handle.stop.set()
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
    if snap.get("source") == "emitter":
        for s in snap.get("stages", []):
            if s["name"] == "schedule_lag" and s["p99_ms"] > g.max_schedule_lag_ms:
                handle.abort_reason = f"emitter schedule lag p99 {s['p99_ms']:.0f} ms > {g.max_schedule_lag_ms:.0f} ms"
    if snap.get("source") == "emitter" or snap.get("source") == "collector":
        emitted = sum(v.get("counters", {}).get("emitted_total", 0) for k, v in handle.live.items() if k.startswith("emitter:"))
        decided = sum(v.get("counters", {}).get("decided_total", 0) for k, v in handle.live.items() if k.startswith("collector:"))
        if emitted - decided > g.max_consumer_lag:
            handle.abort_reason = f"pipeline backlog {emitted - decided:.0f} > {g.max_consumer_lag}"
    if handle.abort_reason:
        log(f"GUARDRAIL: {handle.abort_reason} -> aborting run")
        handle.stop.set()


def run_scenario(sc: Scenario, ep: Endpoints | None = None, run_id: str | None = None,
                 log: Callable[[str], None] = print, handle: RunHandle | None = None,
                 collectors: int | None = None, report: bool = True) -> dict[str, Any]:
    ep = ep or Endpoints()
    ctx = mp.get_context("spawn")
    run_id = run_id or new_run_id(sc)
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    handle = handle or RunHandle(run_id, run_dir)
    handle.run_id, handle.run_dir = run_id, run_dir
    handle.rate_mult = ctx.Value("d", 1.0)
    handle.stop = ctx.Event()
    (run_dir / "scenario.json").write_text(json.dumps(sc.model_dump(mode="json"), indent=1))

    t_prep = time.time()
    world = load_or_build(sc.world, sc.seed, sc.world_dir)
    log(f"world {sc.world_dir.name}: {world.n_customers:,} customers, hash {world.content_hash[:12]}")
    created = ensure_topics()
    if created:
        log(f"created topics: {', '.join(created)}")
    try:
        from .seed import seed_profiles
        n = seed_profiles(world)
        if n:
            log(f"seeded {n:,} profile blobs into the online store")
    except Exception as e:  # noqa: BLE001
        log(f"warn: profile seeding skipped ({e})")
    base_tps = sc.traffic.profile.tps if sc.traffic.profile.kind != "step" else (sc.traffic.profile.start_tps or sc.traffic.profile.tps)
    ts = auto_time_scale(world, base_tps) if sc.traffic.time_scale == "auto" else float(sc.traffic.time_scale)
    log(f"time scale {ts:.1f} sim-s per wall-s (natural rate {natural_rate_total(world):.1f} ev/sim-s)")
    apply_topology(sc, run_id, ep, log)
    time.sleep(float(os.environ.get("REALY_CONFIG_SETTLE_S", "2")))

    sync = sc.topology == "gateway-grpc"
    sim0 = sim_start_us(sc)
    if sync:
        from .sync import compile_run_dataset
        t_c = time.time()
        meta = compile_run_dataset(world, sc, run_dir, ts, sim0)
        log(f"compiled {meta['events']:,} events for the sync rail in {time.time() - t_c:.1f}s (sha256 {meta['sha256'][:12]})")

    results = ctx.Queue()
    stop_mon = threading.Event()
    mon = threading.Thread(target=_metrics_monitor, args=(handle, sc, stop_mon, log), daemon=True)
    mon.start()
    n_coll = collectors or max(1, int(sc.traffic.profile.tps // 15000) + (1 if sc.traffic.profile.tps % 15000 else 0))
    n_coll = min(n_coll, 6)
    ready = [ctx.Event() for _ in range(n_coll)]
    drain = ctx.Event()
    expected = ctx.Value("q", -1 if n_coll > 1 else -1)
    coll_procs = [ctx.Process(target=collector_main, args=(run_id, str(run_dir), i, ready[i], drain, expected, results),
                              daemon=True) for i in range(n_coll)]
    for p in coll_procs:
        p.start()
    for e in ready:
        if not e.wait(60):
            raise RuntimeError("collector did not get a partition assignment within 60 s")
    time.sleep(1.0)

    n_shards = 1 if sync else max(1, sc.traffic.generators)
    t_start = now_us() + 3_000_000
    plans = [ShardPlan(shard=s, n_shards=n_shards, run_id=run_id, run_dir=str(run_dir),
                       scenario=sc.model_dump(mode="json"), world_dir=str(sc.world_dir), t_start_us=t_start,
                       time_scale=ts, sim_start_us=sim0, duration_s=sc.traffic.duration_s) for s in range(n_shards)]
    manifest = {"run_id": run_id, "scenario": sc.name, "fingerprint": sc.fingerprint(), "topology": sc.topology,
                "t_start_us": t_start, "warmup_s": sc.traffic.warmup_s, "duration_s": sc.traffic.duration_s,
                "time_scale": ts, "sim_start_us": sim0, "shards": n_shards, "collectors": n_coll,
                "world_hash": world.content_hash, "prepare_s": round(time.time() - t_prep, 2)}
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=1))
    log(f"run {run_id}: {sc.topology}, {n_shards} emitter shard(s), {n_coll} collector(s), "
        f"{sc.traffic.duration_s:.0f}s ({sc.traffic.profile.kind} {sc.traffic.profile.tps:.0f} TPS)")
    if sync:
        from .sync import run_emitter
        target = sc.sync.target or os.environ.get("GATEWAY_GRPC", "localhost:8080")

        def _sync_emit() -> None:
            try:
                results.put(("emitter", 0, run_emitter(run_dir, run_id, t_start, ts, sim0, target, sc.sync.mode,
                                                       sc.sync.channels, sc.sync.deadline_ms, log)))
            except Exception as e:  # noqa: BLE001
                results.put(("emitter", 0, {"error": str(e)}))

        th = threading.Thread(target=_sync_emit, daemon=True)
        th.start()
        gens, ems = [], [th]
    else:
        queues = [ctx.Queue(maxsize=200) for _ in plans]
        gens = [ctx.Process(target=generator_main, args=(p, q, handle.rate_mult, handle.stop), daemon=True)
                for p, q in zip(plans, queues)]
        ems = [ctx.Process(target=emitter_main, args=(p, q, handle.stop, results), daemon=True)
               for p, q in zip(plans, queues)]
        for p in gens + ems:
            p.start()
    handle.phase = "WARMUP"
    emit_summ: dict[int, dict] = {}
    coll_summ: dict[int, dict] = {}
    last_print = 0.0
    while len(emit_summ) < len(ems):
        try:
            kind, idx, summ = results.get(timeout=0.5)
            (emit_summ if kind == "emitter" else coll_summ)[idx] = summ
        except Exception:  # noqa: BLE001
            pass
        el = (now_us() - t_start) / 1e6
        if handle.phase == "WARMUP" and el >= sc.traffic.warmup_s:
            handle.phase = "MEASURE"
        if time.time() - last_print > 5 and el > 0:
            last_print = time.time()
            log(_live_line(handle, el))
        if not any(p.is_alive() for p in ems) and len(emit_summ) < len(ems):
            time.sleep(1)
            while not results.empty():
                kind, idx, summ = results.get()
                (emit_summ if kind == "emitter" else coll_summ)[idx] = summ
            break
    handle.phase = "DRAIN"
    total = sum(s.get("emitted", 0) for s in emit_summ.values())
    expected.value = total if n_coll == 1 else -1
    drain.set()
    log(f"emission complete: {total:,} events; draining")
    deadline = time.time() + 120
    while len(coll_summ) < n_coll and time.time() < deadline:
        try:
            kind, idx, summ = results.get(timeout=1)
            (emit_summ if kind == "emitter" else coll_summ)[idx] = summ
        except Exception:  # noqa: BLE001
            pass
    stop_mon.set()
    handle.stop.set()
    for p in gens + ems + coll_procs:
        p.join(timeout=5)
    handle.phase = "COMPLETE" if not handle.abort_reason else "ABORTED"
    summary = {"run_id": run_id, "run_dir": str(run_dir), "emitters": emit_summ, "collectors": coll_summ,
               "aborted": handle.abort_reason}
    (run_dir / "run.json").write_text(json.dumps(summary, indent=1, default=str))
    if report:
        from ..report.report import build_report
        rep = build_report(run_dir)
        summary["report"] = rep["summary"]
        log(rep["text"])
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
