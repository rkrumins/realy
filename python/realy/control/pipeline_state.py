"""Per-node pipeline state for the live topology map.

The map used to colour every node by its p99 as a share of the e2e budget. That cannot tell a
pipeline that is *starting* (a Flink job being submitted), *warming* (replaying a day of history,
catching up, priming the scoring path) or *down* (a component unreachable) from one that is slow, so
startup painted Flink and Redis red for minutes. Each node now gets an explicit state:

    down      the component is unreachable, or a job it needs is missing / failed
    starting  a Flink job is being submitted, initialising, restarting, or younger than WARM_UP_S
    warming   the run is replaying history / catching up / priming (with progress and the reason)
    ready     healthy; the UI then colours it by live latency (degraded / hot judged on live events)
    degraded  healthy but impaired (e.g. Flink backpressured during the measured window)

Inputs are the run's startup steps (runner/startup.py, read from the run directory, so runs started
by the CLI or a Kubernetes Job are covered too) and component probes refreshed in the background.
Nodes map to components by their `kind`, so new topologies get states without code changes here.
"""
from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

import httpx

from ..runner.startup import fmt_progress
from ..runner.startup import read as read_startup

COMPONENT_OF_KIND = {"flink": "flink", "store": "redis", "service": "gateway", "model": "model", "kafka": "kafka",
                     "sink": "clickhouse", "driver": "driver"}
WARM_UP_S = 30.0
SILENT_S = 5.0            # a Flink job publishes metrics every second while a run measures
TM_STALE_S = 25.0         # heartbeats every 10 s by default: 2.5 intervals without one = unresponsive
CKPT_STICKY_S = 30.0      # a failed checkpoint keeps the node degraded this long
STARTING_JOB_STATES = ("CREATED", "INITIALIZING", "RESTARTING", "RECONCILING")
ACTIVE_PHASES = ("PREPARE", "BACKFILL", "WARMUP", "MEASURE", "DRAIN", "ABORTING")


def _step(steps: dict[str, dict], sid: str) -> dict:
    return steps.get(sid) or {}


def _running(steps: dict[str, dict], sid: str) -> bool:
    return _step(steps, sid).get("status") == "running"


def _progress(step: dict) -> float | None:
    p = step.get("progress") or {}
    if p.get("total"):
        return max(0.0, min(1.0, (p.get("done") or 0) / p["total"]))
    return None


def node_states(nodes: list[dict], ctx: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """State per node id. ctx: {"phase": str|None, "steps": {id: step}, "probes": {component: {...}},
    "now": float, "uses_flink": bool}. Pure: no I/O."""
    phase = ctx.get("phase")
    active = phase in ACTIVE_PHASES
    steps = ctx.get("steps") or {}
    probes = ctx.get("probes") or {}
    now = float(ctx.get("now") or time.time())
    out: dict[str, dict[str, Any]] = {}

    def put(nid: str, state: str, reason: str = "", progress: float | None = None) -> None:
        out[nid] = {"state": state, "reason": reason, "progress": progress}

    for n in nodes:
        nid, comp = n["id"], COMPONENT_OF_KIND.get(n.get("kind", ""), "")
        p = probes.get(comp) or {}
        if p and p.get("ok") is False:
            put(nid, "down", p.get("error") or f"{comp} unreachable")
            continue
        if comp == "flink":
            job = p.get("job") if p else None
            quiet = (ctx.get("silence") or {}).get("flink")
            if not p:
                put(nid, "ready")       # no probe data yet: let live latency speak
            elif p.get("tm_stale_s"):
                put(nid, "down", f"TaskManager unresponsive: no heartbeat for {p['tm_stale_s']:.0f} s")
            elif phase in ("WARMUP", "MEASURE") and quiet is not None and quiet > SILENT_S \
                    and ctx.get("uses_flink"):
                put(nid, "down", f"no metrics from Flink for {quiet:.0f} s (TaskManager down or stalled)")
            elif _running(steps, "flink"):
                put(nid, "starting", _step(steps, "flink").get("detail") or "submitting the feature job")
            elif job is None and phase == "PREPARE" and _step(steps, "flink").get("status") in (None, "pending"):
                put(nid, "starting", "no feature job yet: this run submits one")
            elif job is None:
                put(nid, "down" if ctx.get("uses_flink") else "ready",
                    "no Flink job running" if ctx.get("uses_flink") else "")
            elif job.get("state") in STARTING_JOB_STATES:
                put(nid, "starting", f"job {job['state'].lower()}")
            elif job.get("state") != "RUNNING":
                put(nid, "down", f"job {str(job.get('state')).lower()}")
            elif (job.get("vertices") or 0) and (job.get("vertices_running") or 0) < job["vertices"]:
                put(nid, "starting", f"tasks deploying ({job['vertices_running']}/{job['vertices']} running)")
            elif active and _running(steps, "history"):
                s = _step(steps, "history")
                put(nid, "warming", f"replaying history: {fmt_progress(s.get('progress')) or s.get('detail', '')}", _progress(s))
            elif active and _running(steps, "catchup"):
                s = _step(steps, "catchup")
                put(nid, "warming", f"catching up: {fmt_progress(s.get('progress')) or s.get('detail', '')}", _progress(s))
            elif job.get("start_ms") and now - job["start_ms"] / 1000 < WARM_UP_S:
                put(nid, "starting", f"job started {now - job['start_ms'] / 1000:.0f} s ago (JIT, RocksDB warming)")
            elif phase == "MEASURE" and p.get("backpressure") == "high":
                put(nid, "degraded", "backpressured (high)")
            elif p.get("checkpoints_failed_recent"):
                put(nid, "degraded", f"{p['checkpoints_failed_recent']} checkpoint(s) failed recently")
            else:
                put(nid, "ready")
        elif comp == "redis":
            if active and _running(steps, "store"):
                put(nid, "warming", _step(steps, "store").get("detail") or "clearing stream blobs")
            elif active and ctx.get("uses_flink") and (_running(steps, "history") or _running(steps, "catchup")):
                put(nid, "warming", "bulk load: Flink writes the replayed history")
            else:
                put(nid, "ready")
        elif comp in ("gateway", "model") or (comp == "kafka" and nid == "decisions"):
            if active and _running(steps, "prime"):
                s = _step(steps, "prime")
                what = {"gateway": "scoring", "model": "inferring", "kafka": "auditing"}.get(comp, "scoring")
                put(nid, "warming", f"{what} the prime events (unmeasured): {fmt_progress(s.get('progress')) or s.get('detail', '')}",
                    _progress(s))
            else:
                put(nid, "ready")
        elif comp == "driver":
            if active and _running(steps, "history") and n["id"] in ("emitter",):
                s = _step(steps, "history")
                put(nid, "warming", f"sending history: {fmt_progress(s.get('progress'))}", _progress(s))
            else:
                put(nid, "ready")
        else:
            put(nid, "ready")
    return out


class PipelineProbes:
    """Component probes refreshed in the background (cheap calls every `every_s`, Flink backpressure
    only while a run is active and at most every `bp_every_s`)."""

    def __init__(self, every_s: float = 2.0, bp_every_s: float = 6.0) -> None:
        self.every_s, self.bp_every_s = every_s, bp_every_s
        self.data: dict[str, Any] = {}
        self.active = False
        self._stop = threading.Event()
        self._t: threading.Thread | None = None
        self._last_bp = 0.0
        self._ckpt_failed: tuple[str, int] | None = None
        self._ckpt_fail_at, self._ckpt_fail_n = 0.0, 0

    def start(self) -> None:
        self._t = threading.Thread(target=self._loop, name="pipeline-probes", daemon=True)
        self._t.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.data = self.probe()
            except Exception:  # noqa: BLE001 - probes never break the console
                pass
            self._stop.wait(self.every_s)

    def probe(self) -> dict[str, Any]:
        from . import env
        out: dict[str, Any] = {}
        for comp, fn in (("kafka", env.probe_kafka), ("redis", env.probe_redis), ("gateway", env.probe_gateway),
                         ("model", env.probe_model), ("clickhouse", env.probe_clickhouse)):
            try:
                r = fn()
                out[comp] = {"ok": r.get("status", "up") != "down"}
            except Exception as e:  # noqa: BLE001
                out[comp] = {"ok": False, "error": f"{comp} unreachable ({e.__class__.__name__})"}
        out["flink"] = self.probe_flink(env.endpoints()["flink"])
        return out

    def probe_flink(self, base: str) -> dict[str, Any]:
        try:
            with httpx.Client(base_url=base, timeout=3) as h:
                o = h.get("/overview").json()
                if not o.get("taskmanagers"):
                    return {"ok": False, "error": "no Flink TaskManager registered"}
                tms = h.get("/taskmanagers").json().get("taskmanagers", [])
                now_ms = time.time() * 1000
                stale = [now_ms - tm["timeSinceLastHeartbeat"] for tm in tms if tm.get("timeSinceLastHeartbeat")]
                tm_stale_s = max(stale) / 1000 if stale else 0.0
                jobs = [j for j in h.get("/jobs/overview").json().get("jobs", [])
                        if j["name"].startswith("realy-") and j["state"] not in ("FINISHED", "CANCELED", "FAILED")]
                if not jobs:
                    return {"ok": True, "job": None}
                j = jobs[0]
                d = h.get(f"/jobs/{j['jid']}").json()
                verts = d.get("vertices") or []
                res: dict[str, Any] = {"ok": True, "tm_stale_s": tm_stale_s if tm_stale_s > TM_STALE_S else 0.0, "job": {
                    "jid": j["jid"], "name": j["name"], "state": d.get("state"), "start_ms": d.get("start-time"),
                    "vertices": len(verts), "vertices_running": sum(1 for v in verts if v.get("status") == "RUNNING")}}
                cp = h.get(f"/jobs/{j['jid']}/checkpoints").json().get("counts", {})
                failed = int(cp.get("failed") or 0)
                prev = self._ckpt_failed
                if prev and prev[0] == j["jid"] and failed > prev[1]:
                    self._ckpt_fail_at, self._ckpt_fail_n = time.time(), failed - prev[1]
                self._ckpt_failed = (j["jid"], failed)
                res["checkpoints_failed_recent"] = (self._ckpt_fail_n if time.time() - self._ckpt_fail_at < CKPT_STICKY_S
                                                    else 0)
                if self.active and time.time() - self._last_bp >= self.bp_every_s and d.get("state") == "RUNNING":
                    self._last_bp = time.time()
                    levels = []
                    for v in verts:
                        try:
                            levels.append(h.get(f"/jobs/{j['jid']}/vertices/{v['id']}/backpressure").json()
                                          .get("backpressureLevel", "ok"))
                        except Exception:  # noqa: BLE001
                            pass
                    res["backpressure"] = "high" if "high" in levels else ("low" if "low" in levels else "ok")
                elif self.data.get("flink"):
                    res["backpressure"] = self.data["flink"].get("backpressure")
                return res
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": f"Flink REST unreachable ({e.__class__.__name__})"}


def pipeline_status(runs_dir: Path, run_id: str | None, phase: str | None, topology: str | None,
                    probes: dict[str, Any], build_topology, silence: dict[str, float] | None = None) -> dict[str, Any]:
    """The `pipeline` block of the console's 1 s status message."""
    topo = topology or "gateway-kafka"
    startup = read_startup(Path(runs_dir) / run_id) if run_id else None
    steps = {s["id"]: s for s in (startup or {}).get("steps", [])}
    try:
        nodes = build_topology(topo)["nodes"]
    except Exception:  # noqa: BLE001
        nodes = []
    uses_flink = topo in ("gateway-kafka", "gateway-grpc", "flink-inline")
    states = node_states(nodes, {"phase": phase, "steps": steps, "probes": probes, "now": time.time(),
                                 "uses_flink": uses_flink and phase in ACTIVE_PHASES, "silence": silence or {}})
    return {"topology": topo, "run_id": run_id, "phase": phase, "nodes": states, "startup": startup}
