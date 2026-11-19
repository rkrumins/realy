"""Resource cost per component from a run's live metrics (metrics.jsonl) and infrastructure samples
(infra.jsonl): cores busy, CPU per event, bytes per event, per source over the measured window and per
load level. This is what turns a benchmark into a capacity plan (`realy size --from <run>`).

Every JVM service publishes `cpu_ms` / `cpus` / `throttled_ms` / `heap_used_mb` with its 1 s
snapshot (StageMetrics.ProcessCpu); the Python emitter (+ its generator) and collector publish
`cpu_ms`. "Events" are counted per source in its own unit:
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..runner.infra import summarise as infra_summary

# source -> (kind, name) of the per-event counter or stage count
EVENTS: dict[str, tuple[str, str]] = {
    "emitter": ("counter", "emitted"),          # events produced
    "collector": ("counter", "decided"),        # decisions consumed
    "gateway": ("counter", "decisions"),        # decisions made
    "model": ("counter", "batch_rows"),         # inference rows
    "flink": ("stage", "state_customer"),       # events through the feature stages (every event has a customer)
}
BYTES: dict[str, str] = {"emitter": "bytes", "collector": "decision_bytes", "gateway": "store_read_bytes",
                         "flink": "store_write_bytes"}


def _load(run_dir: Path) -> list[dict[str, Any]]:
    p = run_dir / "metrics.jsonl"
    if not p.exists():
        return []
    out = []
    for line in p.read_text().splitlines():
        try:
            out.append(json.loads(line))
        except ValueError:
            continue
    return out


def _events(s: dict[str, Any]) -> float:
    kind, name = EVENTS.get(s.get("source", ""), ("counter", ""))
    if kind == "counter":
        return float(s.get("counters", {}).get(name, 0) or 0)
    return float(sum(int(st.get("count", 0)) for st in s.get("stages", []) if st.get("name") == name))


def aggregate(snaps: list[dict[str, Any]], t_from_us: int, t_to_us: int) -> dict[str, dict[str, Any]]:
    window_s = max(1e-9, (t_to_us - t_from_us) / 1e6)
    acc: dict[str, dict[str, Any]] = {}
    for s in snaps:
        ts = int(s.get("ts_us", 0))
        src = s.get("source")
        if src not in EVENTS or not t_from_us <= ts < t_to_us:   # half-open: a boundary snapshot belongs to one level
            continue
        a = acc.setdefault(src, {"instances": {}, "events": 0.0, "cpu_ms": 0.0, "bytes": 0.0, "throttled_ms": 0.0,
                                 "heap_max": 0.0, "has_cpu": False})
        c = s.get("counters", {})
        inst = a["instances"].setdefault(s.get("instance", "?"), {"cpus": [], "windows": 0})
        inst["windows"] += 1
        if "cpus" in c:
            inst["cpus"].append(float(c["cpus"]))
        a["events"] += _events(s)
        if "cpu_ms" in c:
            a["cpu_ms"] += float(c["cpu_ms"])
            a["has_cpu"] = True
        a["bytes"] += float(c.get(BYTES.get(src, ""), 0) or 0)
        a["throttled_ms"] += float(c.get("throttled_ms", 0) or 0)
        a["heap_max"] = max(a["heap_max"], float(c.get("heap_used_mb", 0) or 0))
    out = {}
    for src, a in acc.items():
        cpus = sum((sum(i["cpus"]) / len(i["cpus"])) if i["cpus"] else 0 for i in a["instances"].values())
        cores = a["cpu_ms"] / (window_s * 1000) if a["has_cpu"] else None
        ev = a["events"]
        out[src] = {
            "instances": len(a["instances"]),
            "events_per_s": ev / window_s,
            "cpu_cores": cores,
            "cpus_allocated": cpus or None,
            "utilisation": (cores / cpus) if cores is not None and cpus else None,
            "cpu_ms_per_event": (a["cpu_ms"] / ev) if a["has_cpu"] and ev > 0 else None,
            "bytes_per_event": (a["bytes"] / ev) if ev > 0 and a["bytes"] > 0 else None,
            "throttled_ms_per_s": a["throttled_ms"] / window_s,
            "heap_used_mb_max": a["heap_max"] or None,
        }
    return out


def resources(run_dir: Path, manifest: dict[str, Any], levels: list[dict[str, Any]], duration_s: float,
              warmup_s: float) -> dict[str, Any]:
    t0 = int(manifest.get("t_start_us") or 0)
    if not t0:
        return {}
    snaps = _load(run_dir)
    t_from, t_to = t0 + int(warmup_s * 1e6), t0 + int(duration_s * 1e6)
    out: dict[str, Any] = {"window_s": (t_to - t_from) / 1e6, "sources": aggregate(snaps, t_from, t_to),
                           "infra": infra_summary(run_dir, t_from, t_to), "levels": []}
    # per load level (step / ramp profiles): cost per event as load rises - flat until saturation
    for i, lv in enumerate(levels):
        start = float(lv["t_from_s"])
        end = float(levels[i + 1]["t_from_s"]) if i + 1 < len(levels) else duration_s
        a, b = t0 + int((start + 0.2 * (end - start)) * 1e6), t0 + int(end * 1e6)
        agg = aggregate(snaps, a, b)
        out["levels"].append({"level": lv["level"], "target_tps": lv["target_tps"], "decided_tps": lv["decided_tps"],
                              "sources": {k: {f: v[f] for f in ("events_per_s", "cpu_cores", "cpu_ms_per_event",
                                                                 "utilisation")} for k, v in agg.items()}})
    return out


def render(res: dict[str, Any]) -> list[str]:
    if not res.get("sources"):
        return []
    lines = ["resources (measured window):",
             f"  {'source':10s} {'inst':>4s} {'events/s':>9s} {'cores':>6s} {'util':>6s} {'cpu ms/ev':>9s} {'B/ev':>7s}"]

    def f(v: Any, d: int = 2) -> str:
        return "-" if v is None else f"{v:.{d}f}"
    for src, r in sorted(res["sources"].items()):
        lines.append(f"  {src:10s} {r['instances']:4d} {r['events_per_s']:9.0f} {f(r['cpu_cores']):>6s} "
                     f"{f(None if r['utilisation'] is None else r['utilisation'] * 100, 0) + '%':>6s} "
                     f"{f(r['cpu_ms_per_event'], 3):>9s} {f(r['bytes_per_event'], 0):>7s}")
    inf = res.get("infra") or {}
    if inf.get("redis"):
        rd = inf["redis"]
        lines.append(f"  redis: {f(rd['ops_per_s'], 0)} ops/s, {f(rd['cpu_cores'])} cores, out {f(rd['net_out_mb_s'])} MB/s, "
                     f"in {f(rd['net_in_mb_s'])} MB/s, {f(rd['used_memory_mb'], 0)} MB")
    if inf.get("kafka_msgs_per_s"):
        lines.append("  kafka: " + ", ".join(f"{t} {v:.0f} msg/s" for t, v in inf["kafka_msgs_per_s"].items()))
    if inf.get("flink"):
        busiest = max(inf["flink"].items(), key=lambda kv: kv[1].get("busy_max_pct") or 0)
        lines.append(f"  flink busiest operator: {busiest[0]} busy max {f(busiest[1].get('busy_max_pct'), 0)}%, "
                     f"backpressured max {f(busiest[1].get('backpressured_max_pct'), 0)}%")
    return lines
