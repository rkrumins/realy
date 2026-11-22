"""Live replay: a finished (or running) run's telemetry, shaped like the live hub's history so the
console's Live page renders it with the same components. Every run already persists what Live showed:
per-second snapshots of every service (`metrics.jsonl`), its startup steps (`startup.json`), its log
(`run.log`), its phases and live rate changes (`run.json`) and its decisions (`decisions-*.parquet`).

GET /api/runs/{run_id}/telemetry returns
    run       scenario, topology, budget, rate, warm-up / duration, launch and traffic start times
    metrics   {"source:instance": [snapshot, ...]} (downsampled to at most MAX_POINTS per instance)
    logs      [{"run_id", "line", "ts"}]
    startup   startup.json as written at the end (each step's start and end, so its state at any
              moment can be derived)
    markers   [{"t", "kind", "label"}]: phases, warm-up end, rate changes, abort, Flink restarts
    decisions a sample of decisions (up to DECISIONS_PER_S per second) in the live feed's shape
"""
from __future__ import annotations

import collections
import json
import threading
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import APIRouter, HTTPException

MAX_POINTS = 3600
DECISIONS_PER_S = 3


def _json(p: Path) -> dict[str, Any] | None:
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return None


class Replay:
    def __init__(self, runs_dir: Path, explorer: Any = None) -> None:
        self.runs_dir = runs_dir
        self.explorer = explorer
        self._cache: collections.OrderedDict[str, tuple[float, dict[str, Any]]] = collections.OrderedDict()
        self._lock = threading.Lock()

    def run_dir(self, run_id: str) -> Path:
        d = self.runs_dir / run_id
        if "/" in run_id or ".." in run_id or not d.is_dir():
            raise HTTPException(404, "no such run")
        return d

    def telemetry(self, run_id: str) -> dict[str, Any]:
        d = self.run_dir(run_id)
        stamp = max((p.stat().st_mtime for p in (d / "metrics.jsonl", d / "run.json", d / "run.log") if p.exists()),
                    default=0.0)
        with self._lock:
            hit = self._cache.get(run_id)
            if hit and hit[0] == stamp:
                self._cache.move_to_end(run_id)
                return hit[1]
        out = self._build(run_id, d)
        with self._lock:
            self._cache[run_id] = (stamp, out)
            while len(self._cache) > 6:
                self._cache.popitem(last=False)
        return out

    # ------------------------------------------------------------------ build
    def _build(self, run_id: str, d: Path) -> dict[str, Any]:
        man = _json(d / "manifest.json") or {}
        sc = _json(d / "scenario.json") or {}
        run = _json(d / "run.json") or {}
        traffic = sc.get("traffic") or {}
        prof = traffic.get("profile") or {}
        created = man.get("created_at")
        t_start = (man.get("t_start_us") or 0) / 1e6 or None
        info = {"run_id": run_id, "scenario": man.get("scenario") or sc.get("name"), "topology": man.get("topology"),
                "budget": sc.get("budget"), "flows": list((sc.get("traffic") or {}).get("flows") or []),
                "base_tps": prof.get("tps") if prof.get("kind") != "step" else (prof.get("start_tps") or prof.get("tps")),
                "warmup_s": man.get("warmup_s", traffic.get("warmup_s")), "duration_s": man.get("duration_s", traffic.get("duration_s")),
                "started_at": created, "measure_started_at": t_start,
                "ended_at": (run.get("emission_end_us") or 0) / 1e6 or None,
                "verdict": ((_json(d / "report.json") or {}).get("summary") or {}).get("verdict"),
                "complete": (d / "run.json").exists()}
        metrics = self._metrics(d)
        if metrics:
            ts = [s["ts_us"] for snaps in metrics.values() for s in snaps]
            info["first_us"], info["last_us"] = min(ts), max(ts)
        markers = self._markers(run, man, info)
        if not (d / "metrics.jsonl").exists():
            note = "this run has no recorded telemetry (metrics.jsonl): it was recorded before per-second telemetry was kept"
        else:
            note = None
        return {"run": info, "metrics": metrics, "logs": self._logs(run_id, d), "startup": _json(d / "startup.json"),
                "markers": markers, "decisions": self._decisions(run_id, d), **({"note": note} if note else {})}

    def _metrics(self, d: Path) -> dict[str, list[dict[str, Any]]]:
        p = d / "metrics.jsonl"
        if not p.exists():
            return {}
        by: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
        with open(p) as f:
            for line in f:
                try:
                    s = json.loads(line)
                except ValueError:
                    continue
                by[f"{s.get('source', '?')}:{s.get('instance', '?')}"].append(s)
        out = {}
        for k, snaps in by.items():
            snaps.sort(key=lambda s: s.get("ts_us", 0))
            if len(snaps) > MAX_POINTS:            # keep the shape of long runs: every n-th second
                step = int(np.ceil(len(snaps) / MAX_POINTS))
                snaps = snaps[::step]
            out[k] = snaps
        return out

    def _logs(self, run_id: str, d: Path) -> list[dict[str, Any]]:
        from ..runner.run import read_run_log
        lines, _ = read_run_log(d, 0, max_lines=20_000)
        return [{"run_id": run_id, "line": x["line"], "ts": x["ts"]} for x in lines if x.get("ts")]

    def _markers(self, run: dict[str, Any], man: dict[str, Any], info: dict[str, Any]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        base = info.get("started_at")
        for ph, off in (run.get("phases") or {}).items():
            if base is not None and off is not None:
                out.append({"t": base + float(off), "kind": "phase", "label": str(ph).lower()})
        t0 = info.get("measure_started_at")
        if t0 and not any(m["label"] == "warmup" for m in out):
            out.append({"t": t0, "kind": "phase", "label": "warmup"})
        if t0 and info.get("warmup_s") and not any(m["label"] == "measure" for m in out):
            out.append({"t": t0 + float(info["warmup_s"]), "kind": "phase", "label": "measure"})
        for rc in run.get("rate_changes") or []:
            t = rc.get("t") or rc.get("ts") or rc.get("wall")
            if t:
                out.append({"t": float(t), "kind": "rate", "label": f"rate {rc.get('tps') or rc.get('multiplier')}"})
        ab = run.get("abort") or {}
        if run.get("aborted") and ab.get("at"):
            out.append({"t": float(ab["at"]), "kind": "abort", "label": f"abort: {ab.get('reason', '')}"[:80]})
        h = (man.get("health") or {})
        if h.get("flink_restarts"):
            out.append({"t": info.get("ended_at") or base or 0, "kind": "health",
                        "label": f"Flink restarted {h['flink_restarts']}x: {h.get('flink_last_failure') or ''}"[:120]})
        return sorted(out, key=lambda m: m["t"])

    def _decisions(self, run_id: str, d: Path) -> list[dict[str, Any]]:
        """A few decisions per second, with the event fields the live feed shows."""
        import pyarrow.compute as pc

        from .explorer import OUTCOME, PATH, _read
        dec = _read(d, "decisions-*.parquet", ["event_id", "revision", "outcome", "score", "path", "degraded",
                                               "decided_at_us", "t_ingest_us", "t_decision_us", "t_collected_us",
                                               "rule_hits"])
        if dec is None or not dec.num_rows:
            return []
        dec = dec.filter(pc.equal(dec["revision"], 0))
        tcol = next((c for c in ("decided_at_us", "t_decision_us", "t_collected_us") if c in dec.column_names), None)
        if tcol is None or not dec.num_rows:
            return []
        t = dec[tcol].to_numpy()
        sec = t // 1_000_000
        order = np.lexsort((t, sec))
        sec_s = sec[order]
        first = np.r_[True, sec_s[1:] != sec_s[:-1]]
        rank = np.arange(sec_s.size) - np.maximum.accumulate(np.where(first, np.arange(sec_s.size), 0))
        pick = order[rank < DECISIONS_PER_S]
        sub = dec.take(pick).to_pylist()
        rows: dict[int, dict[str, Any]] = {}
        if self.explorer is not None:
            try:
                ix = self.explorer.index(run_id)
                ids = np.asarray([r["event_id"] for r in sub], np.uint64)
                o = np.argsort(ix.c["event_id"])
                sid = ix.c["event_id"][o]
                pos = np.clip(np.searchsorted(sid, ids), 0, max(0, sid.size - 1))
                for e, p_ in zip(ids.tolist(), pos.tolist()):
                    if sid.size and int(sid[p_]) == e:
                        r = ix.rows(np.asarray([o[p_]]))[0]
                        rows[e] = r
            except Exception:  # noqa: BLE001 - the feed still shows outcomes without event fields
                rows = {}
        out = []
        for r in sub:
            e = rows.get(int(r["event_id"]), {})
            hot = (r["t_decision_us"] - r["t_ingest_us"]) / 1000.0 if r.get("t_decision_us") and r.get("t_ingest_us") else None
            out.append({"type": "decision", "run_id": run_id, "event_id": str(r["event_id"]),
                        "customer_id": e.get("customer_id"), "outcome": OUTCOME.get(r.get("outcome")),
                        "score": None if r.get("path") not in (0, 3) else round(float(r.get("score") or 0), 4),
                        "path": PATH.get(r.get("path"), str(r.get("path"))), "degraded": bool(r.get("degraded")),
                        "amount_usd": e.get("amount_usd"), "payment_type": e.get("payment_type"), "channel": e.get("channel"),
                        "merchant_country": e.get("merchant_country"),
                        "rule_hits": [x for x in (r.get("rule_hits") or "").split(",") if x],
                        "hot_path_ms": round(hot, 2) if hot is not None else None, "ts_us": int(r[tcol])})
        return sorted(out, key=lambda x: x["ts_us"])


def make_router(runs_dir: Path, explorer: Any = None) -> APIRouter:
    rp = Replay(runs_dir, explorer)
    r = APIRouter(tags=["replay"])

    @r.get("/api/runs/{run_id}/telemetry")
    def telemetry(run_id: str) -> dict[str, Any]:
        """Everything Live showed for a run: snapshots, log, startup, markers and a decision sample."""
        return rp.telemetry(run_id)

    return r
