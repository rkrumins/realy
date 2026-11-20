"""Dependency health: every probe runs in parallel with a short timeout and is timed, so a dead dependency
shows as `down` in about TIMEOUT seconds instead of hanging the page. A probe returns
{"status": "up" | "degraded" | "down" | "starting", "version": ..., "details": {...}} or raises (= down)."""
from __future__ import annotations

import shutil
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import httpx

from . import config

TIMEOUT = float(config.env("PROBE_TIMEOUT_S", "2.5"))
WARM_UP_S = 5.0     # the worker reports "starting" this long after boot


@dataclass
class Probe:
    id: str
    name: str
    kind: str
    fn: Callable[[], dict[str, Any]]
    url: str | None = None


def timed(fn: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    t0 = time.perf_counter()
    try:
        out = dict(fn())
        out.setdefault("status", "up")
    except Exception as e:  # noqa: BLE001 - surfaced to the UI, never raised
        out = {"status": "down", "details": {"error": f"{e.__class__.__name__}: {str(e)[:200]}"}}
    out["latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    return out


def http_probe(url: str) -> Callable[[], dict[str, Any]]:
    """An HTTP dependency: up when it answers < 500 within the timeout."""
    def fn() -> dict[str, Any]:
        r = httpx.get(url, timeout=TIMEOUT)
        return {"status": "up" if r.status_code < 500 else "down",
                "version": r.headers.get("x-version") or r.headers.get("server"), "details": {"http_status": r.status_code}}
    return fn


def default_probes(store: Any, worker: Any, data_dir: Path) -> list[Probe]:
    def p_store() -> dict[str, Any]:
        n = store.ping()
        size = store.path.stat().st_size if store.path.exists() else 0
        return {"version": f"SQLite {sqlite3.sqlite_version}", "details": {"jobs": n, "size_bytes": size}}

    def p_worker() -> dict[str, Any]:
        st = worker.status()
        details = {"current": (st["current"] or {}).get("id"), "queued": len(st["queue"])}
        if not worker.alive:
            return {"status": "degraded", "details": {**details, "reason": "worker thread not running"}}
        if time.time() - worker.started_at < WARM_UP_S:
            return {"status": "starting", "details": {**details, "reason": "worker just started"}}
        return {"details": details}

    def p_disk() -> dict[str, Any]:
        u = shutil.disk_usage(data_dir)
        free = u.free / max(u.total, 1)
        status = "down" if free < 0.02 else "degraded" if free < 0.10 else "up"
        details: dict[str, Any] = {"free_bytes": u.free, "free_pct": round(100 * free, 1)}
        if status != "up":   # say why in words: "down" alone reads as "unreachable"
            details["reason"] = f"only {100 * free:.1f}% free ({u.free / 1e9:.1f} GB)"
        return {"status": status, "details": details}

    probes = [Probe("store", "Job store", "database", p_store), Probe("worker", "Worker", "job runner", p_worker),
              Probe("disk", "Disk", "data volume", p_disk)]
    # APP_DEPENDENCIES="search=http://search:9200/_cluster/health,cache=http://cache:8080/health"
    for item in filter(None, (s.strip() for s in config.env("DEPENDENCIES").split(","))):
        name, _, url = item.partition("=")
        if url:
            probes.append(Probe(name, name, "http dependency", http_probe(url), url=url))
    return probes


_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="probe")


def run_probes(probes: list[Probe]) -> list[dict[str, Any]]:
    futs = {p.id: _pool.submit(timed, p.fn) for p in probes}
    out = []
    for p in probes:
        try:
            r = futs[p.id].result(timeout=TIMEOUT * 2)
        except FutureTimeout:
            r = {"status": "down", "latency_ms": TIMEOUT * 2000, "details": {"error": "probe timed out"}}
        out.append({"id": p.id, "name": p.name, "kind": p.kind, "status": r.get("status", "down"),
                    "latency_ms": r.get("latency_ms"), "version": r.get("version"), "details": r.get("details", {}),
                    "url": p.url})
    return out


def environment(probes: list[Probe]) -> dict[str, Any]:
    return {"checked_at": datetime.now(timezone.utc).isoformat(), "components": run_probes(probes)}


class ProbeCache:
    """Probes for the 1 s status tick: refreshed at most every `every_s`, never blocking the tick for long."""

    def __init__(self, probes: list[Probe], every_s: float = 5.0) -> None:
        self.probes, self.every_s = probes, every_s
        self.value: list[dict[str, Any]] = []
        self.at = 0.0

    def get(self) -> list[dict[str, Any]]:
        if time.time() - self.at >= self.every_s:
            self.at = time.time()      # set first: concurrent ticks during a slow probe reuse the old value
            self.value = run_probes(self.probes)
        return self.value
