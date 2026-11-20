"""bench.metrics snapshots (realy.bench.v1.MetricsSnapshot as proto3 JSON)."""
from __future__ import annotations

import json
from typing import Iterable

import numpy as np

STAGE_PCTS = (50, 90, 99, 99.9)


def stage_stat(name: str, values_us: Iterable[int]) -> dict | None:
    a = np.asarray(values_us if isinstance(values_us, np.ndarray) else list(values_us), dtype=np.float64)
    if a.size == 0:
        return None
    p50, p90, p99, p999 = np.percentile(a, STAGE_PCTS) / 1000.0
    return {"name": name, "count": int(a.size), "p50_ms": float(p50), "p90_ms": float(p90), "p99_ms": float(p99),
            "p999_ms": float(p999), "max_ms": float(a.max() / 1000.0)}


def snapshot_json(run_id: str, source: str, instance: str, ts_us: int, window_ms: int,
                  counters: dict[str, float], stages: dict[str, Iterable[int]], phase: int = 0) -> bytes:
    st = [s for s in (stage_stat(k, v) for k, v in stages.items()) if s]
    return json.dumps({"run_id": run_id, "source": source, "instance": instance, "ts_us": ts_us,
                       "window_ms": window_ms, "counters": {k: float(v) for k, v in counters.items()},
                       "stages": st, "phase": phase}).encode()
