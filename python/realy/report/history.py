"""Run history in ClickHouse (`realy.runs`): one row per run summary, written after the report and
again after verification (ReplacingMergeTree keeps the latest). Trend dashboards (Grafana
`realy-runs`) and cross-run analysis read it; `realy gate` reads run directories / baseline files, so
history is best-effort and a ClickHouse outage never fails a run.

The column list below is the single source for the DDL (`python -m realy.codegen` -> 06_runs.sql) and
for the insert.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .compare import num, read_json, run_summary

# (column, ClickHouse type, summary key or callable source)
COLUMNS: list[tuple[str, str, str]] = [
    ("run_id", "String", "run_id"),
    ("scenario", "LowCardinality(String)", "scenario"),
    ("topology", "LowCardinality(String)", "topology"),
    ("started_at", "DateTime64(3)", "started_at"),
    ("duration_s", "Nullable(Float32)", "duration_s"),
    ("verdict", "LowCardinality(String)", "verdict"),
    ("matches_expectation", "Nullable(UInt8)", "matches_expectation"),
    ("achieved_tps", "Nullable(Float64)", "achieved_tps"),
    ("e2e_p50_ms", "Nullable(Float64)", "e2e_p50_ms"),
    ("e2e_p99_ms", "Nullable(Float64)", "e2e_p99_ms"),
    ("e2e_p999_ms", "Nullable(Float64)", "e2e_p999_ms"),
    ("hot_path_p99_ms", "Nullable(Float64)", "hot_path_p99_ms"),
    ("within_budget", "Nullable(Float64)", "within_budget"),
    ("emitted", "Nullable(UInt64)", "emitted"),
    ("decided", "Nullable(UInt64)", "decided"),
    ("missing", "Nullable(Int64)", "missing"),
    ("duplicates", "Nullable(Int64)", "duplicates"),
    ("degraded_fraction", "Nullable(Float64)", "degraded_fraction"),
    ("knee_tps", "Nullable(Float64)", "knee_tps"),
    ("sustainable", "Nullable(UInt8)", "sustainable"),
    ("recall_flagged", "Nullable(Float64)", "recall_flagged"),
    ("precision_flagged", "Nullable(Float64)", "precision_flagged"),
    ("false_positive_rate", "Nullable(Float64)", "false_positive_rate"),
    ("verify_passed", "Nullable(UInt8)", "verify_passed"),
    ("exact_set_agreement", "Nullable(Float64)", "exact_set_agreement"),
    ("gateway_cpu_ms_per_event", "Nullable(Float64)", "gateway_cpu_ms_per_event"),
    ("flink_cpu_ms_per_event", "Nullable(Float64)", "flink_cpu_ms_per_event"),
    ("abort_reason", "String", "abort_reason"),
    ("git_sha", "LowCardinality(String)", "git_sha"),
    ("images", "String", "images"),
    ("report", "String CODEC(ZSTD(3))", "report"),
]


def ddl(header: str) -> str:
    cols = ",\n".join(f"  `{c}` {t}" for c, t, _ in COLUMNS)
    return header + f"""
-- One row per benchmark run (written by the driver after the report and after `realy verify`; the
-- newest row per run wins). Source of the Grafana `realy-runs` dashboard and cross-run trend queries.
CREATE TABLE IF NOT EXISTS realy.runs (
{cols},
  `ingested_at` DateTime64(6) DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(started_at)
ORDER BY (scenario, run_id);
"""


def row(run_dir: Path) -> dict[str, Any]:
    s = run_summary(run_dir)
    s["git_sha"] = os.environ.get("REALY_GIT_SHA", "")
    s["images"] = os.environ.get("REALY_IMAGES", "")
    rep = read_json(run_dir / "report.json") or {}
    rep.pop("text", None)
    s["report"] = json.dumps(rep, default=str, allow_nan=False) if _finite(rep) else json.dumps(_clean(rep))
    s["started_at"] = datetime.fromtimestamp(float(s.get("started_at") or 0), timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    out: dict[str, Any] = {}
    for col, typ, key in COLUMNS:
        v = s.get(key)
        if "Float" in typ or "Int" in typ:
            v = num(v)
            if v is not None and "Int" in typ:
                v = int(v)
        elif typ.startswith("String") or "String" in typ:
            v = "" if v is None else str(v)
        out[col] = v
    return out


def _finite(o: Any) -> bool:
    try:
        json.dumps(o, allow_nan=False, default=str)
        return True
    except ValueError:
        return False


def _clean(o: Any) -> Any:
    if isinstance(o, float):
        return o if o == o and o not in (float("inf"), float("-inf")) else None
    if isinstance(o, dict):
        return {k: _clean(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_clean(v) for v in o]
    return o


def store(run_dir: Path, clickhouse: str | None = None, log=None) -> bool:
    """Best-effort insert; returns False (and logs once) when ClickHouse is unreachable or disabled."""
    if os.environ.get("REALY_RUN_HISTORY", "1") == "0":
        return False
    url = clickhouse or os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")
    try:
        import httpx
        r = httpx.post(url, params={"query": "INSERT INTO realy.runs FORMAT JSONEachRow"},
                       content=json.dumps(row(run_dir), allow_nan=False), timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:  # noqa: BLE001 - history must never fail a run
        if log:
            log(f"run history: not stored in ClickHouse ({type(e).__name__}: {str(e)[:120]})")
        return False
