"""One flat summary per run, and run-vs-run comparison. Shared by the console (`/api/runs`,
`/api/compare`), the regression gate (`realy gate`) and the run-history table (`realy.runs`)."""
from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any


def read_json(p: Path) -> dict[str, Any] | None:
    try:
        return json.loads(p.read_text())   # Python's json accepts the bare NaN some reports contain
    except (OSError, ValueError):
        return None


def num(v: Any) -> float | None:
    """A finite float, or None (NaN / missing / non-numeric)."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


HEARTBEAT_FRESH_S = 30.0


def run_status(run_dir: Path, man: dict, rep: dict, run: dict) -> tuple[str, str | None]:
    """(status, live phase). A run without a report is `running` only while its owner's heartbeat is
    fresh; otherwise its owner died mid-run (`incomplete`), whatever process that was."""
    if rep:
        aborted = run.get("aborted") or (rep.get("summary") or {}).get("verdict") == "ABORTED"
        return ("aborted" if aborted else "done"), None
    hb = read_json(run_dir / "heartbeat")
    if hb and time.time() - float(hb.get("ts") or 0) < HEARTBEAT_FRESH_S:
        return "running", hb.get("phase")
    if run:
        return ("aborted" if run.get("aborted") else "incomplete"), None
    return ("incomplete" if man else "incomplete"), None


def run_summary(run_dir: Path) -> dict[str, Any]:
    man = read_json(run_dir / "manifest.json") or {}
    rep = read_json(run_dir / "report.json") or {}
    ver = read_json(run_dir / "verify.json")
    run = read_json(run_dir / "run.json") or {}
    s = rep.get("summary") or {}
    status, live_phase = run_status(run_dir, man, rep, run)
    ab = run.get("abort") or {}
    det = rep.get("detection") or {}
    src = (rep.get("resources") or {}).get("sources") or {}
    knee = s.get("knee") or {}
    return {
        "run_id": run_dir.name, "scenario": man.get("scenario") or rep.get("scenario"),
        "topology": man.get("topology") or rep.get("topology"),
        "started_at": (man.get("t_start_us") or 0) / 1e6 or (run_dir.stat().st_mtime if run_dir.exists() else 0),
        "duration_s": man.get("duration_s"), "status": status, "phase": live_phase,
        "abort_kind": ab.get("kind"), "abort_phase": ab.get("phase"), "measured_s": s.get("measured_s"),
        "owner": man.get("owner"),
        "verdict": s.get("verdict"), "reasons": s.get("reasons", []),
        "expected_verdict": s.get("expected_verdict"), "matches_expectation": s.get("matches_expectation"),
        "achieved_tps": s.get("achieved_emit_tps"), "e2e_p50_ms": s.get("e2e_p50_ms"),
        "e2e_p99_ms": s.get("e2e_p99_ms"), "e2e_p999_ms": s.get("e2e_p999_ms"), "hot_path_p99_ms": s.get("hot_path_p99_ms"),
        "within_budget": s.get("within_budget"), "emitted": s.get("emitted"), "decided": s.get("decided"),
        "missing": s.get("missing"), "duplicates": s.get("duplicates"), "degraded_fraction": s.get("degraded_fraction"),
        "knee_tps": knee.get("target_tps"), "has_levels": bool(rep.get("levels")), "sustainable": s.get("sustainable"),
        "recall_flagged": det.get("recall_flagged"), "precision_flagged": det.get("precision_flagged"),
        "false_positive_rate": det.get("false_positive_rate"),
        "abort_reason": run.get("aborted"), "verify_passed": None if ver is None else ver.get("passed"),
        "exact_set_agreement": ((ver or {}).get("features") or {}).get("exact_set_agreement"),
        "gateway_cpu_ms_per_event": (src.get("gateway") or {}).get("cpu_ms_per_event"),
        "flink_cpu_ms_per_event": (src.get("flink") or {}).get("cpu_ms_per_event"),
        "has_html": (run_dir / "report.html").exists(),
    }


def compare_reports(ra: dict[str, Any], rb: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Per-stage a / b / delta (b - a) for p50, p99, p99.9."""
    stages: dict[str, dict[str, Any]] = {}
    for name in sorted(set(ra.get("stages", {})) | set(rb.get("stages", {}))):
        sa, sb = ra.get("stages", {}).get(name, {}), rb.get("stages", {}).get(name, {})
        row: dict[str, Any] = {}
        for q in ("p50", "p99", "p999"):
            va, vb = num(sa.get(q)), num(sb.get(q))
            row[f"a_{q}"], row[f"b_{q}"] = va, vb
            row[f"d_{q}"] = (vb - va) if va is not None and vb is not None else None
        stages[name] = row
    return stages


def compare_runs(a: Path, b: Path) -> dict[str, Any]:
    ra, rb = read_json(a / "report.json") or {}, read_json(b / "report.json") or {}
    return {"a": run_summary(a), "b": run_summary(b), "stages": compare_reports(ra, rb),
            "levels": {"a": ra.get("levels", []), "b": rb.get("levels", [])},
            "detection": {"a": ra.get("detection", {}), "b": rb.get("detection", {})}}
