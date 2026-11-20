"""`realy gate` (regression gating) and the run-history row written to ClickHouse."""
from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from realy.cli import app
from realy.report.gate import Thresholds, gate, load_baseline, save_baseline
from realy.report.history import COLUMNS, ddl, row


def make_run(root: Path, name: str, *, p99=80.0, hot=40.0, missing=0, dups=0, verdict="PASS", recall=0.95,
             fpr=0.03, knee=None, levels=False, gw_cpu=0.40, verify=True, expect=None, nan=False) -> Path:
    d = root / name
    d.mkdir(parents=True)
    (d / "manifest.json").write_text(json.dumps({"scenario": "smoke", "topology": "gateway-kafka",
                                                 "t_start_us": 1_800_000_000_000_000, "duration_s": 60}))
    summary = {"verdict": verdict, "reasons": [], "emitted": 30000, "decided": 30000 - missing, "missing": missing,
               "duplicates": dups, "achieved_emit_tps": 500, "e2e_p50_ms": 20.0,
               "e2e_p99_ms": float("nan") if nan else p99, "hot_path_p99_ms": hot, "within_budget": 0.998,
               "knee": {"target_tps": knee} if knee else None, "sustainable": True}
    if expect:
        summary.update({"expected_verdict": expect, "matches_expectation": expect == verdict})
    rep = {"summary": summary, "stages": {"e2e": {"p50": 20.0, "p99": p99}},
           "levels": [{"level": 0}] if levels else [],
           "detection": {"recall_flagged": recall, "precision_flagged": 0.5, "false_positive_rate": fpr},
           "resources": {"sources": {"gateway": {"cpu_ms_per_event": gw_cpu}}}}
    # reports can contain bare NaN; write exactly like json.dumps does
    (d / "report.json").write_text(json.dumps(rep))
    if verify is not None:
        (d / "verify.json").write_text(json.dumps({"passed": verify, "features": {"exact_set_agreement": 0.995}}))
    return d


def statuses(g: dict) -> dict[str, str]:
    return {c["check"]: c["status"] for c in g["checks"]}


def test_identical_run_passes(tmp_path: Path):
    base = make_run(tmp_path, "base")
    cand = make_run(tmp_path, "cand", p99=84.0)            # +5% / +4 ms: noise
    g = gate(cand, load_baseline(base))
    assert g["passed"], g["failed"]
    assert statuses(g)["e2e p99"] == "PASS"


def test_regressions_fail_with_named_checks(tmp_path: Path):
    base = make_run(tmp_path, "base", knee=6000, levels=True)
    cand = make_run(tmp_path, "cand", p99=120.0, missing=3, recall=0.90, fpr=0.06, knee=4000, levels=True, gw_cpu=0.6,
                    verify=False)
    g = gate(cand, load_baseline(base))
    assert not g["passed"]
    assert {"zero loss", "verify", "e2e p99", "recall (flagged)", "false-positive rate", "knee",
            "gateway CPU / event"} <= set(g["failed"])


def test_expected_failure_scenarios_and_nan(tmp_path: Path):
    neg = make_run(tmp_path, "neg", verdict="FAIL", expect="FAIL", verify=None)
    assert statuses(gate(neg, None))["verdict"] == "PASS"
    nan = make_run(tmp_path, "nan", nan=True)
    g = gate(nan, load_baseline(make_run(tmp_path, "b2")))
    assert statuses(g)["e2e p99"] == "SKIP"                   # NaN is "not measured", never a crash
    assert statuses(gate(make_run(tmp_path, "nv", verify=None), None, Thresholds(require_verify=True)))["verify"] == "FAIL"


def test_cli_saves_baseline_and_gates(tmp_path: Path):
    base = make_run(tmp_path, "base")
    good = make_run(tmp_path, "good", p99=82.0)
    bad = make_run(tmp_path, "bad", p99=150.0)
    r = CliRunner()
    bl = tmp_path / "baselines" / "smoke.json"
    assert r.invoke(app, ["gate", str(base), "--save-baseline", str(bl)]).exit_code == 0
    assert json.loads(bl.read_text())["summary"]["e2e_p99_ms"] == 80.0
    assert r.invoke(app, ["gate", str(good), "--baseline", str(bl)]).exit_code == 0
    res = r.invoke(app, ["gate", str(bad), "--baseline", str(bl)])
    assert res.exit_code == 1 and "FAIL" in res.output and "e2e p99" in res.output
    assert r.invoke(app, ["gate", str(bad), "--baseline", str(bl), "--p99-regress", "1.0"]).exit_code == 0


def test_history_row_matches_the_ddl_and_is_json_safe(tmp_path: Path):
    run = make_run(tmp_path, "r", nan=True)
    rec = row(run)
    assert [c for c, _, _ in COLUMNS] == list(rec)
    assert rec["e2e_p99_ms"] is None                          # NaN -> NULL
    assert rec["verify_passed"] == 1 and rec["scenario"] == "smoke"
    json.dumps(rec, allow_nan=False)
    sql = ddl("-- h\n")
    assert "CREATE TABLE IF NOT EXISTS realy.runs" in sql and all(f"`{c}`" in sql for c, _, _ in COLUMNS)
    assert save_baseline(run, tmp_path / "b.json").exists()
