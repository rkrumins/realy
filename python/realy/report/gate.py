"""`realy gate`: fail a candidate run that regressed against a baseline (CI / nightly gating).

Absolute checks need no baseline: the verdict (honouring the scenario's `expect`), zero lost
decisions, duplicates within the allowance, and verification passed. Relative checks compare with a
baseline run or a committed baseline file (`realy gate --save-baseline`):
  - e2e and hot-path p99: fail if worse by more than `p99_regress` (relative) AND `min_delta_ms`
    (absolute; noisy runners make small runs jittery)
  - share of events within budget, knee TPS, sustainability
  - detection: recall / precision drops, false-positive-rate rise
  - CPU per event (gateway, Flink): a cost regression shows up here before it shows up as latency
  - feature exactness (verify)
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .compare import num, run_summary


@dataclass
class Thresholds:
    p99_regress: float = 0.10
    min_delta_ms: float = 5.0
    within_drop: float = 0.01
    knee_drop: float = 0.10
    recall_drop: float = 0.02
    precision_drop: float = 0.05
    fpr_rise: float = 0.01
    cpu_regress: float = 0.15
    exact_drop: float = 0.005
    max_duplicates: int = 0
    require_verify: bool = False


def load_baseline(ident: str | Path) -> dict[str, Any]:
    p = Path(ident)
    if p.is_dir():
        return run_summary(p)
    data = json.loads(p.read_text())
    return data.get("summary", data)


def save_baseline(run_dir: Path, path: Path) -> Path:
    s = run_summary(run_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    keep = {k: v for k, v in s.items() if k not in ("reasons", "has_html", "status", "phase", "owner")}
    path.write_text(json.dumps({"scenario": s["scenario"], "saved_from": s["run_id"], "summary": keep}, indent=1) + "\n")
    return path


def gate(candidate: Path, baseline: dict[str, Any] | None, th: Thresholds | None = None) -> dict[str, Any]:
    th = th or Thresholds()
    c = run_summary(candidate)
    b = baseline or {}
    checks: list[dict[str, Any]] = []

    def add(name: str, ok: bool | None, cand: Any = None, base: Any = None, detail: str = "") -> None:
        checks.append({"check": name, "status": "SKIP" if ok is None else ("PASS" if ok else "FAIL"),
                       "candidate": cand, "baseline": base, "detail": detail})

    # ---------------------------------------------------------------- absolute
    if c["status"] not in ("done", "aborted"):
        add("report", False, c["status"], detail="run has no report")
    exp = c.get("expected_verdict")
    if exp:
        add("verdict", c["verdict"] == exp, c["verdict"], detail=f"scenario expects {exp}")
    else:
        add("verdict", c["verdict"] == "PASS", c["verdict"], detail="; ".join(c.get("reasons") or [])[:200])
    add("zero loss", (c.get("missing") or 0) == 0 and (c.get("decided") or 0) > 0, c.get("missing"),
        detail=f"{c.get('decided')} decided")
    add("duplicates", (c.get("duplicates") or 0) <= th.max_duplicates, c.get("duplicates"),
        detail=f"allowed {th.max_duplicates}")
    if c.get("verify_passed") is None:
        add("verify", False if th.require_verify else None, None, detail="no verify.json")
    else:
        add("verify", bool(c["verify_passed"]), c["verify_passed"])

    # ---------------------------------------------------------------- relative to the baseline
    if not b:
        add("baseline", None, detail="no baseline: relative checks skipped")
    else:
        if b.get("scenario") and c.get("scenario") and b["scenario"] != c["scenario"]:
            add("same scenario", False, c["scenario"], b["scenario"], "comparing different scenarios")

        def latency(name: str, key: str) -> None:
            cv, bv = num(c.get(key)), num(b.get(key))
            if cv is None or bv is None:
                add(name, None, cv, bv, "not measured")
                return
            worse = cv - bv
            ok = not (worse > th.min_delta_ms and cv > bv * (1 + th.p99_regress))
            add(name, ok, round(cv, 1), round(bv, 1),
                f"{worse:+.1f} ms ({(cv / bv - 1) if bv else 0:+.0%}); fails beyond +{th.p99_regress:.0%} and +{th.min_delta_ms:g} ms")
        latency("e2e p99", "e2e_p99_ms")
        latency("hot-path p99", "hot_path_p99_ms")

        def drop(name: str, key: str, allowed: float, higher_is_better: bool = True, fmt: str = "{:.3f}") -> None:
            cv, bv = num(c.get(key)), num(b.get(key))
            if cv is None or bv is None:
                add(name, None, None if cv is None else round(cv, 4), None if bv is None else round(bv, 4),
                    "not measured in " + ("both runs" if cv is None and bv is None else "the baseline" if bv is None else "the candidate"))
                return
            delta = (bv - cv) if higher_is_better else (cv - bv)
            add(name, delta <= allowed, round(cv, 4), round(bv, 4),
                f"{'drop' if higher_is_better else 'rise'} {fmt.format(delta)} (allowed {fmt.format(allowed)})")
        drop("within budget", "within_budget", th.within_drop)
        drop("recall (flagged)", "recall_flagged", th.recall_drop)
        drop("precision (flagged)", "precision_flagged", th.precision_drop)
        drop("false-positive rate", "false_positive_rate", th.fpr_rise, higher_is_better=False)
        drop("feature exactness", "exact_set_agreement", th.exact_drop)

        ck, bk = num(c.get("knee_tps")), num(b.get("knee_tps"))
        if c.get("has_levels") and b.get("has_levels"):
            if bk is None and ck is not None:
                add("knee", False, ck, None, "baseline never saturated; candidate does")
            elif bk is not None and ck is not None:
                add("knee", ck >= bk * (1 - th.knee_drop), ck, bk, f"fails below -{th.knee_drop:.0%}")
            else:
                add("knee", True, ck, bk, "no saturation")
        if b.get("sustainable") is True:
            add("sustainable", c.get("sustainable") is not False, c.get("sustainable"), True)
        for comp_ in ("gateway", "flink"):
            key = f"{comp_}_cpu_ms_per_event"
            cv, bv = num(c.get(key)), num(b.get(key))
            if cv is not None and bv is not None and bv > 0:
                add(f"{comp_} CPU / event", cv <= bv * (1 + th.cpu_regress), round(cv, 4), round(bv, 4),
                    f"{cv / bv - 1:+.0%} (allowed +{th.cpu_regress:.0%})")
    failed = [x for x in checks if x["status"] == "FAIL"]
    return {"candidate": c["run_id"], "scenario": c.get("scenario"), "baseline": b.get("run_id") or b.get("saved_from"),
            "passed": not failed, "failed": [x["check"] for x in failed], "checks": checks, "thresholds": asdict(th)}


def render(g: dict[str, Any]) -> str:
    lines = [f"gate {g['candidate']} ({g['scenario']}) vs {g['baseline'] or 'no baseline'}: "
             f"{'PASS' if g['passed'] else 'FAIL'}"]
    for x in g["checks"]:
        cv = "" if x["candidate"] is None else x["candidate"]
        bv = "" if x["baseline"] is None else x["baseline"]
        lines.append(f"  {x['status']:4s} {x['check']:22s} {str(cv):>12s} {str(bv):>12s}  {x['detail']}")
    return "\n".join(lines)
