"""Insights: the report turned into ranked findings.

Deterministic, rule-based and data-driven: every threshold and cost assumption comes from
contracts/insights.yaml (copied into the report). Each insight carries a severity, a one-line title,
the evidence numbers it rests on, a drill-down (an Explorer query, a rule, a typology, a run) and a
suggested next step. Ranked critical > warning > info > good, then by impact.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import yaml

from ..scenario import REPO_ROOT

CONFIG_PATH = Path(os.environ.get("REALY_INSIGHTS", REPO_ROOT / "contracts" / "insights.yaml"))
SEVERITY = {"critical": 0, "warning": 1, "info": 2, "good": 3}


def load_config(path: Path | None = None) -> dict[str, Any]:
    p = Path(path or CONFIG_PATH)
    try:
        return yaml.safe_load(p.read_text()) or {}
    except OSError:
        return {}


def _pct(x: float | None, nd: int = 1) -> str:
    return "n/a" if x is None else f"{100 * x:.{nd}f}%"


def _usd(x: float | None) -> str:
    return "n/a" if x is None else f"${x:,.0f}"


def base_rate_precision(recall: float | None, fpr: float | None, p: float) -> float | None:
    """Precision at fraud share p: recall*p / (recall*p + fpr*(1-p))."""
    if recall is None or fpr is None:
        return None
    den = recall * p + fpr * (1 - p)
    return recall * p / den if den > 0 else None


def _advice(stage: str, topology: str | None) -> str:
    """What to try for a dominant tail stage, per topology (contracts/topologies.yaml `advice`)."""
    try:
        from ..topologies import advice_for
        return advice_for(stage, topology)
    except Exception:  # noqa: BLE001 - a missing or broken contract must not fail a report
        return ""


def build_insights(rep: dict[str, Any], cfg: dict[str, Any], prev: dict[str, Any] | None = None,
                   registry: Any = None) -> list[dict[str, Any]]:
    th = cfg.get("thresholds") or {}
    out: list[dict[str, Any]] = []

    def add(severity: str, area: str, title: str, detail: str = "", evidence: dict | None = None,
            link: dict | None = None, action: str = "", impact: float = 0.0) -> None:
        out.append({"severity": severity, "area": area, "title": title, "detail": detail,
                    "evidence": evidence or {}, "link": link, "action": action, "impact": float(impact)})

    s = rep.get("summary") or {}
    d = rep.get("detection") or {}
    man = rep.get("manifest") or {}

    # ------------------------------------------------------------------ correctness
    if s.get("missing"):
        add("critical", "correctness", f"{s['missing']:,} decisions missing",
            "events were emitted but no decision reached the audit log", {"missing": s["missing"]},
            {"kind": "verify"}, "run `pulsebench verify` and check the ClickHouse audit gaps", 1e9)
    elif s.get("emitted"):
        add("good", "correctness", f"every one of {s['emitted']:,} events got exactly one decision",
            "zero missing; duplicates are deduplicated by revision", {"emitted": s["emitted"], "duplicates": s.get("duplicates", 0)})
    gates = man.get("gates") or {}
    for g, ok in gates.items():
        if ok is False:
            add("critical", "startup", f"readiness gate failed: {g.replace('_', ' ')}",
                "the measured window started without it; results describe a pipeline that was not ready",
                {"gate": g}, {"kind": "startup"}, "see the Startup section for the step that did not complete", 1e8)

    # ------------------------------------------------------------------ evaluation honesty
    ev = d.get("evaluation") or {}
    if ev.get("sample") == "in-sample":
        add("critical", "detection", "detection numbers are in-sample",
            ev.get("reason") or "the model was trained on this world: recall and precision are optimistic",
            {"model": ev.get("model_version")}, {"kind": "models"},
            "evaluate on a world (seed) the model never saw: fraud-eval, or train on another run", 1e7)
    elif ev.get("sample") == "unknown":
        add("warning", "detection", "cannot tell whether detection is in- or out-of-sample",
            ev.get("reason") or "", {"model": ev.get("model_version")}, {"kind": "models"},
            "retrain with `pulsebench train` so the model records its training provenance", 5e6)

    # ------------------------------------------------------------------ detection
    fraud_total = d.get("fraud_usd_total") or 0.0
    # money that moved: approved, passed a step-up, or reviewed only after payment (card authorisations);
    # reports before step-up only have "approved"
    paid_key = "usd_paid" if "fraud_usd_paid" in d else "usd_approved"
    approved = d.get("fraud_usd_paid", d.get("fraud_usd_approved")) or 0.0
    by_type = d.get("by_type") or {}
    if by_type and approved > 0:
        worst = max(by_type.items(), key=lambda kv: kv[1].get(paid_key) or 0)
        name, t = worst
        share = (t.get(paid_key) or 0) / approved
        meta = registry.by_name.get(name) if registry is not None else None
        hint = f"signals: {', '.join(meta.signals[:4])}" if meta and meta.signals else ""
        add("warning" if share > 0.25 else "info", "detection",
            f"{(meta.title if meta and meta.title else name)} lets the most fraud through: {_usd(t.get(paid_key))} "
            f"({_pct(share, 0)} of the fraud $ {'paid' if paid_key == 'usd_paid' else 'approved'})",
            f"{_pct(t.get('recall_declined'), 0)} of its events declined, {_pct(t.get('recall_flagged'), 0)} flagged"
            + (f", {_pct(t.get('recall_stopped'), 0)} stopped" if t.get("recall_stopped") is not None else "") + "; "
            f"{t.get('approved_before_first_decline_mean', 0):.1f} events approved before the first decline per campaign",
            {"typology": name, paid_key: t.get(paid_key), "recall_declined": t.get("recall_declined")},
            {"kind": "explorer", "query": f"fraud:{name}"},
            ("open its campaigns in the Explorer and look at what the approved events have in common"
             + (f"; {hint}" if hint else "")), t.get(paid_key) or 0)
    floor = float(th.get("typology_decline_floor", 0.5))
    min_ev = int(th.get("typology_min_events", 20))
    # "stopped" (declined, failed / abandoned a step-up, held review) where the report has it
    stop_key, verb = ("recall_stopped", "stopped") if any("recall_stopped" in t for t in by_type.values()) \
        else ("recall_declined", "declined")
    weak = [(n, t) for n, t in by_type.items() if t.get("events", 0) >= min_ev and (t.get(stop_key) or 0) < floor]
    for n, t in sorted(weak, key=lambda kv: kv[1].get(stop_key) or 0)[:3]:
        add("warning", "detection", f"{n} is {verb} only {_pct(t.get(stop_key), 0)} of the time",
            f"{t.get('events', 0):,} events in {t.get('campaigns', 0)} campaigns; flagged {_pct(t.get('recall_flagged'), 0)}"
            + (f", declined {_pct(t.get('recall_declined'), 0)}" if stop_key == "recall_stopped" else ""),
            {"typology": n, stop_key: t.get(stop_key)}, {"kind": "explorer", "query": f"fraud:{n}"},
            "compare its served features with a caught campaign; a rule or a decision-history feature may be missing",
            (t.get(paid_key) or 0) * 0.5)

    # ------------------------------------------------------------------ step-up
    su = d.get("stepup")
    if su:
        pass_warn = float(th.get("stepup_pass_warn", 0.25))
        for n, t in sorted(su.get("by_type", {}).items(), key=lambda kv: -(kv[1].get("usd_passed") or 0)):
            if t.get("challenged", 0) >= min_ev and t.get("pass", 0) / max(1, t["challenged"]) > pass_warn:
                meta = registry.by_name.get(n) if registry is not None else None
                add("warning", "detection",
                    f"step-up does not stop {(meta.title if meta and meta.title else n)}: {t['pass']:,} of "
                    f"{t['challenged']:,} challenged passed it ({_usd(t.get('usd_passed'))})",
                    "its actors can complete the step-up (a phished one-time code, a SIM swap, a victim authorising "
                    "their own payment), so challenging it only delays the loss",
                    {"typology": n, "passed": t["pass"], "challenged": t["challenged"], "usd_passed": t.get("usd_passed")},
                    {"kind": "stepup"}, "decline it, or hold it for review, instead of challenging (Policy Lab / rules-whatif)",
                    t.get("usd_passed") or 0)
        legit_n = max(1, int(d.get("legit_events") or 0))
        ab = int(su.get("legit_abandoned") or 0)
        if ab:
            share = ab / legit_n
            add("warning" if share > float(th.get("stepup_abandon_warn", 0.001)) else "info", "friction",
                f"{ab:,} good customers gave up at a step-up: {_usd(su.get('lost_sales_usd'))} of lost sales",
                f"{su.get('legit_challenged', 0):,} good customers challenged ({_pct(su.get('legit_challenge_rate'), 2)}), "
                f"{su.get('legit_failed', 0):,} failed it",
                {"legit_abandoned": ab, "lost_sales_usd": su.get("lost_sales_usd"),
                 "legit_challenge_rate": su.get("legit_challenge_rate")}, {"kind": "stepup"},
                "narrow the challenge band or the CHALLENGE rules where the challenged payments are mostly good",
                su.get("lost_sales_usd") or 0)
        if su.get("fraud_stopped"):
            add("good", "detection", f"step-up stopped {su['fraud_stopped']:,} fraudulent payments",
                f"of {su.get('fraud_challenged', 0):,} fraud payments challenged; "
                f"{_pct(su.get('fraud_challenge_rate'), 1)} of all fraud was challenged",
                {"fraud_stopped": su["fraud_stopped"]}, {"kind": "stepup"})

    rules = d.get("by_rule") or {}
    pfloor, minf = float(th.get("rule_precision_floor", 0.1)), int(th.get("rule_min_fires", 50))
    noisy = [(rid, r) for rid, r in rules.items() if r.get("hits", 0) >= minf and (r.get("precision") or 0) < pfloor
             and not rid.startswith("SCORE_")]
    for rid, r in sorted(noisy, key=lambda kv: -(kv[1].get("legit") or 0))[:3]:
        add("warning", "detection", f"rule {rid} is mostly false positives: precision {_pct(r.get('precision'))}",
            f"{r.get('hits', 0):,} fires, {r.get('legit', 0):,} on good customers ({_pct(r.get('legit_hit_rate'), 2)} of them)",
            {"rule": rid, "precision": r.get("precision"), "legit": r.get("legit")}, {"kind": "rule", "id": rid},
            "replay the run without it, or with a tighter condition, in the Policy Lab (or `pulsebench rules-whatif`)",
            (r.get("legit") or 0) * 10)
    fpr = d.get("false_positive_rate")
    if fpr is not None and fpr > float(th.get("false_positive_rate_warn", 0.02)):
        add("warning", "detection", f"{_pct(fpr, 2)} of good customers were sent to review or declined",
            "above the configured friction budget", {"false_positive_rate": fpr}, {"kind": "explorer", "query": "outcome:REVIEW"},
            "the rules with the most legitimate hits are listed above", fpr * 1e6)
    p = float(cfg.get("production_base_rate", 0.001))
    prec_p = base_rate_precision(d.get("recall_flagged"), fpr, p)
    if prec_p is not None and prec_p > 0:      # 0 when nothing fraudulent was flagged (recall 0): no ratio to give
        add("info", "detection", f"at a {_pct(p)} fraud rate, about 1 in {max(1, round(1 / prec_p)):,} flags would be fraud",
            f"measured precision {_pct(d.get('precision_flagged'))} is at this run's fraud share "
            f"({_pct((d.get('model') or {}).get('fraud_base_rate'), 2)}); recall {_pct(d.get('recall_flagged'))} and "
            f"FPR {_pct(fpr, 2)} carry over to a real portfolio, precision does not",
            {"production_base_rate": p, "precision_at_base_rate": prec_p})
    m = d.get("model") or {}
    total = (m.get("scored") or 0) + (m.get("rules_only_fallback") or 0)
    fb = (m.get("rules_only_fallback") or 0) / total if total else 0.0
    if fb > float(th.get("fallback_share_warn", 0.01)):
        add("warning", "latency", f"{_pct(fb)} of decisions fell back to rules only",
            f"the model missed its deadline; rules alone flagged {_pct(m.get('fallback_recall_flagged'), 0)} of the fraud "
            f"they saw vs {_pct(d.get('recall_flagged'), 0)} overall",
            {"fallback_share": fb, "fallback_recall_flagged": m.get("fallback_recall_flagged")}, {"kind": "latency"},
            "check host CPU and the model-call p99; hedging (`hedge_after_ms`) cuts deadline misses", fb * 1e5)
    if m.get("roc_auc") is not None and ev.get("sample") == "out-of-sample":
        add("good", "detection", f"out of sample, the model ranks fraud above good payments {_pct(m['roc_auc'], 1)} of the time",
            f"ROC-AUC {m['roc_auc']:.3f}, PR-AUC {m.get('pr_auc', 0):.3f} on a world it never saw",
            {"roc_auc": m["roc_auc"], "pr_auc": m.get("pr_auc")})
    lab = d.get("labels") or {}
    if lab.get("coverage") is not None:
        cov_floor = float(th.get("label_coverage_warn", 0.85))
        sized = {n: c for n, c in (lab.get("by_type") or {}).items() if (by_type.get(n) or {}).get("events", 0) >= min_ev}
        slow = sorted(((n, c) for n, c in sized.items() if c < cov_floor), key=lambda kv: kv[1])
        if lab["coverage"] < cov_floor or slow:
            worst = ", ".join(f"{n} {_pct(c, 0)}" for n, c in slow[:4])
            add("warning", "labels",
                (f"a training set cut {lab.get('maturity_days'):g} days after this window labels "
                 + (f"only {_pct(lab['coverage'], 0)} of its fraud" if lab["coverage"] < cov_floor
                    else f"{_pct(lab['coverage'], 0)} of its fraud, but much less of some typologies")),
                "late chargebacks and unreported fraud train as legitimate" + (f"; least covered: {worst}" if worst else ""),
                {"coverage": lab["coverage"], "maturity_days": lab.get("maturity_days"), "by_type": dict(slow)},
                {"kind": "labels"},
                "train on a window whose labels have matured (60+ days), or exclude recent rows rather than "
                "learning them as legitimate", (1 - min([c for _, c in slow] or [lab["coverage"]])) * 1e4)

    # ------------------------------------------------------------------ latency
    tail = rep.get("tail_attribution") or {}
    share = tail.get("share") or {}
    if tail.get("dominant") and share.get(tail["dominant"], 0) >= float(th.get("tail_stage_share", 0.4)):
        st = tail["dominant"]
        add("info" if s.get("verdict") == "PASS" else "warning", "latency",
            f"{st.replace('_', ' ')} holds {_pct(share[st], 0)} of the slowest 1% of events",
            f"slowest 1% threshold {tail.get('threshold_ms', 0):.0f} ms; mean {tail['mean_ms_by_stage'].get(st, 0):.0f} ms in {st}",
            {"stage": st, "share": share[st]}, {"kind": "latency"},
            _advice(st, rep.get("topology")))
    if s.get("verdict") == "PASS":
        add("good", "latency", f"within budget: e2e p99 {s.get('e2e_p99_ms', 0):.0f} ms, "
            f"{_pct(s.get('within_budget'), 2)} of events inside {rep.get('budget', {}).get('e2e_p99_ms', 0):.0f} ms",
            evidence={"e2e_p99_ms": s.get("e2e_p99_ms"), "within_budget": s.get("within_budget")})

    # ------------------------------------------------------------------ startup
    if s.get("prepare_s") and s["prepare_s"] > float(th.get("startup_warn_s", 180)):
        steps = sorted(((x["label"], x.get("duration_s") or 0) for x in (rep.get("startup") or {}).get("steps", [])
                        if x.get("id") not in ("measure", "warmup")), key=lambda kv: -kv[1])[:3]
        add("info", "startup", f"preparation took {s['prepare_s']:.0f} s before measuring",
            "; ".join(f"{n.lower()} {v:.0f} s" for n, v in steps), {"prepare_s": s["prepare_s"]}, {"kind": "startup"},
            "keep the pipeline on one world between runs (warm state skips the replay), or shorten backfill_days")

    # ------------------------------------------------------------------ regression vs the previous run
    if prev:
        ps = prev.get("summary") or {}
        pd_ = prev.get("detection") or {}
        pct = float(th.get("regression_pct", 10)) / 100
        cur99, old99 = s.get("e2e_p99_ms"), ps.get("e2e_p99_ms")
        if cur99 and old99 and cur99 > old99 * (1 + pct):
            add("warning", "regression", f"e2e p99 {cur99:.0f} ms vs {old99:.0f} ms in {prev.get('run_id')}",
                f"+{100 * (cur99 / old99 - 1):.0f}% against the previous run of this scenario",
                {"previous": prev.get("run_id"), "p99": cur99, "previous_p99": old99},
                {"kind": "compare", "a": prev.get("run_id"), "b": rep.get("run_id")}, "open Compare to see which stage moved")
        cr, orr = d.get("recall_flagged"), pd_.get("recall_flagged")
        if cr is not None and orr is not None and cr < orr - 0.02:
            add("warning", "regression", f"recall {_pct(cr)} vs {_pct(orr)} in {prev.get('run_id')}",
                "a rule, threshold or model change, or a different world, may explain it",
                {"previous": prev.get("run_id")}, {"kind": "compare", "a": prev.get("run_id"), "b": rep.get("run_id")})

    out.sort(key=lambda x: (SEVERITY[x["severity"]], -x["impact"]))
    for x in out:
        x.pop("impact", None)
    return out


def previous_report(run_dir: Path, scenario: str) -> dict[str, Any] | None:
    """The most recent earlier run of the same scenario with a report (for regression insights)."""
    run_dir = Path(run_dir)
    best = None
    for d in run_dir.parent.glob(f"{scenario}-*"):
        if d == run_dir or not (d / "report.json").exists():
            continue
        if d.name < run_dir.name and (best is None or d.name > best.name):
            best = d
    if best is None:
        return None
    try:
        return json.loads((best / "report.json").read_text())
    except (OSError, ValueError):
        return None
