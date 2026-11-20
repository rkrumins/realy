"""Report insights: data-driven thresholds, ranking, and the numbers they rest on."""
from __future__ import annotations

from realy.report.insights import base_rate_precision, build_insights, load_config
from realy.usecases import registry


def _rep(**over):
    rep = {"run_id": "r2", "budget": {"e2e_p99_ms": 150}, "manifest": {"gates": {"features_caught_up": True}},
           "summary": {"verdict": "PASS", "emitted": 1000, "missing": 0, "duplicates": 0, "e2e_p99_ms": 90,
                       "within_budget": 0.999, "prepare_s": 60},
           "detection": {"fraud_usd_total": 100_000, "fraud_usd_approved": 20_000, "recall_flagged": 0.9,
                         "precision_flagged": 0.3, "false_positive_rate": 0.04,
                         "by_type": {"bust_out": {"events": 60, "campaigns": 20, "recall_flagged": 0.4, "recall_declined": 0.05,
                                                  "usd_approved": 15_000, "approved_before_first_decline_mean": 3.0},
                                     "card_testing": {"events": 600, "campaigns": 40, "recall_flagged": 0.9,
                                                      "recall_declined": 0.85, "usd_approved": 5_000,
                                                      "approved_before_first_decline_mean": 2.0}},
                         "by_rule": {"NOISY": {"hits": 900, "fraud": 30, "legit": 870, "precision": 0.033, "legit_hit_rate": 0.03},
                                     "GOOD": {"hits": 300, "fraud": 299, "legit": 1, "precision": 0.997, "legit_hit_rate": 0.0}},
                         "model": {"scored": 900, "rules_only_fallback": 100, "fallback_recall_flagged": 0.3,
                                   "roc_auc": 0.99, "pr_auc": 0.9, "fraud_base_rate": 0.02},
                         "evaluation": {"sample": "out-of-sample"},
                         "labels": {"maturity_days": 30, "coverage": 0.9, "by_type": {"bust_out": 0.3, "card_testing": 0.95}}},
           "tail_attribution": {"dominant": "egress", "share": {"egress": 0.6}, "mean_ms_by_stage": {"egress": 70},
                                "threshold_ms": 120}}
    rep.update(over)
    return rep


def test_the_findings_that_matter_fire_and_rank_first():
    ins = build_insights(_rep(), load_config(), None, registry())
    titles = [x["title"] for x in ins]
    assert any("NOISY" in t and "3.3%" in t for t in titles)
    assert not any("GOOD" in t for t in titles)                                   # precise rules stay quiet
    assert any(t.startswith("Bust-out") and "$15,000" in t for t in titles)       # registry title + $ impact
    assert any("bust_out is declined only 5%" in t for t in titles)
    assert any("10.0% of decisions fell back" in t for t in titles)
    assert any("labels" == x["area"] and "bust_out 30%" in x["detail"] for x in ins)
    sev = [x["severity"] for x in ins]
    assert sev == sorted(sev, key=["critical", "warning", "info", "good"].index)  # ranked
    assert all(x.get("link") is None or "kind" in x["link"] for x in ins)


def test_in_sample_detection_is_critical_and_base_rate_maths():
    ins = build_insights(_rep(detection=dict(_rep()["detection"], evaluation={"sample": "in-sample", "reason": "same world"})),
                         load_config(), None, registry())
    assert ins[0]["severity"] == "critical" and "in-sample" in ins[0]["title"]
    p = base_rate_precision(0.9, 0.04, 0.001)
    assert abs(p - 0.9 * 0.001 / (0.9 * 0.001 + 0.04 * 0.999)) < 1e-12


def test_regression_against_the_previous_run():
    prev = _rep(run_id="r1")
    prev["summary"] = dict(prev["summary"], e2e_p99_ms=60)
    ins = build_insights(_rep(), load_config(), prev, registry())
    reg = [x for x in ins if x["area"] == "regression"]
    assert reg and "90 ms vs 60 ms in r1" in reg[0]["title"] and reg[0]["link"] == {"kind": "compare", "a": "r1", "b": "r2"}


def test_step_up_findings_name_what_it_does_not_stop_and_what_it_costs():
    rep = _rep()
    d = rep["detection"]
    d.update(fraud_usd_paid=30_000, legit_events=100_000, stepup={
        "challenged": 2_000, "legit_challenged": 1_500, "fraud_challenged": 500, "legit_challenge_rate": 0.015,
        "fraud_challenge_rate": 0.3, "legit": {"pass": 1_350, "fail": 30, "abandon": 120},
        "fraud": {"pass": 150, "fail": 350, "abandon": 0}, "fraud_stopped": 350, "fraud_passed": 150,
        "fraud_usd_passed": 9_000, "legit_abandoned": 120, "lost_sales_usd": 48_000, "legit_failed": 30,
        "by_type": {"account_takeover": {"challenged": 200, "pass": 140, "fail": 60, "abandon": 0, "usd_passed": 8_500},
                    "card_testing": {"challenged": 300, "pass": 10, "fail": 290, "abandon": 0, "usd_passed": 500}}})
    for t in d["by_type"].values():
        t["usd_paid"] = t["usd_approved"] + 2_000
        t["recall_stopped"] = t["recall_declined"] + 0.05
    ins = build_insights(rep, load_config(), None, registry())
    titles = [x["title"] for x in ins]
    assert any(t.startswith("step-up does not stop Account takeover") and "140 of 200" in t for t in titles)
    assert not any("does not stop" in t and "card testing" in t.lower() for t in titles)      # 3% pass: it works
    lost = next(x for x in ins if "gave up at a step-up" in x["title"])
    assert lost["severity"] == "warning" and "$48,000" in lost["title"] and lost["link"] == {"kind": "stepup"}
    assert any(x["severity"] == "good" and "stopped 350" in x["title"] for x in ins)
    # money that moved, and "stopped", once the report has them
    assert any(t.startswith("Bust-out") and "$17,000" in t and "fraud $ paid" in t for t in titles)
    assert any("bust_out is stopped only 10%" in t for t in titles)
