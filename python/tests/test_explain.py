"""Decision explanations: attribution completeness, policy branches and label stories."""
from __future__ import annotations

import numpy as np

from realy.explain import explain_rules, integrated_gradients, label_story, policy_path
from realy.features.schema import load_schema
from realy.usecases import registry
from realy.verify.verify import ProxyScorer, Rules

DAY = 86_400_000_000


def test_integrated_gradients_sum_to_the_logit_difference():
    sc = ProxyScorer()
    rng = np.random.default_rng(3)
    for _ in range(5):
        x = sc.mean + sc.std * rng.normal(0, 2, sc.mean.shape)
        ig = integrated_gradients(sc, x, steps=256)
        assert abs(ig["attribution"].sum() - (ig["logit"] - ig["baseline_logit"])) < 0.02 * max(1.0, abs(ig["logit"]))
        assert abs(ig["score"] - float(sc.score(x[None, :].astype(np.float32))[0])) < 1e-4   # same model as served


def test_policy_branches_mirror_the_decision_precedence():
    rules = Rules((__import__("pathlib").Path(__file__).parents[2] / "rules" / "baseline.yaml").read_text())
    mk = lambda *acts: [{"id": f"R{i}", "action": a, "hit": True} for i, a in enumerate(acts)]  # noqa: E731
    assert policy_path(mk("REVIEW", "DECLINE"), 0.1, 0.55, 0.9, "DECLINE", "FULL")["branch"] == "decline rule"
    assert policy_path(mk("REVIEW"), 0.95, 0.55, 0.9, "DECLINE", "FULL")["branch"] == "score decline"
    assert policy_path(mk("REVIEW"), 0.2, 0.55, 0.9, "REVIEW", "FULL")["branch"] == "review rule"
    assert policy_path([], 0.6, 0.55, 0.9, "REVIEW", "FULL")["branch"] == "score review"
    p = policy_path([], None, 0.55, 0.9, "APPROVE", "RULES_ONLY_FALLBACK")
    assert p["branch"] == "approve" and "without the model" in p["why"]
    # step-up: a CHALLENGE rule, and the challenge band, only for payments that can be stepped up
    assert policy_path(mk("CHALLENGE"), 0.1, 0.55, 0.9, "CHALLENGE", "FULL")["branch"] == "challenge rule"
    p = policy_path(mk("CHALLENGE"), 0.1, 0.55, 0.9, "REVIEW", "FULL", challengeable=False)
    assert p["branch"] == "challenge rule, not challengeable" and "review instead" in p["why"]
    assert policy_path([], 0.6, 0.55, 0.9, "CHALLENGE", "FULL", challenge=0.55)["branch"] == "score challenge"
    p = policy_path([], 0.6, 0.55, 0.9, "REVIEW", "FULL", challenge=0.55, challengeable=False)
    assert p["branch"] == "score review" and "cannot be stepped up" in p["why"]
    # the branch agrees with the verifier's decide() on random cases
    rng = np.random.default_rng(1)
    code = {"decline rule": 3, "score decline": 3, "challenge rule": 4, "challenge rule, not challengeable": 2,
            "score challenge": 4, "review rule": 2, "score review": 2, "approve": 1}
    for _ in range(400):
        acts = [a for a in ("DECLINE", "CHALLENGE", "REVIEW") if rng.random() < 0.3]
        score = float(rng.random())
        can = bool(rng.random() < 0.5)
        dec = rules.decide([("x", a) for a in acts], score, can)
        p = policy_path(mk(*acts), score, rules.review, rules.decline, None, "FULL", rules.challenge, can)
        assert code[p["branch"]] == dec and p["expected"] == {1: "APPROVE", 2: "REVIEW", 3: "DECLINE", 4: "CHALLENGE"}[dec]


def test_rule_explanations_match_the_verifier_and_show_the_values():
    from pathlib import Path
    text = (Path(__file__).parents[2] / "rules" / "baseline.yaml").read_text()
    names = [f.name for f in load_schema().features]
    fmap = {n: 0.0 for n in names}
    fmap.update(card_small_cnp_count_10m=3.0, amount_usd=1.2)
    ev = {"payment_type": "CARD_PURCHASE", "channel": "ECOM", "entry_mode": "ECOM_NON_3DS", "card_present": False,
          "is_atm": False, "merchant_country": "GB", "ip_country": "GB", "cvv_match": True, "avs_match": True}
    res = explain_rules(text, fmap, ev)
    served = {rid for rid, _ in Rules(text).evaluate(fmap, ev)}
    assert {r["id"] for r in res if r["hit"]} == served and "CARD_TESTING" in served
    ct = next(r for r in res if r["id"] == "CARD_TESTING")
    assert ct["values"]["f.card_small_cnp_count_10m"] == 3.0


def test_label_stories():
    reg = registry()
    t = reg.by_name["card_testing"]
    end = 100 * DAY
    base = dict(event_id=1, event_time_us=end - DAY, is_fraud=True, fraud_type=t.id, campaign_id=7,
                campaign_role=t.role_code("test"), label_is_fraud=True, label_source=1, label_type=t.id)
    s = label_story(dict(base, label_delay_us=5 * DAY), reg, end, 30, (3, 11))
    assert s["title"] == "fraud: card_testing, test (3 of 11)" and s["training"].startswith("fraud")
    late = label_story(dict(base, label_delay_us=40 * DAY), reg, end, 30)
    assert late["training"].startswith("LEGITIMATE") and "past a training set cut 30 days" in late["training"]
    assert label_story(dict(base, label_delay_us=40 * DAY), reg, end, 60)["training"].startswith("fraud")
    assert label_story(dict(base, label_delay_us=-1, label_is_fraud=False), reg, end)["training"].startswith("LEGITIMATE (never")
    ff = label_story(dict(base, is_fraud=False, fraud_type=0, campaign_id=0, campaign_role=0, label_type=10,
                          label_delay_us=10 * DAY), reg, end)
    assert ff["truth"] == "legitimate" and "friendly fraud" in ff["observed"]
    b = reg.by_name["bust_out"]
    bu = label_story(dict(base, is_fraud=False, fraud_type=0, campaign_role=b.role_code("build_up"),
                          label_is_fraud=False, label_delay_us=-1), reg, end)
    assert "bust_out campaign's story (build up)" in bu["title"]
