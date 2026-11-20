"""Unit tests for the instrument's own logic (no Kafka/Docker needed)."""
from __future__ import annotations

import json
import math

import numpy as np
import pytest

from realy.budget.check import check, sample
from realy.common import control as ctl
from realy.features import load_schema
from realy.features.derived import event_view
from realy.features.engine import ReferenceEngine
from realy.scenario import SCENARIO_DIR, load_scenario
from realy.verify.verify import ProxyScorer, Rules

MIN = 60_000_000
T0 = 1_800_000_000_000_000


def ev(t, amount, card_present, merchant, lat=0.0, lon=0.0, device=0, **kw):
    base = dict(event_id=t, event_time_us=t, payment_type=1, customer_id=1, account_id=1, card_id=10, card_type=1,
                amount_minor=amount, currency="GBP", amount_usd_minor=amount, merchant_id=merchant, mcc=5411,
                merchant_country="GB", terminal_id=0, channel=1 if card_present else 2,
                entry_mode=1 if card_present else 6, card_present=card_present, lat=lat, lon=lon, device_id=device,
                ip_v4=0, ip_country="", ip_asn=0, payee_id=0, cvv_match=True, avs_match=True, recurring=False,
                credit_limit_usd=0)
    base.update(kw)
    return base


def mask_of(schema, e):
    import cel
    ctx = cel.Context()
    ctx.add_variable("event", event_view(e))
    ctx.add_variable("f", {})
    m = 0
    for i, expr in enumerate(schema.filters.values()):
        if cel.compile(expr).execute(ctx):
            m |= 1 << i
    return m


def run(engine, schema, e):
    out = [0.0] * len(schema.features)
    engine.process(e, mask_of(schema, e), out)
    return out


def test_reference_engine_matches_java_semantics():
    """Same cases as java EntityAggregatorTest (the two engines must agree)."""
    s = load_schema()
    eng = ReferenceEngine(s, ["customer", "card"])
    for i in range(4):
        o = run(eng, s, ev(T0 + i * MIN, 150, False, 7))
    assert o[s.index_of("card_small_cnp_count_10m")] == 4
    assert o[s.index_of("card_count_5m")] == 4
    o = run(eng, s, ev(T0 + 20 * MIN, 150, False, 7))
    assert o[s.index_of("card_small_cnp_count_10m")] == 1
    o = run(eng, s, ev(T0 + 21 * MIN, 5000, False, 8))
    assert o[s.index_of("card_small_cnp_count_10m")] == 1
    assert o[s.index_of("card_distinct_merchants_10m")] == 2
    assert o[s.index_of("card_amount_sum_1h")] == 5 * 150 + 5000
    assert o[s.index_of("card_secs_since_last")] == 60


def test_geo_speed_and_novelty():
    s = load_schema()
    eng = ReferenceEngine(s, ["customer", "card"])
    g = s.index_of("card_geo_speed_kmh")
    assert run(eng, s, ev(T0, 2000, True, 1, 51.5, -0.12))[g] == 0
    assert run(eng, s, ev(T0 + 30 * MIN, 2000, True, 2, 40.71, -74.0))[g] > 9000
    nov = s.index_of("cust_device_novel")
    assert run(eng, s, ev(T0 + 40 * MIN, 100, False, 3, device=99))[nov] == 1
    assert run(eng, s, ev(T0 + 41 * MIN, 100, False, 3, device=99))[nov] == 0
    assert run(eng, s, ev(T0 + 42 * MIN, 100, False, 3, device=100))[nov] == 1


def test_budget_check_flags_brief_example_infeasible():
    r = check(load_scenario("user-example-100ms"), n=40_000)
    assert r["verdict"] == "INFEASIBLE"
    assert r["fallback_rate"] > 0.5
    ok = check(load_scenario("smoke"), n=40_000)
    assert ok["verdict"] == "FEASIBLE"


def test_latency_sampling_matches_parameters():
    rng = np.random.default_rng(1)
    v, _, _ = sample({"lognormal": {"median_ms": 5, "p99_ms": 40}}, 200_000, rng)
    assert 4.8 < np.median(v) < 5.2
    assert 36 < np.percentile(v, 99) < 44
    v, e, t = sample({"uniform": {"min_ms": 30, "max_ms": 100}, "error_rate": 0.1}, 100_000, rng)
    assert v.min() >= 30 and v.max() <= 100 and 0.09 < e.mean() < 0.11


@pytest.mark.parametrize("path", sorted(SCENARIO_DIR.glob("*.yaml")), ids=lambda p: p.stem)
def test_every_scenario_validates(path):
    sc = load_scenario(path)
    cfg = sc.scoring_config()
    assert cfg["rule_set_yaml"] and cfg["budget_ms"] > 0


def test_control_json_preserves_presence_of_defaults():
    m = ctl.control_message("r1", "gateway", scoring={"self_inclusion": False, "feature_deadline_ms": 0})
    raw = json.loads(ctl.to_json(m))
    assert raw["scoring"] == {"self_inclusion": False, "feature_deadline_ms": 0}
    with pytest.raises(Exception):
        ctl.control_message("r1", "gateway", scoring={"no_such_field": 1})


def test_rules_and_scorer():
    s = load_schema()
    rules = Rules((SCENARIO_DIR.parent / "rules" / "baseline.yaml").read_text())
    f = {x.name: 0.0 for x in s.features}
    f["card_small_cnp_count_10m"] = 3.0
    hits = rules.evaluate(f, event_view(ev(T0, 150, False, 1)))
    assert ("CARD_TESTING", "DECLINE") in hits
    assert rules.decide(hits, float("nan")) == 3
    assert rules.decide([], 0.95) == 3 and rules.decide([], 0.1) == 1
    # v4: the review band steps up a payment that can be stepped up, and reviews one that cannot
    assert rules.decide([], 0.6, True) == 4 and rules.decide([], 0.6, False) == 2
    sc = ProxyScorer()
    x = np.zeros((1, len(s.features)), np.float32)
    p = float(sc.score(x)[0])
    assert 0 < p < 0.05 and not math.isnan(p)
