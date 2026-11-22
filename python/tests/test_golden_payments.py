"""The golden payments set (contracts/testdata/golden/payments, see realy/golden.py): the Python twin must
reproduce what the Java scoring path recorded for it (GoldenPaymentsTest), feature by feature, rule by
rule, score and outcome; and the set itself must still be what the generator produces.

Java replays the events as the deployed pipeline does: the gateway scores each event before the feature
job folds it in. Customer and card blobs carry aggregator state, so their features include the event
(re-windowed at its time) and equal the reference exactly; so do profile and derived features. The
other entities' blobs carry values, which the gateway approximates with the event's contribution (the
run verifier reports those as an agreement rate); the Java digest pins them byte for byte.
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest

from realy import golden
from realy.features import load_schema
from realy.features.derived import compute_derived, event_view
from realy.features.engine import ReferenceEngine
from realy.policy import RuleSet
from realy.verify.verify import ProxyScorer

D = golden.OUT
J = D / "java"
STATE_ENTITIES = ("customer", "card")


def _tsv() -> list[dict[str, str]]:
    with gzip.open(J / "decisions.tsv.gz", "rt") as f:
        head, *rows = f.read().splitlines()
    cols = head.split("\t")
    return [dict(zip(cols, r.split("\t"))) for r in rows]


@pytest.fixture(scope="module")
def recorded():
    if not (J / "digests.tsv").exists():
        pytest.skip("Java side not recorded")
    schema = load_schema()
    nf = len(schema.features)
    feats = np.frombuffer(gzip.decompress((J / "features.f32.gz").read_bytes()), "<f4").reshape(-1, nf)
    return schema, feats, _tsv()


@pytest.fixture(scope="module")
def events():
    t = pq.read_table(D / "events.parquet")
    return {c: t[c].to_numpy(zero_copy_only=False) for c in t.column_names}


def test_golden_set_is_what_the_generator_produces():
    """A generator change that alters payments shows up here first (rebuild with python -m realy.golden)."""
    man = json.loads((D / "manifest.json").read_text())
    payloads, _, world = golden.generate(man["events"])
    import hashlib
    assert hashlib.sha256(b"".join(payloads)).hexdigest() == man["events_sha256"]
    assert world.content_hash == man["world_hash"]
    assert golden.scenario().fingerprint() == man["scenario_fingerprint"]


def test_state_layout_and_schema_pinned():
    """The state layout and schema version decide whether warm Flink state is reused: a refactor must not
    move them for payments by accident."""
    man = json.loads((D / "manifest.json").read_text())
    schema = load_schema()
    assert schema.state_layout() == man["state_layout"]
    assert schema.version == man["schema_version"]


def test_twin_reproduces_java_features_rules_score_and_outcome(recorded, events):
    import cel
    from realy.world import build_world
    schema, served, rows = recorded
    n = len(rows)
    assert served.shape[0] == n == events["event_id"].shape[0]
    assert [int(r["event_id"]) for r in rows] == events["event_id"].astype(np.uint64).tolist()
    world = build_world(golden.SPEC, golden.SEED, None)
    from realy.runner.seed import profile_entities
    profiles = {e: (world.profile_arrays(e), schema.profile_features(e),
                    world.profile_rows(e, events[schema.entities[e]])) for e in profile_entities(schema)}
    engine = ReferenceEngine(schema)
    filt = [cel.compile(x) for x in schema.filters.values()]
    names = [c for c in events if c not in ("t_intended_us", "t_intended_offset_us")]
    lists = {k: events[k].tolist() for k in names}
    shared = [e for e in schema.stream_entities() if e not in STATE_ENTITIES]
    idx = {e: [s.index for s in schema.stream_features(e)] for e in schema.stream_entities()}
    bad: dict[str, int] = {}
    buf = [0.0] * len(schema.features)
    for i in range(n):
        ev = {k: lists[k][i] for k in names}
        view = event_view(ev)
        ctx = cel.Context()
        ctx.add_variable("event", view)
        ctx.add_variable("f", {})
        mask = sum(1 << b for b, p in enumerate(filt) if p.execute(ctx))
        engine.process(ev, mask, buf)
        v = np.asarray(buf, np.float64).astype(np.float32)
        # Shared entities: the gateway serves their stored values plus this event's contribution, an
        # approximation `verify` reports as an agreement rate; Java's byte digest pins them. Derived
        # features are checked on the inputs as served.
        for e in shared:
            v[idx[e]] = served[i][idx[e]]
        attrs = None
        for ent, (arr, specs, prow) in profiles.items():
            r = int(prow[i])
            for s in specs:
                v[s.index] = np.float32(arr[s.field][r]) if r >= 0 else np.float32(s.default)
            if ent == "customer" and r >= 0:
                attrs = {a: np.float32(arr[a][r]) for a in schema.profile_attributes.get("customer", [])}
        compute_derived(schema, ev, attrs, v)
        close = np.isclose(served[i], v, rtol=1e-5, atol=1e-5)
        for k in np.flatnonzero(~close):
            nm = schema.features[k].name
            bad[nm] = bad.get(nm, 0) + 1
    assert not bad, f"features where the twin differs from Java (events): {bad}"

    # rules, score, outcome from the served vectors, as `verify` does for a run
    cfg = json.loads((D / "scoring_config.json").read_text())
    rules = RuleSet(cfg["rule_set_yaml"]).with_thresholds(cfg.get("review_threshold"), cfg.get("decline_threshold"),
                                                          cfg.get("challenge_threshold"))
    scorer = ProxyScorer()
    py = scorer.score(served)
    rule_bad, score_bad, out_bad = [], [], []
    for i, r in enumerate(rows):
        ev = {k: lists[k][i] for k in names}
        view = event_view(ev)
        hits = rules.evaluate({f.name: float(served[i][f.index]) for f in schema.features}, view)
        if {h for h, _ in hits} != {x for x in r["rule_hits"].split(",") if x}:
            rule_bad.append((r["event_id"], sorted(h for h, _ in hits), r["rule_hits"]))
        java_score = float(np.frombuffer(int(r["score_bits"], 16).to_bytes(4, "little"), "<f4")[0])
        if abs(float(py[i]) - java_score) > 1e-5:
            score_bad.append((r["event_id"], float(py[i]), java_score))
        got = ("APPROVE", "REVIEW", "DECLINE", "CHALLENGE")[rules.decide(hits, java_score, rules.challengeable(view)) - 1]
        if got != r["outcome"]:
            out_bad.append((r["event_id"], got, r["outcome"]))
    assert not rule_bad, rule_bad[:5]
    assert not score_bad, score_bad[:5]
    assert not out_bad, out_bad[:5]


def test_scenario_fingerprints_pinned():
    """A bundled scenario's fingerprint is its decisions' policy_version: unless its file was edited, it
    must not move (re-pin a deliberate change with python -m realy.golden --scenarios)."""
    import hashlib
    from realy.scenario import SCENARIO_DIR, load_scenario
    pins = json.loads(golden.SCENARIOS.read_text())["scenarios"]
    moved = []
    for name, pin in pins.items():
        f = SCENARIO_DIR / f"{name}.yaml"
        if not f.exists() or hashlib.sha256(f.read_bytes()).hexdigest() != pin["file_sha256"]:
            continue  # edited or removed on purpose
        if load_scenario(f).fingerprint() != pin["fingerprint"]:
            moved.append(name)
    assert not moved, f"fingerprints moved without a scenario edit (a new setting missing from FINGERPRINT_OPTIONAL?): {moved}"


def test_new_scenario_settings_are_fingerprint_optional():
    """Every setting added since the pins must be listed in FINGERPRINT_OPTIONAL, so its default never
    changes an existing scenario's policy_version."""
    from realy.scenario import FINGERPRINT_OPTIONAL, Scenario
    pinned = set(json.loads(golden.SCENARIOS.read_text())["settings"])
    now = golden.scenario_paths(Scenario(name="x").model_dump(mode="json"))
    new = [p for p in now if p not in pinned and not any(p == o or p.startswith(o + ".") for o in FINGERPRINT_OPTIONAL)]
    assert not new, f"add these new scenario settings to FINGERPRINT_OPTIONAL in scenario.py: {new}"
