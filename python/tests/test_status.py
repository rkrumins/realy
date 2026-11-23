"""Entity status as change events folded into state (pipeline payments-status): the `latest` aggregation, `gate:`
on stream features, the schema rules around them, and the simulated systems of record that publish the changes."""
from __future__ import annotations

import collections

import pytest

from realy.features import aggregations as A
from realy.features.schema import FeatureSpec


@pytest.fixture
def status(monkeypatch):
    from realy.bindings.pipeline import pipeline
    monkeypatch.setenv("PIPELINE", "payments-status")
    return pipeline()


def _spec(value="v", window_us=0, default=0.0):
    return FeatureSpec(index=0, name="x", source="stream", entity="card", agg="latest", value=value,
                       window_us=window_us, granularity_us=window_us, default=default)


def test_latest_holds_the_last_change_and_reads_the_one_before_it_earlier():
    a, w = A.get("latest"), A.Window()
    s = _spec()
    assert a.read(s, w, {"v": 0}, 100, True) == 0.0                 # nothing yet: the default
    assert a.apply(s, w, {"v": 2.0}, 100, True) == 2.0              # BLOCKED
    assert a.read(s, w, {}, 10**15, True) == 2.0                    # holds however long after: no window
    assert a.apply(s, w, {"v": 9.0}, 50, True) == 0.0               # an older change arrived late: ignored (as of
    assert a.read(s, w, {}, 120, True) == 2.0                       # its own time the value was still the default)
    assert a.apply(s, w, {"v": 1.0}, 200, True) == 1.0              # ACTIVE again
    assert a.read(s, w, {}, 150, True) == 2.0                       # before the latest change: the value before it
    assert a.apply(s, w, {"v": 7.0}, 300, False) == 1.0             # with `value:`, a non-matching change is not folded
    assert a.sticky(0) and not a.sticky(3600)


def test_latest_without_a_value_is_the_filter_bit_of_the_latest_change():
    a, w = A.get("latest"), A.Window()
    s = _spec(value=None)
    assert a.apply(s, w, {}, 100, True) == 1.0      # a blocking change
    assert a.apply(s, w, {}, 200, False) == 0.0     # an unblocking one (every folded change counts)
    s1 = _spec(value=None, window_us=1_000, default=-1.0)
    w1 = A.Window()
    a.apply(s1, w1, {}, 100, True)
    assert a.read(s1, w1, {}, 1_100, True) == 1.0 and a.read(s1, w1, {}, 1_101, True) == -1.0   # expired


def test_conformance_cases_cover_both_modes_of_latest():
    cases = [c for c in A.conformance_cases()["cases"] if c["agg"] == "latest"]
    assert len(cases) == 3 and any("value" in c and c["value"] is None for c in cases)
    with_value = next(c for c in cases if "value" not in c and c["window_s"] == 0)
    assert any(st["apply"] != 0 for st in with_value["steps"])


def test_the_status_schema_extends_rich_with_status_and_gated_features(status):
    from realy.features.schema import load_schema
    s = load_schema()
    names = s.names
    from realy.scenario import REPO_ROOT
    rich = load_schema(REPO_ROOT / "contracts/features/feature_schema_rich.yaml")
    assert names[:73] == rich.names                                                       # appended only
    blocked = s.features[names.index("card_blocked")]
    assert (blocked.agg, blocked.window_us, blocked.value, blocked.on) == ("latest", 0, None, ("card_status_changed",))
    gated = s.features[names.index("dev_blocked_card_attempts_24h")]
    assert gated.gate == "card_blocked" and s.gate_index(gated) == blocked.index
    assert s.stream_entities() == ["customer", "account", "card", "device", "ip", "merchant", "terminal", "payee"]
    assert status.validate() == []
    # the layouts of the base and rich schemas are untouched by `gate` joining the layout
    assert rich.state_layout() != s.state_layout()


def _schema(tmp_path, feats: str):
    p = tmp_path / f"s{abs(hash(feats))}.yaml"
    p.write_text("schema_version: 9\nentities: {customer: {key: customer_id}, card: {key: card_id}, device: {key: device_id}}\n"
                 "filters: {}\ncountries: [US]\nfeatures:\n" + feats)
    from realy.features.schema import _load_schema
    return _load_schema(str(p))


def test_schema_rules_for_windows_and_gates(tmp_path):
    with pytest.raises(ValueError, match="needs a `window`"):
        _schema(tmp_path, "  - {name: c, source: stream, entity: card, agg: count}\n")
    with pytest.raises(ValueError, match="computed after the customer stage"):
        _schema(tmp_path, "  - {name: b, source: stream, entity: card, agg: latest}\n"
                          "  - {name: g, source: stream, entity: customer, agg: count, gate: b, window: 1h, granularity: 1m}\n")
    with pytest.raises(ValueError, match="not a stream feature defined before it"):
        _schema(tmp_path, "  - {name: g, source: stream, entity: device, agg: count, gate: b, window: 1h, granularity: 1m}\n"
                          "  - {name: b, source: stream, entity: card, agg: latest}\n")
    ok = _schema(tmp_path, "  - {name: b, source: stream, entity: card, agg: latest, window: 1h}\n"
                           "  - {name: g, source: stream, entity: device, agg: count, gate: b, window: 1h, granularity: 1m}\n")
    assert ok.features[0].granularity_us == ok.features[0].window_us      # a bucketless shape needs no granularity


def test_a_gated_feature_folds_only_while_its_gate_is_on(status):
    from realy.features.engine import ReferenceEngine
    from realy.features.schema import load_schema
    s = load_schema()
    eng = ReferenceEngine(s, ["card", "device"], default_type="payment", scored_types=["payment"])
    bit = 1 << list(s.filters).index("card_blocking")
    idx = s.names.index("dev_blocked_card_attempts_24h")
    blk = s.names.index("card_blocked")

    blank = {f.value: 0 for f in s.stream_features() if f.value} | {"lat": 0.0, "lon": 0.0}

    def pay(t):
        out = [0.0] * len(s.features)
        eng.process(blank | {"event_time_us": t, "card_info.id": 10, "device_id": 7}, 0, out, "payment")
        return out

    assert pay(1_000)[idx] == 0.0
    eng.process(blank | {"event_time_us": 2_000, "card_info.id": 10, "status": 2}, bit, None, "card_status_changed")
    out = pay(3_000)
    assert out[blk] == 1.0 and out[idx] == 1.0           # the card stage's card_blocked gated the device's count
    assert pay(4_000)[idx] == 2.0
    eng.process(blank | {"event_time_us": 5_000, "card_info.id": 10, "status": 1}, 0, None, "card_status_changed")
    assert pay(6_000)[idx] == 2.0 and pay(6_000)[blk] == 0.0   # unblocked: no longer counted, the count stays


def _gen(status_spec, n_batches=12, seed=5, mix=None):
    from realy.gen.generator import Generator, sim_start_us
    from realy.scenario import Scenario, WorldSpec
    from realy.world import build_world
    spec = WorldSpec(customers=2000, merchants=300, payees=800)
    sc = Scenario(name="t", seed=seed, world=spec, pipeline="payments-status",
                  traffic={"status": status_spec, "event_blocks": ["card_info", "account_info"]},
                  fraud={"background_per_1k": 40.0, "mix": mix or {"lost_stolen": 1.0, "card_testing": 1.0,
                                                                     "account_takeover": 1.0}})
    g = Generator(build_world(spec, seed, None), sc, 0, 1)
    t = sim_start_us(sc)
    dt = int(400 / g.natural_rate_per_sim_s() * 1e6)
    batches = []
    for _ in range(n_batches):
        batches.append(g.generate(400, t, t + dt))
        t += dt
    return sc, g, batches


def test_the_systems_of_record_publish_changes_in_time_order(status):
    from realy.bindings import event_type
    from realy.bindings.codec import message_class
    sc, g, batches = _gen({"issuer_block_delay_s": 1.0, "temporary_locks_per_1k": 5.0, "aml_holds_per_1k": 5.0,
                           "initial_listed_mules": 0.5})
    by = collections.Counter()
    for b in batches:
        times = [r.t_us for r in b.records]
        assert times == sorted(times)
        for r in b.records:
            if not r.type:
                continue
            by[r.type] += 1
            m = message_class(event_type(r.type).message).FromString(r.payload)
            assert m.event_time_us == r.t_us and not r.scored
            key = m.payee_id if r.type == "payee_listing_changed" else m.customer_id
            assert r.key == str(key).encode()
    summary = g.sor.summary()["changes"]
    assert by["card_status_changed"] > 0 and by["account_status_changed"] > 0
    assert summary.get("card_status_changed:INITIAL", 0) > 0 and summary.get("card_status_changed:ISSUER_MONITORING", 0) > 0
    assert summary.get("card_status_changed:TEMPORARY_LOCK", 0) > 0
    assert by["payee_listing_changed"] == summary["payee_listing_changed:INITIAL"] > 0


def test_genuine_payments_stop_on_a_blocked_card_and_fraud_continues(status):
    from realy.cards.v1 import cards_pb2
    _, g, batches = _gen({"issuer_block_delay_s": 1.0, "initial_blocked_cards": 0.05})
    blocked: dict[int, list[list[int]]] = {}       # card -> [from, until) intervals, from the change feed
    for b in batches:
        for r in b.records:
            if r.type == "card_status_changed":
                m = cards_pb2.CardStatusChanged.FromString(r.payload)
                iv = blocked.setdefault(m.card_id, [])
                if m.status != 1 and (not iv or iv[-1][1] is not None):
                    iv.append([m.event_time_us, None])
                elif m.status == 1 and iv and iv[-1][1] is None:
                    iv[-1][1] = m.event_time_us
    assert g.sor.dropped > 0
    genuine_in, fraud_in = 0, 0
    for b in batches:
        c = b.columns
        for i in range(len(b)):
            t = int(c["event_time_us"][i])
            if any(lo <= t < (hi if hi is not None else 1 << 62) for lo, hi in blocked.get(int(c["card_id"][i]), [])):
                if c["is_fraud"][i]:
                    fraud_in += 1
                else:
                    genuine_in += 1
    assert genuine_in == 0 and fraud_in > 0


def test_status_changes_are_deterministic_and_leave_plain_payments_alone(status):
    _, _, a = _gen({}, n_batches=4)
    _, _, b = _gen({}, n_batches=4)
    assert [(r.type, r.payload) for x in a for r in x.records] == [(r.type, r.payload) for x in b for r in x.records]
    from realy.scenario import Scenario
    base = Scenario(name="t")
    assert base.fingerprint() == Scenario(name="t", traffic={"status": None}).fingerprint()


def test_status_needs_a_pipeline_with_the_change_feeds(monkeypatch):
    monkeypatch.setenv("PIPELINE", "payments")
    with pytest.raises(ValueError, match="payments-status"):
        _gen({}, n_batches=1)
