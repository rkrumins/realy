"""Message types that carry a subset of the keyed entities (pipeline payments-ext): the generator's
standalone messages, the reference engine's per-type folding (the Java twin is MessageTypesTest), and the
pipeline's routing."""
from __future__ import annotations

import collections

import pytest

from realy.bindings.codec import message_class
from realy.bindings.pipeline import pipeline

RATES = {"login": 0.6, "profile_change": 0.05, "card_lifecycle": 0.05, "device_telemetry": 0.3}


@pytest.fixture
def ext(monkeypatch):
    monkeypatch.setenv("PIPELINE", "payments-ext")
    return pipeline()


def _gen(event_types, n_batches=10, seed=5):
    from realy.gen.generator import Generator, sim_start_us
    from realy.scenario import Scenario, WorldSpec
    from realy.world import build_world
    spec = WorldSpec(customers=2000, merchants=300, payees=800)
    sc = Scenario(name="t", seed=seed, world=spec, traffic={"event_types": event_types},
                  fraud={"background_per_1k": 20.0, "mix": {"account_takeover": 1.0, "card_testing": 1.0}})
    g = Generator(build_world(spec, seed, None), sc, 0, 1)
    t = sim_start_us(sc)
    dt = int(300 / g.natural_rate_per_sim_s() * 1e6)
    batches = []
    for _ in range(n_batches):
        batches.append(g.generate(300, t, t + dt))
        t += dt
    return sc, batches


def test_the_pipeline_routes_types_by_topic_and_role(ext):
    assert ext.standalone("login") and ext.standalone("device_telemetry")
    assert not ext.standalone("payment") and not ext.standalone("payment_initiation")
    assert ext.extra_topics() == {"payments.devices.telemetry": ["device_telemetry"]}
    assert ext.topic_of("device_telemetry") == "payments.devices.telemetry"
    assert ext.partition_key_of("device_telemetry") == "device"
    assert pipeline("payments").extra_topics() == {}
    assert ext.validate() == []


def test_standalone_messages_carry_their_payments_entities(ext):
    sc, batches = _gen(RATES)
    by_type = collections.Counter()
    payments = 0
    for b in batches:
        cols = b.columns
        by_id = {}
        for i in range(len(b)):
            by_id.setdefault(int(cols["customer_id"][i]), []).append(i)
        payments += len(b)
        times = [r.t_us for r in b.records]
        assert times == sorted(times)
        for r in b.records:
            if not r.type:
                assert r.scored
                continue
            assert not r.scored
            by_type[r.type] += 1
            t = __import__("realy.bindings", fromlist=["event_type"]).event_type(r.type)
            m = message_class(t.message).FromString(r.payload)
            assert getattr(m, t.time) == r.t_us
            if r.type == "device_telemetry":
                assert r.key == str(m.device_id).encode() and m.device_id != 0
            else:
                assert r.key == str(m.customer_id).encode()
                assert int(m.customer_id) in by_id   # one of the batch's payers
    assert set(by_type) == set(RATES)
    # the mix is per payment generated (Poisson around the rate)
    assert 0.45 * payments < by_type["login"] < 0.75 * payments
    assert by_type["device_telemetry"] > by_type["profile_change"]


def test_payments_are_unchanged_by_the_message_mix(ext):
    sc0, b0 = _gen({})
    sc1, b1 = _gen(RATES)
    assert [p for b in b0 for p in b.payloads] == [p for b in b1 for p in b.payloads]
    assert sc0.fingerprint() == type(sc0).model_validate({**sc0.model_dump(), "traffic": {**sc0.traffic.model_dump(), "event_types": {}}}).fingerprint()
    assert b0[0].records is None                      # no mix: the plain payment path, as before


def test_generation_is_deterministic(ext):
    _, a = _gen(RATES, n_batches=4)
    _, b = _gen(RATES, n_batches=4)
    assert [(r.type, r.payload) for x in a for r in x.records] == [(r.type, r.payload) for x in b for r in x.records]


def test_a_type_that_is_not_standalone_is_rejected(ext):
    with pytest.raises(ValueError, match="not a standalone"):
        _gen({"payment_initiation": 0.5}, n_batches=1)


def _login(t, ok, ip, device=5):
    """A login as verify reads it from the run's log: decoded by its binding's codec (enums as numbers)."""
    from realy.bindings import event_type
    from realy.identity.v1 import identity_pb2 as pb
    m = pb.LoginEvent(event_id=t, event_time_us=t, customer_id=1, device_id=device, ip_v4=ip,
                      outcome=pb.LOGIN_SUCCESS if ok else pb.LOGIN_FAILED_PASSWORD)
    return event_type("login").codec.row(pb.LoginEvent.FromString(m.SerializeToString()))


def _payment(t, customer=1, device=5):
    """A payment as the event log holds it (every field; enums as their numbers)."""
    from realy.bindings import event_type
    ev = event_type("payment").codec.defaults()
    ev.update({"event_id": t, "event_time_us": t, "customer_id": customer, "card_id": 10, "device_id": device,
               "ip_v4": 7, "merchant_id": 3, "amount_usd_minor": 2500, "payment_type": 1, "merchant_country": "GB",
               "ip_country": "GB", "mcc": 5411})
    return ev


def _mask(schema, ev, ty):
    import cel
    from realy.bindings import event_type
    from realy.features.derived import event_view
    ctx = cel.Context()
    ctx.add_variable("event", event_view(ev) if ty == "payment" else event_type(ty).codec.view(ev))
    ctx.add_variable("f", {})
    return sum(1 << b for b, p in enumerate(schema.compiled_filters(ty, "payment")) if p is not None and p.execute(ctx))


def test_a_login_folds_and_a_payment_reads_without_being_counted(ext):
    from realy.features.engine import ReferenceEngine
    from realy.features.schema import load_schema
    s = load_schema()
    assert len(s.features) == 72
    eng = ReferenceEngine(s, ["customer", "device"], default_type="payment", scored_types=["payment"])
    T0, MIN = 1_800_000_000_000_000, 60_000_000
    for i in range(3):
        ev = _login(T0 + i * MIN, False, 100 + i)
        eng.process(ev, _mask(s, ev, "login"), None, event_type="login")
    ev = _login(T0 + 3 * MIN, True, 100)
    eng.process(ev, _mask(s, ev, "login"), None, event_type="login")
    idx = {f.name: f.index for f in s.features}
    out = [0.0] * len(s.features)
    p = _payment(T0 + 4 * MIN)
    eng.process(p, _mask(s, p, "payment"), out, event_type="payment")
    assert out[idx["cust_failed_logins_1h"]] == 3
    assert out[idx["cust_distinct_login_ips_24h"]] == 3
    assert out[idx["cust_count_1h"]] == 1
    assert out[idx["dev_distinct_customers_all_24h"]] == 1
    p2 = _payment(T0 + 5 * MIN, customer=2)
    out2 = [0.0] * len(s.features)
    eng.process(p2, _mask(s, p2, "payment"), out2, event_type="payment")
    assert out2[idx["cust_failed_logins_1h"]] == 0          # another customer
    assert out2[idx["dev_distinct_customers_all_24h"]] == 2  # customer 1 by its login and payment, 2 by its payment


def test_verify_folds_standalone_messages_in_emission_order(ext):
    import pyarrow as pa
    from realy.features.schema import load_schema
    from realy.identity.v1 import identity_pb2 as pb
    from realy.devices.v1 import devices_pb2 as dpb
    from realy.verify.verify import split_standalone, typed_stream
    T0 = 1_800_000_000_000_000
    rows = [("login", T0 + 2, pb.LoginEvent(event_id=1, event_time_us=T0 + 2, customer_id=4, device_id=9, ip_v4=3,
                                            outcome=pb.LOGIN_FAILED_MFA).SerializeToString()),
            ("device_telemetry", T0 + 1, dpb.DeviceTelemetry(event_id=2, event_time_us=T0 + 1, device_id=9, ip_v4=3,
                                                             emulator=True, risk_score=0.7).SerializeToString()),
            ("payment_initiation", T0, b"")]
    parts = pa.table({"type": [r[0] for r in rows], "event_time_us": [r[1] for r in rows],
                      "customer_key": ["4", "9", "4"], "payload": [r[2] for r in rows]})
    rest, typed = split_standalone(parts)
    assert rest["type"].to_pylist() == ["payment_initiation"]
    assert typed["type"].to_pylist() == ["login", "device_telemetry"]
    s = load_schema()
    items = typed_stream(typed, s)
    assert [(t, ty) for t, ty, _, _ in items] == [(T0 + 1, "device_telemetry"), (T0 + 2, "login")]
    _, _, tel, tmask = items[0]
    _, _, login, lmask = items[1]
    assert tel["device_id"] == 9 and tel["ip_v4"] == 3 and "customer_id" not in tel
    bit = {n: 1 << i for i, n in enumerate(s.filters)}
    assert tmask & bit["device_tampered"] and not tmask & bit["login_failed"]
    assert lmask & bit["login_failed"] and not lmask & bit["device_tampered"]
    assert login["customer_id"] == 4 and login["event_time_us"] == T0 + 2


def test_the_report_gives_the_message_mix_per_decided_payment(ext, tmp_path):
    import json
    from realy.report.report import _render_mix, message_mix
    (tmp_path / "emit-0.json").write_text(json.dumps({"typed_sent": {"login": 600, "device_telemetry": 300}}))
    (tmp_path / "emit-1.json").write_text(json.dumps({"typed_sent": {"login": 50, "payment_initiation": 7}}))
    mx = message_mix(tmp_path, 1000)
    assert mx["by_type"] == {"device_telemetry": {"sent": 300, "per_payment": 0.3}, "login": {"sent": 650, "per_payment": 0.65}}
    assert mx["messages"] == 950 and "login 650 (0.65/payment)" in _render_mix(mx)[0]
    assert message_mix(tmp_path / "none", 10) is None


def test_a_candidate_rule_set_that_extends_resolves_with_its_base(tmp_path):
    import yaml
    from realy.scenario import REPO_ROOT, resolve_rule_file, resolve_rule_set
    ext = yaml.safe_load(resolve_rule_file(REPO_ROOT / "rules" / "baseline-ext.yaml"))
    base = yaml.safe_load(resolve_rule_set("baseline"))
    ids = [r["id"] for r in ext["rules"]]
    assert ids[:len(base["rules"])] == [r["id"] for r in base["rules"]] and "ATO_FAILED_LOGINS_NEW_DEVICE" in ids
    assert ext["version"] == "baseline-v5-ext1"
    # a copy elsewhere still finds its base in rules/
    (tmp_path / "mine.yaml").write_text("extends: baseline\nversion: mine\nrules: []\n")
    assert len(yaml.safe_load(resolve_rule_file(tmp_path / "mine.yaml"))["rules"]) == len(base["rules"])
