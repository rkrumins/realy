"""The declarative flow generator (contracts/transactions/*.yaml `generate:`) and the card payment flow:
several flows in one run, each payment sent as its messages, assembling back into exactly what the
generator drew."""
from __future__ import annotations

import collections

import pytest

from realy.bindings import event_type
from realy.bindings.codec import message_class
from realy.bindings.transactions import Assembler, parse, transaction
from realy.gen.flows import validate_generate

FLOWS = ["a2a_transfer", "card_payment"]


def _stream(flows=FLOWS, n_batches=25, mix=None, friendly=2.0):
    from realy.gen.generator import Generator, sim_start_us
    from realy.scenario import Scenario, WorldSpec
    from realy.world import build_world
    spec = WorldSpec(customers=3000, merchants=400, payees=1500)
    sc = Scenario(name="t", seed=11, world=spec, traffic={"flows": flows},
                  fraud={"background_per_1k": 10.0, "friendly_fraud_per_1k": friendly,
                         "mix": mix or {"app_scam_mule": 1.0, "account_takeover": 1.0, "card_testing": 1.0}})
    g = Generator(build_world(spec, 11, None), sc, 0, 1)
    t = sim_start_us(sc)
    dt = int(200 / g.natural_rate_per_sim_s() * 1e6)
    unified, records = {}, []
    for _ in range(n_batches):
        b = g.generate(200, t, t + dt)
        t += dt
        for i in range(len(b)):
            unified[int(b.columns["event_id"][i])] = {k: b.columns[k][i] for k in b.columns}
        records += b.records
    return unified, records + g.flows.drain()


def _assemble(records, flows=FLOWS):
    a = Assembler([transaction(n) for n in flows])
    pay = event_type("payment").codec
    out = []
    for r in records:
        c = pay if not r.type else event_type(r.type).codec
        out += a.process(r.type or "payment", c.row(message_class(c.full_name).FromString(r.payload)))
    return out + a.close()


@pytest.mark.parametrize("name", FLOWS)
def test_contracts_and_their_generators_are_valid(name):
    t = transaction(name)
    assert t.validate() == [] and validate_generate(t) == []


def test_every_payment_assembles_back_exactly_across_both_flows():
    from realy.features.derived import PAYMENT_TYPE
    unified, records = _stream()
    pay = event_type("payment").codec
    outs = _assemble(records)
    by_id = {int(o.row["event_id"]): o for o in outs if o.kind in ("assembled", "passthrough")}
    assert set(by_id) == set(unified)                  # every payment decided once, nothing extra
    kinds = collections.Counter((o.transaction or "-") for o in by_id.values())
    assert kinds["card_payment"] > 500 and kinds["a2a_transfer"] > 100
    card = PAYMENT_TYPE.index("CARD_PURCHASE")
    for eid, u in unified.items():
        o = by_id[eid]
        if int(u["payment_type"]) == card:
            assert o.transaction == "card_payment"
        for f in pay.fields:
            want = u[f.name]
            want = want.item() if hasattr(want, "item") else want
            if o.transaction == "a2a_transfer" and f.name in ("device_id", "ip_v4", "ip_country", "ip_asn") \
                    and not o.parts_present & 0b10:
                want = f.default                                  # no device context sent
            assert o.row[f.name] == want, (eid, o.transaction, f.name, o.row[f.name], want)


def test_a_3ds_result_comes_first_and_only_with_a_3ds_outcome():
    unified, records = _stream(flows=["card_payment"])
    t = transaction("card_payment")
    outs = [o for o in _assemble(records, ["card_payment"]) if o.kind == "assembled"]
    none = event_type("payment").codec.by_name["three_ds"].default
    with_3ds = [o for o in outs if o.parts_present & t.bit("three_ds")]
    assert with_3ds and len(with_3ds) < len(outs)
    for o in outs:
        has = bool(o.parts_present & t.bit("three_ds"))
        assert has == (int(unified[int(o.row["event_id"])]["three_ds"]) != none)
        assert o.context["secs_from_3ds"] < 0 or not has or o.context["secs_from_3ds"] > 0
    # the authentication is sent before its authorisation, so the decision never waits for it
    first = {}
    for r in records:
        if r.type == "card_authentication":
            m = message_class(event_type(r.type).message).FromString(r.payload)
            first.setdefault(m.auth_id, r.t_us)
    for o in with_3ds:
        assert first[int(o.row["event_id"])] < int(o.row["event_time_us"])


def test_typologies_shape_the_card_lifecycle():
    unified, records = _stream(flows=["card_payment"], n_batches=40, mix={"card_testing": 3.0}, friendly=30.0)
    fraud = {e: int(u["fraud_type"]) or int(u["label_type"]) for e, u in unified.items()}
    from realy.usecases import registry
    names = registry().fraud_type_names()
    captured, chargebacks, reasons = collections.Counter(), collections.Counter(), collections.defaultdict(collections.Counter)
    for r in records:
        if r.type in ("card_capture", "chargeback"):
            m = message_class(event_type(r.type).message).FromString(r.payload)
            kind = names.get(fraud.get(m.auth_id, 0), "legit") if fraud.get(m.auth_id) else "legit"
            if r.type == "card_capture":
                captured[kind] += 1
            else:
                chargebacks[kind] += 1
                reasons[kind][event_type("chargeback").codec.by_name["reason"].enum_names[m.reason][1]] += 1
    auths = collections.Counter(names.get(f, "legit") if f else "legit" for e, f in fraud.items()
                                if int(unified[e]["payment_type"]) == 1)
    assert captured["card_testing"] < 0.2 * auths["card_testing"]          # tested, almost never captured
    assert captured["legit"] > 0.95 * auths["legit"]
    assert chargebacks["friendly_fraud"] > 0.6 * captured["friendly_fraud"]
    assert set(reasons["friendly_fraud"]) <= {"CB_FRAUD_CARD_ABSENT", "CB_NOT_RECEIVED"}


def test_part_ids_are_unique_across_flows_and_never_a_payments_id():
    unified, records = _stream()
    ids = collections.Counter()
    for r in records:
        if r.type:
            m = message_class(event_type(r.type).message).FromString(r.payload)
            ids[m.event_id] += 1
    parts = {i for i in ids if i >> 47 & 1}
    assert parts and all(n == 1 for i, n in ids.items() if i in parts)
    assert not parts & set(unified)


def test_the_step_language_reports_its_mistakes():
    raw = {"name": "x", "link": "payment_id", "messages": {"initiation": "payment_initiation", "status": "payment_status"},
           "assemble": {"trigger": "initiation", "fields": {"event_id": "initiation.event_id"}},
           "generate": {"steps": [{"role": "status", "after": "nowhere", "set": {"nope": 1}},
                                  {"role": "initiation", "bogus": 1}]}}
    p = validate_generate(parse(raw))
    assert any("unknown keys" in x for x in p)
    raw["generate"]["steps"][1].pop("bogus")
    p = validate_generate(parse(raw))
    assert any("'nowhere'" in x for x in p) and any("'nope'" in x for x in p)
