"""Nested messages inside TransactionEvent: the Python codec reads them as the Java one does (dotted leaves,
lists, maps, nested views), CEL rules over them agree, and absent blocks change nothing."""
from __future__ import annotations

import cel

from realy.bindings.codec import codec
from realy.payments.v1 import transaction_pb2 as pb

TE = "realy.payments.v1.TransactionEvent"


def rich() -> pb.TransactionEvent:
    return pb.TransactionEvent(
        event_id=7, event_time_us=1_000, customer_id=3, card_id=11,
        card_info=pb.CardInfo(id=11, bin="411111", bin_country="US", status=pb.CARD_STATUS_BLOCKED, tokenized=True,
                              wallet="APPLE_PAY"),
        account_info=pb.AccountInfo(id=5, balance_usd_minor=-1200, flags=["AML_HOLD", "OVERDRAWN"]),
        merchant_info=pb.MerchantInfo(id=9, mcc=5411, risk_tier=3, address=pb.Address(country="GB", city="London")),
        authentication=pb.Authentication(three_ds=pb.THREE_DS_AUTHENTICATED, attempts=2),
        items=[pb.LineItem(sku="A", qty=1, amount_usd_minor=500), pb.LineItem(sku="B", qty=3, amount_usd_minor=900)],
        attributes={"channel_app": "ios"})


def test_rows_have_dotted_leaves_lists_and_maps():
    c = codec(TE)
    row = c.row(pb.TransactionEvent.FromString(rich().SerializeToString()))
    assert row["card_info.status"] == pb.CARD_STATUS_BLOCKED and row["card_info.id"] == 11
    assert row["merchant_info.address.country"] == "GB" and row["authentication.attempts"] == 2
    assert row["account_info.flags"] == ["AML_HOLD", "OVERDRAWN"] and row["attributes"] == {"channel_app": "ios"}
    assert [i["qty"] for i in row["items"]] == [1, 3]
    assert c.numeric("card_info.status", "CARD_STATUS_BLOCKED") == 2.0 and c.numeric("card_info.tokenized", True) == 1.0
    assert {"card_info.status", "items", "attributes"} <= set(c.by_name)
    assert [f.name for f in c.fields] == [f.name for f in c.fields if "." not in f.name]   # flat fields unchanged


def test_views_nest_and_absent_blocks_read_defaults():
    c = codec(TE)
    v = c.view(c.row(rich()))
    assert v["card_info"]["status"] == "CARD_STATUS_BLOCKED" and v["merchant_info"]["address"]["city"] == "London"
    assert v["items"][1]["sku"] == "B" and v["authentication"]["three_ds"] == "THREE_DS_AUTHENTICATED"
    flat = c.view(c.row(pb.TransactionEvent(event_id=1, card_id=4)))
    assert flat["card_info"]["status"] == "CARD_STATUS_UNSPECIFIED" and flat["items"] == [] and flat["attributes"] == {}
    # a row logged before the blocks existed (no such keys at all) reads the same defaults
    old = c.view({"event_id": 1, "card_id": 4})
    assert old["card_info"] == flat["card_info"] and old["items"] == []


def test_rules_over_nested_fields_agree_with_java():
    """The same expressions NestedCodecTest (Java) evaluates, with the same results."""
    c = codec(TE)
    ev = c.view(c.row(rich()))
    flat = c.view(c.row(pb.TransactionEvent(event_id=1)))

    def ok(expr, e):
        ctx = cel.Context()
        ctx.add_variable("event", e)
        ctx.add_variable("f", {})
        return bool(cel.evaluate(expr, ctx))
    assert ok("event.card_info.status == 'CARD_STATUS_BLOCKED'", ev)
    assert ok("event.merchant_info.address.country == 'GB' && event.merchant_info.risk_tier >= 3", ev)
    assert ok("size(event.items) == 2 && event.items.exists(i, i.qty > 2)", ev)
    assert ok("'AML_HOLD' in event.account_info.flags", ev)
    assert ok("'channel_app' in event.attributes && event.attributes['channel_app'] == 'ios'", ev)
    assert ok("event.card_info.wallet != '' && event.authentication.attempts > 1", ev)
    assert not ok("event.card_info.status == 'CARD_STATUS_BLOCKED' || size(event.items) > 0", flat)


def test_payments_without_blocks_are_byte_identical():
    e = pb.TransactionEvent(event_id=1, card_id=4, amount_usd_minor=500)
    assert e.SerializeToString() == pb.TransactionEvent(event_id=1, card_id=4, amount_usd_minor=500,
                                                        card_info=None).SerializeToString()
    assert not e.HasField("card_info")


def _gen(blocks, n_batches=4, seed=9):
    from realy.gen.generator import Generator, sim_start_us
    from realy.scenario import Scenario, WorldSpec
    from realy.world import build_world
    spec = WorldSpec(customers=2000, merchants=300, payees=800)
    sc = Scenario(name="t", seed=seed, world=spec, traffic={"event_blocks": blocks},
                  fraud={"background_per_1k": 30.0, "mix": {"account_takeover": 1.0, "card_testing": 1.0,
                                                            "app_scam_mule": 1.0}})
    g = Generator(build_world(spec, seed, None), sc, 0, 1)
    t = sim_start_us(sc)
    dt = int(400 / g.natural_rate_per_sim_s() * 1e6)
    out = []
    for _ in range(n_batches):
        out.append(g.generate(400, t, t + dt))
        t += dt
    return out


def test_drawn_blocks_are_what_the_payload_carries():
    from realy.gen.blocks import BLOCKS
    c = codec(TE)
    batches = _gen(list(BLOCKS))
    seen = {b: 0 for b in ("card_info", "merchant_info", "payee_info", "authentication", "items")}
    for b in batches:
        cols = b.columns
        for i, payload in enumerate(b.payloads):
            msg = pb.TransactionEvent.FromString(payload)
            row = c.row(msg)
            for k, v in row.items():
                if k not in cols:
                    continue
                want = cols[k][i]
                want = want.item() if hasattr(want, "item") else want
                if k == "items":
                    assert [dict(x) for x in v] == [dict(x) for x in want], (i, k)
                elif k == "attributes":
                    assert dict(v) == dict(want), (i, k)
                elif k == "account_info.flags":
                    assert list(v) == list(want), (i, k)
                elif isinstance(v, float):
                    assert abs(v - float(want)) < 1e-9, (i, k)
                else:
                    assert v == want, (i, k, v, want)
            for blk in seen:
                seen[blk] += int(msg.HasField(blk) if blk != "items" else len(msg.items) > 0)
            if msg.card_id == 0:
                assert not msg.HasField("card_info") and cols["card_info.bin"][i] == ""
    assert all(v > 0 for v in seen.values()), seen


def test_flat_payloads_are_unchanged_by_blocks_being_available():
    a = _gen([], n_batches=2)
    b = _gen([], n_batches=2)
    assert [p for x in a for p in x.payloads] == [p for x in b for p in x.payloads]
    assert not any(k.startswith("card_info") for k in a[0].columns)


def test_the_event_log_round_trips_blocks_for_verify(tmp_path, monkeypatch):
    """Blocks written to the event log as the driver writes them, read back as verify reads them, give the
    same views and the same rich features as the in-memory rows."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    from realy.features.derived import event_view
    from realy.features.engine import ReferenceEngine
    from realy.features.schema import load_schema
    from realy.gen.blocks import ARROW_TYPES, BLOCKS
    monkeypatch.setenv("PIPELINE", "payments-rich")
    batches = _gen(list(BLOCKS), n_batches=2)
    cols = {k: np.concatenate([b.columns[k] for b in batches]) for k in batches[0].columns}
    pq.write_table(pa.table({k: (pa.array(list(v), type=ARROW_TYPES[k]) if k in ARROW_TYPES else v)
                             for k, v in cols.items()}), tmp_path / "events-0.parquet")
    t = pq.read_table(tmp_path / "events-0.parquet")
    back = {c: t[c].to_numpy(zero_copy_only=False).tolist() for c in t.column_names}
    s = load_schema()
    assert len(s.features) == 73
    eng_a = ReferenceEngine(s, ["customer", "card"], default_type="payment", scored_types=["payment"])
    eng_b = ReferenceEngine(s, ["customer", "card"], default_type="payment", scored_types=["payment"])
    import cel
    filt = s.compiled_filters()
    n = len(back["event_id"])
    names = [f.name for f in s.features]
    for i in range(n):
        mem = {k: (v[i].item() if hasattr(v[i], "item") else v[i]) for k, v in cols.items()}
        disk = {k: v[i] for k, v in back.items()}
        va, vb = event_view(mem), event_view(disk)
        assert va == vb, i
        ctx = cel.Context()
        ctx.add_variable("event", vb)
        ctx.add_variable("f", {})
        mask = sum(1 << b for b, p in enumerate(filt) if p is not None and p.execute(ctx))
        oa, ob = [0.0] * len(names), [0.0] * len(names)
        eng_a.process(mem, mask, oa)
        eng_b.process(disk, mask, ob)
        assert oa == ob, i
    assert any(v["card_info"]["status"] != "CARD_STATUS_ACTIVE" for v in [event_view({k: back[k][i] for k in back})
                                                                           for i in range(n)] if v["card_info"]["id"])
