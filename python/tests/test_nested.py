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
