"""Bindings (contracts/entities.yaml, events/*.yaml, pipelines/*.yaml) and the generic codec: on the
payment event it must reproduce today's hand-written view and warehouse columns exactly."""
from __future__ import annotations

import numpy as np
import pyarrow.parquet as pq

from realy import golden
from realy.bindings import event_type, pipeline, registry


def test_registry_and_exactness():
    reg = registry()
    assert reg.names[:7] == ["customer", "card", "device", "ip", "merchant", "terminal", "payee"]
    assert reg.owners("card") == ["customer"] and reg.owners("terminal") == ["merchant"]
    assert reg.exact("customer") == ["customer", "card", "account", "txn"]
    from realy.features import load_schema
    # the Flink job's stage order is the registry order (part of the state layout)
    stream = load_schema().stream_entities()
    assert stream == [e for e in reg.names if e in stream]


def test_payments_pipeline_is_valid():
    p = pipeline("payments")
    assert p.validate() == []
    assert p.scored() == ["payment"] and p.default_type == "payment" and p.key_prefix() == ""


def _legacy_view(ev: dict) -> dict:
    """The hand-written payment view the generic codec replaced (kept to prove they agree)."""
    enums = {"payment_type": ["PAYMENT_TYPE_UNSPECIFIED", "CARD_PURCHASE", "ATM_WITHDRAWAL", "TRANSFER", "CARD_REFUND"],
             "channel": ["CHANNEL_UNSPECIFIED", "POS", "ECOM", "ATM", "MOTO", "IN_APP", "ONLINE_BANKING"],
             "entry_mode": ["ENTRY_MODE_UNSPECIFIED", "CHIP", "CONTACTLESS", "MAGSTRIPE", "MANUAL", "WALLET_TOKEN",
                            "ECOM_3DS", "ECOM_NON_3DS", "CREDENTIAL_ON_FILE", "BANK_APP"],
             "card_type": ["CARD_TYPE_UNSPECIFIED", "DEBIT", "CREDIT", "PREPAID", "VIRTUAL"],
             "three_ds": ["THREE_DS_NONE", "THREE_DS_ATTEMPTED", "THREE_DS_AUTHENTICATED"]}
    ints = ("event_id", "event_time_us", "customer_id", "account_id", "card_id", "amount_minor", "amount_usd_minor",
            "merchant_id", "mcc", "terminal_id", "device_id", "ip_v4", "ip_asn", "payee_id", "credit_limit_usd",
            "scheme_score")
    v: dict = {}
    for k in ints:
        x = int(ev[k])
        v[k] = x - (1 << 64) if x >= 1 << 63 else x
    for k in ("card_present", "cvv_match", "avs_match", "recurring", "pin_verified", "cryptogram_valid"):
        v[k] = bool(ev[k])
    for k, names in enums.items():
        i = int(ev[k])
        v[k] = names[i] if 0 <= i < len(names) else str(i)
    for k in ("currency", "merchant_country", "ip_country"):
        v[k] = str(ev[k])
    v["lat"], v["lon"] = float(ev["lat"]), float(ev["lon"])
    return v


def test_generic_view_equals_the_payment_view_on_the_golden_events():
    from realy.features.derived import event_view
    event_view_legacy = _legacy_view
    t = pq.read_table(golden.OUT / "events.parquet")
    names = [c for c in t.column_names]
    lists = {k: t[k].to_pylist() for k in names}
    c = event_type("payment").codec
    for i in range(t.num_rows):
        ev = {k: lists[k][i] for k in names}
        v = event_view(ev)
        assert v.pop("txn")["transaction"] == ""             # a one-message payment: the default context
        assert c.view(ev) == v == event_view_legacy(ev), i


def test_absent_fields_read_their_proto3_default():
    """An event logged before a field existed: every field the row lacks reads its default."""
    c = event_type("payment").codec
    v = c.view({"event_id": 1, "event_time_us": 2})
    assert v["three_ds"] == "THREE_DS_NONE" and v["pin_verified"] is False and v["scheme_score"] == 0
    assert v["payment_type"] == "PAYMENT_TYPE_UNSPECIFIED" and v["merchant_country"] == "" and v["lat"] == 0.0


def test_unknown_enum_and_big_uint64_follow_the_shared_rules():
    c = event_type("payment").codec
    v = c.view({"payment_type": 42, "customer_id": (1 << 64) - 5})
    assert v["payment_type"] == "42" and v["customer_id"] == -5


def test_warehouse_columns_are_the_descriptors():
    from realy.codegen import EVENT_COLS
    assert event_type("payment").codec.columns() == EVENT_COLS


def test_bad_binding_is_reported():
    from realy.bindings.events import EventType
    t = EventType(name="x", message="realy.payments.v1.TransactionEvent", id="event_id", time="currency",
                  entities={"customer": "nope", "planet": "customer_id"})
    msgs = " | ".join(t.validate())
    assert "time field 'currency' is string" in msgs and "'nope' is not a field" in msgs and "'planet'" in msgs


def test_codec_reads_decoded_messages():
    from realy.bindings.codec import message_class
    cls = message_class("realy.payments.v1.TransactionEvent")
    with __import__("gzip").open(golden.OUT / "events.pb.gz") as f:
        data = f.read()
    # first varint-delimited record
    n, shift, pos = 0, 0, 0
    while True:
        b = data[pos]; pos += 1
        n |= (b & 0x7F) << shift; shift += 7
        if not b & 0x80:
            break
    msg = cls.FromString(data[pos:pos + n])
    row = event_type("payment").codec.row(msg)
    t = pq.read_table(golden.OUT / "events.parquet").slice(0, 1).to_pylist()[0]
    for k, v in row.items():
        assert (np.float32(v) == np.float32(t[k])) if isinstance(v, float) else v == t[k], k


def test_pipeline_snapshot_is_json_and_complete():
    import json
    snap = pipeline("payments").snapshot()
    json.dumps(snap)
    assert snap["exact_entities"] == ["customer", "card", "account", "txn"]
    assert snap["event_types"]["payment"]["scored"] is True
    assert len(snap["event_types"]["payment"]["fields"]) == 32


def test_feature_schema_entities_agree_with_the_payment_binding():
    """The feature schema's entity -> key field map is the payment binding's (one source of truth)."""
    from realy.features import load_schema
    b = event_type("payment").entities
    for ent, f in load_schema().entities.items():
        assert b.get(ent) == f, (ent, f, b.get(ent))
