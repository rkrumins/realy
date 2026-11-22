"""Derived-feature function library + CEL bindings — mirrors DerivedFeatures.java / EventView.java."""
from __future__ import annotations

import math
from typing import Any

import numpy as np

from .engine import haversine_km
from .schema import FeatureSchema


def _payment_codec():
    from ..bindings.codec import codec
    return codec("realy.payments.v1.TransactionEvent")


def _names(field: str) -> list[str]:
    """An enum of the payment event as a list indexed by number (from the protobuf contract)."""
    names = dict(_payment_codec().by_name[field].enum_names)
    return [names.get(i, str(i)) for i in range(max(names) + 1)]


PAYMENT_TYPE = _names("payment_type")
CHANNEL = _names("channel")
ENTRY_MODE = _names("entry_mode")
CARD_TYPE = _names("card_type")
THREE_DS = _names("three_ds")
ENUMS = {"payment_type": PAYMENT_TYPE, "channel": CHANNEL, "entry_mode": ENTRY_MODE, "card_type": CARD_TYPE,
         "three_ds": THREE_DS}


def event_view(ev: dict, txn: dict | None = None) -> dict[str, Any]:
    """CEL `event` binding of a payment: the generic codec of its protobuf message (realy.bindings): enums
    by name, integers as int (uint64 as signed 64-bit), bools, doubles, strings; a field the row lacks (an
    event logged before it existed) reads its proto3 default. `event.txn` is the context of the
    multi-message transaction it was assembled from (realy.bindings.transactions), or the default context
    of every contract for a one-message payment, so expressions over it never fail. Mirrors EventView.java."""
    v = _PAYMENT.view(ev)
    v["txn"] = txn if txn is not None else (ev.get("txn") or _default_txn())
    return v


def _default_txn() -> dict[str, Any]:
    global _DEFAULT_TXN
    if _DEFAULT_TXN is None:
        from ..bindings.transactions import default_context
        _DEFAULT_TXN = default_context()
    return _DEFAULT_TXN


_DEFAULT_TXN: dict[str, Any] | None = None


_PAYMENT = _payment_codec()


_PROGRAMS: dict[str, Any] = {}


def _compiled(expr: str) -> Any:
    p = _PROGRAMS.get(expr)
    if p is None:
        import cel
        p = _PROGRAMS[expr] = cel.compile(expr)
    return p


def compute_derived(schema: FeatureSchema, ev: dict, attrs: dict | None, v: np.ndarray,
                    txn: dict | None = None) -> None:
    """Fill derived features of float32 vector v in place (other features already filled); `txn` is the
    event's multi-message transaction context (event.txn), None for a one-message payment."""
    def idx(name: str) -> int:
        try:
            return schema.index_of(name)
        except KeyError:
            return -1

    ctx = None
    for s in schema.features:
        if s.source != "derived":
            continue
        if s.expr is not None:
            # expression features: the rules' environment (`event` view + `f`, every feature filled so far)
            import cel
            if ctx is None:
                ctx = cel.Context()
                ctx.add_variable("event", event_view(ev, txn))
            ctx.add_variable("f", {o.name: float(v[o.index]) for o in schema.features})
            v[s.index] = np.float32(float(_compiled(s.expr).execute(ctx)))
            continue
        fn = s.fn
        pt, em = int(ev["payment_type"]), int(ev["entry_mode"])
        if fn == "amount_usd":
            x = ev["amount_usd_minor"] / 100.0
        elif fn == "is_cnp":
            x = 1.0 if pt == 1 and not ev["card_present"] else 0.0
        elif fn == "is_contactless":
            x = 1.0 if em == 2 else 0.0
        elif fn == "is_atm":
            x = 1.0 if pt == 2 else 0.0
        elif fn == "is_transfer":
            x = 1.0 if pt == 3 else 0.0
        elif fn == "is_foreign":
            mc = str(ev["merchant_country"])
            if attrs is None or not mc:
                x = 0.0
            else:
                ci = schema.country_index(mc)
                x = 1.0 if ci >= 0 and ci != int(attrs["home_country_idx"]) else 0.0
        elif fn == "dist_from_home_km":
            if attrs is None or (ev["lat"] == 0.0 and ev["lon"] == 0.0):
                x = 0.0
            else:
                x = haversine_km(float(attrs["home_lat"]), float(attrs["home_lon"]), float(ev["lat"]), float(ev["lon"]))
        elif fn == "amount_to_avg_ratio":
            i = idx("cust_avg_ticket_usd")
            avg = float(v[i]) if i >= 0 else 40.0
            x = (ev["amount_usd_minor"] / 100.0) / max(avg, 1.0)
        elif fn == "local_hour":
            secs = int(ev["event_time_us"]) // 1_000_000 + (int(attrs["tz_offset_min"]) * 60 if attrs else 0)
            x = (secs % 86_400) / 3600.0
        elif fn == "credit_utilisation_24h":
            lim = int(ev["credit_limit_usd"])
            i = idx("card_amount_sum_24h")
            x = 0.0 if lim <= 0 or i < 0 else (float(v[i]) / 100.0) / lim
        else:
            raise ValueError(f"unknown derived fn {fn}")
        v[s.index] = np.float32(x)
