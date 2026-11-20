"""Derived-feature function library + CEL bindings — mirrors DerivedFeatures.java / EventView.java."""
from __future__ import annotations

import math
from typing import Any

import numpy as np

from .engine import haversine_km
from .schema import FeatureSchema

PAYMENT_TYPE = ["PAYMENT_TYPE_UNSPECIFIED", "CARD_PURCHASE", "ATM_WITHDRAWAL", "TRANSFER", "CARD_REFUND"]
CHANNEL = ["CHANNEL_UNSPECIFIED", "POS", "ECOM", "ATM", "MOTO", "IN_APP", "ONLINE_BANKING"]
ENTRY_MODE = ["ENTRY_MODE_UNSPECIFIED", "CHIP", "CONTACTLESS", "MAGSTRIPE", "MANUAL", "WALLET_TOKEN", "ECOM_3DS",
              "ECOM_NON_3DS", "CREDENTIAL_ON_FILE", "BANK_APP"]
CARD_TYPE = ["CARD_TYPE_UNSPECIFIED", "DEBIT", "CREDIT", "PREPAID", "VIRTUAL"]
THREE_DS = ["THREE_DS_NONE", "THREE_DS_ATTEMPTED", "THREE_DS_AUTHENTICATED"]
ENUMS = {"payment_type": PAYMENT_TYPE, "channel": CHANNEL, "entry_mode": ENTRY_MODE, "card_type": CARD_TYPE,
         "three_ds": THREE_DS}
INT_FIELDS = ("event_id", "event_time_us", "customer_id", "account_id", "card_id", "amount_minor", "amount_usd_minor",
              "merchant_id", "mcc", "terminal_id", "device_id", "ip_v4", "ip_asn", "payee_id", "credit_limit_usd",
              "scheme_score")
BOOL_FIELDS = ("card_present", "cvv_match", "avs_match", "recurring", "pin_verified", "cryptogram_valid")
# fields added after the first release: event logs written before them read as the proto default
LATER_FIELDS = {"three_ds": 0, "pin_verified": False, "cryptogram_valid": False, "scheme_score": 0}


def event_view(ev: dict) -> dict[str, Any]:
    """CEL `event` binding: enums by name, integers as int, bools, doubles, strings."""
    v: dict[str, Any] = {}
    if any(k not in ev for k in LATER_FIELDS):
        ev = {**LATER_FIELDS, **ev}
    for k in INT_FIELDS:
        v[k] = int(ev[k])
    for k in BOOL_FIELDS:
        v[k] = bool(ev[k])
    for k, names in ENUMS.items():
        i = int(ev[k])
        v[k] = names[i] if 0 <= i < len(names) else str(i)
    for k in ("currency", "merchant_country", "ip_country"):
        v[k] = str(ev[k])
    v["lat"] = float(ev["lat"])
    v["lon"] = float(ev["lon"])
    return v


_PROGRAMS: dict[str, Any] = {}


def _compiled(expr: str) -> Any:
    p = _PROGRAMS.get(expr)
    if p is None:
        import cel
        p = _PROGRAMS[expr] = cel.compile(expr)
    return p


def compute_derived(schema: FeatureSchema, ev: dict, attrs: dict | None, v: np.ndarray) -> None:
    """Fill derived features of float32 vector v in place (other features already filled)."""
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
                ctx.add_variable("event", event_view(ev))
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
