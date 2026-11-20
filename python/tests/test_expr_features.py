"""Derived features written as CEL expressions (`expr`): Python mirrors Java exactly (verifier parity)."""
from __future__ import annotations

import numpy as np
import pytest

from realy.features.derived import compute_derived
from realy.features.schema import SCHEMA_PATH, load_schema


def _schema(tmp_path, extra: str):
    p = tmp_path / "schema.yaml"
    p.write_text(SCHEMA_PATH.read_text().rstrip() + "\n" + extra)
    return load_schema(p)


def _event(**over):
    ev = dict(event_id=1, event_time_us=0, customer_id=1, account_id=1, card_id=1, amount_minor=15_000,
              amount_usd_minor=15_000, merchant_id=1, mcc=5732, terminal_id=0, device_id=1, ip_v4=1, ip_asn=1,
              payee_id=0, credit_limit_usd=0, card_present=False, cvv_match=False, avs_match=False, recurring=False,
              payment_type=1, channel=2, entry_mode=7, card_type=2, currency="USD", merchant_country="GB",
              ip_country="GB", lat=0.0, lon=0.0)
    ev.update(over)
    return ev


def test_expressions_over_the_event_and_other_features(tmp_path):
    s = _schema(tmp_path, '  - { name: cvv_mismatch, source: derived, expr: "event.cvv_match ? 0.0 : 1.0" }\n'
                          '  - { name: avs_fail, source: derived, expr: "!event.avs_match" }\n'
                          '  - { name: big_vs_avg, source: derived, expr: "f.amount_usd > 2.0 * f.cust_avg_ticket_usd ? 1.0 : 0.0" }\n')
    v = np.zeros(len(s.features), np.float32)
    v[s.index_of("cust_avg_ticket_usd")] = 40.0
    compute_derived(s, _event(), None, v)
    assert (v[s.index_of("cvv_mismatch")], v[s.index_of("avs_fail")], v[s.index_of("big_vs_avg")]) == (1.0, 1.0, 1.0)
    compute_derived(s, _event(cvv_match=True, avs_match=True, amount_usd_minor=5_000), None, v)
    assert (v[s.index_of("cvv_mismatch")], v[s.index_of("avs_fail")], v[s.index_of("big_vs_avg")]) == (0.0, 0.0, 0.0)


def test_bad_definitions_fail_at_load(tmp_path):
    with pytest.raises(ValueError, match="exactly one of"):
        _schema(tmp_path, '  - { name: y, source: derived, fn: is_cnp, expr: "1.0" }\n')
    with pytest.raises(Exception):
        _schema(tmp_path, '  - { name: x, source: derived, expr: "f.amount_usd +" }\n')
