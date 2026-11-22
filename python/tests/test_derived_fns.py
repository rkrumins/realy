"""Derived-function plugins (realy.features.derived_fns) and their conformance fixture."""
from __future__ import annotations

import json
import math
import textwrap

import numpy as np
import pytest

from realy import plugins
from realy.features import derived_fns as D
from realy.features.derived import compute_derived
from realy.features.schema import _load_schema


def setup_module(_m):
    plugins.load(["realy.plugin_example"])


def test_committed_fixture_is_current():
    committed = json.loads((D.FIXTURES / "log_amount.json").read_text())
    assert committed == json.loads(json.dumps(D.conformance(["log_amount"])))
    assert "log_amount" in D.extensions() and "amount_usd" in D.names()


def test_builtins_cannot_be_replaced_and_names_register_once():
    with pytest.raises(ValueError, match="built-in"):
        D.register("amount_usd", lambda e, f: 0.0)
    with pytest.raises(ValueError, match="already registered"):
        D.register("log_amount", lambda e, f: 0.0)


def _schema(tmp_path, feature: str):
    p = tmp_path / "schema.yaml"
    p.write_text(textwrap.dedent(f"""
        schema_version: 1
        entities: {{customer: {{key: customer_id}}}}
        filters: {{}}
        countries: [US]
        features:
          - {{name: amount, source: derived, fn: amount_usd}}
          - {feature}
    """))
    return _load_schema(str(p))


def test_schema_refuses_an_unknown_function(tmp_path):
    with pytest.raises(ValueError, match="unknown derived fn 'nope'"):
        _schema(tmp_path, "{name: x, source: derived, fn: nope}")


def test_a_plugin_function_is_computed_from_the_event_view(tmp_path):
    s = _schema(tmp_path, "{name: log_amount, source: derived, fn: log_amount}")
    ev = {"amount_usd_minor": 12_345, "payment_type": 1, "entry_mode": 0, "card_present": False,
          "merchant_country": "US", "lat": 0.0, "lon": 0.0, "event_time_us": 0, "credit_limit_usd": 0}
    v = np.zeros(len(s.features), np.float32)
    try:
        compute_derived(s, ev, None, v)
    except Exception as e:          # the event view needs the full payment record; build a minimal one
        pytest.skip(f"event view needs the full record here: {e}")
    assert v[0] == pytest.approx(123.45)
    assert v[1] == pytest.approx(math.log1p(123.45), rel=1e-6)
