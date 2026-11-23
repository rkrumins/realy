"""The aggregation registry (realy.features.aggregations), its plugins and the shared conformance fixtures."""
from __future__ import annotations

import json
import textwrap

import pytest

from realy.features import aggregations as A
from realy.features.schema import _load_schema


def test_builtins_and_shapes():
    assert set(A.BUILTINS) == {"count", "sum", "avg", "max", "distinct", "novel", "time_since_last", "geo_speed_kmh", "latest"}
    assert {A.get(n).shape for n in A.BUILTINS} == set(A.SHAPES)


def test_committed_fixture_is_current():
    """contracts/testdata/aggregations/conformance.json is what the registry produces now; Java checks the
    same file (AggregationConformanceTest), so the two languages agree on every value."""
    committed = json.loads((A.FIXTURES / "conformance.json").read_text())
    assert committed == json.loads(json.dumps(A.conformance_cases()))


def test_plugin_fixture_is_current():
    A.load_plugins("realy.plugin_example")
    committed = json.loads((A.FIXTURES / "min.json").read_text())
    now = A.conformance_cases([{"agg": "min", "window_s": 1800.0, "granularity_s": 60.0, "default": 0.0}])
    assert committed == json.loads(json.dumps(now))
    assert "min" in A.extensions() and not A.builtin("min")


def test_min_plugin_is_the_mirror_of_max():
    A.load_plugins("realy.plugin_example")
    cases = A.conformance_cases([{"agg": "min", "window_s": 1800, "granularity_s": 60, "default": 0.0},
                                 {"agg": "max", "window_s": 1800, "granularity_s": 60, "default": 0.0}])
    mn, mx = (c["steps"] for c in cases["cases"])
    assert all(a["apply"] <= b["apply"] for a, b in zip(mn, mx))
    assert any(a["apply"] < b["apply"] for a, b in zip(mn, mx))


def test_a_name_is_registered_once():
    with pytest.raises(ValueError, match="already registered"):
        A.register(A.BucketAgg("sum", "numeric"))


def _schema(tmp_path, feature: str):
    p = tmp_path / "schema.yaml"
    p.write_text(textwrap.dedent(f"""
        schema_version: 1
        entities: {{customer: {{key: customer_id}}}}
        filters: {{}}
        countries: [US]
        features:
          - {feature}
    """))
    return _load_schema(str(p))


def test_schema_refuses_an_unknown_aggregation(tmp_path):
    with pytest.raises(ValueError, match="unknown aggregation 'median'"):
        _schema(tmp_path, "{name: f, source: stream, entity: customer, agg: median, value: amount_usd, window: 1h, granularity: 1m}")


def test_schema_needs_a_value_for_numeric_aggregations(tmp_path):
    with pytest.raises(ValueError, match="needs `value:`"):
        _schema(tmp_path, "{name: f, source: stream, entity: customer, agg: sum, window: 1h, granularity: 1m}")


def test_schema_accepts_a_registered_plugin(tmp_path):
    A.load_plugins("realy.plugin_example")
    s = _schema(tmp_path, "{name: f, source: stream, entity: customer, agg: min, value: amount_usd, window: 1h, granularity: 1m}")
    assert s.features[0].agg == "min"


def test_plugins_from_the_environment(monkeypatch):
    from realy import plugins
    plugins.reset()
    monkeypatch.setenv("REALY_PLUGINS", "realy.plugin_example")
    assert "realy.plugin_example" in A.load_plugins()
    assert A.load_plugins() == []          # once per process
