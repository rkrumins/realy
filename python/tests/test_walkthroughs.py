"""The developer guides' walkthroughs, executed: a guide that drifts from the code fails here. Each
snippet is read from the doc itself (the fenced block after `<!-- walkthrough:<name> -->`)."""
from __future__ import annotations

import re

import numpy as np
import yaml

from realy.features import load_schema
from realy.features.derived import compute_derived
from realy.features.engine import ReferenceEngine
from realy.features.schema import SCHEMA_PATH
from realy.scenario import REPO_ROOT
from tests.test_core import ev, mask_of

MIN = 60_000_000
T0 = 1_800_000_000_000_000


def snippet(doc: str, name: str) -> str:
    text = (REPO_ROOT / "docs" / doc).read_text()
    m = re.search(rf"<!-- walkthrough:{name}\b.*?-->\s*```\w*\n(.*?)```", text, re.S)
    assert m, f"walkthrough {name} not found in docs/{doc}"
    return m.group(1)


def schema_with(tmp_path, monkeypatch, *snippets: str):
    p = tmp_path / "feature_schema.yaml"
    p.write_text(SCHEMA_PATH.read_text().rstrip("\n") + "\n" + "".join(snippets))
    monkeypatch.setenv("FEATURE_SCHEMA", str(p))
    s = load_schema()
    assert len(s.features) == len(load_schema(SCHEMA_PATH).features) + len(snippets)          # appended, not inserted
    return s


def test_expression_feature_walkthrough(tmp_path, monkeypatch):
    s = schema_with(tmp_path, monkeypatch, snippet("dev/features.md", "expression-feature"))
    name = yaml.safe_load(snippet("dev/features.md", "expression-feature"))[0]["name"]
    i = s.index_of(name)
    attrs = {"home_lat": 0.0, "home_lon": 0.0, "home_country_idx": 1, "tz_offset_min": 0}

    def value(e: dict, avg_ticket: float) -> float:
        v = np.zeros(len(s.features), np.float32)
        v[s.index_of("cust_avg_ticket_usd")] = avg_ticket
        compute_derived(s, e, attrs, v)
        return float(v[i])
    online = ev(T0, 90_000, False, 7, three_ds=1)                          # $900, 3DS attempted only
    assert value(online, 50.0) == 1.0                                     # 18x the usual ticket
    assert value(online, 500.0) == 0.0                                    # within the usual range
    assert value(ev(T0, 90_000, False, 7, three_ds=2), 50.0) == 0.0       # authenticated
    assert value(ev(T0, 90_000, True, 7), 50.0) == 0.0                    # card present


def test_stream_feature_walkthrough(tmp_path, monkeypatch):
    s = schema_with(tmp_path, monkeypatch, snippet("dev/features.md", "stream-feature"))
    name = yaml.safe_load(snippet("dev/features.md", "stream-feature"))[0]["name"]
    i = s.index_of(name)
    eng = ReferenceEngine(s, ["device"])
    out = [0.0] * len(s.features)
    seen = []
    for k, payee in enumerate([600, 601, 600, 602, 603]):
        e = ev(T0 + k * MIN, 25_000, False, 0, device=500, payment_type=3, card_id=0, payee_id=payee, channel=6)
        eng.process(e, mask_of(s, e), out)
        seen.append(out[i])
    assert seen == [1.0, 2.0, 2.0, 3.0, 4.0]                              # distinct payees on the device in 24 h
    late = ev(T0 + 25 * 60 * MIN, 25_000, False, 0, device=500, payment_type=3, card_id=0, payee_id=604, channel=6)
    eng.process(late, mask_of(s, late), out)
    assert out[i] == 1.0                                                  # the earlier ones left the 24 h window
