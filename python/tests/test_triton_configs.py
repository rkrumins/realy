"""Triton reads its input positionally: each committed config must declare exactly the width of the
model it serves (the features that model was trained on), and that width can never exceed today's
schema (features are only appended, so a model's inputs are the first N of today's vector, which is
what the gateway sends: KServeModelClient.forServedModel reads N from the model's metadata)."""
from __future__ import annotations

import json
import re

from realy.features import load_schema
from realy.scenario import REPO_ROOT

TRITON = REPO_ROOT / "models" / "triton"


def _weights_by_version() -> dict[str, dict]:
    out = {}
    for p in (REPO_ROOT / "models" / "proxy").glob("*.weights.json"):
        w = json.loads(p.read_text())
        out[w.get("version")] = w
    return out


def test_triton_input_dims_match_the_served_model():
    weights = _weights_by_version()
    schema = [f.name for f in load_schema().features]
    configs = sorted(TRITON.glob("*/config.pbtxt"))
    assert configs
    for cfg in configs:
        text = cfg.read_text()
        dims = int(re.search(r'name: "input__0".*?dims: \[ *(\d+) *\]', text, re.S).group(1))
        version = re.search(r'key: "realy_version" value: \{ string_value: "([^"]+)"', text).group(1)
        w = weights.get(version)
        assert w is not None, f"{cfg}: no models/proxy weights for {version}"
        names = w.get("feature_names") or []
        assert dims == len(w["mean"]) == len(names), f"{cfg}: dims {dims}, model {version} has {len(w['mean'])} inputs"
        # positional = by name: the model's inputs are the first `dims` features of today's schema
        assert names == schema[:dims], f"{cfg}: {version}'s inputs are not a prefix of the feature schema"
