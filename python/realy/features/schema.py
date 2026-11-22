"""Loader for contracts/features/feature_schema.yaml (see that file for semantics)."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import yaml

from ..scenario import REPO_ROOT

SCHEMA_PATH = REPO_ROOT / "contracts" / "features" / "feature_schema.yaml"
_DUR = re.compile(r"^(\d+)(s|m|h|d)$")
_UNIT_US = {"s": 1_000_000, "m": 60_000_000, "h": 3_600_000_000, "d": 86_400_000_000}


def parse_duration_us(text: str) -> int:
    m = _DUR.match(str(text).strip())
    if not m:
        raise ValueError(f"bad duration {text!r} (use e.g. 10s, 5m, 1h, 7d)")
    return int(m.group(1)) * _UNIT_US[m.group(2)]


@dataclass(frozen=True)
class FeatureSpec:
    index: int
    name: str
    source: str                  # stream | profile | derived
    entity: str | None = None
    agg: str | None = None
    value: str | None = None
    filter: str | None = None
    window_us: int = 0
    granularity_us: int = 0
    default: float = 0.0
    field: str | None = None     # profile
    fn: str | None = None        # derived: a library function (features/derived.py)
    expr: str | None = None      # derived: a CEL expression over `event` and `f` returning a double

    @property
    def n_buckets(self) -> int:
        return self.window_us // self.granularity_us if self.granularity_us else 0


@dataclass
class FeatureSchema:
    version: int
    entities: dict[str, str]                 # entity -> event key field
    filters: dict[str, str]                  # name -> CEL
    features: list[FeatureSpec]
    countries: list[str]
    profile_attributes: dict[str, list[str]] = field(default_factory=dict)

    @property
    def names(self) -> list[str]:
        return [f.name for f in self.features]

    def index_of(self, name: str) -> int:
        return self._by_name[name].index

    def __post_init__(self) -> None:
        self._by_name = {f.name: f for f in self.features}

    def stream_features(self, entity: str | None = None) -> list[FeatureSpec]:
        return [f for f in self.features if f.source == "stream" and (entity is None or f.entity == entity)]

    def profile_features(self, entity: str) -> list[FeatureSpec]:
        return [f for f in self.features if f.source == "profile" and f.entity == entity]

    def pad(self, X: "np.ndarray") -> "np.ndarray":
        """Feature rows of an earlier, narrower schema (features are only ever appended) widened to this
        one with each new feature's declared default."""
        import numpy as np
        X = np.asarray(X, np.float32)
        n = len(self.features)
        if X.ndim != 2 or X.shape[1] >= n:
            return X
        tail = np.asarray([f.default for f in self.features[X.shape[1]:]], np.float32)
        return np.hstack([X, np.repeat(tail[None, :], X.shape[0], axis=0)])

    def state_layout(self) -> str:
        """Fingerprint of everything the keyed stream state and the online-store blobs are laid out by:
        the stream features (entity, aggregation, value, filter, window, granularity, in order) and the
        filters they use. Warm state built under another layout is unusable (runner provenance);
        derived and profile features do not touch it."""
        import hashlib
        import json
        spec = [[f.name, f.entity, f.agg, f.value, f.filter, self.filters.get(f.filter or "", ""), f.window_us,
                 f.granularity_us, f.default] for f in self.stream_features()]
        return hashlib.sha256(json.dumps(spec).encode()).hexdigest()[:12]

    def stream_entities(self) -> list[str]:
        seen: list[str] = []
        for f in self.features:
            if f.source == "stream" and f.entity not in seen:
                seen.append(f.entity)  # type: ignore[arg-type]
        return seen

    def country_index(self, code: str) -> int:
        try:
            return self.countries.index(code)
        except ValueError:
            return -1


def load_schema(path: str | Path | None = None) -> FeatureSchema:
    """The feature schema: `path`, else $FEATURE_SCHEMA (the same override the Java services honour),
    else contracts/features/feature_schema.yaml. Cached per path."""
    import os
    return _load_schema(str(path or os.environ.get("FEATURE_SCHEMA") or SCHEMA_PATH))


@lru_cache(maxsize=8)
def _load_schema(path: str) -> FeatureSchema:
    raw = yaml.safe_load(Path(path).read_text())
    feats: list[FeatureSpec] = []
    for i, f in enumerate(raw["features"]):
        src = f["source"]
        kw: dict = dict(index=i, name=f["name"], source=src, default=float(f.get("default", 0.0)))
        if src == "stream":
            if f["entity"] not in raw["entities"]:
                raise ValueError(f"{f['name']}: unknown entity {f['entity']}")
            if f.get("filter") and f["filter"] not in raw["filters"]:
                raise ValueError(f"{f['name']}: unknown filter {f['filter']}")
            w, g = parse_duration_us(f["window"]), parse_duration_us(f["granularity"])
            if w % g:
                raise ValueError(f"{f['name']}: window must be a multiple of granularity")
            from . import aggregations
            if f.get("agg") not in aggregations.names():
                raise ValueError(f"{f['name']}: unknown aggregation {f.get('agg')!r} "
                                 f"(registered: {', '.join(aggregations.names())})")
            if aggregations.get(f["agg"]).input in ("numeric", "key") and not f.get("value"):
                raise ValueError(f"{f['name']}: aggregation {f['agg']} needs `value:` (the event field it reads)")
            kw.update(entity=f["entity"], agg=f["agg"], value=f.get("value"), filter=f.get("filter"),
                      window_us=w, granularity_us=g)
        elif src == "profile":
            kw.update(entity=f["entity"], field=f["field"])
        elif src == "derived":
            if (f.get("fn") is None) == (f.get("expr") is None):
                raise ValueError(f"{f['name']}: a derived feature needs exactly one of `fn` or `expr`")
            if f.get("expr") is not None:
                import cel
                cel.compile(f["expr"])          # fail at load, not on the first event
            kw.update(fn=f.get("fn"), expr=f.get("expr"))
        else:
            raise ValueError(f"{f['name']}: unknown source {src}")
        feats.append(FeatureSpec(**kw))
    return FeatureSchema(
        version=int(raw["schema_version"]),
        entities={k: v["key"] for k, v in raw["entities"].items()},
        filters=dict(raw["filters"]),
        features=feats,
        countries=list(raw["countries"]),
        profile_attributes={k: list(v) for k, v in (raw.get("profile_attributes") or {}).items()},
    )
