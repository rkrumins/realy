"""Loader for contracts/features/feature_schema.yaml (see that file for semantics)."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import yaml

from ..scenario import REPO_ROOT

SCHEMA_PATH = REPO_ROOT / "contracts" / "features" / "feature_schema.yaml"
ENTITY_ENUM = {"customer": 1, "card": 2, "device": 3, "ip": 4, "merchant": 5, "terminal": 6, "payee": 7}
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
    fn: str | None = None        # derived

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


@lru_cache(maxsize=4)
def load_schema(path: str | Path = SCHEMA_PATH) -> FeatureSchema:
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
            kw.update(entity=f["entity"], agg=f["agg"], value=f.get("value"), filter=f.get("filter"),
                      window_us=w, granularity_us=g)
        elif src == "profile":
            kw.update(entity=f["entity"], field=f["field"])
        elif src == "derived":
            kw.update(fn=f["fn"])
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
