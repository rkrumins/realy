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
    on: tuple[str, ...] | None = None   # stream: `event_types`, the types that fold into it (None: the default type)

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
    filter_on: dict[str, tuple[str, ...]] = field(default_factory=dict)   # name -> types (absent: the default type)

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
        # `event_types` joins the layout only when given, so a payment-only schema keeps its hash
        spec = [[f.name, f.entity, f.agg, f.value, f.filter, self.filters.get(f.filter or "", ""), f.window_us,
                 f.granularity_us, f.default] + ([list(f.on)] if f.on is not None else []) for f in self.stream_features()]
        return hashlib.sha256(json.dumps(spec).encode()).hexdigest()[:12]

    def filter_applies(self, name: str, event_type: str, default_type: str = "payment") -> bool:
        """Whether filter `name` is evaluated for events of `event_type` (its `event_types`; else the default type)."""
        on = self.filter_on.get(name)
        return event_type in on if on is not None else event_type == default_type

    def compiled_filters(self, event_type: str | None = None, default_type: str = "payment") -> list:
        """The filters compiled (CEL), by bit, for events of `event_type` (default: the default type); a filter
        that does not apply to the type is None (its bit stays 0), as Java's Filters.mask does."""
        import cel
        ty = event_type or default_type
        return [cel.compile(expr) if self.filter_applies(n, ty, default_type) else None
                for n, expr in self.filters.items()]

    def folds(self, f: FeatureSpec, event_type: str, default_type: str = "payment") -> bool:
        """Whether an event of `event_type` folds into stream feature `f` (otherwise it only reads it)."""
        return event_type in f.on if f.on is not None else event_type == default_type

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


def schema_path(path: str | Path | None = None) -> Path:
    """The feature schema file this process uses: `path`, else $FEATURE_SCHEMA (the override the Java
    services honour too), else the pipeline's `features` (contracts/pipelines/<PIPELINE>.yaml), else
    contracts/features/feature_schema.yaml."""
    import os
    if path or os.environ.get("FEATURE_SCHEMA"):
        return Path(path or os.environ["FEATURE_SCHEMA"])
    try:
        from ..bindings.pipeline import pipeline
        f = pipeline().features
        if f:
            p = Path(f) if Path(f).is_absolute() else REPO_ROOT / f
            if p.exists():
                return p
    except KeyError:
        pass
    return SCHEMA_PATH


def schema_bundle(path: str | Path) -> str:
    """The schema as both languages read and hash it: a file that `extends:` another is preceded by its
    base (recursively), as YAML documents separated by `---`. A file without `extends:` is its own text,
    so a schema that extends nothing keeps the hash it always had."""
    p = Path(path)
    text = p.read_text()
    base = (yaml.safe_load(text) or {}).get("extends")
    if not base:
        return text
    return schema_bundle(p.parent / base) + "\n---\n" + text


def _merge(docs: list[dict]) -> dict:
    """Merge a base-first chain of schema documents: entities, filters and profile attributes add up (a
    filter may not be redefined differently), countries and the version come from the last that sets
    them, features are appended in order (append-only: a base's features keep their tensor indices)."""
    out: dict = {"entities": {}, "filters": {}, "profile_attributes": {}, "features": [], "countries": []}
    for d in docs:
        out["entities"].update(d.get("entities") or {})
        for k, v in (d.get("filters") or {}).items():
            if k in out["filters"] and out["filters"][k] != v:
                raise ValueError(f"filter {k} is redefined differently by an extending schema")
            out["filters"][k] = v
        out["profile_attributes"].update(d.get("profile_attributes") or {})
        if d.get("countries"):
            out["countries"] = list(d["countries"])
        if "schema_version" in d:
            out["schema_version"] = d["schema_version"]
        out["features"] += list(d.get("features") or [])
    return out


def load_schema(path: str | Path | None = None) -> FeatureSchema:
    """The feature schema (see schema_path), with what it extends. Cached per path."""
    return _load_schema(str(schema_path(path)))


@lru_cache(maxsize=8)
def _load_schema(path: str) -> FeatureSchema:
    docs = [d for d in yaml.safe_load_all(schema_bundle(path)) if d]
    raw = _merge(docs) if len(docs) > 1 else docs[0]
    names = [f["name"] for f in raw["features"]]
    dup = sorted({n for n in names if names.count(n) > 1})
    if dup:
        raise ValueError(f"features defined twice: {', '.join(dup)}")
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
            if True in f or "on" in f:          # YAML 1.1 reads a bare `on:` key as the boolean true
                raise ValueError(f"{f['name']}: name the folding types with `event_types:` (not `on:`)")
            on = f.get("event_types")
            if on is not None and (not isinstance(on, list) or not on or not all(isinstance(x, str) for x in on)):
                raise ValueError(f"{f['name']}: `event_types` lists the event types that fold into it, e.g. [payment, login]")
            kw.update(entity=f["entity"], agg=f["agg"], value=f.get("value"), filter=f.get("filter"),
                      window_us=w, granularity_us=g, on=tuple(on) if on is not None else None)
        elif src == "profile":
            kw.update(entity=f["entity"], field=f["field"])
        elif src == "derived":
            if (f.get("fn") is None) == (f.get("expr") is None):
                raise ValueError(f"{f['name']}: a derived feature needs exactly one of `fn` or `expr`")
            if f.get("expr") is not None:
                import cel
                cel.compile(f["expr"])          # fail at load, not on the first event
            else:
                from . import derived_fns
                if not derived_fns.has(f["fn"]):
                    raise ValueError(f"{f['name']}: unknown derived fn {f['fn']!r} "
                                     f"(registered: {', '.join(derived_fns.names())})")
            kw.update(fn=f.get("fn"), expr=f.get("expr"))
        else:
            raise ValueError(f"{f['name']}: unknown source {src}")
        feats.append(FeatureSpec(**kw))
    filters, filter_on = {}, {}
    for k, v in (raw["filters"] or {}).items():
        if isinstance(v, dict):                 # {when: CEL, event_types: [types]}: evaluated for those types only
            if "when" not in v or True in v or "on" in v:
                raise ValueError(f"filter {k}: a scoped filter is {{when: <CEL>, event_types: [types]}}")
            filters[k] = v["when"]
            if v.get("event_types") is not None:
                filter_on[k] = tuple(v["event_types"])
        else:
            filters[k] = v
    return FeatureSchema(
        version=int(raw["schema_version"]),
        entities={k: v["key"] for k, v in raw["entities"].items()},
        filters=filters, filter_on=filter_on,
        features=feats,
        countries=list(raw["countries"]),
        profile_attributes={k: list(v) for k, v in (raw.get("profile_attributes") or {}).items()},
    )
