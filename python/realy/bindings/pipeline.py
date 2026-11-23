"""Pipelines (contracts/pipelines/<name>.yaml): which event types flow, how they are partitioned, which
features and model run. PIPELINE selects one (default payments)."""
from __future__ import annotations

import functools
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from ..scenario import REPO_ROOT
from .events import EventType, event_type
from .registry import registry

PIPELINES_DIR = REPO_ROOT / "contracts" / "pipelines"


@dataclass
class Pipeline:
    name: str
    namespace: str = ""
    partition_key: str = "customer"
    topics: dict[str, str] = field(default_factory=dict)
    default_type: str = "payment"
    event_types: dict[str, dict[str, Any]] = field(default_factory=dict)
    features: str | None = None
    model: dict[str, Any] = field(default_factory=dict)
    transactions: list[str] = field(default_factory=list)
    source: str | None = None

    def types(self) -> dict[str, EventType]:
        return {n: event_type(n) for n in self.event_types}

    def scored(self) -> list[str]:
        return [n for n, spec in self.event_types.items() if (spec or {}).get("scored", False)]

    def exact_entities(self) -> list[str]:
        return registry().exact(self.partition_key)

    def key_prefix(self) -> str:
        """Redis key prefix of this pipeline's namespace ("" = today's keys)."""
        return f"{self.namespace}:" if self.namespace else ""

    def snapshot(self) -> dict[str, Any]:
        """Everything this pipeline's events mean, resolved: written to each run directory
        (pipeline.json), so a run is read back with the bindings it ran with."""
        reg = registry()
        types = {}
        for n, t in self.types().items():
            types[n] = {"message": t.message, "id": t.id, "time": t.time, "time_unit": t.time_unit,
                        "entities": t.entities, "location": t.location, "amount": t.amount,
                        "scored": bool((self.event_types[n] or {}).get("scored")),
                        "fields": [{"name": f.name, "number": f.number, "kind": f.kind} for f in t.codec.fields]}
        return {"name": self.name, "namespace": self.namespace, "partition_key": self.partition_key,
                "default_type": self.default_type, "topics": self.topics, "features": self.features,
                "entities": {e.name: {"id": e.id, "owned_by": e.owned_by, "enum": e.enum} for e in reg.entities.values()},
                "exact_entities": self.exact_entities(), "event_types": types, "transactions": self.transactions}

    def _validate_schema(self) -> list[str]:
        return _schema_problems(self)

    def topic_of(self, event_type: str) -> str:
        """The Kafka topic an event type travels on (its `topic`, else the events topic)."""
        return self.topics[(self.event_types.get(event_type) or {}).get("topic", "events")]

    def partition_key_of(self, event_type: str) -> str:
        return (self.event_types.get(event_type) or {}).get("partition_key", self.partition_key)

    def part_types(self) -> set[str]:
        """The event types that are parts of the pipeline's multi-message transactions."""
        from .transactions import transaction
        return {ty for n in self.transactions for ty in transaction(n).roles.values()}

    def standalone(self, event_type: str) -> bool:
        """A type that is neither the default type nor a transaction part: it folds into features on its own
        (PipelineBinding.standalone)."""
        return event_type != self.default_type and event_type in self.event_types and event_type not in self.part_types()

    def extra_topics(self) -> dict[str, list[str]]:
        """Topics other than the events topic, with the event types that travel on each."""
        out: dict[str, list[str]] = {}
        events = self.topics.get("events")
        for t in self.event_types:
            topic = self.topic_of(t)
            if topic != events:
                out.setdefault(topic, []).append(t)
        return out

    def validate(self) -> list[str]:
        problems = []
        reg = registry()
        if self.partition_key not in reg:
            problems.append(f"pipeline {self.name}: partition_key {self.partition_key!r} is not an entity")
        if self.default_type not in self.event_types:
            problems.append(f"pipeline {self.name}: default_type {self.default_type!r} is not one of its event types")
        for n in self.event_types:
            try:
                t = event_type(n)
            except KeyError as e:
                problems.append(str(e))
                continue
            problems += t.validate()
            spec = self.event_types[n] or {}
            topic, key = spec.get("topic", "events"), spec.get("partition_key", self.partition_key)
            if topic not in self.topics:
                problems.append(f"pipeline {self.name}: event type {n}'s topic {topic!r} is not one of its topics")
            if key not in t.entities:
                problems.append(f"pipeline {self.name}: event type {n} does not carry its topic's partition key "
                                f"({key}): its records cannot be keyed")
        problems += self._validate_schema()
        from .transactions import transaction
        for n in self.transactions:
            try:
                tx = transaction(n)
            except KeyError as e:
                problems.append(str(e))
                continue
            problems += tx.validate()
            for role, ty in tx.roles.items():
                if ty not in self.event_types:
                    problems.append(f"pipeline {self.name}: transaction {n} role {role} ({ty}) is not one of its event types")
        return problems


def _event_fields(expr: str) -> set[str]:
    import re
    return set(re.findall(r"\bevent\.([A-Za-z_][A-Za-z0-9_]*)", expr))


def _schema_problems(p: "Pipeline") -> list[str]:
    """The feature schema against the pipeline's event types: every type a feature or filter names is one of
    the pipeline's, carries the feature's entity and its value field, and has every field a scoped filter
    reads (a payment's fields are not a login's)."""
    if not p.features:
        return []
    from ..features.schema import REPO_ROOT, _load_schema
    path = Path(p.features) if Path(p.features).is_absolute() else REPO_ROOT / p.features
    if not path.exists():
        return [f"pipeline {p.name}: features file {p.features} does not exist"]
    try:
        schema = _load_schema(str(path))
    except Exception as e:  # noqa: BLE001 - a broken schema is a problem to report, not a crash
        return [f"pipeline {p.name}: features {p.features}: {e}"]
    out: list[str] = []
    fields = {}
    for n in p.event_types:
        try:
            fields[n] = {f.name for f in event_type(n).codec.fields}
        except KeyError:
            continue
    for name, types in schema.filter_on.items():
        for ty in types:
            if ty not in p.event_types:
                out.append(f"filter {name}: event type {ty!r} is not one of pipeline {p.name}'s")
            elif ty in fields:
                missing = _event_fields(schema.filters[name]) - fields[ty]
                if missing:
                    out.append(f"filter {name}: {ty} has no field {', '.join(sorted(missing))}")
    for f in schema.stream_features():
        for ty in f.on or ():
            if ty not in p.event_types:
                out.append(f"{f.name}: event type {ty!r} is not one of pipeline {p.name}'s")
                continue
            if ty == p.default_type or ty not in fields:
                continue
            t = event_type(ty)
            if f.entity not in t.entities:
                out.append(f"{f.name}: {ty} does not carry its entity ({f.entity})")
            if f.value and f.value not in fields[ty]:
                out.append(f"{f.name}: {ty} has no field {f.value} (its `value`)")
            if f.filter and not schema.filter_applies(f.filter, ty, p.default_type):
                out.append(f"{f.name}: its filter {f.filter} is not evaluated for {ty} (add it to the filter's event_types)")
    return out


@functools.lru_cache(maxsize=8)
def _load(path: str) -> Pipeline:
    raw = yaml.safe_load(Path(path).read_text()) or {}
    return Pipeline(name=raw["name"], namespace=raw.get("namespace") or "", partition_key=raw.get("partition_key", "customer"),
                    topics=dict(raw.get("topics") or {}), default_type=raw.get("default_type", "payment"),
                    event_types={k: dict(v or {}) for k, v in (raw.get("event_types") or {}).items()},
                    features=raw.get("features"), model=dict(raw.get("model") or {}),
                    transactions=list(raw.get("transactions") or []), source=path)


def pipeline(name: str | None = None) -> Pipeline:
    name = name or os.environ.get("PIPELINE") or "payments"
    p = Path(name)
    if not p.suffix:
        p = Path(os.environ.get("REALY_PIPELINES_DIR") or PIPELINES_DIR) / f"{name}.yaml"
    if not p.exists():
        raise KeyError(f"no pipeline {name!r} ({p})")
    return _load(str(p))
