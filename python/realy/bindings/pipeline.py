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
                "exact_entities": self.exact_entities(), "event_types": types}

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
            if self.partition_key not in t.entities:
                problems.append(f"pipeline {self.name}: event type {n} does not carry the partition key "
                                f"({self.partition_key}): its records cannot be keyed")
        return problems


@functools.lru_cache(maxsize=8)
def _load(path: str) -> Pipeline:
    raw = yaml.safe_load(Path(path).read_text()) or {}
    return Pipeline(name=raw["name"], namespace=raw.get("namespace") or "", partition_key=raw.get("partition_key", "customer"),
                    topics=dict(raw.get("topics") or {}), default_type=raw.get("default_type", "payment"),
                    event_types={k: dict(v or {}) for k, v in (raw.get("event_types") or {}).items()},
                    features=raw.get("features"), model=dict(raw.get("model") or {}), source=path)


def pipeline(name: str | None = None) -> Pipeline:
    name = name or os.environ.get("PIPELINE") or "payments"
    p = Path(name)
    if not p.suffix:
        p = Path(os.environ.get("REALY_PIPELINES_DIR") or PIPELINES_DIR) / f"{name}.yaml"
    if not p.exists():
        raise KeyError(f"no pipeline {name!r} ({p})")
    return _load(str(p))
