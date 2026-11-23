"""The entity registry (contracts/entities.yaml): every kind of thing events refer to and state is kept for."""
from __future__ import annotations

import functools
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from ..scenario import REPO_ROOT

REGISTRY_PATH = REPO_ROOT / "contracts" / "entities.yaml"
ID_TYPES = ("u64", "u32", "string")


@dataclass(frozen=True)
class Entity:
    name: str
    ordinal: int
    id: str = "u64"
    enum: str | None = None
    owned_by: str | None = None
    format: str | None = None


@dataclass
class Registry:
    entities: dict[str, Entity] = field(default_factory=dict)

    @property
    def names(self) -> list[str]:
        return list(self.entities)

    def __getitem__(self, name: str) -> Entity:
        e = self.entities.get(name)
        if e is None:
            raise KeyError(f"unknown entity {name!r} (contracts/entities.yaml has {self.names})")
        return e

    def __contains__(self, name: str) -> bool:
        return name in self.entities

    def owners(self, name: str) -> list[str]:
        """The chain of owners of an entity, nearest first (card -> [customer])."""
        out, seen = [], {name}
        cur = self[name].owned_by
        while cur:
            if cur in seen:
                raise ValueError(f"entity ownership cycle through {cur!r}")
            out.append(cur)
            seen.add(cur)
            cur = self[cur].owned_by
        return out

    def stage_of(self, name: str) -> str:
        """The entity whose keyed stage computes `name`'s features: itself, unless it is owned and does not
        directly follow its owner in registry order, then its owner's stage (keyed by its own id inside the owner's
        key: an account in its customer's stage). Mirrors EntityRegistry.stageOf (Java)."""
        owner = self.entities[name].owned_by
        if owner is None or self.names.index(name) == self.names.index(owner) + 1:
            return name
        return self.stage_of(owner)

    def evaluation_order(self, present) -> list[str]:
        """The order features are computed in for the entities in `present`: stage by stage in registry order,
        each stage's own entity first, then those it hosts. A `gate:` reads a feature computed earlier in this
        order. Mirrors EntityRegistry.evaluationOrder."""
        out = []
        for s in self.names:
            if self.stage_of(s) != s:
                continue
            if s in present:
                out.append(s)
            out += [n for n in self.names if n != s and n in present and self.stage_of(n) == s]
        return out

    def exact(self, partition: str) -> list[str]:
        """Entities whose events a pipeline partitioned by `partition` sees in one total order: the
        partition entity and every entity it owns, directly or through a chain. Their features are exact;
        the others are shared across partitions (verified as an agreement rate)."""
        return [n for n in self.names if n == partition or partition in self.owners(n)]


def load_registry(path: str | Path | None = None) -> Registry:
    raw = yaml.safe_load(Path(path or REGISTRY_PATH).read_text()) or {}
    ents = raw.get("entities") or {}
    reg = Registry()
    for i, (name, spec) in enumerate(ents.items()):
        spec = spec or {}
        e = Entity(name=name, ordinal=i, id=spec.get("id", "u64"), enum=spec.get("enum"),
                   owned_by=spec.get("owned_by"), format=spec.get("format"))
        if e.id not in ID_TYPES:
            raise ValueError(f"entity {name}: id must be one of {ID_TYPES}, not {e.id!r}")
        reg.entities[name] = e
    for e in reg.entities.values():
        if e.owned_by and e.owned_by not in reg.entities:
            raise ValueError(f"entity {e.name}: owned_by {e.owned_by!r} is not an entity")
        reg.owners(e.name)          # rejects cycles
    return reg


@functools.lru_cache(maxsize=4)
def _cached(path: str) -> Registry:
    return load_registry(path)


def registry() -> Registry:
    return _cached(os.environ.get("REALY_ENTITIES") or str(REGISTRY_PATH))
