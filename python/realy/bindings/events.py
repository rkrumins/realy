"""Event type bindings (contracts/events/<type>.yaml): what a protobuf message means to the pipeline."""
from __future__ import annotations

import functools
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from ..scenario import REPO_ROOT
from .codec import TypeCodec, codec
from .registry import registry

EVENTS_DIR = REPO_ROOT / "contracts" / "events"


@dataclass
class EventType:
    name: str
    message: str
    id: str
    time: str
    time_unit: str = "us"
    id_hash: str | None = None
    entities: dict[str, str] = field(default_factory=dict)   # registry entity -> field
    location: dict[str, str] | None = None
    amount: str | None = None
    labels: dict[str, Any] = field(default_factory=dict)
    warehouse: dict[str, Any] = field(default_factory=dict)
    source: str | None = None

    @property
    def codec(self) -> TypeCodec:
        return codec(self.message)

    def entity_field(self, entity: str) -> str | None:
        return self.entities.get(entity)

    def validate(self) -> list[str]:
        """Problems with this binding against its protobuf message and the entity registry."""
        problems = []
        try:
            c = self.codec
        except KeyError as e:
            return [str(e)]
        reg = registry()

        def need(f: str | None, what: str, kinds=None) -> None:
            if f is None:
                return
            fld = c.by_name.get(f)
            if fld is None:
                problems.append(f"{self.name}: {what} field {f!r} is not a field of {self.message}")
            elif kinds and fld.kind not in kinds:
                problems.append(f"{self.name}: {what} field {f!r} is {fld.kind}, needs one of {kinds}")
        need(self.id, "id", ("u64", "int", "u32") if not self.id_hash else ("string",))
        need(self.time, "time", ("int", "u64"))
        for ent, f in self.entities.items():
            if ent not in reg:
                problems.append(f"{self.name}: entity {ent!r} is not in contracts/entities.yaml")
                continue
            kinds = ("string",) if reg[ent].id == "string" else ("u64", "u32", "int")
            need(f, f"entity {ent}", kinds)
        for k in ("lat", "lon"):
            need((self.location or {}).get(k), f"location {k}", ("float",))
        need(self.amount, "amount", ("int", "u64", "u32"))
        if self.time_unit not in ("us", "ms", "s"):
            problems.append(f"{self.name}: time unit {self.time_unit!r} (us, ms or s)")
        return problems


def _parse(raw: dict[str, Any], source: str | None = None) -> EventType:
    idv, tv = raw.get("id", "event_id"), raw.get("time", "event_time_us")
    return EventType(
        name=raw["name"], message=raw["message"],
        id=idv["field"] if isinstance(idv, dict) else idv,
        id_hash=idv.get("hash") if isinstance(idv, dict) else None,
        time=tv["field"] if isinstance(tv, dict) else tv,
        time_unit=tv.get("unit", "us") if isinstance(tv, dict) else "us",
        entities=dict(raw.get("entities") or {}), location=raw.get("location"), amount=raw.get("amount"),
        labels=dict(raw.get("labels") or {}), warehouse=dict(raw.get("warehouse") or {}), source=source)


@functools.lru_cache(maxsize=4)
def _load(directory: str) -> dict[str, EventType]:
    out: dict[str, EventType] = {}
    for p in sorted(Path(directory).glob("*.yaml")):
        t = _parse(yaml.safe_load(p.read_text()) or {}, str(p))
        if t.name in out:
            raise ValueError(f"event type {t.name!r} bound twice ({out[t.name].source}, {p})")
        out[t.name] = t
    return out


def event_types() -> dict[str, EventType]:
    return _load(os.environ.get("REALY_EVENTS_DIR") or str(EVENTS_DIR))


def event_type(name: str) -> EventType:
    t = event_types().get(name)
    if t is None:
        raise KeyError(f"no event type {name!r} (contracts/events has {sorted(event_types())})")
    return t
