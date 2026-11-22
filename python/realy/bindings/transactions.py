"""Transactions formed by several linked messages (contracts/transactions/<name>.yaml), and the reference
assembler: the Python twin of the Flink assembly operator (io.realy.flink.AssembleFunction).

A transaction's messages (its roles: initiation, device context, payee check, ...) share a link id. When
the trigger and every required part have arrived (with the optional parts that arrived before them), the
parts are assembled into the unified event (`into`, the payment event) plus a context (CEL `event.txn.*`).
Lifecycle messages after that move its stage; a scored stage is decided again. One customer's messages
are totally ordered (they share the partition key), so assembly is a pure function of that order: the twin
reproduces it exactly, except for a required part that arrives after the wall-clock wait, which the
pipeline records in the decision (parts_present / partial) and the verifier replays.
"""
from __future__ import annotations

import functools
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from ..scenario import REPO_ROOT
from .events import event_type

TRANSACTIONS_DIR = REPO_ROOT / "contracts" / "transactions"
CONTEXT_KINDS = ("from", "count", "changed", "elapsed")


@dataclass(frozen=True)
class FieldMap:
    target: str
    role: str | None = None
    field: str | None = None
    const: Any = None
    enum_map: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class Stage:
    name: str
    role: str
    when: str | None = None
    scored: bool = False
    terminal: bool = False
    fields: tuple[FieldMap, ...] = ()


@dataclass(frozen=True)
class ContextSpec:
    name: str
    kind: str                       # from | count | changed | elapsed
    role: str | None = None         # from / count
    field: str | None = None        # from / changed
    between: tuple[str, str] | None = None   # changed / elapsed


@dataclass
class Transaction:
    name: str
    link: str
    roles: dict[str, str]           # role -> event type
    into: str
    trigger: str
    require: tuple[str, ...]
    optional: tuple[str, ...]
    wait_ms: int
    on_timeout: str
    fields: tuple[FieldMap, ...]
    stages: tuple[Stage, ...]
    context: tuple[ContextSpec, ...]
    source: str | None = None
    generate: dict[str, Any] = field(default_factory=dict)   # synthetic expansion parameters (gen/flows.py)

    @property
    def role_names(self) -> list[str]:
        return list(self.roles)

    def bit(self, role: str) -> int:
        return 1 << self.role_names.index(role)

    def role_of(self, type_name: str) -> list[str]:
        return [r for r, t in self.roles.items() if t == type_name]

    def context_kind(self, spec: ContextSpec) -> str:
        """Value kind of a context entry: string (enums, strings), bool, int or float."""
        if spec.kind == "count":
            return "int"
        if spec.kind == "changed":
            return "bool"
        if spec.kind == "elapsed":
            return "float"
        f = event_type(self.roles[spec.role]).codec.by_name[spec.field]
        return {"enum": "string", "string": "string", "bool": "bool", "float": "float"}.get(f.kind, "int")

    def context_default(self, spec: ContextSpec) -> Any:
        k = self.context_kind(spec)
        if k == "string" and spec.kind == "from":
            f = event_type(self.roles[spec.role]).codec.by_name[spec.field]
            if f.kind == "enum":
                return dict(f.enum_names).get(0, "0")
            return ""
        return {"bool": False, "int": 0, "float": 0.0, "string": ""}[k]

    def validate(self) -> list[str]:
        p: list[str] = []
        try:
            into = event_type(self.into).codec
        except KeyError as e:
            return [str(e)]
        for r, t in self.roles.items():
            try:
                c = event_type(t).codec
            except KeyError as e:
                p.append(f"{self.name}: role {r}: {e}")
                continue
            if self.link not in c.by_name:
                p.append(f"{self.name}: role {r} ({t}) has no link field {self.link!r}")
        for r in (self.trigger, *self.require, *self.optional):
            if r not in self.roles:
                p.append(f"{self.name}: assemble names role {r!r}, which is not one of {self.role_names}")
        for fm in self.fields + tuple(f for s in self.stages for f in s.fields):
            if fm.target not in into.by_name:
                p.append(f"{self.name}: {fm.target!r} is not a field of {self.into}")
            if fm.role is not None:
                if fm.role not in self.roles:
                    p.append(f"{self.name}: field {fm.target} reads unknown role {fm.role!r}")
                elif fm.field not in event_type(self.roles[fm.role]).codec.by_name:
                    p.append(f"{self.name}: field {fm.target} reads {fm.role}.{fm.field}, which does not exist")
        for s in self.stages:
            if s.role not in self.roles:
                p.append(f"{self.name}: stage {s.name} names unknown role {s.role!r}")
        for c in self.context:
            if c.kind not in CONTEXT_KINDS:
                p.append(f"{self.name}: context {c.name}: kind must be one of {CONTEXT_KINDS}")
            for r in [c.role] if c.role else list(c.between or ()):
                if r not in self.roles:
                    p.append(f"{self.name}: context {c.name} names unknown role {r!r}")
        if self.on_timeout not in ("score_partial", "rules_only"):
            p.append(f"{self.name}: on_timeout must be score_partial or rules_only")
        return p


def _field_maps(raw: dict[str, Any]) -> tuple[FieldMap, ...]:
    out = []
    for target, spec in (raw or {}).items():
        if isinstance(spec, str):
            role, f = spec.split(".", 1)
            out.append(FieldMap(target, role, f))
        elif "const" in spec:
            out.append(FieldMap(target, const=spec["const"]))
        else:
            role, f = spec["from"].split(".", 1)
            out.append(FieldMap(target, role, f, enum_map=tuple(sorted((spec.get("map") or {}).items()))))
    return tuple(out)


def _context(raw: dict[str, Any]) -> tuple[ContextSpec, ...]:
    out = []
    for name, spec in (raw or {}).items():
        if "from" in spec:
            role, f = spec["from"].split(".", 1)
            out.append(ContextSpec(name, "from", role=role, field=f))
        elif "count" in spec:
            out.append(ContextSpec(name, "count", role=spec["count"]))
        elif "changed" in spec:
            a, b = spec["between"]
            out.append(ContextSpec(name, "changed", field=spec["changed"], between=(a, b)))
        elif "elapsed" in spec:
            a, b = spec["elapsed"]
            out.append(ContextSpec(name, "elapsed", between=(a, b)))
        else:
            raise ValueError(f"context {name}: one of from / count / changed / elapsed")
    return tuple(out)


def parse(raw: dict[str, Any], source: str | None = None) -> Transaction:
    a = raw.get("assemble") or {}
    stages = []
    for name, s in (raw.get("stages") or {}).items():
        stages.append(Stage(name, s["message"], s.get("when"), bool(s.get("scored")), bool(s.get("terminal")),
                            _field_maps(s.get("fields") or {})))
    return Transaction(name=raw["name"], link=raw["link"], roles=dict(raw["messages"]), into=a.get("into", "payment"),
                       trigger=a["trigger"], require=tuple(a.get("require") or ()), optional=tuple(a.get("optional") or ()),
                       wait_ms=int(a.get("wait_ms", 150)), on_timeout=a.get("on_timeout", "score_partial"),
                       fields=_field_maps(a.get("fields") or {}), stages=tuple(stages), context=_context(raw.get("context")),
                       source=source, generate=dict(raw.get("generate") or {}))


@functools.lru_cache(maxsize=4)
def _load(directory: str) -> dict[str, Transaction]:
    out = {}
    for p in sorted(Path(directory).glob("*.yaml")):
        t = parse(yaml.safe_load(p.read_text()) or {}, str(p))
        out[t.name] = t
    return out


def transactions() -> dict[str, Transaction]:
    return _load(os.environ.get("REALY_TRANSACTIONS_DIR") or str(TRANSACTIONS_DIR))


def transaction(name: str) -> Transaction:
    t = transactions().get(name)
    if t is None:
        raise KeyError(f"no transaction {name!r} (contracts/transactions has {sorted(transactions())})")
    return t


def default_context(txns: list[Transaction] | None = None) -> dict[str, Any]:
    """`event.txn` of an event that is not part of a multi-message transaction: every context name of
    every contract with its default, so expressions over it never fail on a missing key."""
    out: dict[str, Any] = {"transaction": "", "stage": "", "partial": False, "parts": 0}
    for t in (txns if txns is not None else list(transactions().values())):
        for c in t.context:
            out.setdefault(c.name, t.context_default(c))
    return out


# --------------------------------------------------------------------------------------------- assembly
@dataclass
class Open:
    """One transaction in flight."""
    txn: Transaction
    txn_id: int
    partition: int = 0              # its partition key (the customer): only that key's messages close its wait
    parts: dict[str, dict[str, Any]] = field(default_factory=dict)   # role -> latest row of that role
    first: dict[str, dict[str, Any]] = field(default_factory=dict)   # role -> first row of that role
    counts: dict[str, int] = field(default_factory=dict)
    arrived: int = 0                                                  # bitmask of roles seen
    assembled: bool = False
    partial: bool = False
    stage: str = ""
    trigger_time_us: int | None = None
    held: list[tuple[str, dict[str, Any]]] = field(default_factory=list)   # stage messages before assembly


@dataclass
class Assembled:
    """An event the assembler emits for scoring and features: the unified row, its context, its kind."""
    kind: str                       # assembled | stage | passthrough
    row: dict[str, Any]             # unified event fields (enums as numbers, the event-log shape)
    context: dict[str, Any]         # event.txn (CEL), typed
    txn_id: int = 0
    transaction: str = ""
    stage: str = ""
    parts_present: int = 0
    partial: bool = False
    read_only: bool = False         # a re-scored stage: read the features, do not fold it into state again


class Assembler:
    """The reference assembler for one pipeline's transactions. Feed one customer stream (or all streams:
    state is keyed by (transaction, link id)) in partition order with `process(type, row)`; it returns the
    events to score, in order. `close()` assembles what is still waiting (end of the stream)."""

    def __init__(self, txns: list[Transaction], into: str = "payment", wait_ms_override: int | None = None) -> None:
        self.txns = txns
        self.by_type: dict[str, list[tuple[Transaction, list[str]]]] = {}
        for t in txns:
            for ty in set(t.roles.values()):
                self.by_type.setdefault(ty, []).append((t, t.role_of(ty)))
        self.open: dict[tuple[str, int], Open] = {}
        self.into = event_type(into).codec
        self.default_ctx = default_context(txns)
        self.wait_ms_override = wait_ms_override
        self._when: dict[str, Any] = {}
        self.forced: dict[tuple[str, int], int] = {}   # (txn, id) -> parts_present recorded by the pipeline
        from .pipeline import pipeline
        self.partition_field = event_type(into).entities[pipeline().partition_key]

    # ---------------------------------------------------------------- entry points
    def process(self, type_name: str, row: dict[str, Any]) -> list[Assembled]:
        """One message, in its partition's order. Like the Flink operator (keyed by the partition entity),
        only a message of the same key closes an overdue transaction's wait (a wall-clock timer does it in
        the pipeline when none comes; the content is the same in an event-time ordered stream)."""
        out: list[Assembled] = []
        t_us = int(row.get("event_time_us") or 0)
        key = int(row.get(self.partition_field) or 0)
        out += self._deadlines(t_us, key)
        entries = self.by_type.get(type_name)
        if not entries:
            if type_name == "payment":
                return out + [Assembled("passthrough", dict(row), dict(self.default_ctx))]
            return out
        for txn, roles in entries:
            tid = int(row[txn.link])
            o = self.open.get((txn.name, tid))
            if o is None:
                o = self.open[(txn.name, tid)] = Open(txn, tid, key)
            for role in roles:
                out += self._part(o, role, row)
        return out

    def close(self) -> list[Assembled]:
        return self._deadlines(None, None)

    def force(self, transaction: str, txn_id: int, parts_present: int) -> None:
        """Replay a decision the pipeline recorded: assemble this transaction with exactly these parts
        (a wall-clock timeout the event order alone cannot reproduce)."""
        self.forced[(transaction, int(txn_id))] = int(parts_present)

    # ---------------------------------------------------------------- internals
    def _wait_us(self, txn: Transaction) -> int:
        return int((self.wait_ms_override if self.wait_ms_override is not None else txn.wait_ms) * 1000)

    def _deadlines(self, t_us: int | None, key: int | None) -> list[Assembled]:
        out = []
        for o in list(self.open.values()):
            if o.assembled or o.trigger_time_us is None or (key is not None and o.partition != key):
                continue
            if t_us is None or t_us > o.trigger_time_us + self._wait_us(o.txn):
                o.partial = True
                out += self._assemble(o)
        return out

    def _part(self, o: Open, role: str, row: dict[str, Any]) -> list[Assembled]:
        txn = o.txn
        out: list[Assembled] = []
        if o.assembled and any(s.role == role for s in txn.stages):
            o.parts[role] = row
            o.first.setdefault(role, row)
            o.counts[role] = o.counts.get(role, 0) + 1
            o.arrived |= txn.bit(role)
            return self._stage(o, role, row)
        if not o.assembled and role not in (txn.trigger, *txn.require, *txn.optional):
            o.held.append((role, row))   # a lifecycle message before assembly: applied right after it
            return out
        if o.assembled:                  # a late optional / required part: later stages' context sees it
            o.parts[role] = row
            o.first.setdefault(role, row)
            o.counts[role] = o.counts.get(role, 0) + 1
            o.arrived |= txn.bit(role)
            return out
        o.parts[role] = row
        o.first.setdefault(role, row)
        o.counts[role] = o.counts.get(role, 0) + 1
        o.arrived |= txn.bit(role)
        if role == txn.trigger:
            o.trigger_time_us = int(row.get("event_time_us") or 0)
        forced = self.forced.get((txn.name, o.txn_id))
        need = (txn.bit(txn.trigger) | sum(txn.bit(r) for r in txn.require)) if forced is None else forced
        if o.trigger_time_us is not None and (o.arrived & need) == need:
            out += self._assemble(o)
        return out

    def _assemble(self, o: Open) -> list[Assembled]:
        txn = o.txn
        o.assembled = True
        o.stage = "assembled"
        row = self._unified(o, txn.fields)
        out = [Assembled("assembled", row, self._ctx(o), o.txn_id, txn.name, "assembled", o.arrived, o.partial)]
        held, o.held = o.held, []
        for role, r in held:
            o.parts[role] = r
            o.first.setdefault(role, r)
            o.counts[role] = o.counts.get(role, 0) + 1
            o.arrived |= txn.bit(role)
            out += self._stage(o, role, r)
        return out

    def _stage(self, o: Open, role: str, row: dict[str, Any]) -> list[Assembled]:
        from .events import event_type as et
        out = []
        for s in o.txn.stages:
            if s.role != role:
                continue
            if s.when:
                p = self._when.get(s.when)
                if p is None:
                    import cel
                    p = self._when[s.when] = cel.compile(s.when)
                ctx = __import__("cel").Context()
                ctx.add_variable("event", et(o.txn.roles[role]).codec.view(row))
                if not p.execute(ctx):
                    continue
            o.stage = s.name
            if s.scored:
                base = self._unified(o, o.txn.fields)
                over = self._unified(o, s.fields, base)
                out.append(Assembled("stage", over, self._ctx(o), o.txn_id, o.txn.name, s.name, o.arrived, o.partial,
                                     read_only=True))
            break
        return out

    def _unified(self, o: Open, maps: tuple[FieldMap, ...], base: dict[str, Any] | None = None) -> dict[str, Any]:
        """The unified event's fields from the parts: proto3 defaults, then each mapping (a part that has not
        arrived leaves the default)."""
        from .events import event_type as et
        row = dict(base) if base is not None else self.into.defaults()
        for fm in maps:
            tf = self.into.by_name[fm.target]
            if fm.role is None:
                row[fm.target] = self._enum_number(tf, fm.const) if tf.kind == "enum" else fm.const
                continue
            part = o.parts.get(fm.role)
            if part is None:
                continue
            v = part.get(fm.field)
            if v is None:
                continue
            if fm.enum_map:
                src = et(o.txn.roles[fm.role]).codec.by_name[fm.field]
                name = dict(src.enum_names).get(int(v), str(int(v)))
                target = dict(fm.enum_map).get(name)
                v = self._enum_number(tf, target) if target is not None else 0
            row[fm.target] = v
        return row

    @staticmethod
    def _enum_number(f, name: Any) -> int:
        if isinstance(name, int):
            return name
        for n, nm in f.enum_names:
            if nm == name:
                return n
        raise ValueError(f"{f.name}: no enum value {name!r}")

    def _ctx(self, o: Open) -> dict[str, Any]:
        """`event.txn`: the defaults of every contract, then this transaction's values."""
        from .events import event_type as et
        txn = o.txn
        ctx = dict(self.default_ctx)
        ctx.update({"transaction": txn.name, "stage": o.stage, "partial": o.partial, "parts": o.arrived})
        for c in txn.context:
            if c.kind == "from":
                part = o.parts.get(c.role)
                if part is None:
                    ctx[c.name] = txn.context_default(c)
                    continue
                f = et(txn.roles[c.role]).codec.by_name[c.field]
                v = part.get(c.field)
                if f.kind == "enum":
                    ctx[c.name] = dict(f.enum_names).get(int(v), str(int(v)))
                elif f.kind == "bool":
                    ctx[c.name] = bool(v)
                elif f.kind == "float":
                    ctx[c.name] = float(v)
                elif f.kind == "string":
                    ctx[c.name] = str(v)
                else:
                    ctx[c.name] = int(v)
            elif c.kind == "count":
                ctx[c.name] = int(o.counts.get(c.role, 0))
            elif c.kind == "changed":
                a, b = (o.first.get(c.between[0]), o.parts.get(c.between[1]))
                ctx[c.name] = bool(a is not None and b is not None and a.get(c.field) != b.get(c.field))
            elif c.kind == "elapsed":
                a, b = (o.first.get(c.between[0]), o.parts.get(c.between[1]))
                ctx[c.name] = (float((int(b["event_time_us"]) - int(a["event_time_us"])) / 1e6)
                               if a is not None and b is not None else 0.0)
        return ctx
