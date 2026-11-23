"""A protobuf message type compiled once into converters: the CEL `event` view (enums by name, integers as
int, bools, doubles, strings), the proto3 defaults (an event logged before a field existed reads its
default) and the warehouse columns. Mirrors the Java codec (io.realy.scoring.binding), which `verify`
and the golden tests keep equal.

Shared rules for both languages: an enum number without a name reads as its decimal string; a uint64
reads as the signed 64-bit value (Java's long); a uint32 reads unsigned.
"""
from __future__ import annotations

import functools
import importlib
from dataclasses import dataclass
from typing import Any

from google.protobuf.descriptor import Descriptor
from google.protobuf.descriptor import FieldDescriptor as FD

_SIGNED = {FD.TYPE_INT32, FD.TYPE_INT64, FD.TYPE_SINT32, FD.TYPE_SINT64, FD.TYPE_SFIXED32, FD.TYPE_SFIXED64}
_U32 = {FD.TYPE_UINT32, FD.TYPE_FIXED32}
_U64 = {FD.TYPE_UINT64, FD.TYPE_FIXED64}
# ClickHouse column types per proto scalar type (enums: Int8 when every number fits, else Int32)
_CH = {FD.TYPE_INT32: "Int32", FD.TYPE_SINT32: "Int32", FD.TYPE_SFIXED32: "Int32", FD.TYPE_INT64: "Int64",
       FD.TYPE_SINT64: "Int64", FD.TYPE_SFIXED64: "Int64", FD.TYPE_UINT32: "UInt32", FD.TYPE_FIXED32: "UInt32",
       FD.TYPE_UINT64: "UInt64", FD.TYPE_FIXED64: "UInt64", FD.TYPE_BOOL: "UInt8", FD.TYPE_DOUBLE: "Float64",
       FD.TYPE_FLOAT: "Float32", FD.TYPE_STRING: "String", FD.TYPE_BYTES: "String"}


@dataclass(frozen=True)
class Field:
    name: str
    number: int
    kind: str                        # int | u32 | u64 | bool | float | string | bytes | enum
    ch_type: str
    enum_names: tuple[tuple[int, str], ...] = ()
    enum_type: str | None = None     # full name of the enum

    @property
    def default(self) -> Any:
        return {"bool": False, "float": 0.0, "string": "", "bytes": b""}.get(self.kind, 0)


def _kind(fd: FD) -> str:
    t = fd.type
    if t == FD.TYPE_ENUM:
        return "enum"
    if t in _SIGNED:
        return "int"
    if t in _U32:
        return "u32"
    if t in _U64:
        return "u64"
    if t == FD.TYPE_BOOL:
        return "bool"
    if t in (FD.TYPE_DOUBLE, FD.TYPE_FLOAT):
        return "float"
    if t == FD.TYPE_STRING:
        return "string"
    if t == FD.TYPE_BYTES:
        return "bytes"
    return "message"


def _is_repeated(fd: FD) -> bool:
    lab = getattr(fd, "is_repeated", None)
    if lab is not None:
        return bool(lab)
    return fd.label == FD.LABEL_REPEATED


MAX_DEPTH = 4                         # sub-messages inlined below the top level (WireCodec.MAX_DEPTH)
_TIMES = {"google.protobuf.Timestamp", "google.protobuf.Duration"}
_WRAPPERS = {"google.protobuf.DoubleValue", "google.protobuf.FloatValue", "google.protobuf.Int64Value",
             "google.protobuf.UInt64Value", "google.protobuf.Int32Value", "google.protobuf.UInt32Value",
             "google.protobuf.BoolValue", "google.protobuf.StringValue"}
_SKIPPED = {"google.protobuf.Any", "google.protobuf.Struct", "google.protobuf.Value", "google.protobuf.ListValue",
            "google.protobuf.BytesValue", "google.protobuf.Empty"}


@dataclass(frozen=True)
class Collection:
    """A repeated or map field: one value per event (a list / a dict) at its dotted path."""
    name: str
    number: int
    kind: str                         # list | map
    elem: Field | None                # a scalar element (list) / scalar map value
    elem_type: str | None             # a message element / map value: its full name
    key_kind: str | None = None       # a map's key kind


def _leaf(fd: FD, name: str) -> Field:
    k = _kind(fd)
    if k == "enum":
        vals = tuple((v.number, v.name) for v in fd.enum_type.values)
        ch = "Int8" if all(-128 <= n <= 127 for n, _ in vals) else "Int32"
        return Field(name, fd.number, k, ch, vals, fd.enum_type.full_name)
    return Field(name, fd.number, k, _CH[fd.type])


def _converter(f: Field):
    if f.kind in ("int", "u32"):
        return int
    if f.kind == "u64":
        return _signed64
    if f.kind == "bool":
        return bool
    if f.kind == "float":
        return float
    if f.kind == "string":
        return str
    if f.kind == "bytes":
        return bytes
    return functools.partial(_enum_name, dict(f.enum_names))


class TypeCodec:
    """Compiled once per message type; `view` and `defaults` are per event. Mirrors WireCodec (Java):
    every singular scalar reachable through singular sub-messages is a leaf named by its dotted path
    (card_info.status); a repeated field is a list (a repeated message: a list of its rows / views), a map
    field a dict; Timestamp / Duration as microseconds, wrappers unwrapped; bytes-typed messages, Any and
    Struct skipped; MAX_DEPTH levels inlined. An absent sub-message reads as all defaults."""

    def __init__(self, descriptor: Descriptor) -> None:
        self.descriptor = descriptor
        self.full_name = descriptor.full_name
        leaves: list[Field] = []
        colls: list[Collection] = []
        self._tree = self._compile(descriptor, "", 0, leaves, colls)
        self.leaves: tuple[Field, ...] = tuple(leaves)
        self.collections: tuple[Collection, ...] = tuple(colls)
        # the flat (top-level, scalar) fields: what the warehouse and the reference list by default
        self.fields: tuple[Field, ...] = tuple(f for f in leaves if "." not in f.name)
        self.by_name: dict[str, Any] = {f.name: f for f in leaves}
        self.by_name.update({c.name: c for c in colls})
        self.top_names = {fd.name for fd in descriptor.fields}
        self._enum = {f.name: dict(f.enum_names) for f in leaves if f.kind == "enum"}
        self._defaults: dict[str, Any] = {f.name: f.default for f in leaves}
        self._defaults.update({c.name: ([] if c.kind == "list" else {}) for c in colls})
        self._node_leaves: dict[int, tuple[str, ...]] = {}
        self._node_default: dict[int, dict[str, Any]] = {}

    def _compile(self, d: Descriptor, prefix: str, depth: int, leaves: list, colls: list) -> list:
        """The view tree: [(name, kind, payload)] with kind leaf | node | wkt | list | map."""
        tree = []
        for fd in d.fields:
            path = prefix + fd.name
            msg = fd.message_type
            if msg is not None and msg.GetOptions().map_entry:
                kf, vf = msg.fields_by_number[1], msg.fields_by_number[2]
                if vf.type == FD.TYPE_BYTES or (vf.message_type is not None and vf.message_type.full_name in _SKIPPED):
                    continue
                elem = _leaf(vf, "value") if vf.message_type is None else None
                c = Collection(path, fd.number, "map", elem, vf.message_type.full_name if vf.message_type else None, _kind(kf))
                colls.append(c)
                tree.append((fd.name, "map", c))
            elif _is_repeated(fd):
                if fd.type == FD.TYPE_BYTES or (msg is not None and msg.full_name in _SKIPPED):
                    continue
                elem = _leaf(fd, "value") if msg is None else None
                c = Collection(path, fd.number, "list", elem, msg.full_name if msg is not None else None)
                colls.append(c)
                tree.append((fd.name, "list", c))
            elif msg is not None:
                if msg.full_name in _TIMES:
                    f = Field(path, fd.number, "int", "Int64")
                    leaves.append(f)
                    tree.append((fd.name, "time", f))
                elif msg.full_name in _WRAPPERS:
                    f = _leaf(msg.fields_by_number[1], path)
                    leaves.append(f)
                    tree.append((fd.name, "wrapped", f))
                elif msg.full_name in _SKIPPED or depth >= MAX_DEPTH:
                    continue
                else:
                    tree.append((fd.name, "node", self._compile(msg, path + ".", depth + 1, leaves, colls)))
            elif fd.type == FD.TYPE_BYTES:
                continue
            else:
                f = _leaf(fd, path)
                leaves.append(f)
                tree.append((fd.name, "leaf", f))
        return tree

    # ------------------------------------------------------------------ views
    def defaults(self) -> dict[str, Any]:
        """proto3 defaults of every field (what a reader sees for an absent field)."""
        return {k: (list(v) if isinstance(v, list) else dict(v) if isinstance(v, dict) else v)
                for k, v in self._defaults.items()}

    def view(self, ev: dict[str, Any]) -> dict[str, Any]:
        """CEL `event` from a row of field values (enums as numbers, as the event logs store them): nested
        dicts for sub-messages, lists, dicts. A field the row lacks (an event logged before the field
        existed) reads its proto3 default."""
        return self._view(self._tree, ev)

    def _view(self, tree: list, ev: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for name, kind, x in tree:
            if kind == "node":
                # a block the row does not carry at all (most payments): one shared view of its defaults
                leaves = self._node_leaves.get(id(x))
                if leaves is None:
                    leaves = self._node_leaves[id(x)] = tuple(_tree_keys(x))
                if not any(k in ev for k in leaves):
                    d = self._node_default.get(id(x))
                    if d is None:
                        d = self._node_default[id(x)] = self._view(x, {})
                    out[name] = d
                else:
                    out[name] = self._view(x, ev)
                continue
            if kind in ("leaf", "time", "wrapped"):
                v = ev.get(x.name)
                out[name] = _converter(x)(x.default if v is None else v)
                continue
            v = ev.get(x.name)
            if kind == "list":
                items = [] if v is None else list(v)
                if x.elem_type:
                    ec = codec(x.elem_type)
                    out[name] = [ec.view(i) for i in items]
                else:
                    fn = _converter(x.elem)
                    out[name] = [fn(i) for i in items]
            else:
                pairs = {} if v is None else (dict(v) if not isinstance(v, list) else dict(v))
                if x.elem_type:
                    ec = codec(x.elem_type)
                    out[name] = {k: ec.view(i) for k, i in pairs.items()}
                else:
                    fn = _converter(x.elem)
                    out[name] = {k: fn(i) for k, i in pairs.items()}
        return out

    def row(self, msg: Any) -> dict[str, Any]:
        """Field values of a decoded message (enums as numbers): the event-log row shape, nested leaves under
        their dotted path, lists and dicts (a repeated message: a list of its rows)."""
        out: dict[str, Any] = {}
        self._row(self._tree, msg, out)
        return out

    def _row(self, tree: list, msg: Any, out: dict[str, Any]) -> None:
        for name, kind, x in tree:
            v = getattr(msg, name)
            if kind == "node":
                self._row(x, v, out)
            elif kind == "leaf":
                out[x.name] = v
            elif kind == "time":
                out[x.name] = int(v.seconds) * 1_000_000 + int(v.nanos) // 1_000
            elif kind == "wrapped":
                out[x.name] = v.value
            elif kind == "list":
                out[x.name] = [codec(x.elem_type).row(i) for i in v] if x.elem_type else list(v)
            else:
                out[x.name] = {k: codec(x.elem_type).row(i) for k, i in v.items()} if x.elem_type else dict(v)

    def enum_name(self, field: str, number: int) -> str:
        return _enum_name(self._enum[field], number)

    def numeric(self, field: str, value: Any) -> float:
        """A field's value as a number, the way stream features read it (WireCodec.numeric): an enum as its
        number, a bool as 1 / 0."""
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str) and field in self._enum:
            for num, name in self._enum[field].items():
                if name == value:
                    return float(num)
            try:
                return float(int(value))
            except ValueError:
                return 0.0
        return 0.0

    def columns(self, overrides: dict[str, str] | None = None, nested: bool = False) -> list[tuple[str, str]]:
        """(column, ClickHouse type) for every flat field, in field order; with `nested`, also every nested
        leaf (`card_info.status`), lists as Array(T) (a repeated message: one Array per leaf of it,
        `items.sku`) and maps as Map(String, T)."""
        o = overrides or {}
        if not nested:
            return [(f.name, o.get(f.name, f.ch_type)) for f in self.fields]
        out = [(f.name, o.get(f.name, f.ch_type)) for f in self.leaves]
        for c in self.collections:
            if c.elem is not None:
                t = c.elem.ch_type
                out.append((c.name, f"Array({t})" if c.kind == "list" else f"Map(String, {t})"))
            elif c.kind == "list":
                for f in codec(c.elem_type).leaves:
                    out.append((f"{c.name}.{f.name}", f"Array({f.ch_type})"))
        return out


def _tree_keys(tree: list):
    """Every row key under a view tree (its leaves' and collections' dotted names)."""
    for _name, kind, x in tree:
        if kind == "node":
            yield from _tree_keys(x)
        else:
            yield x.name


def _signed64(x: Any) -> int:
    x = int(x)
    return x - (1 << 64) if x >= 1 << 63 else x


def _enum_name(names: dict[int, str], x: Any) -> str:
    i = int(x)
    n = names.get(i)
    return n if n is not None else str(i)


@functools.lru_cache(maxsize=1)
def _import_stubs() -> None:
    """Import every generated *_pb2 module under realy/ so the default descriptor pool knows them (found
    on disk: importing the other packages to discover them would pull in the console, the trainer...)."""
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    for f in sorted(root.rglob("*_pb2.py")):
        rel = f.relative_to(root.parent).with_suffix("")
        importlib.import_module(".".join(rel.parts))


def descriptor(full_name: str) -> Descriptor:
    from google.protobuf import descriptor_pool
    _import_stubs()
    try:
        return descriptor_pool.Default().FindMessageTypeByName(full_name)
    except KeyError:
        raise KeyError(f"no protobuf message {full_name!r} (is it under contracts/proto, with Python stubs "
                       f"generated by scripts/gen-proto-python.sh?)") from None


def message_class(full_name: str):
    from google.protobuf import message_factory
    return message_factory.GetMessageClass(descriptor(full_name))


@functools.lru_cache(maxsize=64)
def codec(full_name: str) -> TypeCodec:
    return TypeCodec(descriptor(full_name))
