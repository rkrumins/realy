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


class TypeCodec:
    """Compiled once per message type; `view` and `defaults` are per event."""

    def __init__(self, descriptor: Descriptor) -> None:
        self.descriptor = descriptor
        self.full_name = descriptor.full_name
        fields = []
        for fd in descriptor.fields:          # field-number order as declared
            k = _kind(fd)
            if k == "message" or _is_repeated(fd):
                continue                       # nested / repeated fields are not part of the flat view
            if k == "enum":
                vals = tuple((v.number, v.name) for v in fd.enum_type.values)
                ch = "Int8" if all(-128 <= n <= 127 for n, _ in vals) else "Int32"
                fields.append(Field(fd.name, fd.number, k, ch, vals, fd.enum_type.full_name))
            else:
                fields.append(Field(fd.name, fd.number, k, _CH[fd.type]))
        self.fields: tuple[Field, ...] = tuple(fields)
        self.by_name = {f.name: f for f in self.fields}
        self._enum = {f.name: dict(f.enum_names) for f in self.fields if f.kind == "enum"}
        self._defaults = {f.name: f.default for f in self.fields}
        # converters compiled once: (name, fn(value) -> view value)
        conv = []
        for f in self.fields:
            if f.kind in ("int", "u32"):
                conv.append((f.name, int))
            elif f.kind == "u64":
                conv.append((f.name, _signed64))
            elif f.kind == "bool":
                conv.append((f.name, bool))
            elif f.kind == "float":
                conv.append((f.name, float))
            elif f.kind == "string":
                conv.append((f.name, str))
            elif f.kind == "bytes":
                conv.append((f.name, bytes))
            else:
                names = self._enum[f.name]
                conv.append((f.name, functools.partial(_enum_name, names)))
        self._conv = tuple(conv)

    # ------------------------------------------------------------------ views
    def defaults(self) -> dict[str, Any]:
        """proto3 defaults of every field (what a reader sees for an absent field)."""
        return dict(self._defaults)

    def view(self, ev: dict[str, Any]) -> dict[str, Any]:
        """CEL `event` from a row of field values (enums as numbers, as the event logs store them). A
        field the row lacks (an event logged before the field existed) reads its proto3 default."""
        d = self._defaults
        out: dict[str, Any] = {}
        for name, fn in self._conv:
            x = ev.get(name)
            out[name] = fn(d[name] if x is None else x)
        return out

    def row(self, msg: Any) -> dict[str, Any]:
        """Field values of a decoded message (enums as numbers): the event-log row shape."""
        return {f.name: getattr(msg, f.name) for f in self.fields}

    def enum_name(self, field: str, number: int) -> str:
        return _enum_name(self._enum[field], number)

    def columns(self, overrides: dict[str, str] | None = None) -> list[tuple[str, str]]:
        """(column, ClickHouse type) for every flat field, in field order."""
        o = overrides or {}
        return [(f.name, o.get(f.name, f.ch_type)) for f in self.fields]


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
