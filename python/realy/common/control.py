"""bench.control publishing (proto3 JSON ControlMessage) — the runtime config plane."""
from __future__ import annotations

import json
from typing import Any

from google.protobuf import json_format

from ..bench.v1 import control_pb2
from .clock import now_us
from .kafka import CONTROL, make_producer

_version = 0


def control_message(run_id: str, target: str, **cmd: Any) -> control_pb2.ControlMessage:
    global _version
    _version += 1
    msg = control_pb2.ControlMessage(run_id=run_id, issued_at_us=now_us(), version=now_us(), target=target)
    raw: dict[str, Any] = {"run_id": run_id, "issued_at_us": msg.issued_at_us, "version": msg.version, "target": target}
    for k, v in cmd.items():
        if isinstance(v, dict):
            json_format.ParseDict(v, getattr(msg, k), ignore_unknown_fields=False)  # validates the command
            raw[k] = v
        elif isinstance(v, int) and k == "phase":
            msg.phase = v
            raw[k] = v
        else:
            getattr(msg, k).CopyFrom(v)
            raw[k] = json.loads(json_format.MessageToJson(v, preserving_proto_field_name=True))
    wrapped = _Msg(msg)
    wrapped._raw = raw
    return wrapped


class _Msg:
    """ControlMessage plus the presence-preserving JSON it was built from."""

    def __init__(self, msg: control_pb2.ControlMessage) -> None:
        self._msg = msg
        self._raw: dict[str, Any] | None = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._msg, name)


def to_json(msg) -> bytes:
    """Wire JSON. Uses the caller's original command dict when available: proto3 JSON printing drops
    fields set to their default (0/false), and services apply config with *presence* semantics
    (every field present overrides), so presence must survive serialization."""
    raw = getattr(msg, "_raw", None)
    if raw is not None:
        return json.dumps(raw, separators=(",", ":")).encode()
    return json_format.MessageToJson(msg, preserving_proto_field_name=True, indent=None).encode()


def publish(messages: list[control_pb2.ControlMessage], producer=None) -> None:
    p = producer or make_producer(acks="all")
    for m in messages:
        # Compacted topic keyed by target+command: the latest config of each kind survives compaction.
        key = f"{m.target}:{m.WhichOneof('cmd')}".encode()
        p.produce(CONTROL, value=to_json(m), key=key)
    p.flush(10)


def parse_json(data: bytes) -> dict[str, Any]:
    return json.loads(data)
