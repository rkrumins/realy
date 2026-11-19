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
    for k, v in cmd.items():
        sub = getattr(msg, k)
        if isinstance(v, dict):
            json_format.ParseDict(v, sub, ignore_unknown_fields=False)
        elif isinstance(v, int) and k == "phase":
            msg.phase = v
        else:
            sub.CopyFrom(v)
    return msg


def to_json(msg) -> bytes:
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
