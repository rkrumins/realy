"""`realy store get`: decode online-store blobs (contracts/storage/redis.md) into named features.

    realy store get customer 1234        # fs:customer:1234 (Flink) + pf:customer:1234 (profile)
    realy store get card 98765 --json

Shows what the gateway reads for an entity before it builds the model tensor: the stream features
Flink wrote (values as of the entity's last event), how old they are, and whether the blob carries
the aggregator state the gateway re-windows at scoring time (state blobs, default customer/card).
"""
from __future__ import annotations

import struct
import time
from typing import Any

from ..features import load_schema

HDR = struct.Struct("<HHqq")
STATE_FLAG = 0x8000


def decode(raw: bytes) -> dict[str, Any]:
    ver, n, as_of, updated = HDR.unpack_from(raw, 0)
    off = HDR.size
    values = list(struct.unpack_from(f"<{n}f", raw, off))
    off += 4 * n
    out: dict[str, Any] = {"schema_version": ver & ~STATE_FLAG, "state_blob": bool(ver & STATE_FLAG),
                           "as_of_us": as_of, "updated_at_us": updated, "values": values, "bytes": len(raw)}
    if ver & STATE_FLAG and len(raw) >= off + 12:
        last_event, slen = struct.unpack_from("<QI", raw, off)
        out.update(last_event_id=last_event, state_bytes=slen)
    return out


def get(entity: str, entity_id: int, r: Any = None) -> dict[str, Any]:
    from .seed import redis_client
    r = r or redis_client()
    schema = load_schema()
    res: dict[str, Any] = {"entity": entity, "id": entity_id}
    now = time.time() * 1e6
    raw = r.get(f"fs:{entity}:{entity_id}")
    if raw:
        b = decode(raw)
        names = [f.name for f in schema.features if f.source == "stream" and f.entity == entity]
        b["features"] = dict(zip(names, b.pop("values")))
        b["age_s"] = round((now - b["updated_at_us"]) / 1e6, 1)
        b["ttl_s"] = r.ttl(f"fs:{entity}:{entity_id}")
        res["stream"] = {"key": f"fs:{entity}:{entity_id}", **b}
    raw = r.get(f"pf:{entity}:{entity_id}")
    if raw:
        b = decode(raw)
        names = [f.name for f in schema.profile_features(entity)] + list(schema.profile_attributes.get(entity, []))
        b["features"] = dict(zip(names, b.pop("values")))
        res["profile"] = {"key": f"pf:{entity}:{entity_id}", **b}
    return res


def render(res: dict[str, Any]) -> str:
    lines = []
    for part in ("stream", "profile"):
        b = res.get(part)
        if not b:
            lines.append(f"{part}: (no {'fs' if part == 'stream' else 'pf'}:{res['entity']}:{res['id']} key)")
            continue
        head = f"{b['key']}  {b['bytes']} B  schema v{b['schema_version']}"
        if part == "stream":
            head += (f"  {'STATE blob: last event ' + str(b.get('last_event_id')) + ', state ' + str(b.get('state_bytes')) + ' B' if b['state_blob'] else 'values blob'}"
                     f"  written {b['age_s']} s ago  TTL {b['ttl_s']} s")
        lines.append(head)
        for k, v in b["features"].items():
            lines.append(f"  {k:32s} {v:14.4f}")
    return "\n".join(lines)
