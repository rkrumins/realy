"""Contract compatibility: protobuf messages only ever grow. A committed snapshot
(contracts/proto/compat.lock.json) holds every message's fields (number, name, type) and every enum's
values; `tests/test_proto_compat.py` fails when a field or value is removed, renumbered or retyped.
Adding fields is allowed: refresh the snapshot with `python -m realy.proto_compat --update`."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from .scenario import REPO_ROOT

LOCK = REPO_ROOT / "contracts" / "proto" / "compat.lock.json"


def snapshot() -> dict:
    from .docsgen import _parse_proto
    out: dict = {}
    for f in sorted((REPO_ROOT / "contracts" / "proto").rglob("*.proto")):
        for b in _parse_proto(f.read_text()):
            if b["kind"] == "message":
                out[f"{f.stem}.{b['name']}"] = {str(i["num"]): {"name": i["name"], "type": i["type"]} for i in b["items"]}
            elif b["kind"] == "enum":
                out[f"{f.stem}.{b['name']}"] = {str(i["num"]): {"name": i["name"]} for i in b["items"]}
    return out


def breaking(old: dict, new: dict) -> list[str]:
    """Changes that break readers or stored data: a message / enum, field or value removed, or a number
    reused with another name or type. Additions are fine."""
    problems = []
    for msg, fields in old.items():
        if msg not in new:
            problems.append(f"{msg}: removed")
            continue
        for num, f in fields.items():
            g = new[msg].get(num)
            if g is None:
                problems.append(f"{msg}: field {num} ({f['name']}) removed; mark it `reserved` instead")
            elif g.get("type") != f.get("type"):
                problems.append(f"{msg}: field {num} ({f['name']}) changed type {f.get('type')} -> {g.get('type')}")
            elif g["name"] != f["name"]:
                problems.append(f"{msg}: number {num} renamed {f['name']} -> {g['name']} (wire-compatible, but JSON "
                                "and ClickHouse columns use the name)")
    return problems


if __name__ == "__main__":
    if "--update" in sys.argv:
        LOCK.write_text(json.dumps(snapshot(), indent=1, sort_keys=True) + "\n")
        print(LOCK)
    else:
        print("\n".join(breaking(json.loads(LOCK.read_text()), snapshot())) or "compatible")
