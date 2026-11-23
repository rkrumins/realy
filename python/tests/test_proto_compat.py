"""Protobuf contracts only grow: no field or enum value is removed, renumbered or retyped."""
import json

from realy.proto_compat import LOCK, breaking, snapshot


def test_contracts_are_backward_compatible():
    old, new = json.loads(LOCK.read_text()), snapshot()
    assert breaking(old, new) == [], "breaking proto change (see realy/proto_compat.py)"
    added = sorted(f"{m}.{n}" for m, fs in new.items() for n in fs if n not in old.get(m, {}))
    assert not added, f"new fields {added}: run `python -m realy.proto_compat --update` and commit the snapshot"


def test_the_check_catches_breaking_changes():
    old = {"transaction.TransactionEvent": {"1": {"name": "event_id", "type": "fixed64"}, "2": {"name": "amount", "type": "int64"}}}
    assert breaking(old, {"transaction.TransactionEvent": {"1": {"name": "event_id", "type": "fixed64"}}})
    assert breaking(old, {"transaction.TransactionEvent": {"1": {"name": "event_id", "type": "uint64"},
                                                           "2": {"name": "amount", "type": "int64"}}})
    assert breaking(old, {}) == ["transaction.TransactionEvent: removed"]
    grown = {"transaction.TransactionEvent": {**old["transaction.TransactionEvent"], "3": {"name": "new", "type": "bool"}}}
    assert breaking(old, grown) == []
