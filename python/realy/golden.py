"""Golden payments set: a small, fixed, fully deterministic stream of payment events plus everything
needed to score it offline (profile blobs, the complete scoring config), so that a refactor of the
event path can prove it changed nothing for payments.

    contracts/testdata/golden/payments/
        manifest.json          world spec, seed, counts, scenario fingerprint, schema version / layout
        events.pb.gz           TransactionEvent payloads, varint-length-delimited, in event-time order
        events.parquet         the same events as columns (proto field names, ground truth), as the
                               emitter logs them (the Python twin reads these)
        profiles.bin.gz        every pf:<entity>:<id> blob: (u16 key length, key, u32 blob length, blob)*
        scoring_config.json    the complete ScoringConfig (rules inlined) the gateway would receive
        java/                  what the Java scoring path produced for these events (GoldenPaymentsTest
                               records it with -Dgolden.record=true and checks it otherwise)

Java (`GoldenPaymentsTest`) replays the events through the feature aggregators and the scoring
orchestrator exactly as the Flink job and gateway do, and compares its canonical decisions and final
state blobs byte for byte with java/. Python (`tests/test_golden_payments.py`) recomputes features, rules,
score and outcome with the twin and compares them with java/.

    python -m realy.golden         rebuild the event set (only when the generator changes on purpose;
                                   then re-record the Java side and review the diff)
    python -m realy.golden --scenarios   re-pin the bundled scenarios' fingerprints (scenarios.json)
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import struct
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .scenario import REPO_ROOT, Scenario, WorldSpec

OUT = REPO_ROOT / "contracts" / "testdata" / "golden" / "payments"
SPEC = WorldSpec(customers=3000, merchants=400, payees=1500)
SEED = 11
N_EVENTS = 6000
# Deadlines far above any in-process call: the golden decisions must never depend on timing.
SCORING = {"feature_deadline_ms": 60_000, "model_deadline_ms": 60_000, "budget_ms": 100}
PROFILE_UPDATED_US = 1_700_000_000_000_000


def scenario() -> Scenario:
    return Scenario(name="golden-payments", seed=SEED, world=SPEC,
                    fraud={"background_per_1k": 0.0, "friendly_fraud_per_1k": 2.0},
                    scoring=dict(SCORING))


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def generate(n_events: int = N_EVENTS) -> tuple[list[bytes], dict[str, np.ndarray], Any]:
    """The golden events: everyday behaviour plus one campaign of every typology, in event-time order."""
    from .gen.generator import Generator, sim_start_us
    from .world import build_world

    sc = scenario()
    world = build_world(SPEC, SEED, None)
    gen = Generator(world, sc, 0, 1)
    t0 = sim_start_us(sc)
    rate = max(1e-9, gen.natural_rate_per_sim_s())
    span_us = int(n_events / rate * 1e6)
    kinds = [k for k in gen.reg.names() if gen.reg.by_name[k].campaigns()]
    for i, kind in enumerate(kinds):           # staggered over the first two thirds of the window
        gen.schedule_campaign(kind, t0 + int(span_us * (0.05 + 0.6 * i / max(1, len(kinds)))))
    payloads: list[bytes] = []
    cols: list[dict[str, np.ndarray]] = []
    t, per = t0, 200
    dt = int(per / rate * 1e6)
    while len(payloads) < n_events:
        b = gen.generate(per, t, t + dt)
        t += dt
        payloads.extend(b.payloads)
        cols.append(b.columns)
    keep = n_events
    merged = {k: np.concatenate([c[k] for c in cols])[:keep] for k in cols[0]}
    return payloads[:keep], merged, world


def write(out: Path = OUT, n_events: int = N_EVENTS) -> dict[str, Any]:
    from .features import load_schema
    from .runner.seed import profile_blobs

    out.mkdir(parents=True, exist_ok=True)
    payloads, cols, world = generate(n_events)
    with gzip.GzipFile(out / "events.pb.gz", "wb", mtime=0) as f:
        for p in payloads:
            f.write(_varint(len(p)) + p)
    pq.write_table(pa.table(cols), out / "events.parquet", compression="zstd")
    schema = load_schema()
    n_prof = 0
    with gzip.GzipFile(out / "profiles.bin.gz", "wb", mtime=0) as f:
        for key, blob in profile_blobs(world, schema, PROFILE_UPDATED_US):
            k = key.encode()
            f.write(struct.pack("<H", len(k)) + k + struct.pack("<I", len(blob)) + blob)
            n_prof += 1
    sc = scenario()
    (out / "scoring_config.json").write_text(json.dumps(sc.scoring_config(), indent=1, sort_keys=True) + "\n")
    fraud = cols.get("is_fraud")
    manifest = {
        "world": SPEC.model_dump(mode="json"), "seed": SEED, "events": len(payloads), "profiles": n_prof,
        "fraud_events": int(np.asarray(fraud).sum()) if fraud is not None else None,
        "fraud_types": sorted({int(x) for x in np.asarray(cols.get("fraud_type", []))} - {0}),
        "scenario_fingerprint": sc.fingerprint(), "world_hash": world.content_hash,
        "schema_version": schema.version, "state_layout": schema.state_layout(),
        "events_sha256": hashlib.sha256(b"".join(payloads)).hexdigest(),
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n")
    return manifest


FLOWS = REPO_ROOT / "contracts" / "testdata" / "golden" / "flows"


FLOW_CONTRACTS = ["a2a_transfer", "card_payment"]


def generate_flows(n_batches: int = 30):
    """A stream of payments sent as their messages: transfers (a2a_transfer) and card purchases
    (card_payment: 3DS, authorisation, capture, reversal, refund, chargeback), with APP scams, account
    takeover and card testing in it, the rest one-message payments: every record in the order the pipeline
    receives it."""
    from .gen.generator import Generator, sim_start_us
    from .world import build_world
    sc = Scenario(name="golden-flows", seed=SEED, world=SPEC, traffic={"flows": FLOW_CONTRACTS},
                  fraud={"background_per_1k": 8.0, "friendly_fraud_per_1k": 5.0,
                         "mix": {"app_scam_mule": 1.0, "account_takeover": 1.0, "investment_scam": 0.5,
                                 "card_testing": 1.0}})
    g = Generator(build_world(SPEC, SEED, None), sc, 0, 1)
    t = sim_start_us(sc)
    dt = int(200 / g.natural_rate_per_sim_s() * 1e6)
    records = []
    for _ in range(n_batches):
        records += g.generate(200, t, t + dt).records
        t += dt
    return records + g.flows.drain()


def write_flows(out: Path = FLOWS) -> dict[str, Any]:
    """records.bin.gz: (u16 type length, type, u32 payload length, payload)* in arrival order ("" type =
    the unified payment event); expected.jsonl.gz: what the reference assembler emits, in order."""
    from .bindings import event_type
    from .bindings.codec import message_class
    from .bindings.transactions import Assembler, transaction
    out.mkdir(parents=True, exist_ok=True)
    import cel

    from .features import load_schema
    from .features.derived import event_view
    from .features.engine import ReferenceEngine
    records = generate_flows()
    a = Assembler([transaction(n) for n in FLOW_CONTRACTS])
    pay = event_type("payment").codec
    pay_cls = message_class(pay.full_name)
    expected = []
    # customer and card features in the twin's order: assembled and passed-through events are folded into
    # state, re-scored stages only read it (the Flink job must give exactly these)
    schema = load_schema()
    engine = ReferenceEngine(schema, ["customer", "card"])
    filters = [cel.compile(x) for x in schema.filters.values()]
    watched = [f for f in schema.features if f.source == "stream" and f.entity in ("customer", "card")]
    buf = [0.0] * len(schema.features)

    def emit(outs):
        for o in outs:
            row = {f.name: (o.row[f.name].item() if hasattr(o.row[f.name], "item") else o.row[f.name])
                   for f in pay.fields}
            ctx = cel.Context()
            ctx.add_variable("event", event_view(row, o.context))
            ctx.add_variable("f", {})
            mask = sum(1 << b for b, p in enumerate(filters) if p.execute(ctx))
            (engine.read if o.read_only else engine.process)(row, mask, buf)
            expected.append({"kind": o.kind, "event_id": int(row["event_id"]), "transaction": o.transaction,
                             "stage": o.stage, "parts_present": o.parts_present, "partial": o.partial,
                             "read_only": o.read_only, "event": pay_cls(**row).SerializeToString().hex(),
                             "context": o.context,
                             "features": {f.name: float(np.float32(buf[f.index])) for f in watched}})
    with gzip.GzipFile(out / "records.bin.gz", "wb", mtime=0) as f:
        for r in records:
            t = r.type.encode()
            f.write(struct.pack("<H", len(t)) + t + struct.pack("<I", len(r.payload)) + r.payload)
            c = event_type(r.type or "payment").codec
            emit(a.process(r.type or "payment", c.row(message_class(c.full_name).FromString(r.payload))))
    n_before_close = len(expected)
    emit(a.close())
    with gzip.GzipFile(out / "expected.jsonl.gz", "wb", mtime=0) as f:
        for e in expected:
            f.write((json.dumps(e, sort_keys=True) + "\n").encode())
    kinds = {}
    for e in expected:
        kinds[e["kind"]] = kinds.get(e["kind"], 0) + 1
    by_txn: dict[str, int] = {}
    for e in expected:
        if e["transaction"]:
            by_txn[e["transaction"]] = by_txn.get(e["transaction"], 0) + 1
    man = {"records": len(records), "outputs": len(expected), "kinds": kinds, "transactions": FLOW_CONTRACTS,
           "outputs_by_transaction": by_txn,
           "closed_at_end": len(expected) - n_before_close,   # still waiting when the stream ended
           "partial": sum(1 for e in expected if e["partial"]),
           "records_sha256": hashlib.sha256(b"".join(r.payload for r in records)).hexdigest()}
    (out / "manifest.json").write_text(json.dumps(man, indent=1, sort_keys=True) + "\n")
    return man


SCENARIOS = REPO_ROOT / "contracts" / "testdata" / "golden" / "scenarios.json"


def scenario_paths(d: dict[str, Any], prefix: str = "") -> list[str]:
    """Every setting of a scenario as a dotted path (free-form dict settings stop at their own name)."""
    out = []
    for k, v in d.items():
        p = f"{prefix}{k}"
        if isinstance(v, dict) and v and k not in ("scoring", "model_server", "expect", "mix", "params"):
            out += scenario_paths(v, p + ".")
        else:
            out.append(p)
    return sorted(out)


def scenario_pins() -> dict[str, Any]:
    """The fingerprint of every bundled scenario, with the hash of its file (a deliberate edit of the file
    is allowed to change it), and every setting the Scenario model had when they were pinned."""
    from .scenario import SCENARIO_DIR, Scenario, load_scenario
    pins = {}
    for f in sorted(SCENARIO_DIR.glob("*.yaml")):
        pins[f.stem] = {"file_sha256": hashlib.sha256(f.read_bytes()).hexdigest(),
                        "fingerprint": load_scenario(f).fingerprint()}
    return {"settings": scenario_paths(Scenario(name="x").model_dump(mode="json")), "scenarios": pins}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--events", type=int, default=N_EVENTS)
    ap.add_argument("--scenarios", action="store_true", help="re-pin the bundled scenarios' fingerprints")
    ap.add_argument("--flows", action="store_true", help="rebuild the golden multi-message transaction stream")
    a = ap.parse_args(argv)
    if a.flows:
        print(json.dumps(write_flows(), indent=1))
        return
    if a.scenarios:
        SCENARIOS.write_text(json.dumps(scenario_pins(), indent=1, sort_keys=True) + "\n")
        print(f"pinned {SCENARIOS}")
        return
    print(json.dumps(write(a.out, a.events), indent=1))


if __name__ == "__main__":
    main()
