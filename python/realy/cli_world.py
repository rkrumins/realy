"""`realy world ...` commands: build / profile / compile.

Compiled dataset format (consumed by the Java gRPC load emitter, java/load-emitter):

    <out>                 binary, little-endian
        header  8 bytes   magic  b"RLYDS\\x00\\x01\\x00"   (format version 1)
                8 bytes   u64    number of records
        records           u32 length, then `length` bytes of a serialized
                          realy.payments.v1.TransactionEvent; records are in emission order
                          (event_id = (gen_id << 48) | seq ascending, event_time ascending)
    <out>.parquet         one row per record, same order: every TransactionEvent field (proto field
                          names) + ground truth is_fraud / fraud_type / campaign_id
    <out>.labels.parquet  label_time_us, event_id, label (serialized realy.payments.v1.FraudLabel)
    <out>.json            manifest: scenario, fingerprint, world hash, time_scale, sim_start_us,
                          target_tps, batch_wall_ms, sha256 of all payload bytes

The emitter replays records at the scenario rate; the n-th record's intended send time is
derived from the rate profile, not stored in the file.
"""
from __future__ import annotations

import hashlib
import json
import struct
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import typer

world_app = typer.Typer(help="Synthetic world: build, profile, compile datasets.", no_args_is_help=True)
DATASET_MAGIC = b"RLYDS\x00\x01\x00"


def _scenario(name: str | None, customers: int | None, seed: int | None, merchants: int | None):
    from .scenario import Scenario, WorldSpec, load_scenario
    sc = load_scenario(name) if name else Scenario(name="adhoc")
    if customers is not None or merchants is not None:
        ws = sc.world.model_copy(update={k: v for k, v in (("customers", customers), ("merchants", merchants)) if v is not None})
        sc = sc.model_copy(update={"world": WorldSpec.model_validate(ws.model_dump())})
    if seed is not None:
        sc = sc.model_copy(update={"seed": seed})
    return sc


@world_app.command("build")
def build(scenario: str = typer.Option(None, help="scenario name or path (uses its world spec + seed)"),
          customers: int = typer.Option(None), merchants: int = typer.Option(None), seed: int = typer.Option(None),
          out: Path = typer.Option(None, help="output directory (default: scenario world_dir)")) -> None:
    """Build (or rebuild) a deterministic world and print its content hash."""
    from .world import build_world
    sc = _scenario(scenario, customers, seed, merchants)
    out = out or sc.world_dir
    t = time.perf_counter()
    w = build_world(sc.world, sc.seed, out)
    typer.echo(json.dumps({"dir": str(out), "customers": w.n_customers, "merchants": w.n_merchants,
                           "cards": int(w.cards["card_id"].shape[0]), "content_hash": w.content_hash,
                           "seconds": round(time.perf_counter() - t, 2)}, indent=2))


@world_app.command("profile")
def profile_cmd(world: Path = typer.Option(None, help="world directory"), scenario: str = typer.Option(None),
                events: int = typer.Option(200_000), json_out: Path = typer.Option(None, "--json")) -> None:
    """Generate a sample stream at the natural rate and print realism statistics."""
    from .world import World, load_or_build
    from .world.profile import print_profile, profile
    sc = _scenario(scenario, None, None, None)
    w = World.load(world) if world else load_or_build(sc.world, sc.seed, sc.world_dir)
    rep = profile(w, sc, events)
    print_profile(rep)
    if json_out:
        json_out.write_text(json.dumps(rep, indent=2))


@world_app.command("compile")
def compile_cmd(scenario: str = typer.Option(..., help="scenario name or path"),
                out: Path = typer.Option(..., help="dataset file"), events: int = typer.Option(100_000),
                batch_wall_ms: float = typer.Option(100.0)) -> None:
    """Pre-generate a dataset (length-delimited TransactionEvents + parquet sidecars)."""
    from .gen import Generator, auto_time_scale, sim_start_us
    from .world import load_or_build
    sc = _scenario(scenario, None, None, None)
    w = load_or_build(sc.world, sc.seed, sc.world_dir)
    info = compile_dataset(w, sc, out, events, batch_wall_ms, Generator, auto_time_scale, sim_start_us)
    typer.echo(json.dumps(info, indent=2))


def compile_dataset(w, sc, out: Path, events: int, batch_wall_ms: float, Generator, auto_time_scale, sim_start_us) -> dict:
    tps = sc.traffic.profile.tps
    ts = auto_time_scale(w, tps) if sc.traffic.time_scale == "auto" else float(sc.traffic.time_scale)
    gen = Generator(w, sc, 0, 1)
    t0 = sim_start_us(sc)
    gen.schedule_timeline(t0, ts)
    per = max(1, int(round(tps * batch_wall_ms / 1000.0)))
    dt = int(ts * batch_wall_ms * 1000)
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha256()
    cols: list[dict] = []
    labels: list[tuple[int, int, bytes]] = []
    n = 0
    t = t0
    with out.open("wb") as f:
        f.write(DATASET_MAGIC + struct.pack("<Q", 0))
        while n < events:
            b = gen.generate(min(per, events - n), t, t + dt)
            t += dt
            for p in b.payloads:
                f.write(struct.pack("<I", len(p)))
                f.write(p)
                h.update(p)
            n += len(b)
            cols.append(b.columns)
            for (lt, lb), eid in zip(b.labels, _label_event_ids(b)):
                labels.append((lt, eid, lb))
        f.seek(len(DATASET_MAGIC))
        f.write(struct.pack("<Q", n))
    merged = {k: np.concatenate([c[k] for c in cols]) for k in cols[0]}
    pq.write_table(pa.table({k: pa.array(v) for k, v in merged.items()}), str(out) + ".parquet", compression="zstd")
    pq.write_table(pa.table({"label_time_us": pa.array([x[0] for x in labels], pa.int64()),
                             "event_id": pa.array([x[1] for x in labels], pa.uint64()),
                             "label": pa.array([x[2] for x in labels], pa.binary())}), str(out) + ".labels.parquet")
    info = {"scenario": sc.name, "fingerprint": sc.fingerprint(), "world_hash": w.content_hash, "records": n,
            "time_scale": ts, "sim_start_us": t0, "target_tps": tps, "batch_wall_ms": batch_wall_ms,
            "fraud_events": int(merged["is_fraud"].sum()), "labels": len(labels), "payload_sha256": h.hexdigest()}
    Path(str(out) + ".json").write_text(json.dumps(info, indent=2))
    return info


def _label_event_ids(batch) -> list[int]:
    from .payments.v1 import label_pb2
    out = []
    for _, lb in batch.labels:
        m = label_pb2.FraudLabel()
        m.ParseFromString(lb)
        out.append(m.event_id)
    return out


def read_dataset(path: Path):
    """Iterate payloads of a compiled dataset (used by tests and tools)."""
    with Path(path).open("rb") as f:
        head = f.read(16)
        if head[:8] != DATASET_MAGIC:
            raise ValueError("not a realy dataset")
        (n,) = struct.unpack("<Q", head[8:])
        for _ in range(n):
            (ln,) = struct.unpack("<I", f.read(4))
            yield f.read(ln)
