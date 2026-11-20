"""Synchronous (gRPC) rail: compile the run's events into a dataset (same generator, same seed, so
the bytes are identical to what the async emitter would send), replay them open-loop with the Java
load emitter against the gateway, and convert its per-event results for the report.

The Kafka collector still runs: every sync decision must also land in the audit log
(payments.decisions) before the gateway answers, so audit completeness is checked independently.
"""
from __future__ import annotations

import hashlib
import json
import os
import struct
import subprocess
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ..emit.driver import BLOCK_WALL_S
from ..scenario import REPO_ROOT, Scenario

DATASET_MAGIC = b"RLYDS\x00\x01\x00"
EMITTER = Path(os.environ.get("REALY_LOAD_EMITTER",
                              REPO_ROOT / "java" / "load-emitter" / "build" / "install" / "load-emitter" / "bin" / "load-emitter"))
STAGES = ["queue_wait", "deserialize", "shuffle", "state_features", "feature_store_write", "feature_lookup",
          "feature_assemble", "model_call", "model_batch_wait", "model_server_queue", "model_server_compute", "rules",
          "policy", "audit_ack", "sink", "async_wait"]
GR_DTYPE = np.dtype([("event_id", "<u8"), ("t_intended_us", "<i8"), ("t_sent_us", "<i8"), ("t_response_us", "<i8"),
                     ("t_ingest_us", "<i8"), ("t_decision_us", "<i8"), ("outcome", "i1"), ("path", "i1"),
                     ("degraded", "u1"), ("status", "u1")] + [(f"rs_{s}", "<i4") for s in STAGES])


def compile_run_dataset(world, sc: Scenario, run_dir: Path, time_scale: float, sim0: int) -> dict:
    """Generate the whole run (following the rate profile) into dataset.bin + events-0.parquet."""
    from ..gen import Generator
    gen = Generator(world, sc, 0, 1)
    gen.schedule_timeline(sim0, time_scale)
    prof = sc.traffic.profile
    rng = np.random.default_rng([sc.seed, 0, 7])
    out = run_dir / "dataset.bin"
    h = hashlib.sha256()
    writer = None
    n = 0
    carry = 0.0
    wall_t = 0.0
    with out.open("wb") as f:
        f.write(DATASET_MAGIC + struct.pack("<Q", 0))
        while wall_t < sc.traffic.duration_s:
            rate = prof.rate_at(wall_t)
            if sc.traffic.arrivals == "poisson":
                k = int(rng.poisson(rate * BLOCK_WALL_S))
            else:
                exp = rate * BLOCK_WALL_S + carry
                k = int(exp)
                carry = exp - k
            s0 = sim0 + int(wall_t * time_scale * 1e6)
            s1 = sim0 + int((wall_t + BLOCK_WALL_S) * time_scale * 1e6)
            b = gen.generate(k, s0, s1)
            for p in b.payloads:
                f.write(struct.pack("<I", len(p)))
                f.write(p)
                h.update(p)
            if len(b):
                cols = dict(b.columns)
                cols["t_intended_offset_us"] = ((b.event_time_us - sim0) / time_scale).astype(np.int64)
                t = pa.table(cols)
                if writer is None:
                    writer = pq.ParquetWriter(run_dir / "events-0.parquet", t.schema, compression="zstd")
                writer.write_table(t)
            n += len(b)
            wall_t += BLOCK_WALL_S
        f.seek(8)
        f.write(struct.pack("<Q", n))
    if writer is not None:
        writer.close()
    meta = {"events": n, "sha256": h.hexdigest(), "time_scale": time_scale, "sim_start_us": sim0}
    (run_dir / "dataset.json").write_text(json.dumps(meta, indent=1))
    return meta


def run_emitter(run_dir: Path, run_id: str, t_start_us: int, time_scale: float, sim0: int, target: str,
                mode: str = "unary", channels: int = 4, deadline_ms: int = 0, log=print, stop=None) -> dict:
    """Run the Java open-loop gRPC emitter. When `stop` (an Event) is set, the emitter is asked to stop
    through its stop file: it ends emission, drains in-flight calls and writes results for the events
    it sent, so an aborted sync run still gets an accurate partial report."""
    if not EMITTER.exists():
        raise FileNotFoundError(f"{EMITTER} not built (cd java && ./gradlew :load-emitter:installDist)")
    out = run_dir / "grpc-0.bin"
    cmd = [str(EMITTER), "--dataset", str(run_dir / "dataset.bin"), "--target", target, "--run-id", run_id,
           "--start-at-us", str(t_start_us), "--time-scale", repr(time_scale), "--sim-start-us", str(sim0),
           "--channels", str(channels), "--mode", mode, "--deadline-ms", str(deadline_ms), "--out", str(out),
           "--stop-file", str(run_dir / "emitter.stop")]
    env = dict(os.environ, HOSTNAME="0", JAVA_OPTS=os.environ.get("EMITTER_JAVA_OPTS", "-XX:+UseZGC -XX:+ZGenerational -Xmx3g"))
    with (run_dir / "emitter.log").open("w") as lf:
        proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT, env=env)
        stop_sent_at = None
        while True:
            try:
                rc = proc.wait(timeout=0.25)
                break
            except subprocess.TimeoutExpired:
                pass
            if stop is not None and stop.is_set() and stop_sent_at is None:
                (run_dir / "emitter.stop").touch()
                stop_sent_at = time.time()
            if stop_sent_at is not None and time.time() - stop_sent_at > 30:   # drain + write took too long
                proc.terminate()
                try:
                    rc = proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    rc = proc.wait()
                break
    if rc != 0:
        raise RuntimeError(f"load emitter exited {rc}; see {run_dir / 'emitter.log'}")
    return convert_results(run_dir)


def convert_results(run_dir: Path) -> dict:
    raw = (run_dir / "grpc-0.bin").read_bytes()
    if raw[:5] != b"RLYGR":
        raise ValueError("bad emitter output")
    (n,) = struct.unpack("<Q", raw[8:16])
    rec = np.frombuffer(raw, dtype=GR_DTYPE, offset=16, count=n)
    cols = {name: rec[name].copy() for name in GR_DTYPE.names}
    ev = cols["event_id"]
    cols["gen_id"] = (ev >> np.uint64(48)).astype(np.uint16)
    cols["seq"] = (ev & np.uint64(0xFFFFFFFFFFFF)).astype(np.uint64)
    cols["ack_us"] = np.full(n, -1, np.int64)
    pq.write_table(pa.table(cols), run_dir / "emit-0.parquet", compression="zstd")
    summary = json.loads((run_dir / "grpc-0.bin.json").read_text())
    st = cols["status"]
    summary.update({"ok": int((st == 0).sum()), "deadline": int((st == 1).sum()), "error": int((st == 2).sum()),
                    "no_response": int((st == 3).sum()), "gen_id": 0, "shard": 0})
    (run_dir / "emit-0.json").write_text(json.dumps(summary, indent=1))
    return summary
