"""Open-loop paced Kafka emission (one generator process + one emitter process per shard).

Generator process: produces events block by block (20 ms of wall time each) *ahead of* the
schedule (bounded lookahead), writes the ground-truth event log to Parquet and hands serialized
batches to the emitter through a queue. Generation never stalls the pacing loop.

Emitter process: sends every event at its *intended* wall time (derived from its simulation event
time: intended = (event_time - sim_start) / time_scale). The intended time is stamped into the
`x-bench-trace` header, so if the emitter (or Kafka) falls behind, the delay is charged to the
measured latency instead of silently stretching the schedule (coordinated-omission safe). The
emitter records, per event, the schedule lag (t_sent - t_intended) and the broker ack latency.
"""
from __future__ import annotations

import heapq
import json
import queue as queue_mod
import time
import traceback
import zlib
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any

import numpy as np

from ..common.clock import DRIVER_DOMAIN, now_us

LOOKAHEAD_S = 1.0
BLOCK_WALL_S = 0.02


@dataclass
class ShardPlan:
    shard: int
    n_shards: int
    run_id: str
    run_dir: str
    scenario: dict[str, Any]
    world_dir: str
    t_start_us: int          # wall time of schedule origin
    time_scale: float
    sim_start_us: int
    duration_s: float
    events_topic: str = "payments.events.raw"
    write_events: bool = True
    # Legacy: True = the whole history is one prime segment. Superseded by extra["history"]: ordered
    # segments {"mode": "bulk"|"prime", "days"} (runner.plan_history). Prime events go out WITHOUT
    # `x-backfill`, so the gateway / Flink-inline score them (JIT, connections, caches warm up); they
    # stay untraced, so the collector ignores their decisions and the report excludes them.
    prime: bool = False
    extra: dict[str, Any] = field(default_factory=dict)


def history_segments(plan: "ShardPlan", backfill_days: float | None) -> list[dict[str, Any]]:
    segs = plan.extra.get("history")
    if segs is not None:
        return [s for s in segs if float(s.get("days") or 0) > 0]
    days = float(backfill_days or 0)
    return [{"mode": "prime" if plan.prime else "bulk", "days": days}] if days > 0 else []


# --------------------------------------------------------------------------------------- generator
def _wait_go(go, stop) -> None:
    """Wait for the measured phase to start; return early when the run is stopped meanwhile."""
    if go is None:
        return
    while not go.wait(0.2):
        if stop.is_set():
            return


def generator_main(plan: ShardPlan, q, rate_mult, stop, start=None, go=None) -> None:
    from ..runner.lifecycle import watch_parent
    watch_parent()
    try:
        _generator(plan, q, rate_mult, stop, start, go)
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        q.put(("error", traceback.format_exc()))


def _generator(plan: ShardPlan, q, rate_mult, stop, start=None, go=None) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from ..gen import Generator
    from ..scenario import Scenario
    from ..world import World

    sc = Scenario.model_validate(plan.scenario)
    world = World.load(plan.world_dir)
    gen = Generator(world, sc, plan.shard, plan.n_shards)
    gen.schedule_timeline(plan.sim_start_us, plan.time_scale)
    profile = sc.traffic.profile
    rng = np.random.default_rng([sc.seed, plan.shard, 7])
    ts = plan.time_scale
    writer = None
    buf: list[dict[str, np.ndarray]] = []
    buf_rows = 0
    wall_t = 0.0
    carry = 0.0

    def flush() -> None:
        nonlocal writer, buf, buf_rows
        if not buf:
            return
        cols = {k: np.concatenate([b[k] for b in buf]) for k in buf[0]}
        table = pa.table(cols)
        if writer is None:
            writer = pq.ParquetWriter(Path(plan.run_dir) / f"events-{plan.shard}.parquet", table.schema, compression="zstd")
        writer.write_table(table)
        buf, buf_rows = [], 0

    # ------------------------------------------------------------ history segments (unmeasured)
    backfilled = 0
    segs = history_segments(plan, sc.traffic.backfill_days)
    if segs:
        block = 3_600_000_000  # one simulated hour per batch
        rate = gen.natural_rate_per_sim_s()
        bcarry = 0.0
        # segment boundaries in sim time, oldest first, ending at the measured window's start
        total_us = int(sum(float(x["days"]) for x in segs) * 86_400_000_000)
        bounds, t_b = [], plan.sim_start_us - total_us
        for x in segs:
            t_e = t_b + int(float(x["days"]) * 86_400_000_000)
            bounds.append((t_b, t_e))
            t_b = t_e
        bounds[-1] = (bounds[-1][0], plan.sim_start_us)
        for si, (t, t_end) in enumerate(bounds):
            while t < t_end and not stop.is_set():
                t1 = min(t_end, t + block)
                exp = rate * (t1 - t) / 1e6 + bcarry
                k = int(exp)
                bcarry = exp - k
                b = gen.generate(k, t, t1)
                if len(b):
                    q.put(("backfill", si, b.payloads, b.keys, b.event_ids, b.labels))
                    backfilled += len(b)
                    if plan.write_events:
                        cols = dict(b.columns)
                        cols["t_intended_us"] = np.full(len(b), -1, np.int64)
                        cols["backfill"] = np.ones(len(b), np.bool_)
                        buf.append(cols)
                        buf_rows += len(b)
                        if buf_rows >= 100_000:
                            flush()
                t = t1
    q.put(("backfill_done", backfilled))
    _wait_go(go, stop)
    t_start = int(start.value) if start is not None and start.value > 0 else plan.t_start_us

    while wall_t < plan.duration_s and not stop.is_set():
        ahead = wall_t - (now_us() - t_start) / 1e6
        if ahead > LOOKAHEAD_S:
            time.sleep(min(0.05, ahead - LOOKAHEAD_S))
            continue
        rate = profile.rate_at(wall_t) * rate_mult.value / plan.n_shards
        if sc.traffic.arrivals == "poisson":
            n = int(rng.poisson(rate * BLOCK_WALL_S))
        else:
            expected = rate * BLOCK_WALL_S + carry
            n = int(expected)
            carry = expected - n
        sim0 = plan.sim_start_us + int(wall_t * ts * 1e6)
        sim1 = plan.sim_start_us + int((wall_t + BLOCK_WALL_S) * ts * 1e6)
        b = gen.generate(n, sim0, sim1)
        if len(b):
            offs = ((b.event_time_us - plan.sim_start_us) / ts).astype(np.int64)
            # cumulative CPU of this generator process rides along, so the emitter can report the
            # driver's whole CPU cost per event (sizing the driver pool)
            q.put(("batch", offs, b.payloads, b.keys, b.event_ids, b.labels, time.process_time() * 1000.0))
            if plan.write_events:
                cols = dict(b.columns)
                cols["t_intended_us"] = offs + t_start
                cols["backfill"] = np.zeros(len(b), np.bool_)
                buf.append(cols)
                buf_rows += len(b)
                if buf_rows >= 100_000:
                    flush()
        wall_t += BLOCK_WALL_S
    flush()
    if writer is not None:
        writer.close()
    q.put(("done", gen.seq))


# ----------------------------------------------------------------------------------------- emitter
class _Timing:
    """Growable per-event timing columns."""

    def __init__(self, cap: int) -> None:
        self.n = 0
        self.event_id = np.zeros(cap, np.uint64)
        self.intended = np.zeros(cap, np.int64)
        self.sent = np.zeros(cap, np.int64)
        self.ack = np.full(cap, -1, np.int64)

    def grow(self) -> None:
        cap = self.event_id.shape[0] * 2
        for name in ("event_id", "intended", "sent"):
            a = getattr(self, name)
            b = np.zeros(cap, a.dtype)
            b[: a.shape[0]] = a
            setattr(self, name, b)
        a = self.ack
        b = np.full(cap, -1, np.int64)
        b[: a.shape[0]] = a
        self.ack = b


def emitter_main(plan: ShardPlan, q, stop, results, metrics_topic: str = "bench.metrics", start=None, go=None) -> None:
    from ..runner.lifecycle import watch_parent
    watch_parent()
    try:
        results.put(("emitter", plan.shard, _emitter(plan, q, stop, metrics_topic, results, start, go)))
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        results.put(("emitter", plan.shard, {"error": traceback.format_exc()}))


def _emitter(plan: ShardPlan, q, stop, metrics_topic: str, results=None, start=None, go=None) -> dict[str, Any]:
    from ..bench.v1 import trace_pb2
    from ..common.kafka import LABELS, make_producer
    from ..common.metrics import snapshot_json

    producer = make_producer()
    est = int(max(1000, plan.scenario.get("traffic", {}).get("profile", {}).get("tps", 1000)) * plan.duration_s
              / plan.n_shards * 1.5) + 10_000
    tm = _Timing(min(est, 50_000_000))
    errors = [0]
    acks_this_sec: list[int] = []

    def on_delivery(pos: int, err, msg) -> None:
        if err is not None:
            errors[0] += 1
            return
        lat = msg.latency()
        us = int(lat * 1e6) if lat is not None else now_us() - int(tm.sent[pos])
        tm.ack[pos] = us
        acks_this_sec.append(us)

    BT = trace_pb2.BenchTrace
    run_id = plan.run_id
    run_b = run_id.encode()
    # ------------------------------------------------------------ history segments: untraced, never measured
    # bulk: unpaced, `x-backfill` (Flink builds state, the gateway skips them); prime: paced at the
    # scenario's starting rate and scored, so the pipeline reaches the steady state it is measured in
    # (JIT tiers, broker / page cache, RocksDB compaction, ClickHouse ingest)
    segs = history_segments(plan, (plan.scenario.get("traffic") or {}).get("backfill_days"))
    hdr = {"bulk": [("x-backfill", b"1"), ("x-run-id", run_b + b":backfill")], "prime": [("x-run-id", run_b + b":prime")]}
    prime_rate = float(plan.extra.get("prime_tps") or 0.0)
    backfilled = primed = 0
    seg_i, seg_t0, seg_n = -1, 0.0, 0
    while not stop.is_set():
        try:
            item = q.get(timeout=0.5)
        except queue_mod.Empty:
            continue
        if item[0] == "backfill":
            si = item[1] if isinstance(item[1], int) else 0
            mode = segs[si]["mode"] if si < len(segs) else "bulk"
            if si != seg_i:                      # a new segment: pacing restarts from its first event
                seg_i, seg_t0, seg_n = si, time.monotonic(), 0
            bf_hdr = hdr.get(mode, hdr["bulk"])
            paced = mode == "prime" and prime_rate > 0
            for p_, k_ in zip(item[2], item[3]):
                if paced and seg_n % 50 == 0:
                    ahead = seg_n / prime_rate - (time.monotonic() - seg_t0)
                    if ahead > 0.002:
                        producer.poll(0)
                        time.sleep(ahead)
                while True:
                    try:
                        producer.produce(plan.events_topic, p_, k_, headers=bf_hdr)
                        break
                    except BufferError:
                        producer.poll(0.01)
                backfilled += 1
                seg_n += 1
                if mode == "prime":
                    primed += 1
            for _, lb in item[5]:
                producer.produce(LABELS, lb, headers=[h for h in bf_hdr if h[0] == "x-run-id"])
            producer.poll(0)
        elif item[0] == "backfill_done":
            break
        elif item[0] == "error":
            raise RuntimeError("generator failed:\n" + item[1])
    producer.flush(120)
    if results is not None:
        results.put(("backfill", plan.shard, {"events": backfilled, "primed": primed}))
    _wait_go(go, stop)
    t0 = int(start.value) if start is not None and start.value > 0 else plan.t_start_us
    ts = plan.time_scale
    sim0 = plan.sim_start_us
    labels_heap: list[tuple[int, bytes]] = []
    labels_sent = 0
    cur = None
    i = 0
    done = False
    total_gen = None
    next_metrics = (now_us() // 1_000_000 + 1) * 1_000_000
    sec_lags: list[int] = []
    sec_sent = 0
    sec_bytes = 0
    gen_cpu_ms = 0.0          # generator process, cumulative (reported with each batch)
    gen_cpu_prev = 0.0
    own_cpu_prev = time.process_time() * 1000.0
    topic = plan.events_topic

    while not stop.is_set():
        if cur is None or i >= len(cur[1]):
            try:
                item = q.get(timeout=0.002) if not done else None
            except queue_mod.Empty:
                item = None
            if item is not None:
                kind = item[0]
                if kind == "batch":
                    cur = item
                    i = 0
                    if len(item) > 6:
                        gen_cpu_ms = item[6]
                    for lt in item[5]:
                        heapq.heappush(labels_heap, lt)
                elif kind == "done":
                    done = True
                    total_gen = item[1]
                    cur = None
                elif kind == "error":
                    raise RuntimeError("generator failed:\n" + item[1])
            elif done:
                break
        now = now_us()
        if cur is not None:
            offs, payloads, keys, ids = cur[1], cur[2], cur[3], cur[4]
            off_now = now - t0
            while i < len(payloads) and offs[i] <= off_now:
                pos = tm.n
                if pos >= tm.event_id.shape[0]:
                    tm.grow()
                eid = int(ids[i])
                intended = t0 + int(offs[i])
                sent = now_us()
                payload = payloads[i]
                hdr = BT(run_id=run_id, gen_id=eid >> 48, seq=eid & 0xFFFFFFFFFFFF, t_intended_us=intended,
                         t_sent_us=sent, driver_domain=DRIVER_DOMAIN,
                         payload_crc32=zlib.crc32(payload)).SerializeToString()
                while True:
                    try:
                        producer.produce(topic, payload, keys[i], headers=[("x-bench-trace", hdr), ("x-run-id", run_b)],
                                         timestamp=intended // 1000, on_delivery=partial(on_delivery, pos))
                        break
                    except BufferError:
                        producer.poll(0.001)
                tm.event_id[pos] = eid
                tm.intended[pos] = intended
                tm.sent[pos] = sent
                tm.n += 1
                sec_lags.append(sent - intended)
                sec_sent += 1
                sec_bytes += len(payload)
                i += 1
            sim_now = sim0 + int((now - t0) * ts)
            while labels_heap and labels_heap[0][0] <= sim_now:
                _, lb = heapq.heappop(labels_heap)
                producer.produce(LABELS, lb, headers=[("x-run-id", run_b)])
                labels_sent += 1
        producer.poll(0)
        if now >= next_metrics:
            own_cpu = time.process_time() * 1000.0
            cpu_ms = (own_cpu - own_cpu_prev) + max(0.0, gen_cpu_ms - gen_cpu_prev)
            own_cpu_prev, gen_cpu_prev = own_cpu, gen_cpu_ms
            producer.produce(metrics_topic, snapshot_json(
                run_id, "emitter", f"shard-{plan.shard}", now, 1000,
                counters={"emitted": sec_sent, "emitted_total": tm.n, "acked": len(acks_this_sec),
                          "errors_total": errors[0], "labels_total": labels_sent,
                          "bytes": sec_bytes, "cpu_ms": cpu_ms, "cpus": 2},   # emitter + generator process
                stages={"schedule_lag": sec_lags, "produce_ack": acks_this_sec}))
            sec_lags, sec_sent, sec_bytes = [], 0, 0
            acks_this_sec = []
            next_metrics += 1_000_000
        # sleep until the next event is due (sub-ms granularity), never past it
        if cur is not None and i < len(cur[1]):
            wait_us = (t0 + int(cur[1][i])) - now_us()
            if wait_us > 300:
                time.sleep(min(wait_us - 200, 1000) / 1e6)
        else:
            time.sleep(0.0002)

    # flush remaining labels (so the training view has ground truth) and all deliveries
    while labels_heap:
        producer.produce(LABELS, heapq.heappop(labels_heap)[1], headers=[("x-run-id", run_b)])
        labels_sent += 1
        if labels_sent % 10000 == 0:
            producer.poll(0)
    remaining = producer.flush(60)
    n = tm.n
    import pyarrow as pa
    import pyarrow.parquet as pq
    ev = tm.event_id[:n]
    pq.write_table(pa.table({
        "event_id": ev,
        "gen_id": (ev >> np.uint64(48)).astype(np.uint16),
        "seq": (ev & np.uint64(0xFFFFFFFFFFFF)).astype(np.uint64),
        "t_intended_us": tm.intended[:n],
        "t_sent_us": tm.sent[:n],
        "ack_us": tm.ack[:n],
    }), Path(plan.run_dir) / f"emit-{plan.shard}.parquet", compression="zstd")
    lag = tm.sent[:n] - tm.intended[:n]
    summary = {
        "shard": plan.shard, "gen_id": plan.shard, "emitted": int(n), "generated": total_gen, "backfilled": backfilled,
        "errors": errors[0], "undelivered": int(remaining), "labels": labels_sent,
        "first_seq": int(ev[0] & np.uint64(0xFFFFFFFFFFFF)) if n else None,
        "last_seq": int(ev[-1] & np.uint64(0xFFFFFFFFFFFF)) if n else None,
        "schedule_lag_p99_ms": float(np.percentile(lag, 99) / 1000) if n else None,
        "schedule_lag_max_ms": float(lag.max() / 1000) if n else None,
    }
    (Path(plan.run_dir) / f"emit-{plan.shard}.json").write_text(json.dumps(summary, indent=1))
    return summary
