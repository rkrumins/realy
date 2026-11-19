"""Decision collector: consumes payments.decisions, correlates every decision with its emitted event
via (gen_id, seq) and timestamps it on arrival in the driver clock domain. Writes columnar Parquet
for the report, detects duplicates/unknowns on the fly and publishes live 1 s stats.

Latency decomposition per event (all µs):
  e2e        = t_collected - t_intended        (driver clock; coordinated-omission safe)
  ingress    = t_ingest    - t_sent            (driver -> service clock; same host locally)
  hot_path   = t_decision  - t_ingest          (service clock)
  egress     = t_collected - t_decision        (service -> driver clock)
plus every stage span recorded by the services (feature lookup, model, rules, Flink operators ...).
"""
from __future__ import annotations

import json
import time
import traceback
from pathlib import Path
from typing import Any

import numpy as np

from ..common.clock import now_us

# Stage enum values (contracts/proto/realy/bench/v1/trace.proto) -> report column
STAGE_COLS = {
    11: "queue_wait", 12: "deserialize", 20: "shuffle", 21: "state_features", 22: "feature_store_write",
    30: "feature_lookup", 31: "feature_assemble", 40: "model_call", 41: "model_batch_wait",
    42: "model_server_queue", 43: "model_server_compute", 50: "rules", 51: "policy", 60: "audit_ack",
    61: "sink", 81: "async_wait",
}
INGEST, DECISION, FALLBACK = 10, 70, 80
SUMMED = {20, 21, 81}  # may appear once per keyed stage / async operator


class _Cols:
    FIELDS = {
        "event_id": np.uint64, "gen_id": np.uint16, "seq": np.uint64, "revision": np.uint8, "outcome": np.int8,
        "score": np.float32, "path": np.int8, "degraded": np.bool_, "ingress_mode": np.int8,
        "decided_at_us": np.int64, "t_intended_us": np.int64, "t_sent_us": np.int64, "t_ingest_us": np.int64,
        "t_decision_us": np.int64, "t_collected_us": np.int64, "broker_ts_ms": np.int64, "n_rule_hits": np.uint8,
        "fallback": np.int8, "model_batch": np.int32, "feature_staleness_ms": np.float32, "payload_crc_ok": np.int8,
        "ingest_domain": np.uint32, "decision_domain": np.uint32,
        **{f"st_{v}": np.int32 for v in STAGE_COLS.values()},
    }

    def __init__(self) -> None:
        self.data: dict[str, list] = {k: [] for k in self.FIELDS}
        self.reasons: list[str] = []
        self.rules: list[str] = []
        self.model_version: list[str] = []
        # full span list per decision (parallel lists) -> exact waterfall in the console Explorer
        self.span_stage: list[list[int]] = []
        self.span_attr: list[list[int]] = []
        self.span_domain: list[list[int]] = []
        self.span_start: list[list[int]] = []
        self.span_dur: list[list[int]] = []
        self.n = 0

    def table(self):
        import pyarrow as pa
        cols = {k: np.asarray(v, dtype=t) for k, v in self.data.items() for t in [self.FIELDS[k]]}
        cols["reason_codes"] = np.asarray(self.reasons, dtype=object)
        cols["rule_hits"] = np.asarray(self.rules, dtype=object)
        cols["model_version"] = np.asarray(self.model_version, dtype=object)
        t = pa.table(cols)
        for name, typ, vals in (("span_stage", pa.int16(), self.span_stage), ("span_attr", pa.int32(), self.span_attr),
                                ("span_domain", pa.uint32(), self.span_domain), ("span_start_us", pa.int64(), self.span_start),
                                ("span_dur_us", pa.int32(), self.span_dur)):
            t = t.append_column(name, pa.array(vals, type=pa.list_(typ)))
        return t


def collector_main(run_id: str, run_dir: str, index: int, ready, drain, expected, results,
                   group: str | None = None, idle_timeout_s: float = 10.0, drain_timeout_s: float = 60.0,
                   read_committed: bool = False) -> None:
    try:
        results.put(("collector", index, _collect(run_id, run_dir, index, ready, drain, expected,
                                                  group or f"collector-{run_id}", idle_timeout_s, drain_timeout_s,
                                                  read_committed)))
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        results.put(("collector", index, {"error": traceback.format_exc()}))


def _collect(run_id, run_dir, index, ready, drain, expected, group, idle_timeout_s, drain_timeout_s,
             read_committed=False) -> dict[str, Any]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from ..common.kafka import DECISIONS, METRICS, make_consumer, make_producer
    from ..common.metrics import snapshot_json
    from ..scoring.v1 import decision_pb2

    consumer = make_consumer(group, **{"auto.offset.reset": "latest",
                                       "isolation.level": "read_committed" if read_committed else "read_uncommitted"})
    assigned = []

    def on_assign(c, parts):
        assigned.extend(parts)
        ready.set()

    consumer.subscribe([DECISIONS], on_assign=on_assign)
    mprod = make_producer(acks=1, **{"enable.idempotence": False, "linger.ms": 20})
    out_dir = Path(run_dir)
    writer = None
    fwriter = None
    cols = _Cols()
    feats_ids: list[int] = []
    feats_vals: list[list[float]] = []
    feats_missing: list[bytes] = []
    seen: dict[int, np.ndarray] = {}
    dups = 0
    foreign = 0
    received = 0
    unique = 0
    finals = 0
    DR = decision_pb2.DecisionRecord
    sec: dict[str, list[int]] = {k: [] for k in ("e2e", "ingress", "hot_path", "egress")}
    sec_stage: dict[str, list[int]] = {}
    sec_outcomes = {1: 0, 2: 0, 3: 0}
    sec_count = 0
    sec_degraded = 0
    next_metrics = (now_us() // 1_000_000 + 1) * 1_000_000
    last_msg = time.time()
    drain_started = None

    def flush() -> None:
        nonlocal writer, cols, fwriter
        if cols.n:
            t = cols.table()
            if writer is None:
                writer = pq.ParquetWriter(out_dir / f"decisions-{index}.parquet", t.schema, compression="zstd")
            writer.write_table(t)
            cols = _Cols()
        if feats_ids:
            ft = pa.table({"event_id": np.asarray(feats_ids, np.uint64),
                           "values": pa.array(feats_vals, type=pa.list_(pa.float32())),
                           "missing": pa.array(feats_missing, type=pa.binary())})
            if fwriter is None:
                fwriter = pq.ParquetWriter(out_dir / f"features-{index}.parquet", ft.schema, compression="zstd")
            fwriter.write_table(ft)
            feats_ids.clear(); feats_vals.clear(); feats_missing.clear()

    while True:
        # Block only until the FIRST message (poll returns as soon as one is available), then drain
        # whatever else librdkafka has prefetched without waiting. A batch consume(n, timeout) would
        # wait for n messages or the full timeout -> artificial egress latency at low rates; a
        # near-zero timeout loop would burn a core. Neither is acceptable in a measuring instrument.
        first = consumer.poll(0.05)
        msgs = [first] + consumer.consume(num_messages=1000, timeout=0) if first is not None else []
        t_recv = now_us()
        if msgs:
            last_msg = time.time()
        for m in msgs:
            if m.error():
                continue
            d = DR.FromString(m.value())
            tr = d.trace
            if tr.run_id and tr.run_id != run_id:
                foreign += 1
                continue
            if not tr.run_id and not d.HasField("trace"):
                foreign += 1
                continue
            received += 1
            eid = d.event_id
            g = eid >> 48
            s = eid & 0xFFFFFFFFFFFF
            if d.revision > 0:
                finals += 1
            else:
                arr = seen.get(g)
                if arr is None or s >= arr.shape[0]:
                    size = max(1 << 16, (s + 1) * 2)
                    na = np.zeros(size, np.bool_)
                    if arr is not None:
                        na[: arr.shape[0]] = arr
                    seen[g] = arr = na
                if arr[s]:
                    dups += 1
                    continue
                arr[s] = True
                unique += 1
            t_ingest = -1
            t_dec = -1
            ingest_dom = 0
            dec_dom = 0
            fb = 0
            st = {}
            ss, sa, sd, s0, sl = [], [], [], [], []
            for sp in tr.spans:
                stg = sp.stage
                ss.append(stg); sa.append(sp.attr); sd.append(sp.domain); s0.append(sp.start_us); sl.append(sp.dur_us)
                if stg == INGEST:
                    t_ingest, ingest_dom = sp.start_us, sp.domain
                elif stg == DECISION:
                    t_dec, dec_dom = sp.start_us, sp.domain
                elif stg == FALLBACK:
                    fb = sp.attr
                elif stg in STAGE_COLS:
                    if stg in SUMMED:
                        st[stg] = st.get(stg, 0) + sp.dur_us
                    else:
                        st[stg] = sp.dur_us
            c = cols.data
            c["event_id"].append(eid); c["gen_id"].append(g); c["seq"].append(s); c["revision"].append(d.revision)
            c["outcome"].append(d.outcome); c["score"].append(d.score); c["path"].append(d.path)
            c["degraded"].append(d.degraded); c["ingress_mode"].append(d.ingress_mode)
            c["decided_at_us"].append(d.decided_at_us); c["t_intended_us"].append(tr.t_intended_us)
            c["t_sent_us"].append(tr.t_sent_us); c["t_ingest_us"].append(t_ingest); c["t_decision_us"].append(t_dec)
            c["t_collected_us"].append(t_recv); c["broker_ts_ms"].append(m.timestamp()[1])
            c["n_rule_hits"].append(min(255, len(d.rule_hits))); c["fallback"].append(fb)
            c["model_batch"].append(d.model.batch_size); c["ingest_domain"].append(ingest_dom)
            c["decision_domain"].append(dec_dom)
            stale = -1.0
            if d.HasField("features"):
                fv = d.features
                for fr in fv.freshness:
                    if fr.source == 1 and fr.found and fr.updated_at_us > 0:
                        stale = max(stale, (t_dec if t_dec > 0 else d.decided_at_us) / 1000.0 - fr.updated_at_us / 1000.0)
                if d.revision == 0:
                    feats_ids.append(eid); feats_vals.append(list(fv.values)); feats_missing.append(fv.missing_bitmap)
            c["feature_staleness_ms"].append(stale)
            c["payload_crc_ok"].append(-1)
            for stg, colname in STAGE_COLS.items():
                c[f"st_{colname}"].append(st.get(stg, -1))
            cols.reasons.append(",".join(d.reason_codes))
            cols.rules.append(",".join(h.rule_id for h in d.rule_hits))
            cols.model_version.append(d.model.model_version)
            cols.span_stage.append(ss); cols.span_attr.append(sa); cols.span_domain.append(sd)
            cols.span_start.append(s0); cols.span_dur.append(sl)
            cols.n += 1
            if d.revision == 0:
                sec_count += 1
                sec_outcomes[d.outcome] = sec_outcomes.get(d.outcome, 0) + 1
                sec_degraded += d.degraded
                if tr.t_intended_us:
                    sec["e2e"].append(t_recv - tr.t_intended_us)
                if t_ingest > 0 and tr.t_sent_us:
                    sec["ingress"].append(t_ingest - tr.t_sent_us)
                if t_ingest > 0 and t_dec > 0:
                    sec["hot_path"].append(t_dec - t_ingest)
                if t_dec > 0:
                    sec["egress"].append(t_recv - t_dec)
                for stg, v in st.items():
                    sec_stage.setdefault(STAGE_COLS[stg], []).append(v)
        if cols.n >= 50_000:
            flush()
        now = now_us()
        if now >= next_metrics:
            stages = dict(sec)
            stages.update(sec_stage)
            mprod.produce(METRICS, snapshot_json(run_id, "collector", f"c{index}", now, 1000, counters={
                "decided": sec_count, "decided_total": unique, "duplicates_total": dups,
                "approve": sec_outcomes.get(1, 0), "review": sec_outcomes.get(2, 0), "decline": sec_outcomes.get(3, 0),
                "degraded": sec_degraded, "foreign_total": foreign}, stages=stages))
            mprod.poll(0)
            sec = {k: [] for k in sec}
            sec_stage = {}
            sec_outcomes = {1: 0, 2: 0, 3: 0}
            sec_count = sec_degraded = 0
            next_metrics += 1_000_000
        if drain.is_set():
            if drain_started is None:
                drain_started = time.time()
            exp = expected.value
            if exp >= 0 and unique >= exp:
                break
            if time.time() - drain_started > drain_timeout_s or time.time() - last_msg > idle_timeout_s:
                break
    flush()
    if writer is not None:
        writer.close()
    if fwriter is not None:
        fwriter.close()
    consumer.close()
    mprod.flush(5)
    summary = {"index": index, "received": received, "unique": unique, "duplicates": dups, "foreign": foreign,
               "finals": finals, "partitions": len(assigned)}
    (out_dir / f"collect-{index}.json").write_text(json.dumps(summary, indent=1))
    return summary
