"""Capacity plan: cost per event per component x target load / headroom, mapped onto GKE node pools,
with queueing (Erlang C) and the latency budget composed by the Monte Carlo budget checker.

Every cost is tagged `measured` (from a run's report: `realy size --from RUN`) or `assumed` (the
defaults below, from docs/capacity-planning.md). A plan built only on assumptions is a starting point;
re-run step-ramp / baseline-10k on the target machine types and size again with `--from`.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from ..scenario import REPO_ROOT
from .catalog import GKE, POOLS, Machine

# value, unit, provenance - all per event at the stated component
DEFAULTS: dict[str, tuple[float, str, str]] = {
    "gateway_cpu_ms": (0.40, "ms CPU / decision", "capacity-planning.md (0.3-0.5 ms)"),
    "flink_cpu_ms": (1.20, "ms CPU / event", "7 keyed stages + RocksDB + Redis writes (estimate)"),
    "emitter_cpu_ms": (0.06, "ms CPU / event", "driver: 15-25k events/s per emitter shard"),
    "collector_cpu_ms": (0.07, "ms CPU / decision", "driver: ~15k decisions/s per collector"),
    "event_bytes": (250, "B / event", "TransactionEvent protobuf"),
    "decision_bytes": (1500, "B / decision", "DecisionRecord with full inputs"),
    "store_read_bytes": (2900, "B / event", "7 stream blobs (customer + card as state blobs) + 2 profiles"),
    "store_write_bytes": (2600, "B / event", "7 stream blob writes (customer + card as state blobs)"),
    "redis_ops_per_event": (16, "ops / event", "9 GETs (gateway) + 7 SETs (Flink)"),
    "model_rows_per_s_per_gpu": (50_000, "rows/s per L4", "proxy MLP + GBDT with dynamic batching (conservative)"),
    "model_batch_overhead_ms": (1.0, "ms fixed cost per batch", "kernel launches + H2D/D2H copies (estimate)"),
    "model_queue_delay_ms": (1.0, "ms max batching delay", "Triton max_queue_delay_microseconds"),
    "clickhouse_rows_per_core": (25_000, "rows/s per core", "Kafka-engine protobuf parse + MV inserts"),
    "clickhouse_compression": (6.0, "x", "ZSTD / Delta codecs"),
    "kafka_mb_s_per_broker": (100, "MB/s per broker (in + replication + out)", "n2-standard-8 + pd-ssd at headroom"),
    "redis_ops_per_shard": (100_000, "ops/s per primary shard", "pipelined GET/SET, Redis 7+"),
    "redis_mb_s_per_shard": (150, "MB/s per shard", "network per shard"),
    "hot_path_p50_ms": (25.0, "ms", "measured locally at 500 TPS"),
}
# active entities per customer, and typical blob / state sizes (bytes)
ENTITY_RATIO = {"customer": 1.0, "card": 1.5, "device": 0.8, "ip": 1.0, "merchant": 0.05, "terminal": 0.1, "payee": 0.3}
STATE_BYTES = {"customer": 1000, "card": 800, "device": 300, "ip": 300, "merchant": 2500, "terminal": 500, "payee": 500}
VALUES_BLOB_BYTES = 90
PROFILE_BYTES = 150


@dataclass
class Inputs:
    tps: float = 10_000
    peak: float = 2.0
    headroom: float = 0.55
    zones: int = 3
    retention_days: int = 30
    customers: int = 10_000_000
    state_entities: tuple[str, ...] = ("customer", "card")
    run: Path | None = None
    scenario: str = "baseline-10k"
    overrides: dict[str, float] = field(default_factory=dict)


@dataclass
class Cost:
    value: float
    unit: str
    source: str   # measured | assumed | override
    note: str


def _ceil(x: float) -> int:
    return int(math.ceil(x - 1e-9))


def _round_up(n: int, k: int) -> int:
    return _ceil(n / k) * k if k > 1 else n


def erlang_c(c: int, a: float) -> float:
    """P(wait) for M/M/c with offered load a = lambda/mu (stable iff a < c)."""
    if a <= 0:
        return 0.0
    if a >= c:
        return 1.0
    b = 1.0
    for k in range(1, c + 1):   # Erlang B recursion, numerically stable for large c
        b = a * b / (k + a * b)
    return c * b / (c - a * (1 - b))


def gpu_batching(rows_per_s: float, capacity_rows_s: float, overhead_ms: float, delay_ms: float) -> tuple[float, float, float]:
    """Dynamic batching on one GPU. While a batch runs, the next one accumulates, so the batch size
    settles where b = lambda * t(b), with t(b) = overhead + b / capacity:
        b = lambda * overhead / (1 - lambda / capacity)
    Returns (batch rows, batch service ms, p99 queueing ms ~ max batching delay + one batch)."""
    lam, cap = rows_per_s, capacity_rows_s
    if lam >= cap:
        return float("inf"), float("inf"), float("inf")
    b = max(1.0, lam * overhead_ms / 1000 / (1 - lam / cap))
    t = overhead_ms + 1000 * b / cap
    return b, t, delay_ms + t


def wait_p99_ms(c: int, lam: float, service_ms: float) -> float:
    """p99 of the queueing delay in M/M/c: P(W > t) = C * exp(-(c*mu - lambda) t)."""
    if lam <= 0 or service_ms <= 0:
        return 0.0
    mu = 1000.0 / service_ms
    a = lam / mu
    if a >= c:
        return float("inf")
    pw = erlang_c(c, a)
    if pw <= 0.01:
        return 0.0
    return 1000.0 * math.log(pw / 0.01) / (c * mu - lam)


# ------------------------------------------------------------------------------------------ costs
def _measured(run: Path) -> tuple[dict[str, float], dict[str, Any], list[str]]:
    rep = json.loads((run / "report.json").read_text())
    res = rep.get("resources") or {}
    src = res.get("sources") or {}
    inf = res.get("infra") or {}
    m: dict[str, float] = {}
    warn: list[str] = []

    def take(key: str, source: str, field_: str) -> None:
        v = (src.get(source) or {}).get(field_)
        if v is not None and v == v and v > 0:
            m[key] = float(v)
    # CPU per event: at low load fixed per-process costs (poll loops, metrics, GC) are spread over few
    # events and overstate it. With load levels (step-ramp), use the highest level below the knee.
    levels = res.get("levels") or []
    knee = ((rep.get("summary") or {}).get("knee") or {}).get("level")
    usable = [lv for lv in levels if knee is None or lv["level"] < knee]
    top = usable[-1] if usable else None
    cpu_src = {k: dict(v) for k, v in src.items()}
    if top:
        for name, v in (top.get("sources") or {}).items():
            if v.get("cpu_ms_per_event"):
                cpu_src.setdefault(name, {}).update(cpu_ms_per_event=v["cpu_ms_per_event"], events_per_s=v["events_per_s"])
    for key, name in (("gateway_cpu_ms", "gateway"), ("flink_cpu_ms", "flink"), ("emitter_cpu_ms", "emitter"),
                      ("collector_cpu_ms", "collector")):
        cv = (cpu_src.get(name) or {}).get("cpu_ms_per_event")
        if cv is not None and cv == cv and cv > 0:
            m[key] = float(cv)
            rate = (cpu_src.get(name) or {}).get("events_per_s") or 0
            if rate and rate < 2000:
                warn.append(f"{name} cost measured at only {rate:,.0f} events/s: fixed per-process overhead inflates "
                            "CPU per event at low load - calibrate from a step-ramp that reaches the design rate")
    take("event_bytes", "emitter", "bytes_per_event")
    take("decision_bytes", "collector", "bytes_per_event")
    take("store_read_bytes", "gateway", "bytes_per_event")
    take("store_write_bytes", "flink", "bytes_per_event")
    gw_rate = (src.get("gateway") or {}).get("events_per_s") or (src.get("emitter") or {}).get("events_per_s")
    if inf.get("redis", {}).get("ops_per_s") and gw_rate:
        m["redis_ops_per_event"] = inf["redis"]["ops_per_s"] / gw_rate
    hp = (rep.get("stages") or {}).get("hot_path", {}).get("p50")
    if hp:
        m["hot_path_p50_ms"] = float(hp)
    for name, s in src.items():
        if (s.get("utilisation") or 0) > 0.8:
            warn.append(f"{name} ran at {s['utilisation']:.0%} of its CPUs in the measured run: CPU contention "
                        "inflates its per-event cost (measure on dedicated nodes for a tight plan)")
        if (s.get("throttled_ms_per_s") or 0) > 50:
            warn.append(f"{name} was CFS-throttled {s['throttled_ms_per_s']:.0f} ms/s: raise its CPU limit "
                        "(requests == limits) before trusting its cost")
    if not src:
        warn.append(f"{run.name}: report has no `resources` section (run predates CPU capture): all costs assumed")
    # measured stage distributions for the latency composition (lognormal fit: median = p50, p99 = p99)
    stage_map = {"produce_ack": "produce_ack", "ingress": "ingress_transit", "audit_ack": "audit_ack",
                 "egress": "egress_transit", "shuffle": "flink_shuffles", "state_features": "flink_state"}
    infra: dict[str, Any] = {}
    for st, key in stage_map.items():
        s = (rep.get("stages") or {}).get(st)
        if s and s.get("count") and s.get("p50") and s.get("p99") and s["p99"] >= s["p50"] > 0:
            infra[key] = {"lognormal": {"median_ms": float(s["p50"]), "p99_ms": float(s["p99"])}}
    return m, infra, warn


def costs(inp: Inputs) -> tuple[dict[str, Cost], dict[str, Any], list[str]]:
    measured: dict[str, float] = {}
    infra: dict[str, Any] = {}
    warn: list[str] = []
    if inp.run is not None:
        measured, infra, warn = _measured(inp.run)
    out = {}
    for k, (v, unit, note) in DEFAULTS.items():
        if k in inp.overrides:
            out[k] = Cost(float(inp.overrides[k]), unit, "override", note)
        elif k in measured:
            out[k] = Cost(measured[k], unit, "measured", f"{inp.run.name if inp.run else ''}")
        else:
            out[k] = Cost(float(v), unit, "assumed", note)
    return out, infra, warn


# ------------------------------------------------------------------------------------------ plan
def _pods_per_node(m: Machine, pod_cpu: float, pod_mem: float) -> int:
    return max(0, min(int(m.alloc_cpu // pod_cpu), int(m.alloc_mem_gib // pod_mem)))


def plan(inp: Inputs) -> dict[str, Any]:
    c, infra_fit, warnings = costs(inp)
    v = {k: x.value for k, x in c.items()}
    P, S, h, Z = inp.tps * inp.peak, inp.tps, inp.headroom, inp.zones
    comps: list[dict[str, Any]] = []

    # ---- scoring gateway (stateless, CPU bound): 2 pods per c3-standard-8, N+1, spread over zones
    m = GKE[POOLS["realy-serving"]]
    pod_cpu = math.floor(m.alloc_cpu / 2 * 2) / 2
    cores = P * v["gateway_cpu_ms"] / 1000 / h
    gw = max(Z, _ceil(cores / pod_cpu)) + 1
    gw_nodes = _round_up(_ceil(gw / _pods_per_node(m, pod_cpu, 6)), Z)
    per_pod = P / max(1, gw - 1)                                   # one replica down
    gw_wait = wait_p99_ms(int(pod_cpu), per_pod, v["gateway_cpu_ms"])
    comps.append({"component": "scoring gateway", "pool": "realy-serving", "machine": m.name, "nodes": gw_nodes,
                  "pods": gw, "pod": {"cpu": pod_cpu, "memory_gib": 6},
                  "detail": {"cores_needed": round(cores, 1), "hpa": {"min": gw, "max": 2 * gw},
                             "in_flight_per_pod": round(per_pod * v["hot_path_p50_ms"] / 1000),
                             "cpu_queue_wait_p99_ms": round(gw_wait, 3)}})

    # ---- Flink: one TaskManager per n2-highmem-8 (slots = cores), +1 spare TM for fast recovery
    m = GKE[POOLS["realy-flink"]]
    tm_cpu = math.floor(m.alloc_cpu)
    tm_mem = math.floor(m.alloc_mem_gib)
    cores = P * v["flink_cpu_ms"] / 1000 / h
    tms_active = max(2, _ceil(cores / tm_cpu))
    parallelism = tms_active * tm_cpu
    partitions = max(48, _round_up(parallelism, 12))
    ents = {e: inp.customers * r for e, r in ENTITY_RATIO.items()}
    state_gb = sum(n * STATE_BYTES[e] for e, n in ents.items()) / 1e9
    comps.append({"component": "Flink feature job", "pool": "realy-flink", "machine": m.name,
                  "nodes": tms_active + 1, "pods": tms_active + 1,        # TMs + one spare TM (fast recovery)
                  "pod": {"cpu": tm_cpu, "memory_gib": tm_mem},
                  "detail": {"cores_needed": round(cores, 1), "taskmanagers": tms_active + 1, "slots_per_tm": tm_cpu,
                             "parallelism": parallelism, "max_parallelism": max(256, 4 * parallelism),
                             "rocksdb_state_gb": round(state_gb, 1),
                             "state_per_tm_gb": round(state_gb / tms_active, 1), "local_ssd_gb": m.local_ssd_gb,
                             "checkpoints": "gs://, incremental, every 10-30 s",
                             "jobmanager": "2 vCPU / 4 GiB on the realy-serving pool (Kubernetes HA, 1 standby)"}})
    if state_gb / tms_active > 0.5 * m.local_ssd_gb:
        warnings.append("RocksDB state per TaskManager exceeds half its local SSD: add TaskManagers or local SSDs")

    # ---- Kafka: RF 3 rack-aware across zones, brokers in multiples of the zone count
    m = GKE[POOLS["realy-kafka"]]
    ev_mb, dec_mb = P * v["event_bytes"] / 1e6, P * v["decision_bytes"] / 1e6
    produce = ev_mb + dec_mb
    rf = 3
    fetch = ev_mb * 3 + dec_mb * 2           # events: gateway, Flink, ClickHouse; decisions: ClickHouse, collector
    total = produce * rf + produce * (rf - 1) + fetch
    brokers = _round_up(max(3, _ceil(total / (v["kafka_mb_s_per_broker"]))), Z)
    topics = yaml.safe_load((REPO_ROOT / "contracts" / "kafka" / "topics.yaml").read_text())["topics"]
    ret_h = {t: int(x.get("retention_ms", 0)) / 3.6e6 for t, x in topics.items()}
    steady = lambda mb: mb * S / P           # noqa: E731
    store_gb = (steady(ev_mb) * 3600 * ret_h.get("payments.events.raw", 6)
                + steady(dec_mb) * 3600 * ret_h.get("payments.decisions", 24)) * rf / 1000
    disk_per_broker = max(200, _ceil(store_gb / brokers * 1.5 / 50) * 50)
    comps.append({"component": "Kafka brokers", "pool": "realy-kafka", "machine": m.name, "nodes": brokers,
                  "pods": brokers, "pod": {"cpu": math.floor(m.alloc_cpu) - 1, "memory_gib": 16},
                  "detail": {"produce_mb_s": round(produce, 1), "cluster_traffic_mb_s": round(total, 1),
                             "replication_factor": rf, "min_insync_replicas": 2,
                             "partitions": {"payments.events.raw": partitions, "payments.decisions": partitions},
                             "retained_gb": round(store_gb), "disk_per_broker_gb": disk_per_broker,
                             "disk": "pd-ssd (throughput scales with size)"}})

    # ---- Redis (online store): Memorystore for Redis Cluster / Redis Cluster, 1 replica per shard
    ops = P * v["redis_ops_per_event"]
    net = P * (v["store_read_bytes"] + v["store_write_bytes"]) / 1e6
    mem_gb = (sum(n * (STATE_BYTES[e] + 40 if e in inp.state_entities else VALUES_BLOB_BYTES) for e, n in ents.items())
              + (ents["customer"] + ents["merchant"]) * PROFILE_BYTES) * 1.5 / 1e9
    shards = max(3, _ceil(ops / (v["redis_ops_per_shard"] * h)), _ceil(net / (v["redis_mb_s_per_shard"] * h)),
                 _ceil(mem_gb / (13 * 0.7)))
    comps.append({"component": "Redis online store", "pool": "managed", "machine": "Memorystore Redis Cluster",
                  "nodes": shards * 2, "pods": shards * 2, "pod": {"cpu": None, "memory_gib": 13},
                  "detail": {"ops_per_s": round(ops), "network_mb_s": round(net, 1), "memory_gb": round(mem_gb, 1),
                             "primary_shards": shards, "replicas_per_shard": 1, "node_type": "redis-highmem-medium (13 GB)",
                             "state_blob_entities": list(inp.state_entities)}})

    # ---- model serving: Triton on L4, >= 2 replicas (HA across zones), N+1
    m = GKE[POOLS["realy-gpu"]]
    gpus = max(2, _ceil(P / (v["model_rows_per_s_per_gpu"] * h)) + 1)
    # worst case: one GPU down, the rest share the design load
    batch, batch_ms, model_wait = gpu_batching(P / max(1, gpus - 1), v["model_rows_per_s_per_gpu"],
                                               v["model_batch_overhead_ms"], v["model_queue_delay_ms"])
    comps.append({"component": "model serving (Triton)", "pool": "realy-gpu", "machine": m.name, "nodes": gpus,
                  # GPU nodes also run the NVIDIA driver installer + device plugin: keep one vCPU spare
                  "pods": gpus, "pod": {"cpu": math.floor(m.alloc_cpu) - 1, "memory_gib": 24, "gpu": 1},
                  "detail": {"rows_per_s": round(P), "min_replicas": gpus, "max_replicas": 2 * gpus,
                             "gpu_utilisation_one_down": round(P / max(1, gpus - 1) / v["model_rows_per_s_per_gpu"], 2),
                             "dynamic_batch_rows": round(batch, 1), "batch_ms": round(batch_ms, 2),
                             "queue_wait_p99_ms": round(model_wait, 2)}})

    # ---- ClickHouse: async audit / analytics, 2 replicas + 3 Keeper
    m = GKE[POOLS["realy-clickhouse"]]
    rows = 2 * P
    cores = rows / v["clickhouse_rows_per_core"] / h + 2
    per_replica_nodes = max(1, _ceil(cores / math.floor(m.alloc_cpu)))
    gb_day = S * 86400 * (v["event_bytes"] + v["decision_bytes"]) / v["clickhouse_compression"] / 1e9
    comps.append({"component": "ClickHouse (audit)", "pool": "realy-clickhouse", "machine": m.name,
                  "nodes": 2 * per_replica_nodes, "pods": 2 * per_replica_nodes + 3,
                  "pod": {"cpu": math.floor(m.alloc_cpu), "memory_gib": 24},
                  "detail": {"insert_rows_per_s": round(rows), "cores_per_replica": round(cores, 1),
                             "storage_gb_per_day": round(gb_day, 1), "retention_days": inp.retention_days,
                             "disk_per_replica_gb": _ceil(gb_day * inp.retention_days * 1.3 / 50) * 50,
                             "engine": "ReplicatedReplacingMergeTree + 3 Keeper"}})

    # ---- bench driver (only for benchmarking): one console pod = emitters + collectors (one clock)
    shard_rate = 1000.0 / v["emitter_cpu_ms"] * 0.7
    shards_e = max(1, _ceil(P / shard_rate))
    collectors = max(1, _ceil(P * v["collector_cpu_ms"] / 1000 / 0.7))
    drv_cpu = shards_e * 2 + collectors + 2
    m = GKE["c3-standard-8"] if drv_cpu <= GKE["c3-standard-8"].alloc_cpu else GKE["c3-standard-22"]
    comps.append({"component": "bench driver (realy console)", "pool": "realy-driver", "machine": m.name, "nodes": 1,
                  "pods": 1, "pod": {"cpu": min(drv_cpu, math.floor(m.alloc_cpu)), "memory_gib": 16}, "bench_only": True,
                  "detail": {"generators": shards_e, "collectors": collectors,
                             "note": "dedicated pool: never co-locate the driver with the system under test"}})
    if drv_cpu > GKE["c3-standard-22"].alloc_cpu:
        warnings.append("one driver pod cannot generate this load: split the load across consoles / bench Jobs "
                        "(their clocks differ - compare per-driver e2e only)")

    latency = _latency(inp, infra_fit, gw_wait + model_wait)
    if latency.get("verdict") == "INFEASIBLE":
        warnings.append("the latency budget is not met at this load even with the sized capacity: see `latency`")
    pools: dict[str, dict[str, Any]] = {}
    for x in comps:
        p = pools.setdefault(x["pool"], {"machine": x["machine"], "nodes": 0, "bench_only": bool(x.get("bench_only"))})
        p["nodes"] += x["nodes"]
    prod = [p for p in pools.values() if p["machine"] in GKE and not p["bench_only"]]
    vcpu = sum(GKE[p["machine"]].vcpu * p["nodes"] for p in prod)
    gpus_total = sum(GKE[p["machine"]].gpus * p["nodes"] for p in prod)
    return {"inputs": {"tps": inp.tps, "peak_factor": inp.peak, "design_tps": P, "headroom": h, "zones": Z,
                       "customers": inp.customers, "retention_days": inp.retention_days,
                       "from_run": str(inp.run) if inp.run else None},
            "costs": {k: {"value": x.value, "unit": x.unit, "source": x.source, "note": x.note} for k, x in c.items()},
            "components": comps, "pools": pools,
            "totals": {"nodes": sum(p["nodes"] for p in prod), "vcpu": vcpu,
                       "bench_nodes": sum(p["nodes"] for p in pools.values() if p["bench_only"]),
                       "gpus": gpus_total, "redis_shards": next(x for x in comps if x["pool"] == "managed")["detail"]["primary_shards"]},
            "latency": latency, "warnings": warnings}


def _latency(inp: Inputs, infra: dict[str, Any], queue_ms: float) -> dict[str, Any]:
    from ..budget.check import check
    from ..scenario import load_scenario
    try:
        sc = load_scenario(inp.scenario, {"traffic": {"profile": {"kind": "constant", "tps": inp.tps * inp.peak}}})
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}
    r = check(sc, n=100_000, infra=infra or None)
    e2e, hot = r["e2e"]["p99"] + queue_ms, r["hot_path"]["p99"] + queue_ms
    b = sc.budget
    verdict = ("FEASIBLE" if e2e <= b.e2e_p99_ms and hot <= b.hot_path_p99_ms
               else "MARGINAL" if e2e <= 1.15 * b.e2e_p99_ms else "INFEASIBLE")
    return {"scenario": sc.name, "e2e_p99_ms": round(e2e, 1), "hot_path_p99_ms": round(hot, 1),
            "queueing_p99_ms": round(queue_ms, 2), "budget": {"e2e_p99_ms": b.e2e_p99_ms, "hot_path_p99_ms": b.hot_path_p99_ms},
            "fallback_rate": r["fallback_rate"], "infra": "measured stage distributions" if infra else "default baselines",
            "verdict": verdict}


# ------------------------------------------------------------------------------------------ output
def render(p: dict[str, Any]) -> str:
    i = p["inputs"]
    lines = [f"capacity plan: {i['tps']:,.0f} TPS steady x {i['peak_factor']:g} peak = {i['design_tps']:,.0f} TPS design point, "
             f"{i['headroom']:.0%} target utilisation, {i['zones']} zones"
             + (f" (costs from {i['from_run']})" if i["from_run"] else " (assumed costs - size again with --from <run>)"), ""]
    lines.append(f"{'component':30s} {'pool / machine':36s} {'nodes':>5s} {'pods':>5s}  key numbers")
    for x in p["components"]:
        d = x["detail"]
        key = {
            "scoring gateway": lambda: f"{d['cores_needed']} cores busy, HPA {d['hpa']['min']}-{d['hpa']['max']}",
            "Flink feature job": lambda: f"parallelism {d['parallelism']}, {d['rocksdb_state_gb']} GB state",
            "Kafka brokers": lambda: f"{d['cluster_traffic_mb_s']} MB/s, {d['partitions']['payments.events.raw']} partitions, {d['disk_per_broker_gb']} GB/broker",
            "Redis online store": lambda: f"{d['primary_shards']} shards, {d['ops_per_s']:,} ops/s, {d['network_mb_s']} MB/s, {d['memory_gb']} GB",
            "model serving (Triton)": lambda: f"{d['rows_per_s']:,} rows/s, batch ~{d['dynamic_batch_rows']} rows / {d['batch_ms']} ms",
            "ClickHouse (audit)": lambda: f"{d['insert_rows_per_s']:,} rows/s, {d['storage_gb_per_day']} GB/day",
            "bench driver (realy console)": lambda: f"{d['generators']} generators, {d['collectors']} collectors",
        }.get(x["component"], lambda: "")()
        lines.append(f"{x['component']:30s} {x['pool'] + ' / ' + x['machine']:36s} {x['nodes']:5d} {x['pods']:5d}  {key}")
    t = p["totals"]
    lines += ["", f"production: {t['nodes']} GKE nodes, {t['vcpu']} vCPU, {t['gpus']} L4 GPU(s), {t['redis_shards']} Redis "
                  f"shards (+replicas); benchmarking adds {t['bench_nodes']} driver node(s)"]
    lat = p["latency"]
    if "verdict" in lat:
        lines.append(f"latency at {i['design_tps']:,.0f} TPS ({lat['infra']}): e2e p99 {lat['e2e_p99_ms']} ms, hot path p99 "
                     f"{lat['hot_path_p99_ms']} ms incl. {lat['queueing_p99_ms']} ms queueing -> {lat['verdict']} "
                     f"(budget {lat['budget']['e2e_p99_ms']:.0f} / {lat['budget']['hot_path_p99_ms']:.0f} ms)")
    lines += ["", "per-event costs:"]
    for k, x in p["costs"].items():
        lines.append(f"  {k:26s} {x['value']:>10,.3f}  {x['unit']:40s} [{x['source']}]")
    for w in p["warnings"]:
        lines.append(f"warning: {w}")
    return "\n".join(lines)


def write_overlay(p: dict[str, Any], out_dir: Path) -> Path:
    """Kustomize overlay on top of overlays/gke-l4 applying the plan's replicas, resources, partitions
    and parallelism. Redis is managed (Memorystore) and not patched."""
    out_dir.mkdir(parents=True, exist_ok=True)
    base = Path("..") if False else Path(__import__("os").path.relpath(REPO_ROOT / "deploy" / "k8s" / "overlays" / "gke-l4", out_dir))
    comp = {x["component"]: x for x in p["components"]}
    gw, fl, kf, tr, ch = (comp["scoring gateway"], comp["Flink feature job"], comp["Kafka brokers"],
                          comp["model serving (Triton)"], comp["ClickHouse (audit)"])
    flink_docs = [d for d in yaml.safe_load_all((REPO_ROOT / "deploy/k8s/components/operators/flink.yaml").read_text()) if d]
    patches: list[dict[str, Any]] = [
        {"target": {"kind": "Deployment", "name": "gateway"}, "patch": json.dumps([
            {"op": "replace", "path": "/spec/replicas", "value": gw["pods"]},
            {"op": "replace", "path": "/spec/template/spec/containers/0/resources",
             "value": {"requests": {"cpu": str(gw["pod"]["cpu"]), "memory": f"{gw['pod']['memory_gib']}Gi"},
                       "limits": {"cpu": str(gw["pod"]["cpu"]), "memory": f"{gw['pod']['memory_gib']}Gi"}}}])},
        {"target": {"kind": "HorizontalPodAutoscaler", "name": "gateway"}, "patch": json.dumps([
            {"op": "replace", "path": "/spec/minReplicas", "value": gw["detail"]["hpa"]["min"]},
            {"op": "replace", "path": "/spec/maxReplicas", "value": gw["detail"]["hpa"]["max"]}])},
        {"target": {"kind": "KafkaNodePool", "name": "brokers"}, "patch": json.dumps([
            {"op": "replace", "path": "/spec/replicas", "value": kf["pods"]},
            {"op": "replace", "path": "/spec/storage/volumes/0/size", "value": f"{kf['detail']['disk_per_broker_gb']}Gi"}])},
        {"target": {"group": "serving.kserve.io", "kind": "InferenceService", "name": "fraud-models"}, "patch": json.dumps([
            {"op": "replace", "path": "/spec/predictor/minReplicas", "value": tr["detail"]["min_replicas"]},
            {"op": "replace", "path": "/spec/predictor/maxReplicas", "value": tr["detail"]["max_replicas"]},
            {"op": "replace", "path": "/spec/predictor/model/resources",
             "value": {"requests": {"cpu": str(tr["pod"]["cpu"]), "memory": "24Gi", "nvidia.com/gpu": "1"},
                       "limits": {"cpu": str(tr["pod"]["cpu"]), "memory": "24Gi", "nvidia.com/gpu": "1"}}}])},
        {"target": {"kind": "StatefulSet", "name": "clickhouse"}, "patch": json.dumps([
            {"op": "replace", "path": "/spec/template/spec/containers/0/resources",
             "value": {"requests": {"cpu": str(ch["pod"]["cpu"]), "memory": "24Gi"},
                       "limits": {"cpu": str(ch["pod"]["cpu"]), "memory": "24Gi"}}},
            {"op": "replace", "path": "/spec/volumeClaimTemplates/0/spec/resources/requests/storage",
             "value": f"{ch['detail']['disk_per_replica_gb']}Gi"}])},
    ]
    for topic, parts in kf["detail"]["partitions"].items():
        patches.append({"target": {"kind": "KafkaTopic", "name": topic.replace(".", "-")},
                        "patch": json.dumps([{"op": "replace", "path": "/spec/partitions", "value": parts}])})
    par = fl["detail"]["parallelism"]
    for d in flink_docs:
        args = list(d["spec"]["job"]["args"])
        if "--parallelism" in args:
            args[args.index("--parallelism") + 1] = str(par)
        patches.append({"target": {"kind": "FlinkDeployment", "name": d["metadata"]["name"]}, "patch": json.dumps([
            {"op": "replace", "path": "/spec/job/parallelism", "value": par},
            {"op": "replace", "path": "/spec/job/args", "value": args},
            {"op": "replace", "path": "/spec/taskManager/resource",
             "value": {"cpu": fl["pod"]["cpu"], "memory": f"{fl['pod']['memory_gib']}g"}},
            {"op": "replace", "path": "/spec/flinkConfiguration/taskmanager.numberOfTaskSlots",
             "value": str(fl["detail"]["slots_per_tm"])}])})
    kust = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
            "resources": [str(base)], "patches": patches}
    header = (f"# GENERATED by `realy size` for {p['inputs']['design_tps']:,.0f} TPS (design point) - review before applying.\n"
              f"# Redis: {comp['Redis online store']['detail']['primary_shards']} primary shards on Memorystore Redis Cluster "
              f"(point REDIS_URI at it); not patched here.\n")
    path = out_dir / "kustomization.yaml"
    path.write_text(header + yaml.safe_dump(kust, sort_keys=False, width=200))
    return path
