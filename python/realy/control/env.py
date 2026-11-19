"""Environment page backend: health/version of every component plus Kafka, Flink, Redis and ClickHouse
detail. Every probe has a short timeout and runs in parallel, so a dead component shows as `down`
instead of hanging the page. Endpoints come from the same env vars the runner uses."""
from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Callable

import httpx
from fastapi import APIRouter

router = APIRouter(prefix="/api/env", tags=["environment"])
TIMEOUT = float(os.environ.get("REALY_ENV_TIMEOUT_S", "2.5"))
RELEVANT_TOPICS = ("payments.events.raw", "payments.decisions", "payments.labels", "payments.features",
                   "bench.control", "bench.metrics")


def _url(var: str, default: str) -> str:
    return os.environ.get(var, default).rstrip("/")


def endpoints() -> dict[str, str]:
    return {"kafka": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
            "redis": os.environ.get("REDIS_URI", "redis://localhost:6379"),
            "clickhouse": _url("CLICKHOUSE_URL", "http://localhost:8123"),
            "flink": _url("FLINK_REST", "http://localhost:8081"),
            "gateway": _url("GATEWAY_ADMIN", "http://localhost:8080"),
            "model": _url("MODEL_ADMIN", "http://localhost:8002")}


def links() -> dict[str, str | None]:
    """Browser-facing URLs (may differ from in-cluster service URLs)."""
    return {"flink": os.environ.get("FLINK_UI_URL") or os.environ.get("FLINK_REST") or "http://localhost:8081",
            "grafana": os.environ.get("GRAFANA_URL") or "http://localhost:3000",
            "prometheus": os.environ.get("PROMETHEUS_URL") or "http://localhost:9090"}


def _timed(fn: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    t0 = time.perf_counter()
    try:
        out = fn()
        out.setdefault("status", "up")
    except Exception as e:  # noqa: BLE001 - surfaced to the UI
        out = {"status": "down", "details": {"error": f"{e.__class__.__name__}: {str(e)[:200]}"}}
    out["latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    return out


# ---------------------------------------------------------------------------------- probes
def probe_kafka() -> dict[str, Any]:
    from confluent_kafka.admin import AdminClient
    a = AdminClient({"bootstrap.servers": endpoints()["kafka"], "socket.timeout.ms": int(TIMEOUT * 1000)})
    md = a.list_topics(timeout=TIMEOUT)
    present = [t for t in RELEVANT_TOPICS if t in md.topics]
    status = "up" if len(present) == len(RELEVANT_TOPICS) else "degraded"
    return {"status": status, "version": f"KRaft · {len(md.brokers)} broker(s)",
            "details": {"brokers": len(md.brokers), "topics": len(md.topics),
                        "missing_topics": [t for t in RELEVANT_TOPICS if t not in md.topics]}}


def _redis():
    import redis
    return redis.Redis.from_url(endpoints()["redis"], socket_timeout=TIMEOUT, socket_connect_timeout=TIMEOUT)


def probe_redis() -> dict[str, Any]:
    info = _redis().info()
    return {"version": info.get("redis_version"), "details": {
        "used_memory_human": info.get("used_memory_human"), "ops_per_sec": info.get("instantaneous_ops_per_sec"),
        "keys": sum(v.get("keys", 0) for k, v in info.items() if k.startswith("db") and isinstance(v, dict))}}


def probe_clickhouse() -> dict[str, Any]:
    r = httpx.get(endpoints()["clickhouse"], params={"query": "SELECT version()"}, timeout=TIMEOUT)
    r.raise_for_status()
    rows = httpx.get(endpoints()["clickhouse"], params={
        "query": "SELECT count() FROM system.tables WHERE database = 'realy'"}, timeout=TIMEOUT).text.strip()
    return {"version": r.text.strip(), "status": "up" if rows not in ("", "0") else "degraded",
            "details": {"realy_tables": int(rows or 0)}}


def probe_flink() -> dict[str, Any]:
    o = httpx.get(endpoints()["flink"] + "/overview", timeout=TIMEOUT).json()
    running = int(o.get("jobs-running", 0))
    return {"version": o.get("flink-version"), "status": "up" if o.get("taskmanagers", 0) > 0 else "degraded",
            "details": {"taskmanagers": o.get("taskmanagers"), "slots_total": o.get("slots-total"),
                        "slots_available": o.get("slots-available"), "jobs_running": running}}


def _service(base: str) -> dict[str, Any]:
    httpx.get(base + "/health", timeout=TIMEOUT).raise_for_status()
    cfg = httpx.get(base + "/admin/config", timeout=TIMEOUT).json()
    return cfg


def probe_gateway() -> dict[str, Any]:
    cfg = _service(endpoints()["gateway"])
    return {"version": cfg.get("rule_set_version") or cfg.get("version"),
            "details": {"config_version": cfg.get("version"), "budget_ms": cfg.get("budget_ms"),
                        "feature_source": cfg.get("feature_source"), "echo": cfg.get("echo", False)}}


def probe_model() -> dict[str, Any]:
    cfg = _service(endpoints()["model"])
    return {"version": cfg.get("model_version"),
            "details": {"workers": cfg.get("workers", 0), "max_batch": cfg.get("max_batch", 1),
                        "service_time": cfg.get("service_time")}}


def probe_console() -> dict[str, Any]:
    from importlib.metadata import PackageNotFoundError, version
    try:
        v = version("realy")
    except PackageNotFoundError:
        v = "dev"
    return {"version": v, "details": {"auth": bool(os.environ.get("REALY_CONSOLE_TOKEN"))}}


COMPONENTS = [
    ("kafka", "Kafka", "broker", probe_kafka), ("redis", "Redis", "online store", probe_redis),
    ("clickhouse", "ClickHouse", "audit warehouse", probe_clickhouse), ("flink", "Flink", "stream processor", probe_flink),
    ("gateway", "Scoring gateway", "hot path", probe_gateway), ("model", "Model server", "inference", probe_model),
    ("console", "Console", "control plane", probe_console),
]
_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="env-probe")


@router.get("")
def environment() -> dict[str, Any]:
    eps = endpoints()
    futs = {cid: _pool.submit(_timed, fn) for cid, _, _, fn in COMPONENTS}
    comps = []
    for cid, name, kind, _ in COMPONENTS:
        r = futs[cid].result(timeout=TIMEOUT * 3)
        comps.append({"id": cid, "name": name, "kind": kind, "status": r.get("status", "down"),
                      "latency_ms": r.get("latency_ms"), "version": r.get("version"),
                      "details": r.get("details", {}), "url": eps.get(cid)})
    return {"checked_at": datetime.now(timezone.utc).isoformat(), "components": comps, "links": links()}


# ---------------------------------------------------------------------------------- detail
@router.get("/kafka")
def kafka_detail() -> dict[str, Any]:
    from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
    from confluent_kafka.admin import AdminClient
    bs = endpoints()["kafka"]
    a = AdminClient({"bootstrap.servers": bs})
    md = a.list_topics(timeout=TIMEOUT)
    c = Consumer({"bootstrap.servers": bs, "group.id": "realy-console-probe", "enable.auto.commit": False})
    ends: dict[str, dict[int, int]] = {}
    topics = []
    for name in RELEVANT_TOPICS:
        t = md.topics.get(name)
        if t is None:
            continue
        ends[name] = {}
        total = 0
        for p in t.partitions:
            lo, hi = c.get_watermark_offsets(TopicPartition(name, p), timeout=TIMEOUT)
            ends[name][p] = hi
            total += hi
        topics.append({"name": name, "partitions": len(t.partitions), "end_offset_total": total})
    c.close()
    groups = []
    try:
        listed = a.list_consumer_groups(request_timeout=TIMEOUT).result()
        ids = [g.group_id for g in listed.valid]
        desc = a.describe_consumer_groups(ids, request_timeout=TIMEOUT) if ids else {}
        states = {}
        for gid, f in desc.items():
            try:
                states[gid] = str(f.result().state).split(".")[-1]
            except Exception:  # noqa: BLE001
                states[gid] = "UNKNOWN"
        keep = [g for g in ids if g.startswith(("realy-gateway", "realy-flink", "clickhouse-"))
                or states.get(g) == "STABLE" and not g.startswith(("console-", "control-", "runner-", "lagprobe-"))]
        for gid in sorted(keep)[:40]:
            parts = [TopicPartition(t, p) for t in ends for p in ends[t]]
            res = a.list_consumer_group_offsets([ConsumerGroupTopicPartitions(gid, parts)])[gid].result()
            per_topic: dict[str, int] = {}
            for tp in res.topic_partitions:
                if tp.offset >= 0:
                    per_topic[tp.topic] = per_topic.get(tp.topic, 0) + max(0, ends[tp.topic][tp.partition] - tp.offset)
            for topic, lag in per_topic.items():
                groups.append({"group": gid, "topic": topic, "lag": lag, "state": states.get(gid)})
    except Exception as e:  # noqa: BLE001
        groups.append({"group": "(unavailable)", "topic": "", "lag": None, "state": str(e)[:120]})
    return {"brokers": [{"id": b.id, "host": b.host, "port": b.port} for b in md.brokers.values()],
            "topics": topics, "groups": groups}


@router.get("/flink")
def flink_detail() -> dict[str, Any]:
    base = endpoints()["flink"]
    with httpx.Client(base_url=base, timeout=TIMEOUT) as h:
        o = h.get("/overview").json()
        jobs = []
        for j in h.get("/jobs/overview").json().get("jobs", [])[:10]:
            item = {"id": j["jid"], "name": j["name"], "state": j["state"], "start_time": j.get("start-time"),
                    "duration_ms": j.get("duration"), "checkpoints": None, "vertices": []}
            if j["state"] == "RUNNING":
                d = h.get(f"/jobs/{j['jid']}").json()
                for v in d.get("vertices", []):
                    bp = "unknown"
                    try:
                        bp = h.get(f"/jobs/{j['jid']}/vertices/{v['id']}/backpressure").json().get("backpressureLevel", "ok")
                    except Exception:  # noqa: BLE001
                        pass
                    m = v.get("metrics", {})
                    item["vertices"].append({"name": v["name"], "parallelism": v.get("parallelism"),
                                             "status": v.get("status"), "backpressure": bp,
                                             "read_records": m.get("read-records"), "write_records": m.get("write-records")})
                cp = h.get(f"/jobs/{j['jid']}/checkpoints").json()
                last = (cp.get("latest") or {}).get("completed") or {}
                item["checkpoints"] = {"completed": cp.get("counts", {}).get("completed"),
                                       "failed": cp.get("counts", {}).get("failed"),
                                       "last_duration_ms": last.get("end_to_end_duration"),
                                       "last_size_bytes": last.get("state_size") or last.get("checkpointed_size"),
                                       "last_completed_at": last.get("latest_ack_timestamp")}
            jobs.append(item)
    return {"version": o.get("flink-version"), "ui_url": links()["flink"], "taskmanagers": o.get("taskmanagers"),
            "slots_total": o.get("slots-total"), "slots_available": o.get("slots-available"), "jobs": jobs}


@router.get("/redis")
def redis_detail() -> dict[str, Any]:
    r = _redis()
    info = r.info()
    return {"version": info.get("redis_version"), "used_memory_bytes": info.get("used_memory"),
            "keys": r.dbsize(), "ops_per_sec": info.get("instantaneous_ops_per_sec"),
            "connected_clients": info.get("connected_clients"), "uptime_s": info.get("uptime_in_seconds"),
            "stream_blobs": None, "profile_blobs": None}


@router.get("/clickhouse")
def clickhouse_detail() -> dict[str, Any]:
    base = endpoints()["clickhouse"]
    v = httpx.get(base, params={"query": "SELECT version()"}, timeout=TIMEOUT).text.strip()
    q = ("SELECT table, sum(rows) AS rows, count() AS parts, sum(bytes_on_disk) AS bytes FROM system.parts "
         "WHERE database = 'realy' AND active GROUP BY table ORDER BY table FORMAT JSONEachRow")
    import json
    rows = [json.loads(line) for line in httpx.get(base, params={"query": q}, timeout=TIMEOUT).text.splitlines() if line]
    return {"version": v, "tables": [{"table": r["table"], "rows": int(r["rows"]), "parts": int(r["parts"]),
                                      "bytes": int(r["bytes"])} for r in rows]}
