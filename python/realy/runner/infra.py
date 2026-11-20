"""Infrastructure sampling during a run (infra.jsonl): the shared systems' cost of the load, which the
services' own CPU counters cannot see - Redis ops / CPU / network, Kafka message rates per topic and
Flink operator busy / backpressure time. Feeds the report's `infra` section and `realy size`.

Every probe is best-effort with a short timeout: a sampler must never disturb or fail the run.
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any

import httpx

from ..common.clock import now_us

TOPICS = ("payments.events.raw", "payments.decisions")
INTERVAL_S = float(os.environ.get("REALY_INFRA_INTERVAL_S", "5"))


def sample_redis(r: Any) -> dict[str, Any]:
    info = r.info()
    return {k: info.get(k) for k in ("instantaneous_ops_per_sec", "used_cpu_sys", "used_cpu_user", "used_memory",
                                     "total_net_input_bytes", "total_net_output_bytes", "total_commands_processed")}


def sample_kafka(consumer: Any) -> dict[str, int]:
    from confluent_kafka import TopicPartition
    out = {}
    md = consumer.list_topics(timeout=5)
    for t in TOPICS:
        if t not in md.topics:
            continue
        total = 0
        for p in md.topics[t].partitions:
            _, hi = consumer.get_watermark_offsets(TopicPartition(t, p), timeout=5, cached=False)
            total += hi
        out[t] = total
    return out


def sample_flink(c: httpx.Client) -> dict[str, dict[str, float]]:
    jobs = c.get("/jobs/overview").json().get("jobs", [])
    out: dict[str, dict[str, float]] = {}
    for j in jobs:
        if j.get("state") != "RUNNING" or not str(j.get("name", "")).startswith("realy-"):
            continue
        for v in c.get(f"/jobs/{j['jid']}").json().get("vertices", []):
            m = c.get(f"/jobs/{j['jid']}/vertices/{v['id']}/subtasks/metrics",
                      params={"get": "busyTimeMsPerSecond,backPressuredTimeMsPerSecond", "agg": "max,avg"}).json()
            vals = {x["id"]: x for x in m}
            busy = vals.get("busyTimeMsPerSecond", {})
            bp = vals.get("backPressuredTimeMsPerSecond", {})
            out[v["name"][:60]] = {"parallelism": v.get("parallelism", 0), "busy_avg": busy.get("avg"),
                                   "busy_max": busy.get("max"), "backpressured_max": bp.get("max")}
    return out


class InfraSampler:
    """Writes one JSON line per INTERVAL_S to <run_dir>/infra.jsonl until stopped."""

    def __init__(self, run_dir: Path, flink_rest: str | None, log=print, max_s: float = 7200.0) -> None:
        self.path = run_dir / "infra.jsonl"
        # hard stop even if the run dies without calling stop() (the console process lives on)
        self.deadline = time.time() + max_s
        self.flink_rest = flink_rest
        self.log = log
        self.stop_evt = threading.Event()
        self.thread = threading.Thread(target=self._loop, name="infra-sampler", daemon=True)

    def start(self) -> "InfraSampler":
        self.thread.start()
        return self

    def stop(self) -> None:
        self.stop_evt.set()
        self.thread.join(timeout=15)

    def _loop(self) -> None:
        r = consumer = flink = None
        try:
            from .seed import redis_client
            r = redis_client()
        except Exception:  # noqa: BLE001
            r = None
        try:
            from confluent_kafka import Consumer

            from ..common.kafka import bootstrap
            consumer = Consumer({"bootstrap.servers": bootstrap(), "group.id": "infraprobe", "enable.auto.commit": False})
        except Exception:  # noqa: BLE001
            consumer = None
        if self.flink_rest:
            flink = httpx.Client(base_url=self.flink_rest, timeout=3)
        warned: set[str] = set()
        with open(self.path, "a") as out:
            while not self.stop_evt.wait(INTERVAL_S) and time.time() < self.deadline:
                rec: dict[str, Any] = {"ts_us": now_us()}
                for name, fn, arg in (("redis", sample_redis, r), ("kafka", sample_kafka, consumer),
                                      ("flink", sample_flink, flink)):
                    if arg is None:
                        continue
                    try:
                        rec[name] = fn(arg)
                    except Exception as e:  # noqa: BLE001
                        if name not in warned:
                            self.log(f"infra sampler: {name} unavailable ({type(e).__name__})")
                            warned.add(name)
                out.write(json.dumps(rec) + "\n")
                out.flush()
        if consumer is not None:
            consumer.close()
        if flink is not None:
            flink.close()


def summarise(run_dir: Path, t_from_us: int, t_to_us: int) -> dict[str, Any]:
    """Rates over [t_from, t_to] from infra.jsonl: Redis ops/s, cores, MB/s; Kafka msgs/s; Flink busy."""
    p = run_dir / "infra.jsonl"
    if not p.exists():
        return {}
    rows = []
    for line in p.read_text().splitlines():
        try:
            d = json.loads(line)
        except ValueError:
            continue
        if t_from_us <= d.get("ts_us", 0) <= t_to_us:
            rows.append(d)
    if len(rows) < 2:
        return {}
    out: dict[str, Any] = {"samples": len(rows)}
    rd = [x["redis"] for x in rows if x.get("redis")]
    if len(rd) >= 2:
        dt = (rows[-1]["ts_us"] - rows[0]["ts_us"]) / 1e6
        first, last = rd[0], rd[-1]

        def rate(k: str) -> float | None:
            if first.get(k) is None or last.get(k) is None or dt <= 0:
                return None
            return (float(last[k]) - float(first[k])) / dt
        cpu = (rate("used_cpu_sys") or 0) + (rate("used_cpu_user") or 0)
        out["redis"] = {"ops_per_s": rate("total_commands_processed"), "cpu_cores": cpu,
                        "net_in_mb_s": (rate("total_net_input_bytes") or 0) / 1e6,
                        "net_out_mb_s": (rate("total_net_output_bytes") or 0) / 1e6,
                        "used_memory_mb": max(float(x.get("used_memory") or 0) for x in rd) / 1048576}
    kf = [(x["ts_us"], x["kafka"]) for x in rows if x.get("kafka")]
    if len(kf) >= 2:
        dt = (kf[-1][0] - kf[0][0]) / 1e6
        out["kafka_msgs_per_s"] = {t: (kf[-1][1].get(t, 0) - kf[0][1].get(t, 0)) / dt for t in kf[-1][1]} if dt > 0 else {}
    fl = [x["flink"] for x in rows if x.get("flink")]
    if fl:
        verts: dict[str, dict[str, list[float]]] = {}
        for f in fl:
            for name, v in f.items():
                a = verts.setdefault(name, {"busy_avg": [], "busy_max": [], "bp_max": [], "par": []})
                for k, src in (("busy_avg", "busy_avg"), ("busy_max", "busy_max"), ("bp_max", "backpressured_max")):
                    if v.get(src) is not None:
                        a[k].append(float(v[src]))
                a["par"].append(float(v.get("parallelism") or 0))
        out["flink"] = {n: {"parallelism": int(max(a["par"] or [0])),
                            "busy_avg_pct": (sum(a["busy_avg"]) / len(a["busy_avg"]) / 10) if a["busy_avg"] else None,
                            "busy_max_pct": (max(a["busy_max"]) / 10) if a["busy_max"] else None,
                            "backpressured_max_pct": (max(a["bp_max"]) / 10) if a["bp_max"] else None}
                        for n, a in verts.items()}
    return out
