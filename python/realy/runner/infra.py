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
    out = {k: info.get(k) for k in ("instantaneous_ops_per_sec", "used_cpu_sys", "used_cpu_user", "used_memory",
                                    "maxmemory", "maxmemory_policy", "evicted_keys", "uptime_in_seconds",
                                    "total_net_input_bytes", "total_net_output_bytes", "total_commands_processed")}
    out["profiles_marker"] = bool(r.exists("pf:_world"))
    return out


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
        if not str(j.get("name", "")).startswith("realy-") or j.get("state") in ("FINISHED", "CANCELED", "FAILED"):
            continue
        # job health: state, restarts and the latest failure, whatever the job is doing
        try:
            rs = {x["id"]: x.get("value") for x in c.get(f"/jobs/{j['jid']}/metrics", params={"get": "numRestarts"}).json()}
            exc = (c.get(f"/jobs/{j['jid']}/exceptions", params={"maxExceptions": 1}).json().get("exceptionHistory") or {})
            last = (exc.get("entries") or [{}])[0]
            out[f"_job:{j['name']}"] = {"jid": j["jid"], "state": j.get("state"),
                                        "restarts": float(rs.get("numRestarts") or 0),
                                        "last_failure": (last.get("exceptionName") or "") + (
                                            f": {(last.get('stacktrace') or '').splitlines()[0][:300]}" if last.get("stacktrace") else ""),
                                        "last_failure_task": last.get("taskName")}
        except Exception:  # noqa: BLE001
            pass
        if j.get("state") != "RUNNING":
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


class HealthWatch:
    """Judges each infra sample while the run is in progress, so a broken dependency ends the run with
    its reason instead of a mystery: the online store filling up (Redis rejects writes under
    `noeviction`, or evicts feature state under an LRU policy), the store losing its data (a restarted
    Redis without persistence) and Flink job restarts (counted with their cause in the manifest)."""

    WARN_AT = float(os.environ.get("REALY_STORE_WARN_AT", "0.85"))
    ABORT_AT = float(os.environ.get("REALY_STORE_ABORT_AT", "0.97"))

    def __init__(self, log, abort, note) -> None:
        self.log, self.abort, self.note = log, abort, note
        self.warned: set[str] = set()
        self.marker_seen = False
        self.uptime: float | None = None
        self.evicted0: float | None = None
        self.restarts0: dict[str, float] = {}
        self.restarts: dict[str, float] = {}
        self.peak = 0.0

    def check(self, rec: dict[str, Any]) -> None:
        rd = rec.get("redis") or {}
        if rd:
            used, cap = rd.get("used_memory") or 0, rd.get("maxmemory") or 0
            if cap:
                frac = used / cap
                self.peak = max(self.peak, frac)
                self.note("store_memory_peak", round(self.peak, 4))
                if frac >= self.ABORT_AT:
                    self.abort(f"online store full: Redis uses {used / 2**30:.2f} of {cap / 2**30:.2f} GiB "
                               f"({frac:.0%}); writes fail under '{rd.get('maxmemory_policy')}'. Raise REDIS_MAXMEMORY "
                               "or run a smaller world (see docs/production.md, sizing)")
                elif frac >= self.WARN_AT and "mem" not in self.warned:
                    self.warned.add("mem")
                    self.log(f"WARN online store at {frac:.0%} of Redis maxmemory ({cap / 2**30:.2f} GiB)")
            ev = rd.get("evicted_keys")
            if ev is not None:
                self.evicted0 = ev if self.evicted0 is None else self.evicted0
                if ev > self.evicted0 and "evict" not in self.warned:
                    self.warned.add("evict")
                    self.note("store_evictions", ev - self.evicted0)
                    self.abort(f"online store evicted {ev - self.evicted0:.0f} keys (maxmemory policy "
                               f"'{rd.get('maxmemory_policy')}'): feature state was lost, features are wrong from here")
            up = rd.get("uptime_in_seconds")
            if up is not None and self.uptime is not None and up < self.uptime:
                self.abort("the online store restarted during the run (Redis uptime went back): its feature state "
                           "and profiles were lost")
            self.uptime = up if up is not None else self.uptime
            if rd.get("profiles_marker"):
                self.marker_seen = True
            elif self.marker_seen:
                self.abort("the online store lost its profiles (pf:_world is gone): Redis was flushed or restarted")
        for name, j in (rec.get("flink") or {}).items():
            if not name.startswith("_job:"):
                continue
            jid, n = j.get("jid"), float(j.get("restarts") or 0)
            self.restarts0.setdefault(jid, n)
            extra = n - self.restarts0[jid]
            if extra > self.restarts.get(jid, 0):
                self.restarts[jid] = extra
                self.note("flink_restarts", sum(self.restarts.values()))
                self.note("flink_last_failure", j.get("last_failure"))
                self.log(f"WARN flink job {name[5:]} restarted ({extra:.0f} so far this run; state {j.get('state')}): "
                         f"{j.get('last_failure') or 'no exception recorded'}")


class InfraSampler:
    """Writes one JSON line per INTERVAL_S to <run_dir>/infra.jsonl until stopped. With `on_sample`,
    each sample is also handed to it (HealthWatch)."""

    def __init__(self, run_dir: Path, flink_rest: str | None, log=print, max_s: float = 7200.0,
                 on_sample=None) -> None:
        self.on_sample = on_sample
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
                if self.on_sample is not None:
                    try:
                        self.on_sample(rec)
                    except Exception as e:  # noqa: BLE001 - a judge must never break sampling
                        if "judge" not in warned:
                            self.log(f"infra sampler: health check failed ({type(e).__name__}: {e})")
                            warned.add("judge")
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
