"""Preflight: is the infrastructure a run is about to use there, reachable, configured for this pipeline,
and real or mock? `pulsebench infra check`, the console's Environment page and the runner (before
anything is sent) all run `check()`.

Every component the scenario's topology uses (contracts/topologies.yaml runtime) is probed through the
same settings the services read (KAFKA_BOOTSTRAP + security, REDIS_URI + REDIS_MODE, CLICKHOUSE_*,
FLINK_REST, GATEWAY_ADMIN). The model server is probed through the gateway's own view of it
(GET /admin/info), because the gateway's network and settings are the ones that matter. Each component
gets checks with pass / warn / fail, the detail, and the fix; `implementation` says whether it is the
bundled one, a mock, or yours (external), and `simulated` lists every simulated part of the run (the mock
model's service time, injected delays), so a result is never mistaken for a measurement of real
infrastructure. The run records the result (infra.json, the manifest) and its report shows it.
"""
from __future__ import annotations

import hashlib
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

TIMEOUT = float(os.environ.get("REALY_PREFLIGHT_TIMEOUT_S", "5"))
BUNDLED_HOSTS = {"kafka", "redis", "clickhouse", "flink-jobmanager", "gateway", "mock-model", "localhost", "127.0.0.1",
                 "realy-kafka-bootstrap", "realy-redis", "realy-clickhouse", "realy-gateway", "realy-mock-model"}
RANK = {"pass": 0, "skip": 0, "warn": 1, "fail": 2}


def _host(endpoint: str) -> str:
    e = endpoint.split(",")[0].strip()
    if "://" in e:
        return urlparse(e).hostname or ""
    return e.rsplit(":", 1)[0]


def _impl(endpoint: str) -> str:
    return "bundled" if _host(endpoint) in BUNDLED_HOSTS or _host(endpoint).startswith("realy-") else "external"


class _C:
    """One component's checks."""

    def __init__(self, cid: str, label: str, endpoint: str) -> None:
        self.out: dict[str, Any] = {"component": cid, "label": label, "endpoint": endpoint,
                                    "implementation": _impl(endpoint), "version": None, "facts": {}, "checks": []}

    def add(self, name: str, status: str, detail: str, fix: str = "") -> None:
        self.out["checks"].append({"name": name, "status": status, "detail": detail, "fix": fix})

    def done(self) -> dict[str, Any]:
        st = [c["status"] for c in self.out["checks"]] or ["skip"]
        self.out["status"] = max(st, key=lambda s: RANK[s])
        return self.out


def _guard(c: _C, fn: Callable[[_C], None], what: str, fix: str) -> dict[str, Any]:
    t0 = time.perf_counter()
    try:
        fn(c)
    except Exception as e:  # noqa: BLE001 - every failure becomes a check the operator can act on
        c.add("reachable", "fail", f"{what}: {type(e).__name__}: {str(e)[:200]}", fix)
    c.out["probe_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    return c.done()


# ------------------------------------------------------------------------------------------- Kafka
def check_kafka() -> dict[str, Any]:
    from .common.kafka import TOPICS_PATH, bootstrap, security_config, topic_plan
    ep = bootstrap()
    c = _C("kafka", "Kafka", ep)

    def run(c: _C) -> None:
        import yaml
        from confluent_kafka.admin import AdminClient
        sec = security_config()
        a = AdminClient({"bootstrap.servers": ep, "socket.timeout.ms": int(TIMEOUT * 1000), **sec})
        md = a.list_topics(timeout=TIMEOUT)
        proto = sec.get("security.protocol", "PLAINTEXT")
        c.out["version"] = f"{len(md.brokers)} broker(s), {proto}"
        c.out["facts"].update({"brokers": len(md.brokers), "security_protocol": proto,
                               "sasl_mechanism": sec.get("sasl.mechanism", "")})
        c.add("reachable", "pass", f"{len(md.brokers)} broker(s) over {proto}")
        if c.out["implementation"] == "external" and proto == "PLAINTEXT":
            c.add("security", "warn", "an external cluster reached without TLS or SASL",
                  "set KAFKA_SECURITY_PROTOCOL (SASL_SSL / SSL) and the KAFKA_SASL_* / KAFKA_SSL_* settings")
        plan = topic_plan(md, yaml.safe_load(TOPICS_PATH.read_text()))
        missing = [p["topic"] for p in plan if not p["exists"]]
        c.out["facts"]["topics"] = plan
        mode = os.environ.get("REALY_KAFKA_TOPICS", "manage").lower()
        if missing:
            c.add("topics", "fail" if mode == "verify" else "warn",
                  f"missing: {', '.join(missing)}" + (" (the run creates them)" if mode != "verify" else ""),
                  "create them (contracts/kafka/topics.yaml) or set REALY_KAFKA_TOPICS=manage" if mode == "verify"
                  else "on a managed cluster without create rights, create them and set REALY_KAFKA_TOPICS=verify")
        else:
            c.add("topics", "pass", f"all {len(plan)} contract topics present")
        from .bindings import pipeline
        ev = next((p for p in plan if p["topic"] == pipeline().topics["events"]), None)
        if ev and ev["exists"]:
            c.add("partitions", "pass" if ev["partitions"] >= 2 else "warn",
                  f"{ev['topic']}: {ev['partitions']} partitions (the most gateway consumers / Flink source tasks that "
                  "can share it)", "" if ev["partitions"] >= 2 else "use as many partitions as consumers you will run")
        rf = min((len(t.partitions[0].replicas) for n, t in md.topics.items() if n in {p["topic"] for p in plan}
                  and t.partitions), default=0)
        if rf:
            c.out["facts"]["min_replication"] = rf
            c.add("replication", "pass" if rf >= 3 or c.out["implementation"] == "bundled" else "warn",
                  f"replication factor {rf}", "" if rf >= 3 else "production topics need replication 3 (acks=all "
                  "with min.insync.replicas 2) to survive a broker")
    return _guard(c, run, "cannot reach the brokers", "check KAFKA_BOOTSTRAP, network policy and the security settings")


# ------------------------------------------------------------------------------------------- Redis
def check_redis() -> dict[str, Any]:
    from .common import redis_client
    u = redis_client.uri()
    c = _C("redis", "Redis", f"{urlparse(u).scheme}://{urlparse(u).hostname}:{urlparse(u).port or 6379}")

    def run(c: _C) -> None:
        r = redis_client.client(u, socket_timeout=TIMEOUT, socket_connect_timeout=TIMEOUT)
        mode = redis_client.mode(u)
        lat = []
        for _ in range(30):
            t0 = time.perf_counter()
            r.ping()
            lat.append((time.perf_counter() - t0) * 1000)
        info = r.info("server") if mode == "standalone" else next(iter(r.info("server").values()), {})
        mem = r.info("memory") if mode == "standalone" else next(iter(r.info("memory").values()), {})
        c.out["version"] = f"{info.get('redis_version', '?')} {mode}"
        p99 = sorted(lat)[int(len(lat) * 0.99) - 1]
        c.out["facts"].update({"mode": mode, "tls": u.startswith("rediss://"), "ping_p50_ms": round(statistics.median(lat), 3),
                               "ping_p99_ms": round(p99, 3), "maxmemory_policy": mem.get("maxmemory_policy"),
                               "used_memory_human": mem.get("used_memory_human")})
        c.add("reachable", "pass", f"{mode}, ping p50 {statistics.median(lat):.2f} ms / p99 {p99:.2f} ms")
        if p99 > 5:
            c.add("latency", "warn", f"ping p99 {p99:.1f} ms from here: feature lookups pay at least this per event",
                  "put the store in the scorer's zone / network, or budget for it (scoring.feature_deadline_ms)")
        pol = mem.get("maxmemory_policy") or ""
        if pol.startswith("allkeys"):
            c.add("eviction", "warn", f"maxmemory-policy {pol} can evict profiles and state blobs silently",
                  "use noeviction (and size the memory) or volatile-* so only TTL'd keys are evicted")
        if c.out["implementation"] == "external" and not u.startswith("rediss://"):
            c.add("security", "warn", "an external store reached without TLS", "use a rediss:// URI (REDIS_CA_FILE for a private CA)")
    return _guard(c, run, "cannot reach Redis", "check REDIS_URI (rediss:// for TLS, user:password@ for auth) and REDIS_MODE")


# -------------------------------------------------------------------------------------- ClickHouse
def check_clickhouse() -> dict[str, Any]:
    from .common import clickhouse as chc
    c = _C("clickhouse", "ClickHouse", chc.url())

    def run(c: _C) -> None:
        r = chc.get({"query": "SELECT version()"}, timeout=TIMEOUT)
        if r.status_code in (401, 403) or "AUTHENTICATION_FAILED" in r.text:
            c.add("reachable", "fail", "authentication failed", "set CLICKHOUSE_USER and CLICKHOUSE_PASSWORD(_FILE)")
            return
        r.raise_for_status()
        c.out["version"] = r.text.strip()
        n = int(chc.get({"query": "SELECT count() FROM system.tables WHERE database = 'realy'"}, timeout=TIMEOUT).text.strip() or 0)
        c.out["facts"]["realy_tables"] = n
        c.add("reachable", "pass", f"ClickHouse {c.out['version']}")
        c.add("schema", "pass" if n > 0 else "fail", f"{n} tables in database realy",
              "" if n > 0 else "apply the DDL (make migrate, or contracts/storage/clickhouse/*.sql)")
    return _guard(c, run, "cannot reach ClickHouse", "check CLICKHOUSE_URL (https for TLS) and the credentials")


# ------------------------------------------------------------------------------------------- Flink
def check_flink() -> dict[str, Any]:
    import httpx
    base = os.environ.get("FLINK_REST", "http://localhost:8081").rstrip("/")
    c = _C("flink", "Flink", base)

    def run(c: _C) -> None:
        o = httpx.get(base + "/overview", timeout=TIMEOUT).json()
        c.out["version"] = o.get("flink-version")
        par = int(os.environ.get("FLINK_PARALLELISM", "2"))
        slots, tms = int(o.get("slots-total", 0)), int(o.get("taskmanagers", 0))
        c.out["facts"].update({"taskmanagers": tms, "slots_total": slots, "slots_available": o.get("slots-available"),
                               "parallelism": par})
        c.add("reachable", "pass", f"Flink {o.get('flink-version')}, {tms} TaskManager(s), {slots} slots")
        if slots < par:
            c.add("slots", "fail", f"{slots} slots for parallelism {par}", "add TaskManagers / slots, or lower FLINK_PARALLELISM")
        try:
            cfg = {e["key"]: e["value"] for e in httpx.get(base + "/jobmanager/config", timeout=TIMEOUT).json()}
            ck = cfg.get("state.checkpoints.dir") or cfg.get("execution.checkpointing.dir") or ""
            c.out["facts"]["checkpoints"] = ck
            if ck.startswith("file:") and c.out["implementation"] == "external":
                c.add("checkpoints", "warn", f"checkpoints on a local path ({ck})",
                      "use durable shared storage (s3://, gs://, hdfs://) so a lost TaskManager recovers")
        except Exception:  # noqa: BLE001 - optional detail
            pass
    return _guard(c, run, "cannot reach the Flink REST API", "check FLINK_REST")


# ---------------------------------------------------------------------------- gateway + model
def _schema_sha() -> str:
    from .features.schema import SCHEMA_PATH
    p = Path(os.environ.get("FEATURE_SCHEMA") or SCHEMA_PATH)
    return hashlib.sha256(p.read_text().encode()).hexdigest()[:12]


def check_gateway_and_model(scorer: str) -> list[dict[str, Any]]:
    import httpx
    base = os.environ.get("GATEWAY_ADMIN", "http://localhost:8080").rstrip("/")
    g = _C("gateway", "Scoring gateway", base)
    m = _C("model", "Model server", "")
    info: dict[str, Any] = {}

    def run(c: _C) -> None:
        nonlocal info
        r = httpx.get(base + "/admin/info", timeout=TIMEOUT)
        if r.status_code == 404:
            c.add("reachable", "warn", "this gateway predates /admin/info: its dependencies are not reported",
                  "deploy the current gateway image")
            return
        info = r.json()
        c.out["version"] = info.get("version")
        c.out["facts"] = {k: info.get(k) for k in ("pipeline", "schema", "extensions", "kafka", "redis", "audit", "simulated")}
        c.add("reachable", "pass", f"pipeline {info.get('pipeline')}, {info.get('schema', {}).get('features')} features")
        from .bindings import pipeline
        if info.get("pipeline") != pipeline().name:
            c.add("pipeline", "fail", f"the gateway runs pipeline {info.get('pipeline')}, this run {pipeline().name}",
                  "set PIPELINE on the gateway to the run's pipeline")
        sha = (info.get("schema") or {}).get("sha256")
        if sha and sha != _schema_sha():
            c.add("schema", "fail", "the gateway's feature schema differs from the runner's",
                  "deploy the same contracts to the gateway (image or FEATURE_SCHEMA) as the runner uses")
        ext = (info.get("extensions") or {}).get("aggregations")
        if ext is not None:
            from .features import aggregations as _aggs
            missing = sorted(set(ext) - set(_aggs.names()))
            if missing:
                c.add("extensions", "fail", f"the gateway runs plugin aggregations without a Python twin here: "
                      f"{', '.join(missing)} (verify cannot recompute their features)",
                      "install the twin module and set REALY_AGGREGATION_PLUGINS (docs/dev/extending.md)")
            elif ext:
                c.add("extensions", "pass", f"plugin aggregations {', '.join(ext)}, each with its Python twin")
        red = info.get("redis") or {}
        if red.get("mode") == "mock":
            c.add("store", "warn", "the gateway uses the mock feature store (scoring.feature_source: mock)",
                  "remove feature_source: mock for a real store")
    gw = _guard(g, run, "cannot reach the gateway admin API", "check GATEWAY_ADMIN")
    if gw["implementation"] == "bundled" and (info.get("redis") or {}).get("endpoint"):
        gw["facts"]["store_endpoint"] = info["redis"]["endpoint"]
    md = info.get("model") or {}
    if scorer == "echo":
        m.add("model", "skip", "echo: no model call")
        return [gw, m.done()]
    if not md:
        m.out["implementation"] = "unknown"
        m.add("reachable", "warn", "not reported (the gateway did not answer /admin/info)", "")
        return [gw, m.done()]
    m.out["endpoint"] = md.get("target") or ""
    m.out["implementation"] = "mock" if md.get("mock") else _impl(md.get("target") or "")
    m.out["version"] = f"{md.get('platform') or '?'} {','.join(md.get('versions') or [])}".strip()
    m.out["facts"] = md
    if md.get("metadata_error"):
        m.add("metadata", "fail", f"the gateway could not read the model's metadata: {md['metadata_error']}",
              "check MODEL_TARGET / MODEL_NAME on the gateway, and MODEL_TLS / MODEL_AUTH_* for a secured endpoint")
    else:
        m.add("metadata", "pass", f"{md.get('name')} on {md.get('platform')}, input {md.get('input')} "
              f"[{md.get('served_input_width')}] -> {md.get('output')}")
        sw, rw = md.get("schema_width"), md.get("served_input_width")
        if rw and sw and rw != sw:
            m.add("width", "warn", f"the model reads {rw} inputs, the schema has {sw}: "
                  + ("the first features are sent" if rw < sw else "the extra inputs are zero"),
                  "retrain on the current schema, or this is intended (a model trained before features were added)")
        if not md.get("mock") and m.out["implementation"] == "external" and not md.get("tls"):
            m.add("security", "warn", "an external model server reached without TLS", "set MODEL_TLS=true (MODEL_CA_FILE)")
    return [gw, m.done()]


# ------------------------------------------------------------------------------------------- all
def _dist(d: Any) -> str:
    """A latency distribution (scenario LatencyDist) in words: 'lognormal, median 8 ms, p99 25 ms'."""
    if not isinstance(d, dict) or not d:
        return str(d)
    kind, p = next(iter(d.items()))
    if not isinstance(p, dict):
        return f"{kind} {p}"
    parts = []
    for k, v in p.items():
        name = "" if k == "ms" else k.removesuffix("_ms").replace("_", " ") + " "
        parts.append(f"{name}{v:g} ms" if (k == "ms" or k.endswith("_ms")) and isinstance(v, (int, float)) else f"{name}{v}")
    return f"{kind}, " + ", ".join(parts)


def simulated(scenario: Any, model_mock: bool | None) -> list[dict[str, str]]:
    """What in this run is simulated rather than measured on real infrastructure."""
    out = []
    sc = scenario
    if sc is None:
        return out
    from .topologies import runtime
    rt = runtime(sc.topology)
    if rt.echo:
        out.append({"what": "decisions", "detail": "echo: the gateway answers without features, model or rules"})
    if model_mock and not rt.echo:
        st = (sc.model_server or {}).get("service_time")
        out.append({"what": "model latency", "detail": "the mock model server: inference time is drawn from "
                    + (f"model_server.service_time ({_dist(st)})" if st else "its default service time") + ", not computed"})
    scoring = sc.scoring or {}
    if scoring.get("feature_source") == "mock":
        out.append({"what": "features", "detail": "scoring.feature_source: mock (no online store)"})
    for k, what in (("feature_lookup_delay", "feature-lookup latency"), ("model_extra_delay", "model-call latency")):
        if scoring.get(k):
            out.append({"what": what, "detail": f"scoring.{k} adds injected latency ({_dist(scoring[k])})"})
    return out


def check(scenario: Any = None, topology: str | None = None) -> dict[str, Any]:
    """Probe every component the topology uses (in parallel). `scenario`: a Scenario (its topology and
    simulated settings), or just `topology`."""
    from .topologies import runtime
    topo = topology or (scenario.topology if scenario is not None else "gateway-kafka")
    rt = runtime(topo)
    jobs: list[Callable[[], Any]] = [check_kafka, check_clickhouse]
    if rt.uses_flink or rt.scorer == "gateway":
        jobs.append(check_redis)
    if rt.uses_flink:
        jobs.append(check_flink)
    if rt.scorer in ("gateway", "echo"):
        jobs.append(lambda: check_gateway_and_model(rt.scorer))
    with ThreadPoolExecutor(max_workers=len(jobs)) as ex:
        results = list(ex.map(lambda f: f(), jobs))
    comps: list[dict[str, Any]] = []
    for r in results:
        comps += r if isinstance(r, list) else [r]
    model = next((c for c in comps if c["component"] == "model"), None)
    model_mock = None if model is None else model["implementation"] == "mock"
    status = max((c["status"] for c in comps), key=lambda s: RANK[s], default="pass")
    return {"checked_at": time.time(), "topology": topo, "status": status, "components": comps,
            "simulated": simulated(scenario, model_mock),
            "summary": {c["component"]: {"implementation": c["implementation"], "version": c["version"],
                                         "endpoint": c["endpoint"], "status": c["status"]} for c in comps}}


def blockers(result: dict[str, Any]) -> list[str]:
    """The failed checks that make a run meaningless (it would measure or verify the wrong thing)."""
    return [f"{c['label']}: {k['detail']}" + (f" ({k['fix']})" if k.get("fix") else "")
            for c in result.get("components", []) for k in c["checks"] if k["status"] == "fail"]


def render(result: dict[str, Any]) -> str:
    sym = {"pass": "ok  ", "warn": "WARN", "fail": "FAIL", "skip": "--  "}
    lines = [f"preflight ({result['topology']}): {result['status'].upper()}"]
    for c in result["components"]:
        lines.append(f"  {c['label']:<16} {c['implementation']:<9} {c.get('version') or '':<28} {c.get('endpoint') or ''}")
        for k in c["checks"]:
            lines.append(f"    {sym[k['status']]} {k['name']:<12} {k['detail']}" + (f"\n         fix: {k['fix']}" if k.get("fix") and k["status"] != "pass" else ""))
    if result.get("simulated"):
        lines.append("  simulated in this run:")
        for s in result["simulated"]:
            lines.append(f"    - {s['what']}: {s['detail']}")
    return "\n".join(lines)
