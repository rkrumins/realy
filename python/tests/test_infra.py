"""Real infrastructure: the preflight's verdicts, the managed-Kafka topic check, Redis mode selection and
ClickHouse credentials."""
from __future__ import annotations

from types import SimpleNamespace as NS

import pytest

from realy import infra
from realy.common import clickhouse, redis_client
from realy.common import kafka as kafka_mod
from realy.scenario import Scenario


def test_implementation_is_told_from_the_endpoint():
    assert infra._impl("kafka:29092") == "bundled"
    assert infra._impl("redis://redis:6379") == "bundled"
    assert infra._impl("realy-kafka-bootstrap.prod.svc:9092") == "bundled"
    assert infra._impl("b-1.msk.eu-west-1.amazonaws.com:9096,b-2.msk:9096") == "external"
    assert infra._impl("rediss://clustercfg.fraud.cache.amazonaws.com:6379") == "external"


def test_simulated_parts_are_named():
    sc = Scenario(name="t", topology="gateway-kafka", model_server={"service_time": {"fixed": {"ms": 8}}},
                  scoring={"feature_lookup_delay": {"lognormal": {"median_ms": 2, "p99_ms": 8}}})
    what = {s["what"] for s in infra.simulated(sc, model_mock=True)}
    assert what == {"model latency", "feature-lookup latency"}
    assert {s["what"] for s in infra.simulated(sc, model_mock=False)} == {"feature-lookup latency"}
    echo = Scenario(name="e", topology="echo-kafka")
    assert [s["what"] for s in infra.simulated(echo, model_mock=True)] == ["decisions"]


def test_blockers_are_the_failed_checks_with_their_fix():
    res = {"components": [{"label": "Kafka", "checks": [{"name": "topics", "status": "fail", "detail": "missing: x",
                                                           "fix": "create them"}]},
                          {"label": "Redis", "checks": [{"name": "eviction", "status": "warn", "detail": "d", "fix": ""}]}]}
    assert infra.blockers(res) == ["Kafka: missing: x (create them)"]
    c = infra._C("redis", "Redis", "redis://r:6379")
    infra._guard(c, lambda c: (_ for _ in ()).throw(ConnectionError("refused")), "cannot reach Redis", "check REDIS_URI")
    assert c.out["status"] == "fail" and "refused" in c.out["checks"][0]["detail"]


def _md(topics: dict[str, int], brokers: int = 3):
    return NS(brokers={i: None for i in range(brokers)},
              topics={n: NS(partitions={i: NS(replicas=[1, 2, 3]) for i in range(p)}) for n, p in topics.items()})


def test_verify_mode_never_creates_topics_and_says_what_is_missing(monkeypatch):
    spec = {"topics": {"payments.events.raw": {"partitions": 48, "min_partitions": 12},
                       "payments.decisions": {"partitions": 48}}}
    plan = kafka_mod.topic_plan(_md({"payments.events.raw": 6}), spec)
    by = {p["topic"]: p for p in plan}
    assert by["payments.events.raw"]["partitions"] == 6 and not by["payments.decisions"]["exists"]

    class Admin:
        def __init__(self, *_a, **_k):
            pass

        def list_topics(self, timeout=None):
            return _md({"payments.events.raw": 6})

        def create_topics(self, *_a, **_k):
            raise AssertionError("verify mode must not create topics")

    monkeypatch.setattr(kafka_mod, "AdminClient", Admin)
    monkeypatch.setattr(kafka_mod.yaml, "safe_load", lambda _t: spec)
    monkeypatch.setenv("REALY_KAFKA_TOPICS", "verify")
    with pytest.raises(kafka_mod.TopicsNotReady) as e:
        kafka_mod.ensure_topics()
    msg = str(e.value)
    assert "create payments.decisions (48 partitions)" in msg and "has 6 partitions, needs at least 12" in msg


def test_redis_mode_can_be_forced_and_defaults_to_asking(monkeypatch):
    redis_client._detect.cache_clear()
    monkeypatch.setenv("REDIS_MODE", "cluster")
    assert redis_client.mode("redis://nowhere:1") == "cluster"
    redis_client._detect.cache_clear()
    monkeypatch.setenv("REDIS_MODE", "auto")
    assert redis_client.mode("redis://127.0.0.1:1") == "standalone"     # unreachable: standalone, caller errors
    redis_client._detect.cache_clear()


def test_clickhouse_credentials_travel_in_headers(monkeypatch, tmp_path):
    monkeypatch.delenv("CLICKHOUSE_USER", raising=False)
    assert clickhouse.headers() == {}
    import secrets
    key = secrets.token_hex(8)                 # drawn per run: no credential-shaped literal in the repository
    pw = tmp_path / "pw"
    pw.write_text(key + "\n")
    monkeypatch.setenv("CLICKHOUSE_USER", "bench")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD_FILE", str(pw))
    assert clickhouse.headers() == {"X-ClickHouse-User": "bench", "X-ClickHouse-Key": key}
    monkeypatch.setenv("CLICKHOUSE_URL", "https://ch.example.com:8443/")
    assert clickhouse.url() == "https://ch.example.com:8443"


def test_cluster_info_shapes_are_read_from_one_node():
    """redis-py's cluster client answers INFO from one node (a flat dict) or several ({node: dict})."""
    from realy.infra import _one_node
    flat = {"redis_version": "8.8.4", "maxmemory_policy": "noeviction"}
    assert _one_node(flat) == flat
    assert _one_node({"redis-c1:6379": flat, "redis-c2:6379": {"redis_version": "8.8.4"}}) == flat
    assert _one_node(None) == {} and _one_node("x") == {}


def test_clickhouse_kafka_engine_errors_fail_the_preflight():
    """ClickHouse consumes with its own client: a recent error after its last commit means it is not ingesting."""
    from realy.infra import _C, _check_kafka_engine

    class R:
        def __init__(self, rows):
            self.rows = rows

        def json(self):
            return {"data": self.rows}

    class CH:
        def __init__(self, rows):
            self.rows = rows

        def get(self, params, timeout):
            return R(self.rows)

    failing = [{"table": "decisions_queue", "n": 0, "committed": "1970-01-01 00:00:00", "t": "2026-11-23 00:10:00",
                "err_at": "2026-11-23 00:09:50", "err": "SASL PLAIN mechanism handshake failed"}]
    c = _C("clickhouse", "ClickHouse", "http://clickhouse:8123")
    _check_kafka_engine(c, CH(failing))
    assert c.out["checks"][0]["status"] == "fail" and "decisions_queue" in c.out["checks"][0]["detail"]
    healthy = [dict(failing[0], committed="2026-11-23 00:09:55")]          # committed after the error
    c2 = _C("clickhouse", "ClickHouse", "http://clickhouse:8123")
    _check_kafka_engine(c2, CH(healthy))
    assert c2.out["checks"][0]["status"] == "pass"
