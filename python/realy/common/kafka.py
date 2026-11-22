"""Kafka helpers: latency-tuned client configs (contracts/kafka/topics.yaml) and topic admin."""
from __future__ import annotations

import os
from typing import Any

import yaml
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from ..scenario import REPO_ROOT

TOPICS_PATH = REPO_ROOT / "contracts" / "kafka" / "topics.yaml"
EVENTS = "payments.events.raw"
DECISIONS = "payments.decisions"
LABELS = "payments.labels"
CONTROL = "bench.control"
METRICS = "bench.metrics"


def bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


# Kafka security for a cluster you bring (docs/dev/integration.md): librdkafka settings from the
# environment, applied to every client this package creates. The Java services read the same variables
# (io.realy.common.KafkaProps). Unset -> PLAINTEXT, as in the compose stack.
SECURITY_ENV = {
    "KAFKA_SECURITY_PROTOCOL": "security.protocol",      # PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
    "KAFKA_SASL_MECHANISM": "sasl.mechanism",            # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | OAUTHBEARER
    "KAFKA_SASL_USERNAME": "sasl.username",
    "KAFKA_SASL_PASSWORD": "sasl.password",
    "KAFKA_SSL_CA_LOCATION": "ssl.ca.location",
    "KAFKA_SSL_CERT_LOCATION": "ssl.certificate.location",
    "KAFKA_SSL_KEY_LOCATION": "ssl.key.location",
    "KAFKA_SSL_KEY_PASSWORD": "ssl.key.password",
    "KAFKA_SSL_ENDPOINT_IDENTIFICATION": "ssl.endpoint.identification.algorithm",
}


def security_config(env: dict[str, str] | None = None) -> dict[str, str]:
    e = os.environ if env is None else env
    return {key: e[var] for var, key in SECURITY_ENV.items() if e.get(var)}


def producer_conf(**over: Any) -> dict[str, Any]:
    conf = {
        "bootstrap.servers": bootstrap(),
        "linger.ms": 1,
        "batch.num.messages": 10000,
        "batch.size": 131072,
        "queue.buffering.max.messages": 2_000_000,
        "queue.buffering.max.kbytes": 1_048_576,
        "compression.type": "lz4",
        "acks": "all",
        "enable.idempotence": True,
        # Java-compatible partitioning so the same customer lands on the same partition whether
        # the event was produced by this emitter or mirrored by the (Java) gateway.
        "partitioner": "murmur2_random",
        "statistics.interval.ms": 0,
        **security_config(),
    }
    conf.update(over)
    return conf


def consumer_conf(group: str, **over: Any) -> dict[str, Any]:
    conf = {
        "bootstrap.servers": bootstrap(),
        "group.id": group,
        "enable.auto.commit": False,
        "auto.offset.reset": "latest",
        "fetch.wait.max.ms": 100,  # min.bytes=1 => broker answers on append; no latency cost
        "fetch.min.bytes": 1,
        "queued.min.messages": 20000,
        "enable.partition.eof": False,
        **security_config(),
    }
    conf.update(over)
    return conf


def make_producer(**over: Any) -> Producer:
    return Producer(producer_conf(**over))


def make_consumer(group: str, **over: Any) -> Consumer:
    return Consumer(consumer_conf(group, **over))


class TopicsNotReady(RuntimeError):
    """The cluster lacks topics (or partitions) the pipeline needs and may not create them."""


def topic_plan(md: Any, spec: dict[str, Any], partitions_override: int | None = None) -> list[dict[str, Any]]:
    """Per contract topic: whether it exists, its partitions, and what the contract asks for."""
    out = []
    for name, t in spec["topics"].items():
        want = int(t["partitions"])
        if partitions_override and want > 6:
            want = partitions_override
        elif len(md.brokers) == 1 and want > 12:
            want = 12
        have = md.topics.get(name)
        out.append({"topic": name, "exists": have is not None,
                    "partitions": len(have.partitions) if have is not None else 0, "wanted": want,
                    "min_partitions": int(t.get("min_partitions", 1))})
    return out


def ensure_topics(replication: int | None = None, partitions_override: int | None = None) -> list[str]:
    """Make sure the contract topics exist. REALY_KAFKA_TOPICS = manage (default: create the missing ones)
    | verify (a managed cluster the benchmark may not change: check they exist, with at least their
    `min_partitions`, and say what to create otherwise). Single-broker stacks pass replication=1."""
    spec = yaml.safe_load(TOPICS_PATH.read_text())
    admin = AdminClient({"bootstrap.servers": bootstrap(), **security_config()})
    md = admin.list_topics(timeout=10)
    if os.environ.get("REALY_KAFKA_TOPICS", "manage").lower() == "verify":
        plan = topic_plan(md, spec, partitions_override)
        bad = [p for p in plan if not p["exists"] or p["partitions"] < p["min_partitions"]]
        if bad:
            raise TopicsNotReady("REALY_KAFKA_TOPICS=verify and the cluster is not ready: " + "; ".join(
                (f"create {p['topic']} ({p['wanted']} partitions)" if not p["exists"] else
                 f"{p['topic']} has {p['partitions']} partitions, needs at least {p['min_partitions']}") for p in bad)
                + " (contracts/kafka/topics.yaml has their settings)")
        return []
    existing = set(md.topics)
    rf = replication or int(os.environ.get("KAFKA_REPLICATION", spec["defaults"]["replication_factor"]))
    rf = max(1, min(rf, len(md.brokers)))  # dev stacks run a single broker
    new = []
    for name, t in spec["topics"].items():
        if name in existing:
            continue
        cfg = {str(k): str(v) for k, v in (t.get("config") or {}).items()}
        if "retention_ms" in t:
            cfg["retention.ms"] = str(t["retention_ms"])
        if rf > 1:
            cfg["min.insync.replicas"] = str(min(rf - 1, spec["defaults"]["min_insync_replicas"]))
        parts = int(t["partitions"])
        if partitions_override and parts > 6:
            parts = partitions_override
        elif len(md.brokers) == 1 and parts > 12:
            parts = 12  # single-broker dev stack (matches deploy/compose/kafka/create-topics.sh)
        new.append(NewTopic(name, num_partitions=parts, replication_factor=rf, config=cfg))
    if new:
        for name, fut in admin.create_topics(new).items():
            try:
                fut.result()
            except Exception as e:  # noqa: BLE001 - already exists races are fine
                if "TOPIC_ALREADY_EXISTS" in str(e):
                    continue
                if "AUTHORIZATION" in str(e).upper():
                    raise TopicsNotReady(
                        f"may not create {name} on this cluster ({e}). Create the topics yourself "
                        "(contracts/kafka/topics.yaml) and set REALY_KAFKA_TOPICS=verify") from e
                raise
    return [t.topic for t in new]


_LAG_LOCK = __import__("threading").Lock()
_LAG_ADMIN: dict[str, Any] = {}


def group_lag(group: str, topic: str, since_ms: int | None = None) -> int | None:
    """Total committed-offset lag of a consumer group on a topic (None if the group has no offsets).
    A partition with no committed offset yet counts from `since_ms` (e.g. when the consumer started)
    when given, else from the earliest retained offset; without it, a new group's lag would include
    every older event the topic still retains.

    Called every few seconds by the runner and the console: one admin client per process is reused,
    calls are serialised, and the probe consumer is always closed (librdkafka handles left for the
    garbage collector to destroy on an arbitrary thread are a known way to crash the process)."""
    with _LAG_LOCK:
        return _group_lag(group, topic, since_ms)


def _group_lag(group: str, topic: str, since_ms: int | None) -> int | None:
    from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
    bs = bootstrap()
    admin = _LAG_ADMIN.get(bs)
    if admin is None:
        admin = _LAG_ADMIN[bs] = AdminClient({"bootstrap.servers": bs, **security_config()})
    md = admin.list_topics(topic, timeout=10).topics[topic]
    parts = [TopicPartition(topic, p) for p in md.partitions]
    res = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group, parts)])[group].result()
    committed = {tp.partition: tp.offset for tp in res.topic_partitions}
    if all(o < 0 for o in committed.values()):
        return None
    c = Consumer({"bootstrap.servers": bs, "group.id": f"lagprobe-{group}", **security_config()})
    try:
        base: dict[int, int] = {}
        if since_ms is not None and any(committed.get(p, -1) < 0 for p in md.partitions):
            try:
                for tp in c.offsets_for_times([TopicPartition(topic, p, int(since_ms)) for p in md.partitions], timeout=10):
                    base[tp.partition] = tp.offset
            except Exception:  # noqa: BLE001 - fall back to the earliest offset
                base = {}
        lag = 0
        for p in md.partitions:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            off = committed.get(p, -1)
            if off < 0:
                b0 = base.get(p, -1)
                off = b0 if b0 >= 0 else (hi if p in base else lo)   # offsets_for_times -1: nothing after since_ms
            lag += max(0, hi - off)
        return lag
    finally:
        c.close()


def wait_group_caught_up(group: str, topic: str, max_lag: int, timeout_s: float, log=print,
                         no_offsets_grace_s: float | None = None, poll_s: float = 5.0, stop=None) -> int:
    """Wait until `group`'s lag on `topic` is <= max_lag. Returns the lag, or -1 when it never got a
    numeric lag: on timeout, or after `no_offsets_grace_s` with the group still having no committed
    offsets (lets callers fall back to another signal instead of waiting out the whole timeout).
    `stop()` returning True ends the wait early (returns -2): an aborted run must not sit here."""
    import time
    t0 = time.time()
    end = t0 + timeout_s
    last: object = object()   # sentinel: always log the first poll, even when the group has no offsets yet
    while time.time() < end:
        if stop is not None and stop():
            return -2
        lag = group_lag(group, topic)
        if lag is not None and lag <= max_lag:
            return lag
        if lag is None and no_offsets_grace_s is not None and time.time() - t0 >= no_offsets_grace_s:
            log(f"  {group}: no committed offsets after {no_offsets_grace_s:.0f} s")
            return -1
        if lag != last:
            log(f"  waiting for {group}: lag {lag if lag is not None else 'n/a (no committed offsets)'}")
            last = lag
        t_next = time.time() + poll_s
        while time.time() < t_next:
            if stop is not None and stop():
                return -2
            time.sleep(min(0.25, poll_s))
    return last if isinstance(last, int) else -1
