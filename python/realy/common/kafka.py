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
    }
    conf.update(over)
    return conf


def make_producer(**over: Any) -> Producer:
    return Producer(producer_conf(**over))


def make_consumer(group: str, **over: Any) -> Consumer:
    return Consumer(consumer_conf(group, **over))


def ensure_topics(replication: int | None = None, partitions_override: int | None = None) -> list[str]:
    """Create contract topics if missing. Single-broker stacks pass replication=1."""
    spec = yaml.safe_load(TOPICS_PATH.read_text())
    admin = AdminClient({"bootstrap.servers": bootstrap()})
    md = admin.list_topics(timeout=10)
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
                if "TOPIC_ALREADY_EXISTS" not in str(e):
                    raise
    return [t.topic for t in new]


def group_lag(group: str, topic: str) -> int | None:
    """Total committed-offset lag of a consumer group on a topic (None if the group has no offsets)."""
    from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
    admin = AdminClient({"bootstrap.servers": bootstrap()})
    md = admin.list_topics(topic, timeout=10).topics[topic]
    parts = [TopicPartition(topic, p) for p in md.partitions]
    res = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group, parts)])[group].result()
    committed = {tp.partition: tp.offset for tp in res.topic_partitions}
    if all(o < 0 for o in committed.values()):
        return None
    c = Consumer({"bootstrap.servers": bootstrap(), "group.id": f"lagprobe-{group}"})
    lag = 0
    for p in md.partitions:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        off = committed.get(p, -1)
        lag += hi - (off if off >= 0 else lo)
    c.close()
    return lag


def wait_group_caught_up(group: str, topic: str, max_lag: int, timeout_s: float, log=print) -> int:
    import time
    end = time.time() + timeout_s
    last = None
    while time.time() < end:
        lag = group_lag(group, topic)
        if lag is not None and lag <= max_lag:
            return lag
        if lag != last:
            log(f"  waiting for {group}: lag {lag if lag is not None else 'n/a'}")
            last = lag
        time.sleep(5)
    return last if last is not None else -1
