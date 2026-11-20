"""Live pipeline topology map: nodes/edges per topology, each annotated with the bench.metrics source
that feeds it, so the UI can paint events/s and p99 on every hop from the 1 s snapshots it already
receives. A metric ref is {"source": <emitter|collector|gateway|model|flink>, "counter": name} (rate =
sum of that counter across instances) or {"source", "stage"} (rate = stage sample count, p99 = max p99)."""
from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, HTTPException

from ..features import load_schema

router = APIRouter(tags=["topology"])
TOPOLOGIES = ("gateway-kafka", "gateway-grpc", "flink-inline", "echo-kafka")


def _c(source: str, counter: str) -> dict[str, str]:
    return {"source": source, "counter": counter}


def _s(source: str, stage: str) -> dict[str, str]:
    return {"source": source, "stage": stage}


# Freshness of the online store (Flink pickup -> blob durable in Redis) is not a hot-path latency: it
# decides how stale the gateway's features can be. It gets its own SLO (warn at 25%, hot at 100%)
# instead of a share of the e2e budget, which painted every replay red.
FRESHNESS_SLO_MS = float(os.environ.get("REALY_FRESHNESS_SLO_MS", "1000"))
SLO_STAGES = {"store_update_lag": FRESHNESS_SLO_MS}


def node(id_: str, label: str, kind: str, col: int, row: int, rate=None, p99=None, sub: str | None = None) -> dict:
    out = {"id": id_, "label": label, "kind": kind, "column": col, "row": row, "rate": rate, "p99": p99,
           "subtitle": sub}
    if p99 and p99.get("stage") in SLO_STAGES:
        out["slo_ms"] = SLO_STAGES[p99["stage"]]
    return out


def edge(a: str, b: str, label: str = "", rate=None, p99=None, bulk=None) -> dict:
    out = {"from": a, "to": b, "label": label, "rate": rate, "p99": p99}
    if bulk:
        out["bulk"] = bulk    # history replay rate on this hop (shown while warming, never judged)
    return out


def build(topology: str) -> dict[str, Any]:
    if topology not in TOPOLOGIES:
        raise HTTPException(400, f"topology must be one of {TOPOLOGIES}")
    emitted = _c("emitter", "emitted")
    decided = _c("collector", "decided")
    n: list[dict] = []
    e: list[dict] = []
    if topology == "flink-inline":
        ents = load_schema().stream_entities()
        n += [node("emitter", "Emitter", "driver", 0, 1, emitted, _s("emitter", "schedule_lag"), "open-loop, paced"),
              node("events", "events.raw", "kafka", 1, 1, emitted, _s("emitter", "produce_ack"), "Kafka topic")]
        for i, ent in enumerate(ents):
            n.append(node(f"flink-{ent}", f"keyBy {ent}", "flink", 2 + i // 4, i % 4,
                          _s("flink", f"state_{ent}"), _s("flink", f"state_{ent}"), "RocksDB state"))
        col = 2 + (len(ents) - 1) // 4 + 1
        n += [node("score", "Async scoring", "flink", col, 1, _c("flink", "decisions"), _s("flink", "hot_path"),
                   "orchestrator"),
              node("redis", "Redis", "store", col, 0, _s("collector", "feature_lookup"), _s("collector", "feature_lookup"),
                   "profiles"),
              node("model", "Model server", "model", col, 2, _c("model", "requests"), _s("collector", "model_call"),
                   "KServe v2"),
              node("decisions", "decisions", "kafka", col + 1, 1, decided, _s("collector", "egress"), "audit WAL"),
              node("collector", "Collector", "driver", col + 2, 1, decided, _s("collector", "e2e"), "end to end"),
              node("clickhouse", "ClickHouse", "sink", col + 2, 2, None, None, "async audit")]
        e.append(edge("emitter", "events", "produce", emitted))
        prev = "events"
        for ent in ents:
            e.append(edge(prev, f"flink-{ent}", "keyBy", _s("flink", f"shuffle_{ent}"), _s("flink", f"shuffle_{ent}")))
            prev = f"flink-{ent}"
        e += [edge(prev, "score", "async I/O"), edge("score", "redis", "lookup"), edge("score", "model", "infer"),
              edge("score", "decisions", "sink", decided), edge("decisions", "collector", "consume", decided),
              edge("decisions", "clickhouse", "Kafka engine")]
    elif topology == "echo-kafka":
        n += [node("emitter", "Emitter", "driver", 0, 0, emitted, _s("emitter", "schedule_lag"), "open-loop, paced"),
              node("events", "events.raw", "kafka", 1, 0, emitted, _s("emitter", "produce_ack"), "Kafka topic"),
              node("gateway", "Gateway (echo)", "service", 2, 0, _c("gateway", "decisions"), _s("collector", "hot_path"),
                   "answers immediately"),
              node("decisions", "decisions", "kafka", 3, 0, decided, _s("collector", "egress"), "audit WAL"),
              node("collector", "Collector", "driver", 4, 0, decided, _s("collector", "e2e"), "instrument floor")]
        e += [edge("emitter", "events", "produce", emitted), edge("events", "gateway", "consume", _s("collector", "ingress"),
                                                                    _s("collector", "ingress")),
              edge("gateway", "decisions", "audit", decided), edge("decisions", "collector", "consume", decided)]
    else:
        sync = topology == "gateway-grpc"
        n += [node("emitter", "gRPC emitter" if sync else "Emitter", "driver", 0, 1, emitted,
                   _s("emitter", "e2e" if sync else "schedule_lag"), "open-loop, Java" if sync else "open-loop, paced")]
        if not sync:
            n.append(node("events", "events.raw", "kafka", 1, 1, emitted, _s("emitter", "produce_ack"), "Kafka topic"))
        n += [node("gateway", "Scoring gateway", "service", 2, 1, _c("gateway", "decisions"), _s("gateway", "hot_path"),
                   "sync gRPC" if sync else "Kafka ingress"),
              node("flink", "Flink features", "flink", 2, 0, _s("flink", "state_card"), _s("flink", "store_update_lag"),
                   "7 keyed stages"),
              node("redis", "Redis", "store", 3, 0, _s("flink", "feature_store_write"), _s("gateway", "feature_lookup"),
                   "online store"),
              node("model", "Model server", "model", 3, 2, _c("model", "requests"), _s("gateway", "model_call"),
                   "KServe v2"),
              node("decisions", "decisions", "kafka", 4, 1, _c("gateway", "decisions"), _s("gateway", "audit_ack"),
                   "audit WAL (acks=all)"),
              node("collector", "Collector", "driver", 5, 1, decided, _s("collector", "e2e"), "audit completeness"),
              node("clickhouse", "ClickHouse", "sink", 5, 2, None, None, "async audit")]
        if sync:
            n.append(node("mirror", "events.raw", "kafka", 1, 0, None, None, "mirrored for features"))
            e += [edge("emitter", "gateway", "Score()", emitted), edge("gateway", "mirror", "mirror"),
                  edge("mirror", "flink", "consume"), edge("gateway", "emitter", "response", None, _s("emitter", "e2e"))]
        else:
            e += [edge("emitter", "events", "produce", emitted),
                  edge("events", "gateway", "consume", _s("collector", "ingress"), _s("collector", "ingress")),
                  edge("events", "flink", "consume")]
        e += [edge("flink", "redis", "write", _s("flink", "feature_store_write"), _s("flink", "feature_store_write"),
                   bulk=_s("flink", "feature_store_write_backfill")),
              edge("redis", "gateway", "lookup", None, _s("gateway", "feature_lookup")),
              edge("gateway", "model", "infer", _c("model", "requests"), _s("gateway", "model_call")),
              edge("gateway", "decisions", "audit", _c("gateway", "decisions"), _s("gateway", "audit_ack")),
              edge("decisions", "collector", "consume", decided, _s("collector", "egress")),
              edge("decisions", "clickhouse", "Kafka engine")]
    return {"topology": topology, "nodes": n, "edges": e}


@router.get("/api/topology")
def topology(topology: str = "gateway-kafka") -> dict[str, Any]:
    return build(topology)
