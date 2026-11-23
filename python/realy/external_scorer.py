"""A reference external scorer: the smallest service that satisfies the `external` topology's contract
(docs/dev/external-scorer.md). Use it as a template for your own, in any language, and to try the topology
on the bundled stack:

    python -m realy.external_scorer &          # in the console container (or anywhere with Kafka)
    pulsebench run external-smoke --verify

The contract, all of it:
  in   the pipeline's events topic (contracts/pipelines/<pipeline>.yaml `topics.events`): serialized
       TransactionEvent, keyed by the partition entity; measured events carry an `x-bench-trace` header (a
       serialized realy.bench.v1.BenchTrace); `x-backfill` marks unmeasured history (decide it or skip it:
       it is not measured); `x-event-type` names another message type of a multi-message transaction
  out  the pipeline's decisions topic (`topics.decisions`): one realy.scoring.v1.DecisionRecord per event,
       with `event_id`, an `outcome`, `decided_at_us` and the received BenchTrace in `trace` (add spans of
       your own stages if you like: each needs a clock `domain` of its own); keyed like the events

This scorer decides with one fixed rule (amounts above a threshold go to review), keeps no state, and
copies the trace. It scores plain events only; the parts of multi-message transactions (`x-event-type`)
are skipped, so run it with scenarios without `traffic.flows`.
"""
from __future__ import annotations

import argparse
import os
import signal
import socket
import sys
import time
import zlib

from .common.clock import now_us
from .common.kafka import make_consumer, make_producer
from .bench.v1 import trace_pb2 as bench
from .payments.v1 import transaction_pb2 as epb
from .scoring.v1 import decision_pb2 as dpb


def _domain() -> int:
    """This process's clock domain (spans from different machines are never compared directly)."""
    return zlib.crc32(f"external-scorer:{socket.gethostname()}:{os.getpid()}".encode()) or 1


def decide(ev: "epb.TransactionEvent", review_above_minor: int) -> tuple[int, list[str]]:
    if ev.amount_usd_minor > review_above_minor:
        return dpb.REVIEW, ["EXTERNAL_HIGH_AMOUNT"]
    return dpb.APPROVE, []


def main(argv: list[str] | None = None) -> int:
    from .bindings import pipeline
    t = pipeline().topics
    ap = argparse.ArgumentParser(prog="python -m realy.external_scorer", description=__doc__.split("\n\n")[0])
    ap.add_argument("--events-topic", default=t.get("events", "payments.events.raw"))
    ap.add_argument("--decisions-topic", default=t.get("decisions", "payments.decisions"))
    ap.add_argument("--group", default="realy-external-scorer")
    ap.add_argument("--review-above", type=int, default=500_000, help="amount (USD minor units) sent to review")
    ap.add_argument("--policy-version", default="external-reference-v1")
    a = ap.parse_args(argv)

    consumer = make_consumer(a.group, **{"auto.offset.reset": "latest", "enable.auto.commit": True})
    producer = make_producer(**{"linger.ms": 1, "acks": "all"})
    consumer.subscribe([a.events_topic])
    dom = _domain()
    stop = False

    def _stop(*_: object) -> None:
        nonlocal stop
        stop = True
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    n = skipped = 0
    last = time.time()
    print(f"external scorer: {a.events_topic} -> {a.decisions_topic} (group {a.group})", file=sys.stderr, flush=True)
    while not stop:
        msgs = consumer.consume(num_messages=500, timeout=0.05)
        for m in msgs:
            if m.error():
                continue
            h = dict(m.headers() or [])
            if "x-event-type" in h:               # a part of a multi-message transaction: not handled here
                skipped += 1
                continue
            t_in = now_us()
            ev = epb.TransactionEvent.FromString(m.value())
            outcome, reasons = decide(ev, a.review_above)
            d = dpb.DecisionRecord(event_id=ev.event_id, outcome=outcome, decided_at_us=now_us(),
                                   policy_version=a.policy_version, event=ev)
            d.reason_codes.extend(reasons)
            tr = h.get("x-bench-trace")
            if tr:
                d.trace.ParseFromString(tr)
                d.trace.spans.add(stage=bench.INGEST, domain=dom, start_us=t_in)
                d.trace.spans.add(stage=bench.DECISION, domain=dom, start_us=d.decided_at_us)
            producer.produce(a.decisions_topic, d.SerializeToString(), key=m.key())
            n += 1
        producer.poll(0)
        if time.time() - last >= 10:
            print(f"external scorer: {n:,} decided, {skipped:,} parts skipped", file=sys.stderr, flush=True)
            last = time.time()
    producer.flush(10)
    consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
