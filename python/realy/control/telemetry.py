"""Live telemetry hub: consumes bench.metrics (1 s MetricsSnapshots from emitters, collectors,
gateway, model server and Flink), keeps a ring buffer per source instance (~5 min) so a newly opened
page can backfill its charts, and fans every message out to connected WebSocket clients."""
from __future__ import annotations

import asyncio
import collections
import json
import threading
import time
import uuid
from typing import Any

HISTORY_S = 300
DECISION_FEED_PER_S = 20
OUTCOMES = {1: "APPROVE", 2: "REVIEW", 3: "DECLINE"}
PATHS = {0: "FULL", 1: "RULES_ONLY_FALLBACK", 2: "PROVISIONAL", 3: "FINAL_AFTER_PROVISIONAL", 4: "ECHO"}
PAYMENT_TYPES = ["UNSPECIFIED", "CARD_PURCHASE", "ATM_WITHDRAWAL", "TRANSFER", "CARD_REFUND"]
CHANNELS = ["UNSPECIFIED", "POS", "ECOM", "ATM", "MOTO", "IN_APP", "ONLINE_BANKING"]


class Hub:
    def __init__(self, kafka: bool = True) -> None:
        self.kafka = kafka
        self.rings: dict[str, collections.deque] = {}
        self.logs: collections.deque = collections.deque(maxlen=3000)
        self._clients: set[asyncio.Queue] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self.connected = False
        self.last_error: str | None = None
        self.decisions: collections.deque = collections.deque(maxlen=200)

    # ------------------------------------------------------------------ fan-out
    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=5000)
        self._clients.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._clients.discard(q)

    def publish(self, msg: dict[str, Any]) -> None:
        """Thread-safe broadcast to all WebSocket clients (drops for slow clients)."""
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        for q in list(self._clients):
            loop.call_soon_threadsafe(_put_nowait, q, msg)

    def add_log(self, run_id: str, line: str) -> None:
        msg = {"type": "log", "run_id": run_id, "line": line, "ts": time.time()}
        self.logs.append(msg)
        self.publish(msg)

    def ingest(self, snap: dict[str, Any]) -> None:
        from ..runner.run import normalize_snapshot
        snap = normalize_snapshot(snap)
        key = f"{snap.get('source', '?')}:{snap.get('instance', '?')}"
        with self._lock:
            ring = self.rings.setdefault(key, collections.deque(maxlen=HISTORY_S))
            ring.append(snap)
        self.publish({"type": "metrics", **snap})

    def history(self, run_id: str | None = None, seconds: int = HISTORY_S) -> dict[str, Any]:
        cutoff = (time.time() - seconds) * 1e6
        with self._lock:
            out = {k: [s for s in ring if s["ts_us"] >= cutoff and (not run_id or s.get("run_id") in ("", run_id))]
                   for k, ring in self.rings.items()}
        logs = [m for m in self.logs if not run_id or m["run_id"] == run_id]
        decisions = [m for m in self.decisions if not run_id or m["run_id"] == run_id]
        return {"metrics": {k: v for k, v in out.items() if v}, "logs": logs[-500:], "decisions": decisions[-100:]}

    # ------------------------------------------------------------------ Kafka consumer
    def start(self) -> "Hub":
        if self.kafka:
            threading.Thread(target=self._consume, name="console-telemetry", daemon=True).start()
            threading.Thread(target=self._consume_decisions, name="console-decisions", daemon=True).start()
        return self

    def _consume_decisions(self) -> None:
        """Live decision feed: a rate-limited sample (~20/s) of payments.decisions for the Live page."""
        from ..common.kafka import DECISIONS, make_consumer
        from ..scoring.v1 import decision_pb2
        DR = decision_pb2.DecisionRecord
        while not self._stop.is_set():
            try:
                c = make_consumer(f"console-feed-{uuid.uuid4().hex[:8]}", **{"auto.offset.reset": "latest"})
                c.subscribe([DECISIONS])
                window, sent = int(time.time()), 0
                while not self._stop.is_set():
                    m = c.poll(0.5)
                    if m is None or m.error() or m.value() is None:
                        continue
                    now = int(time.time())
                    if now != window:
                        window, sent = now, 0
                    if sent >= DECISION_FEED_PER_S:
                        continue
                    sent += 1
                    try:
                        d = DR.FromString(m.value())
                    except Exception:  # noqa: BLE001
                        continue
                    t_ing = next((sp.start_us for sp in d.trace.spans if sp.stage == 10), 0)
                    t_dec = next((sp.start_us for sp in d.trace.spans if sp.stage == 70), 0)
                    ev = d.event
                    msg = {"type": "decision", "run_id": d.trace.run_id, "event_id": str(d.event_id),
                           "customer_id": str(ev.customer_id), "outcome": OUTCOMES.get(d.outcome),
                           "score": round(d.score, 4), "path": PATHS.get(d.path, str(d.path)), "degraded": d.degraded,
                           "amount_usd": ev.amount_usd_minor / 100.0,
                           "payment_type": PAYMENT_TYPES[ev.payment_type] if ev.payment_type < len(PAYMENT_TYPES) else "",
                           "channel": CHANNELS[ev.channel] if ev.channel < len(CHANNELS) else "",
                           "merchant_country": ev.merchant_country,
                           "rule_hits": [h.rule_id for h in d.rule_hits],
                           "hot_path_ms": round((t_dec - t_ing) / 1000.0, 2) if t_ing and t_dec else None,
                           "ts_us": d.decided_at_us}
                    self.decisions.append(msg)
                    self.publish(msg)
                c.close()
            except Exception as e:  # noqa: BLE001 - Kafka down: retry
                self.last_error = str(e)
                self._stop.wait(5)

    def shutdown(self) -> None:
        self._stop.set()

    def _consume(self) -> None:
        from ..common.kafka import METRICS, make_consumer
        while not self._stop.is_set():
            try:
                c = make_consumer(f"console-{uuid.uuid4().hex[:8]}", **{"auto.offset.reset": "latest"})
                c.subscribe([METRICS])
                self.connected = True
                self.last_error = None
                while not self._stop.is_set():
                    m = c.poll(0.5)
                    if m is None or m.error() or m.value() is None:
                        continue
                    try:
                        self.ingest(json.loads(m.value()))
                    except ValueError:
                        continue
                c.close()
            except Exception as e:  # noqa: BLE001 - Kafka down: retry
                self.connected = False
                self.last_error = str(e)
                self._stop.wait(5)


def _put_nowait(q: asyncio.Queue, msg: dict[str, Any]) -> None:
    try:
        q.put_nowait(msg)
    except asyncio.QueueFull:
        pass
