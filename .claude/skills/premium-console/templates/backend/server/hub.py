"""Live hub: keeps a short history (metrics, logs) so a newly opened page can backfill its charts, and fans
every message out to connected WebSocket clients. Thread-safe: worker threads publish, the event loop sends."""
from __future__ import annotations

import asyncio
import collections
import threading
import time
from typing import Any

HISTORY_S = 900


class Hub:
    def __init__(self) -> None:
        self.metrics: collections.deque = collections.deque(maxlen=HISTORY_S * 2)
        self.logs: collections.deque = collections.deque(maxlen=3000)
        self._clients: set[asyncio.Queue] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ fan-out
    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=5000)
        self._clients.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._clients.discard(q)

    @property
    def clients(self) -> int:
        return len(self._clients)

    def publish(self, msg: dict[str, Any]) -> None:
        """Broadcast from any thread. A slow client's queue fills up and drops messages; it never blocks others."""
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        for q in list(self._clients):
            loop.call_soon_threadsafe(_put_nowait, q, msg)

    # ------------------------------------------------------------------ producers
    def add_log(self, job_id: str, line: str) -> None:
        msg = {"type": "log", "job_id": job_id, "line": line, "ts": time.time()}
        self.logs.append(msg)
        self.publish(msg)

    def add_metric(self, sample: dict[str, Any]) -> None:
        msg = {"type": "metrics", "ts": time.time(), **sample}
        with self._lock:
            self.metrics.append(msg)
        self.publish(msg)

    def history(self, seconds: int = HISTORY_S, job_id: str | None = None) -> dict[str, Any]:
        cutoff = time.time() - seconds
        with self._lock:
            metrics = [m for m in self.metrics if m["ts"] >= cutoff and (not job_id or m.get("job_id") == job_id)]
        logs = [m for m in self.logs if m["ts"] >= cutoff and (not job_id or m["job_id"] == job_id)]
        return {"metrics": metrics, "logs": logs[-500:]}


def _put_nowait(q: asyncio.Queue, msg: dict[str, Any]) -> None:
    try:
        q.put_nowait(msg)
    except asyncio.QueueFull:
        pass
