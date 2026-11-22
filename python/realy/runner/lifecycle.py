"""Run lifecycle across processes: the pipeline lease, remote abort, and orphan protection.

A benchmark run owns the pipeline under test while it runs. It pushes a complete ScoringConfig and
ModelServerConfig, may switch the Flink job's mode, and measures latency that any other traffic would
distort. Two runs against one pipeline therefore corrupt each other, wherever they were started (two
consoles, a console and a CLI, a Kubernetes bench Job).

**Pipeline lease (Redis).**
  - **Key:** `realy:lease:<pipeline>` (`REALY_PIPELINE`, default `default`). The value is
    `<run_id>\\n<json holder info>`.
  - **Acquire:** `SET NX PX` (TTL `LEASE_TTL_S`).
  - **Heartbeat:** every `HEARTBEAT_S` the owner renews the lease, compare-and-set on its run id.
  - **Release:** compare-and-delete at the end.
  - **Crashes:** a crashed owner's lease expires on its own after the TTL, so a hung or killed process
    never blocks the pipeline for long.
  - **Overrides:** `REALY_ALLOW_CONCURRENT=1` skips the lease entirely (deliberate interference tests).
  - **Redis unreachable:** the run proceeds with a warning (nothing to coordinate on).

**Remote abort.** Two independent channels, so an abort still lands when one of them is the
component that failed:
  - a `bench.control` message (phase ABORTED), consumed by the run's metrics monitor;
  - a Redis flag `realy:abort:<run_id>` (TTL 1 h), polled by the lease heartbeat every second.

**Orphans.** `watch_parent()` runs in every generator / emitter / collector process. If the
orchestrating process dies (console killed, CLI SIGKILLed), the child exits instead of emitting into
a pipeline nobody is measuring.
"""
from __future__ import annotations

import json
import os
import socket
import threading
import time
from pathlib import Path
from typing import Any, Callable

LEASE_TTL_S = 15.0
HEARTBEAT_S = 1.0
ABORT_TTL_S = 3600

_RENEW = """
local v = redis.call('GET', KEYS[1])
if v and string.sub(v, 1, string.len(ARGV[1]) + 1) == ARGV[1] .. '\\n' then
  redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
  return 1
end
if not v then
  local ok = redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3], 'NX')
  if ok then return 2 end
end
return 0
"""
_RELEASE = """
local v = redis.call('GET', KEYS[1])
if v and string.sub(v, 1, string.len(ARGV[1]) + 1) == ARGV[1] .. '\\n' then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


def pipeline() -> str:
    return os.environ.get("REALY_PIPELINE", "default")


def lease_key(name: str | None = None) -> str:
    return f"realy:lease:{name or pipeline()}"


def abort_key(run_id: str) -> str:
    return f"realy:abort:{run_id}"


def owner_id() -> str:
    return f"{os.environ.get('REALY_OWNER') or socket.gethostname()}:{os.getpid()}"


def _redis():
    from ..common.redis_client import client
    return client(socket_timeout=2, socket_connect_timeout=2)


def _decode(raw: bytes | str | None) -> dict[str, Any] | None:
    if raw is None:
        return None
    s = raw.decode() if isinstance(raw, bytes) else raw
    run_id, _, rest = s.partition("\n")
    try:
        info = json.loads(rest) if rest else {}
    except ValueError:
        info = {}
    info["run_id"] = run_id
    return info


class PipelineBusy(RuntimeError):
    """Another run holds the pipeline lease."""

    def __init__(self, holder: dict[str, Any]) -> None:
        self.holder = holder
        age = time.time() - float(holder.get("started_at") or time.time())
        super().__init__(f"pipeline '{pipeline()}' is busy: run {holder.get('run_id')} "
                         f"({holder.get('scenario')}, {holder.get('phase', '?')}, owner {holder.get('owner')}, "
                         f"{age:.0f} s old)")


def holder(r: Any = None) -> dict[str, Any] | None:
    """The current lease holder, or None (free / Redis unreachable)."""
    try:
        r = r or _redis()
        info = _decode(r.get(lease_key()))
        if info is not None:
            ttl = r.pttl(lease_key())
            info["expires_in_s"] = round(ttl / 1000, 1) if ttl and ttl > 0 else None
        return info
    except Exception:  # noqa: BLE001
        return None


def request_abort(run_id: str, reason: str = "aborted by operator", publish: bool = True,
                  log: Callable[[str], None] | None = None) -> dict[str, bool]:
    """Ask a run, wherever it executes, to abort. Both channels are best effort; the result says
    which ones were delivered. An empty run_id over Kafka means "every run" (the console's Stop all)."""
    out = {"redis": False, "kafka": False}
    if run_id:
        try:
            _redis().set(abort_key(run_id), reason, ex=ABORT_TTL_S)
            out["redis"] = True
        except Exception as e:  # noqa: BLE001
            if log:
                log(f"abort {run_id}: Redis channel unavailable ({e.__class__.__name__})")
    if publish:
        try:
            from ..common.control import control_message, publish as kpublish
            kpublish([control_message(run_id, "*", phase=6)])
            out["kafka"] = True
        except Exception as e:  # noqa: BLE001
            if log:
                log(f"abort {run_id or '*'}: Kafka channel unavailable ({e.__class__.__name__})")
    return out


def abort_requested(run_id: str, r: Any = None) -> str | None:
    try:
        v = (r or _redis()).get(abort_key(run_id))
        return v.decode() if isinstance(v, bytes) else v
    except Exception:  # noqa: BLE001
        return None


class RunLease:
    """Pipeline lease for one run, plus the 1 s heartbeat that renews it, polls the Redis abort
    flag and touches `<run_dir>/heartbeat` (lets any reader tell a live run from a crashed one)."""

    def __init__(self, run_id: str, info: dict[str, Any]) -> None:
        self.run_id = run_id
        self.info = {**info, "owner": owner_id(), "pipeline": pipeline(), "started_at": time.time()}
        self.held = False
        self.disabled = os.environ.get("REALY_ALLOW_CONCURRENT", "0") == "1"
        self.unavailable: str | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._r = None

    # ------------------------------------------------------------------ acquire / release
    def _value(self, phase: str | None = None) -> str:
        if phase:
            self.info["phase"] = phase
        self.info["heartbeat_at"] = time.time()
        return f"{self.run_id}\n{json.dumps(self.info)}"

    def acquire(self, wait_s: float = 0.0, stop: Callable[[], bool] | None = None,
                log: Callable[[str], None] = print) -> "RunLease":
        if self.disabled:
            log("run lease: disabled (REALY_ALLOW_CONCURRENT=1): concurrent runs are NOT prevented")
            return self
        deadline = time.time() + max(0.0, wait_s)
        announced = False
        while True:
            try:
                self._r = self._r or _redis()
                if self._r.set(lease_key(), self._value("PREPARE"), nx=True, px=int(LEASE_TTL_S * 1000)):
                    self.held = True
                    self._r.delete(abort_key(self.run_id))   # a stale flag from a reused run id
                    return self
                h = _decode(self._r.get(lease_key()))
            except Exception as e:  # noqa: BLE001
                self.unavailable = f"{e.__class__.__name__}: {e}"
                log(f"warn: run lease unavailable (Redis: {self.unavailable}); concurrent runs are not prevented")
                return self
            if h is None:
                continue          # released between SET and GET: try again at once
            if time.time() >= deadline or (stop and stop()):
                raise PipelineBusy(h)
            if not announced:
                log(f"waiting for the pipeline: {PipelineBusy(h)}")
                announced = True
            time.sleep(1.0)

    def release(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
        if self.held and self._r is not None:
            try:
                self._r.eval(_RELEASE, 1, lease_key(), self.run_id)
            except Exception:  # noqa: BLE001
                pass
        self.held = False

    # ------------------------------------------------------------------ heartbeat
    def start(self, handle: Any, log: Callable[[str], None] = print) -> "RunLease":
        self._thread = threading.Thread(target=self._beat, args=(handle, log), daemon=True,
                                        name=f"lease-{self.run_id}")
        self._thread.start()
        return self

    def _beat(self, handle: Any, log: Callable[[str], None]) -> None:
        hb = Path(handle.run_dir) / "heartbeat"
        warned = False
        while not self._stop.wait(HEARTBEAT_S):
            try:
                hb.parent.mkdir(parents=True, exist_ok=True)
                hb.write_text(json.dumps({"ts": time.time(), "phase": handle.phase, "owner": self.info["owner"]}))
            except OSError:
                pass
            if self.disabled:
                continue
            try:
                self._r = self._r or _redis()
                if self.held or self.unavailable:
                    res = self._r.eval(_RENEW, 1, lease_key(), self.run_id, self._value(handle.phase),
                                       int(LEASE_TTL_S * 1000))
                    if res == 0 and not getattr(handle, "abort_reason", None):
                        h = _decode(self._r.get(lease_key())) or {}
                        handle.request_abort(f"pipeline lease lost to run {h.get('run_id')} "
                                             f"(owner {h.get('owner')}): measurements would interfere", "lease")
                    elif res in (1, 2):
                        self.held, self.unavailable = True, None
                reason = abort_requested(self.run_id, self._r)
                if reason and not getattr(handle, "abort_reason", None):
                    handle.request_abort(reason, "operator")
                warned = False
            except Exception as e:  # noqa: BLE001
                self._r = None
                if not warned:
                    log(f"warn: lease heartbeat failed ({e.__class__.__name__}); retrying")
                    warned = True


# ---------------------------------------------------------------------------------- child processes
def watch_parent(grace_s: float = 1.0, on_orphan: Callable[[], None] | None = None) -> threading.Thread:
    """In a child process: exit when the parent dies (getppid() changes on reparenting)."""
    ppid = os.getppid()

    def loop() -> None:
        while True:
            time.sleep(grace_s)
            if os.getppid() != ppid:
                try:
                    if on_orphan:
                        on_orphan()
                finally:
                    os._exit(3)
    t = threading.Thread(target=loop, daemon=True, name="watch-parent")
    t.start()
    return t
