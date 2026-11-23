"""One Redis client for every Python caller (seeding, the lease, the verifier, the console), standalone or
Redis Cluster, chosen as the Java store chooses (io.realy.scoring.store.RedisOnlineStore):
REDIS_MODE = auto (default: ask the server, INFO cluster) | standalone | cluster. TLS and auth come from
the URI (rediss://user:password@host:port); REDIS_CA_FILE names a private CA for TLS.

Cluster mode works with what the callers do: single-key commands, scans (every primary), multi-key
UNLINK (split across slots), non-transactional pipelines and single-key Lua scripts (the lease)."""
from __future__ import annotations

import functools
import os
from typing import Any

import redis


def uri() -> str:
    return os.environ.get("REDIS_URI", "redis://localhost:6379")


def _tls_kwargs(u: str) -> dict[str, Any]:
    ca = os.environ.get("REDIS_CA_FILE")
    return {"ssl_ca_certs": ca} if ca and u.startswith("rediss://") else {}


@functools.lru_cache(maxsize=8)
def _detect(u: str) -> str:
    want = os.environ.get("REDIS_MODE", "auto").lower()
    if want in ("standalone", "cluster"):
        return want
    try:
        info = redis.Redis.from_url(u, socket_timeout=2, socket_connect_timeout=2, **_tls_kwargs(u)).info("cluster")
        return "cluster" if int(info.get("cluster_enabled", 0)) == 1 else "standalone"
    except redis.RedisError:
        return "standalone"                      # unreachable: the caller's own error says why


def mode(u: str | None = None) -> str:
    """standalone | cluster: what a client for this URI connects as."""
    return _detect(u or uri())


def client(u: str | None = None, **kw: Any) -> redis.Redis:
    """A client for REDIS_URI (or `u`); keyword arguments go to redis-py (timeouts)."""
    u = u or uri()
    kw = {**_tls_kwargs(u), **kw}
    if _detect(u) == "cluster":
        from redis.cluster import RedisCluster
        return RedisCluster.from_url(u, **kw)   # type: ignore[return-value]
    return redis.Redis.from_url(u, **kw)
