"""Seed the online store with static profile features from the world (contracts/storage/redis.md)."""
from __future__ import annotations

import os
import struct

import numpy as np
import redis

from ..common.clock import now_us
from ..features import load_schema
from ..world import World

HDR = struct.Struct("<HHqq")


def redis_client() -> redis.Redis:
    return redis.Redis.from_url(os.environ.get("REDIS_URI", "redis://localhost:6379"))


def blob(values: np.ndarray, version: int, as_of: int, updated: int) -> bytes:
    return HDR.pack(version, values.shape[0], as_of, updated) + values.astype("<f4").tobytes()


def profile_blobs(world: World, schema=None, updated_us: int | None = None):
    """Yield (key, blob) for every profile row of the world: pf:<entity>:<id> (contracts/storage/redis.md).
    Shared by the seeder and the golden test set, so both write the same bytes."""
    schema = schema or load_schema()
    now = now_us() if updated_us is None else int(updated_us)
    for entity, arrays in (("customer", world.customer_profile_arrays()), ("merchant", world.merchant_profile_arrays())):
        feats = [f.field for f in schema.profile_features(entity)]
        attrs = schema.profile_attributes.get(entity, [])
        if not feats + attrs:
            continue
        ids = arrays[f"{entity}_id"]
        mat = np.stack([np.asarray(arrays[c], dtype=np.float32) for c in feats + attrs], axis=1)
        head = HDR.pack(schema.version, mat.shape[1], 0, now)
        body = mat.astype("<f4")
        for i in range(ids.shape[0]):
            yield f"pf:{entity}:{int(ids[i])}", head + body[i].tobytes()


def seed_profiles(world: World, force: bool = False, r: redis.Redis | None = None) -> int:
    r = r or redis_client()
    # Customer / merchant ids repeat across worlds (they are row numbers), so pf:* holds ONE world's
    # profiles at a time. The marker names that world; seed whenever another world is in place.
    marker = "pf:_world"
    cur = r.get(marker)
    if not force and cur is not None and (cur.decode() if isinstance(cur, bytes) else cur) == world.content_hash:
        return 0
    n = 0
    pipe = r.pipeline(transaction=False)
    for key, value in profile_blobs(world):
        pipe.set(key, value)
        n += 1
        if n % 20000 == 0:
            pipe.execute()
    pipe.execute()
    r.set(marker, world.content_hash)
    return n


def clear_stream_blobs(r: redis.Redis | None = None) -> int:
    """Delete all Flink-written stream feature blobs (fs:*), keeping the seeded profiles (pf:*)."""
    r = r or redis_client()
    n = 0
    batch = []
    for k in r.scan_iter(match="fs:*", count=10000):
        batch.append(k)
        if len(batch) >= 5000:
            n += r.unlink(*batch)
            batch = []
    if batch:
        n += r.unlink(*batch)
    return n
