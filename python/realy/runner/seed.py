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


def seed_profiles(world: World, force: bool = False, r: redis.Redis | None = None) -> int:
    r = r or redis_client()
    marker = f"pf:_world:{world.content_hash}"
    if not force and r.exists(marker):
        return 0
    schema = load_schema()
    now = now_us()
    n = 0
    for entity, arrays in (("customer", world.customer_profile_arrays()), ("merchant", world.merchant_profile_arrays())):
        feats = [f.field for f in schema.profile_features(entity)]
        attrs = schema.profile_attributes.get(entity, [])
        ids = arrays[f"{entity}_id"]
        mat = np.stack([np.asarray(arrays[c], dtype=np.float32) for c in feats + attrs], axis=1) if feats + attrs else None
        if mat is None:
            continue
        head = HDR.pack(schema.version, mat.shape[1], 0, now)
        body = mat.astype("<f4")
        pipe = r.pipeline(transaction=False)
        for i in range(ids.shape[0]):
            pipe.set(f"pf:{entity}:{int(ids[i])}", head + body[i].tobytes())
            if i % 20000 == 19999:
                pipe.execute()
        pipe.execute()
        n += ids.shape[0]
    r.set(marker, now)
    return n
