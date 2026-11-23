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
    """Standalone or Redis Cluster (REDIS_MODE, realy.common.redis_client)."""
    from ..common.redis_client import client
    return client()


def blob(values: np.ndarray, version: int, as_of: int, updated: int) -> bytes:
    return HDR.pack(version, values.shape[0], as_of, updated) + values.astype("<f4").tobytes()


def profile_layout(schema, entity: str) -> str:
    """What a pf:<entity>:<id> blob is laid out by: the schema version and its value columns in order."""
    import hashlib
    import json
    cols = [f.field for f in schema.profile_features(entity)] + list(schema.profile_attributes.get(entity, []))
    return hashlib.sha256(json.dumps([schema.version, entity, cols]).encode()).hexdigest()[:16]


def profile_entities(schema) -> list[str]:
    return [e for e in dict.fromkeys(f.entity for f in schema.features if f.source == "profile")
            ] + [e for e in schema.profile_attributes if not schema.profile_features(e)]


def profile_blobs(world: World, schema=None, updated_us: int | None = None, entities=None):
    """Yield (key, blob) for every profile row of the world: pf:<entity>:<id> (contracts/storage/redis.md).
    Shared by the seeder and the golden test set, so both write the same bytes."""
    schema = schema or load_schema()
    now = now_us() if updated_us is None else int(updated_us)
    for entity in (entities if entities is not None else profile_entities(schema)):
        arrays = world.profile_arrays(entity)
        feats = [f.field for f in schema.profile_features(entity)]
        attrs = schema.profile_attributes.get(entity, [])
        if not feats + attrs:
            continue
        if arrays is None:
            raise ValueError(f"the feature schema has profile features for {entity!r} but the world has no "
                             f"{entity} profiles (World.profile_arrays)")
        missing = [c for c in feats + attrs if c not in arrays]
        if missing:
            raise ValueError(f"the world's {entity} profiles lack {missing} (feature schema profile fields)")
        ids = arrays[f"{entity}_id"]
        mat = np.stack([np.asarray(arrays[c], dtype=np.float32) for c in feats + attrs], axis=1)
        head = HDR.pack(schema.version, mat.shape[1], 0, now)
        body = mat.astype("<f4")
        for i in range(ids.shape[0]):
            yield f"pf:{entity}:{int(ids[i])}", head + body[i].tobytes()


def seed_profiles(world: World, force: bool = False, r: redis.Redis | None = None) -> int:
    """Seed every entity whose profiles are not in place: another world, or another blob layout (a
    profile feature added, removed or reordered). One marker per entity, pf:_meta:<entity> =
    {"world": <hash>, "layout": <hash>}; pf:_world names the world (the health watch looks for it)."""
    import json
    r = r or redis_client()
    schema = load_schema()
    todo = []
    for entity in profile_entities(schema):
        want = {"world": world.content_hash, "layout": profile_layout(schema, entity)}
        cur = r.get(f"pf:_meta:{entity}")
        try:
            have = json.loads(cur) if cur is not None else None
        except ValueError:
            have = None
        if force or have != want:
            todo.append((entity, want))
    n = 0
    if todo:
        pipe = r.pipeline(transaction=False)
        for key, value in profile_blobs(world, schema, entities=[e for e, _ in todo]):
            pipe.set(key, value)
            n += 1
            if n % 20000 == 0:
                pipe.execute()
        pipe.execute()
        for entity, want in todo:
            r.set(f"pf:_meta:{entity}", json.dumps(want, sort_keys=True))
    r.set("pf:_world", world.content_hash)
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
