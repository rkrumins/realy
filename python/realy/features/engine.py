"""Python reference implementation of the feature DSL — a line-by-line mirror of
java/scoring-core/.../features/EntityAggregator.java (semantics in feature_schema.yaml).

Used by `realy verify` to recompute every feature independently from the ground-truth event log
and compare it with what the pipeline actually served.
"""
from __future__ import annotations

import math
from typing import Any

from .schema import FeatureSchema, FeatureSpec

MIN_GEO_DT_US = 60_000_000
EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2) - math.radians(lon1)
    s1 = math.sin(dp / 2.0)
    s2 = math.sin(dl / 2.0)
    a = s1 * s1 + math.cos(p1) * math.cos(p2) * s2 * s2
    return 2.0 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(a)))


class _Feat:
    __slots__ = ("max_k", "buckets", "seen", "last_ts", "lat", "lon")

    def __init__(self) -> None:
        self.max_k: int | None = None
        self.buckets: dict[int, list[float]] = {}
        self.seen: dict[Any, int] = {}
        self.last_ts: int | None = None
        self.lat = 0.0
        self.lon = 0.0


def _apply(s: FeatureSpec, f: _Feat, ev: dict, t: int, match: bool) -> float:
    g = s.granularity_us
    k = t // g
    n = s.window_us // g if g else 0
    agg = s.agg
    if agg in ("count", "sum", "max", "avg"):
        f.max_k = k if f.max_k is None else max(f.max_k, k)
        if match:
            v = 1.0 if agg == "count" else float(ev[s.value])
            b = f.buckets.get(k)
            if b is None:
                f.buckets[k] = [v, 1.0]
            elif agg == "max":
                b[0] = max(b[0], v)
            else:
                b[0] += v
                b[1] += 1.0
        lim = f.max_k - n
        for idx in [i for i in f.buckets if i <= lim]:
            del f.buckets[idx]
        a = bsum = 0.0
        mx = None
        for idx, (x, y) in f.buckets.items():
            if k - n < idx <= k:
                if agg == "max":
                    mx = x if mx is None else max(mx, x)
                else:
                    a += x
                    bsum += y
        if agg == "max":
            return mx if mx is not None else s.default
        if agg == "avg":
            return a / bsum if bsum > 0 else s.default
        return a
    if agg == "distinct":
        f.max_k = k if f.max_k is None else max(f.max_k, k)
        if match:
            key = ev[s.value]
            f.seen[key] = max(f.seen.get(key, k), k)
        lim = f.max_k - n
        for key in [x for x, last in f.seen.items() if last <= lim]:
            del f.seen[key]
        return float(sum(1 for last in f.seen.values() if last > k - n))
    if agg == "novel":
        f.max_k = k if f.max_k is None else max(f.max_k, k)
        lim = f.max_k - n
        for key in [x for x, last in f.seen.items() if last <= lim]:
            del f.seen[key]
        if not match:
            return 0.0
        key = ev[s.value]
        last = f.seen.get(key)
        seen = last is not None and last > k - n
        f.seen[key] = max(last, k) if last is not None else k
        return 0.0 if seen else 1.0
    if agg == "time_since_last":
        v = ((t - f.last_ts) / 1e6 if f.last_ts is not None and t >= f.last_ts and t - f.last_ts <= s.window_us
             else s.default)
        if match and (f.last_ts is None or t >= f.last_ts):
            f.last_ts = t
        return v
    if agg == "geo_speed_kmh":
        v = s.default
        if match and f.last_ts is not None and t >= f.last_ts and t - f.last_ts <= s.window_us:
            km = haversine_km(f.lat, f.lon, ev["lat"], ev["lon"])
            dt = max(t - f.last_ts, MIN_GEO_DT_US)
            v = km / (dt / 3.6e9)
        if match and (f.last_ts is None or t >= f.last_ts):
            f.last_ts, f.lat, f.lon = t, ev["lat"], ev["lon"]
        return v
    raise ValueError(f"unknown agg {agg}")


class ReferenceEngine:
    """Stateful per-entity aggregation over events processed in order."""

    def __init__(self, schema: FeatureSchema, entities: list[str] | None = None) -> None:
        self.schema = schema
        self.entities = entities or schema.stream_entities()
        self.specs = {e: schema.stream_features(e) for e in self.entities}
        self.state: dict[str, dict[int, list[_Feat]]] = {e: {} for e in self.entities}
        self.filter_index = {name: i for i, name in enumerate(schema.filters)}

    def process(self, ev: dict, filter_mask: int, out: list[float]) -> None:
        """Update with one event (dict of TransactionEvent fields) and write stream features into out."""
        t = int(ev["event_time_us"])
        for ent in self.entities:
            specs = self.specs[ent]
            key = int(ev[self.schema.entities[ent]])
            if key == 0:
                for s in specs:
                    out[s.index] = s.default
                continue
            feats = self.state[ent].get(key)
            if feats is None:
                feats = [_Feat() for _ in specs]
                self.state[ent][key] = feats
            for s, f in zip(specs, feats):
                match = s.filter is None or bool(filter_mask >> self.filter_index[s.filter] & 1)
                out[s.index] = _apply(s, f, ev, t, match)
