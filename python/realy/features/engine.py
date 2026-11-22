"""Python reference implementation of the feature DSL, the mirror of
java/scoring-core/.../features/EntityAggregator.java (semantics in feature_schema.yaml). Each feature's
aggregation comes from the registry (realy.features.aggregations, the twin of Aggregations.java), so a
registered plugin aggregation is recomputed here exactly as the pipeline computes it.

Used by `realy verify` to recompute every feature independently from the ground-truth event log
and compare it with what the pipeline actually served.
"""
from __future__ import annotations

from . import aggregations as _aggs
from .aggregations import MIN_GEO_DT_US, EARTH_RADIUS_KM, Window as _Feat, haversine_km  # noqa: F401 (re-exported)
from .schema import FeatureSchema, FeatureSpec


def _apply(s: FeatureSpec, f: _Feat, ev: dict, t: int, match: bool) -> float:
    """Fold a matching event into the feature's window, then return its value (the registered
    aggregation's `apply`; realy.features.aggregations)."""
    return _aggs.get(s.agg).apply(s, f, ev, t, match)


def _read(s: FeatureSpec, f: _Feat, ev: dict, t: int, match: bool) -> float:
    """The feature as of t for an event that is not folded into state (a re-scored lifecycle stage of a
    multi-message transaction, already counted when it was assembled): windows without it; novelty,
    time since the last event and speed against the previous events, without recording it. Nothing in
    `f` changes. Mirrors EntityAggregator.read (Java)."""
    return _aggs.get(s.agg).read(s, f, ev, t, match)


class ReferenceEngine:
    """Stateful per-entity aggregation over events processed in order."""

    def __init__(self, schema: FeatureSchema, entities: list[str] | None = None) -> None:
        self.schema = schema
        self.entities = entities or schema.stream_entities()
        self.specs = {e: schema.stream_features(e) for e in self.entities}
        self.aggs = {e: [_aggs.get(s.agg) for s in self.specs[e]] for e in self.entities}
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
            for s, f, a in zip(specs, feats, self.aggs[ent]):
                match = s.filter is None or bool(filter_mask >> self.filter_index[s.filter] & 1)
                out[s.index] = a.apply(s, f, ev, t, match)

    def read(self, ev: dict, filter_mask: int, out: list[float]) -> None:
        """Stream features of an event that is not folded into state (a re-scored stage), as of its time."""
        t = int(ev["event_time_us"])
        for ent in self.entities:
            specs = self.specs[ent]
            key = int(ev[self.schema.entities[ent]])
            feats = self.state[ent].get(key) if key else None
            for i, s in enumerate(specs):
                if key == 0:
                    out[s.index] = s.default
                    continue
                f = feats[i] if feats is not None else _Feat()
                match = s.filter is None or bool(filter_mask >> self.filter_index[s.filter] & 1)
                out[s.index] = self.aggs[ent][i].read(s, f, ev, t, match)
