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
    """Stateful per-entity aggregation over events processed in order.

    Several event types can feed one entity's features (a login feeds customer, device and IP; a payment
    feeds all seven): an event folds into the features whose `event_types` lists its type (the default type when a
    feature names none), a scored event reads the others as of its time without folding, and a fold-only
    event (a login) leaves them alone. Mirrors EntityAggregator (Java)."""

    def __init__(self, schema: FeatureSchema, entities: list[str] | None = None, default_type: str | None = None,
                 scored_types: tuple[str, ...] | None = None) -> None:
        self.schema = schema
        self.entities = entities or schema.stream_entities()
        self.specs = {e: schema.stream_features(e) for e in self.entities}
        self.aggs = {e: [_aggs.get(s.agg) for s in self.specs[e]] for e in self.entities}
        # `gate:` features fold only while their gating feature (computed earlier, in `out`) is non-zero
        self.gates = {e: [schema.gate_index(s) for s in self.specs[e]] for e in self.entities}
        self.state: dict[str, dict[int, list[_Feat]]] = {e: {} for e in self.entities}
        self.filter_index = {name: i for i, name in enumerate(schema.filters)}
        if default_type is None or scored_types is None:
            from ..bindings.pipeline import pipeline
            p = pipeline()
            default_type = default_type or p.default_type
            scored_types = scored_types if scored_types is not None else tuple(p.scored())
        self.default_type = default_type
        self.scored_types = set(scored_types) | {default_type}
        # per entity: which of its features each type folds (cached per type)
        self._folds: dict[tuple[str, str], list[bool]] = {}

    def _fold_mask(self, ent: str, ty: str) -> list[bool]:
        k = (ent, ty)
        m = self._folds.get(k)
        if m is None:
            m = self._folds[k] = [self.schema.folds(s, ty, self.default_type) for s in self.specs[ent]]
        return m

    def process(self, ev: dict, filter_mask: int, out: list[float] | None, event_type: str | None = None) -> None:
        """Update with one event (a dict of its fields, entity keys under the schema's key names) and, for a
        scored type, write the stream features into `out`."""
        ty = event_type or self.default_type
        scored = ty in self.scored_types and out is not None
        t = int(ev["event_time_us"])
        for ent in self.entities:
            specs = self.specs[ent]
            key = int(ev.get(self.schema.entities[ent], 0) or 0)
            if key == 0:
                if scored:
                    for s in specs:
                        out[s.index] = s.default
                continue
            folds = self._fold_mask(ent, ty)
            feats = self.state[ent].get(key)
            if feats is None:
                if not any(folds):
                    if scored:                      # nothing to record: read an empty window
                        for s, a, g in zip(specs, self.aggs[ent], self.gates[ent]):
                            out[s.index] = a.read(s, _Feat(), ev, t, self._match(s, g, filter_mask, out))
                    continue
                feats = [_Feat() for _ in specs]
                self.state[ent][key] = feats
            for s, f, a, fold, g in zip(specs, feats, self.aggs[ent], folds, self.gates[ent]):
                if not fold and not scored:
                    continue
                match = self._match(s, g, filter_mask, out if scored else None)
                v = a.apply(s, f, ev, t, match) if fold else a.read(s, f, ev, t, match)
                if scored:
                    out[s.index] = v

    def read(self, ev: dict, filter_mask: int, out: list[float]) -> None:
        """Stream features of an event that is not folded into state (a re-scored stage), as of its time."""
        t = int(ev["event_time_us"])
        for ent in self.entities:
            specs = self.specs[ent]
            key = int(ev.get(self.schema.entities[ent], 0) or 0)
            feats = self.state[ent].get(key) if key else None
            for i, s in enumerate(specs):
                if key == 0:
                    out[s.index] = s.default
                    continue
                f = feats[i] if feats is not None else _Feat()
                match = self._match(s, self.gates[ent][i], filter_mask, out)
                out[s.index] = self.aggs[ent][i].read(s, f, ev, t, match)

    def _match(self, s: FeatureSpec, gate: int, filter_mask: int, out: list[float] | None) -> bool:
        """The feature's filter bit, and its gate: the gating feature (already in `out`) is non-zero; an event
        without a feature vector (a fold-only type) never passes a gate. Mirrors EntityAggregator.matches."""
        if s.filter is not None and not (filter_mask >> self.filter_index[s.filter] & 1):
            return False
        return gate < 0 or (out is not None and out[gate] != 0)
