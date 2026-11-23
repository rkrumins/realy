"""The aggregation registry, the Python twin of io.realy.scoring.features.Aggregations (Java).

An aggregation kind (feature_schema.yaml `agg:`) keeps its state in one of the shared shapes (buckets,
seen keys, last event, last event with location), so a new one needs no new state encoding. `apply` folds
a matching event into the window and returns the value ("update, then read"); `read` returns the value
without folding the event (a re-scored lifecycle stage). Both languages must give the same values: the
conformance fixtures (contracts/testdata/aggregations/conformance.json) are generated here and checked by
the Java test AggregationConformanceTest, so a Java aggregation without an identical Python twin fails CI.

Adding one (docs/dev/features.md, "A new aggregation"):

    from realy.features.aggregations import BucketAgg, register
    register(BucketAgg("min", "numeric", merge=min, count_merge=False,
                       window=lambda s, a, b, folded, any_: folded if any_ else s.default))

and the same in Java (a subclass of Aggregations.BucketAgg, registered through META-INF/services in a jar
in PLUGINS_DIR). The example plugin is java/plugin-example + realy/plugin_example.py; docs/dev/extending.md.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Callable

MIN_GEO_DT_US = 60_000_000
EARTH_RADIUS_KM = 6371.0088
SHAPES = ("buckets", "seen", "last", "last_geo")
INPUTS = ("none", "numeric", "key", "location", "optional_numeric")


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2) - math.radians(lon1)
    s1 = math.sin(dp / 2.0)
    s2 = math.sin(dl / 2.0)
    a = s1 * s1 + math.cos(p1) * math.cos(p2) * s2 * s2
    return 2.0 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(a)))


class Window:
    """One feature's state, by shape (what an aggregation reads and updates)."""
    __slots__ = ("max_k", "buckets", "seen", "last_ts", "lat", "lon")

    def __init__(self) -> None:
        self.max_k: int | None = None
        self.buckets: dict[int, list[float]] = {}   # bucket index -> [a, b]
        self.seen: dict[Any, int] = {}              # key -> last bucket
        self.last_ts: int | None = None
        self.lat = 0.0
        self.lon = 0.0


def _kn(s: Any, t: int) -> tuple[int, int]:
    g = s.granularity_us
    return t // g, (s.window_us // g if g else 0)


class Aggregation:
    """Base: a name, a state shape, what `value:` names, and apply / read."""
    name: str
    shape: str
    input: str

    def apply(self, s: Any, w: Window, ev: dict, t: int, match: bool) -> float:
        raise NotImplementedError

    def read(self, s: Any, w: Window, ev: dict, t: int, match: bool) -> float:
        raise NotImplementedError

    def sticky(self, window_us: int) -> bool:
        """Whether the feature's state must outlive the keyed-state TTL (a status that holds until it changes:
        `latest` without a window). Mirrors Aggregation.sticky (Java)."""
        return False


class BucketAgg(Aggregation):
    """A bucketed aggregation: a matching event adds its contribution to its bucket (merged with `merge`;
    `count_merge` adds 1 to b on a merge), buckets older than the window are evicted, and the window's
    buckets are folded by `window(s, sum_a, sum_b, folded, any)` (the sum of a by default)."""

    def __init__(self, name: str, input: str, merge: Callable[[float, float], float] | None = None,
                 count_merge: bool = True, contribution: Callable[[Any, dict], float] | None = None,
                 window: Callable[[Any, float, float, float, bool], float] | None = None) -> None:
        self.name, self.shape, self.input = name, "buckets", input
        self.merge = merge
        self.count_merge = count_merge
        self.contribution = contribution or (lambda s, ev: float(ev[s.value]))
        self.window = window or (lambda s, a, b, folded, any_: a)

    def _fold(self, s: Any, w: Window, k: int, n: int) -> float:
        a = bsum = 0.0
        folded = None
        for idx, (x, y) in w.buckets.items():
            if k - n < idx <= k:
                if not self.count_merge:
                    folded = x if folded is None else self.merge(folded, x)
                else:
                    a += x
                    bsum += y
        return self.window(s, a, bsum, folded if folded is not None else 0.0, folded is not None or bsum > 0)

    def apply(self, s: Any, w: Window, ev: dict, t: int, match: bool) -> float:
        k, n = _kn(s, t)
        w.max_k = k if w.max_k is None else max(w.max_k, k)
        if match:
            v = self.contribution(s, ev)
            b = w.buckets.get(k)
            if b is None:
                w.buckets[k] = [v, 1.0]
            elif self.count_merge:
                b[0] = b[0] + v if self.merge is None else self.merge(b[0], v)
                b[1] += 1.0
            else:
                b[0] = self.merge(b[0], v)
        lim = w.max_k - n
        for idx in [i for i in w.buckets if i <= lim]:
            del w.buckets[idx]
        return self._fold(s, w, k, n)

    def read(self, s: Any, w: Window, ev: dict, t: int, match: bool) -> float:
        k, n = _kn(s, t)
        return self._fold(s, w, k, n)


class Distinct(Aggregation):
    name, shape, input = "distinct", "seen", "key"

    def apply(self, s, w, ev, t, match):
        k, n = _kn(s, t)
        w.max_k = k if w.max_k is None else max(w.max_k, k)
        if match:
            key = ev[s.value]
            w.seen[key] = max(w.seen.get(key, k), k)
        lim = w.max_k - n
        for key in [x for x, last in w.seen.items() if last <= lim]:
            del w.seen[key]
        return float(sum(1 for last in w.seen.values() if last > k - n))

    def read(self, s, w, ev, t, match):
        k, n = _kn(s, t)
        return float(sum(1 for last in w.seen.values() if last > k - n))


class Novel(Aggregation):
    name, shape, input = "novel", "seen", "key"

    def apply(self, s, w, ev, t, match):
        k, n = _kn(s, t)
        w.max_k = k if w.max_k is None else max(w.max_k, k)
        lim = w.max_k - n
        for key in [x for x, last in w.seen.items() if last <= lim]:
            del w.seen[key]
        if not match:
            return 0.0
        key = ev[s.value]
        last = w.seen.get(key)
        seen = last is not None and last > k - n
        w.seen[key] = max(last, k) if last is not None else k
        return 0.0 if seen else 1.0

    def read(self, s, w, ev, t, match):
        if not match:
            return 0.0
        k, n = _kn(s, t)
        last = w.seen.get(ev[s.value])
        return 0.0 if (last is not None and last > k - n) else 1.0


class TimeSinceLast(Aggregation):
    name, shape, input = "time_since_last", "last", "none"

    def read(self, s, w, ev, t, match):
        return ((t - w.last_ts) / 1e6 if w.last_ts is not None and t >= w.last_ts and t - w.last_ts <= s.window_us
                else s.default)

    def apply(self, s, w, ev, t, match):
        v = self.read(s, w, ev, t, match)
        if match and (w.last_ts is None or t >= w.last_ts):
            w.last_ts = t
        return v


class GeoSpeed(Aggregation):
    name, shape, input = "geo_speed_kmh", "last_geo", "location"

    def read(self, s, w, ev, t, match):
        if match and w.last_ts is not None and t >= w.last_ts and t - w.last_ts <= s.window_us:
            km = haversine_km(w.lat, w.lon, ev["lat"], ev["lon"])
            return km / (max(t - w.last_ts, MIN_GEO_DT_US) / 3.6e9)
        return s.default

    def apply(self, s, w, ev, t, match):
        v = self.read(s, w, ev, t, match)
        if match and (w.last_ts is None or t >= w.last_ts):
            w.last_ts, w.lat, w.lon = t, ev["lat"], ev["lon"]
        return v


class Latest(Aggregation):
    """The latest value of a change feed (a card's status, an account's freeze), as of the event's time. With
    `value:`, a matching change sets it to that field (an enum reads as its number); without, every folded
    change sets it to its filter bit, so `card_blocked` is 1 after a blocking change and 0 after an unblocking
    one. A change older than the latest is ignored (it arrived late). Read before the latest change's time it
    gives the value before it; with a `window`, a value older than the window reads the default. State: the
    `last_geo` shape (time of the latest change, the value, the value before it). Mirrors Aggregations.Latest."""
    name, shape, input = "latest", "last_geo", "optional_numeric"

    def sticky(self, window_us: int) -> bool:
        return window_us == 0

    def read(self, s, w, ev, t, match):
        if w.last_ts is None:
            return s.default
        if t < w.last_ts:
            return w.lon
        return s.default if s.window_us > 0 and t - w.last_ts > s.window_us else w.lat

    def apply(self, s, w, ev, t, match):
        if (s.value is None or match) and (w.last_ts is None or t >= w.last_ts):
            v = (1.0 if match else 0.0) if s.value is None else float(ev[s.value])
            before = s.default if w.last_ts is None else w.lat
            w.last_ts, w.lat, w.lon = t, v, before
        return self.read(s, w, ev, t, match)


_REGISTRY: dict[str, Aggregation] = {}


def register(a: Aggregation) -> Aggregation:
    """Add an aggregation (a plugin, a test); a name already taken by another is refused."""
    if a.shape not in SHAPES or a.input not in INPUTS:
        raise ValueError(f"aggregation {a.name}: shape one of {SHAPES}, input one of {INPUTS}")
    prev = _REGISTRY.get(a.name)
    if prev is not None and prev is not a:
        raise ValueError(f"aggregation {a.name} is already registered")
    _REGISTRY[a.name] = a
    return a


_PLUGINS_LOADED = False


def _ensure_plugins() -> None:
    global _PLUGINS_LOADED
    if not _PLUGINS_LOADED:
        _PLUGINS_LOADED = True
        load_plugins()


def get(name: str) -> Aggregation:
    a = _REGISTRY.get(name)
    if a is None:
        _ensure_plugins()
        a = _REGISTRY.get(name)
    if a is None:
        raise KeyError(f"unknown aggregation {name!r} (registered: {', '.join(sorted(_REGISTRY))})")
    return a


def names() -> list[str]:
    _ensure_plugins()
    return sorted(_REGISTRY)


for _a in (BucketAgg("count", "none", contribution=lambda s, ev: 1.0),
           BucketAgg("sum", "numeric"),
           BucketAgg("avg", "numeric", window=lambda s, a, b, folded, any_: a / b if b > 0 else s.default),
           BucketAgg("max", "numeric", merge=max, count_merge=False,
                     window=lambda s, a, b, folded, any_: folded if any_ else s.default),
           Distinct(), Novel(), TimeSinceLast(), GeoSpeed(), Latest()):
    register(_a)
BUILTINS = tuple(sorted(_REGISTRY))


def builtin(name: str) -> bool:
    return name in BUILTINS


def extensions() -> list[str]:
    """Registered aggregations that are not built-ins (plugins)."""
    return [n for n in names() if n not in BUILTINS]


def load_plugins(modules: str | None = None) -> list[str]:
    """Import the twin modules of plugins (realy.plugins): `modules` (comma-separated) exactly, or
    REALY_PLUGINS and the `pulsebench.plugins` entry points. Each module registers its aggregations with
    `register`. Returns the modules imported."""
    from .. import plugins
    if modules is not None:
        return plugins.load([m.strip() for m in modules.split(",") if m.strip()])
    return plugins.load()




# ------------------------------------------------------------------------------- conformance fixtures
DEFAULT_CASES = [
    {"agg": "count", "window_s": 600, "granularity_s": 60, "default": 0.0},
    {"agg": "sum", "window_s": 3600, "granularity_s": 300, "default": 0.0},
    {"agg": "avg", "window_s": 3600, "granularity_s": 60, "default": -1.0},
    {"agg": "max", "window_s": 1800, "granularity_s": 60, "default": 0.0},
    {"agg": "distinct", "window_s": 3600, "granularity_s": 600, "default": 0.0},
    {"agg": "novel", "window_s": 7200, "granularity_s": 3600, "default": 0.0},
    {"agg": "time_since_last", "window_s": 86400, "granularity_s": 1, "default": -1.0},
    {"agg": "geo_speed_kmh", "window_s": 86400, "granularity_s": 1, "default": 0.0},
    {"agg": "latest", "window_s": 0, "granularity_s": 0, "default": 0.0},                  # a field's latest value
    {"agg": "latest", "window_s": 0, "granularity_s": 0, "default": 0.0, "value": None},   # the latest filter bit
    {"agg": "latest", "window_s": 3600, "granularity_s": 3600, "default": -1.0},          # expiring after an hour
]


def conformance_events(seed: int = 7, n_events: int = 60) -> list[dict[str, Any]]:
    """A deterministic event sequence with gaps inside a bucket, across buckets and past whole windows."""
    import random
    rnd = random.Random(seed)
    events = []
    t = 1_800_000_000_000_000
    for _ in range(n_events):
        t += rnd.choice([1_000_000, 15_000_000, 59_000_000, 61_000_000, 300_000_000, 3_700_000_000])
        events.append({"t": t, "value": round(rnd.uniform(1, 500), 2), "key": rnd.randint(1, 6),
                       "lat": round(rnd.uniform(-60, 60), 4), "lon": round(rnd.uniform(-170, 170), 4),
                       "match": rnd.random() < 0.8})
    return events


def conformance_cases(cases: list[dict[str, Any]] | None = None, seed: int = 7, n_events: int = 60) -> dict[str, Any]:
    """The values `read` and then `apply` give at each event of the sequence, per case (aggregation,
    window, granularity, default): the fixture both languages must reproduce
    (contracts/testdata/aggregations; checked in Java by AggregationConformance)."""
    events = conformance_events(seed, n_events)
    out = []
    for sp in cases or DEFAULT_CASES:
        a = get(sp["agg"])
        spec = _Spec(sp, a)
        w = Window()
        steps = []
        for e in events:
            ev = {"v": e["value"], "k": e["key"], "lat": e["lat"], "lon": e["lon"]}
            r = a.read(spec, w, ev, e["t"], e["match"])
            v = a.apply(spec, w, ev, e["t"], e["match"])
            steps.append({"read": r, "apply": v})
        out.append({**sp, "steps": steps})
    return {"events": events, "cases": out}


class _Spec:
    """The fields of a FeatureSpec an aggregation reads, for the fixtures."""

    def __init__(self, sp: dict[str, Any], a: Aggregation) -> None:
        # a case with `"value": null` runs an optional-value aggregation without one (`latest` of the filter bit)
        self.value = None if "value" in sp and sp["value"] is None else "k" if a.input == "key" else "v"
        self.window_us = int(sp["window_s"] * 1_000_000)
        self.granularity_us = int(sp["granularity_s"] * 1_000_000)
        self.default = float(sp["default"])


FIXTURES = Path(__file__).resolve().parents[3] / "contracts" / "testdata" / "aggregations"


def write_fixture(path: Path, cases: list[dict[str, Any]] | None = None) -> Path:
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(conformance_cases(cases), separators=(",", ":")) + "\n")
    return path


def _main(argv: list[str] | None = None) -> int:
    """`python -m realy.features.aggregations conformance [--plugin MOD] [--agg NAME[:window_s:granularity_s:default]]...
    [--out FILE]`: write a conformance fixture (the built-ins to contracts/testdata/aggregations/conformance.json
    by default; a plugin's cases to the file you name)."""
    import argparse
    ap = argparse.ArgumentParser(prog="python -m realy.features.aggregations")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("conformance", help="write a conformance fixture")
    c.add_argument("--plugin", action="append", default=[], help="twin module to import (registers its aggregations)")
    c.add_argument("--agg", action="append", default=[], help="NAME or NAME:window_s:granularity_s:default")
    c.add_argument("--out", type=Path, default=None)
    sub.add_parser("list", help="the registered aggregations")
    a = ap.parse_args(argv)
    global _PLUGINS_LOADED
    if a.cmd == "conformance" and a.plugin:
        _PLUGINS_LOADED = True
        load_plugins(",".join(a.plugin))
    if a.cmd == "list":
        for n in names():
            x = get(n)
            print(f"{n:18s} {x.shape:9s} {x.input:9s} {'built-in' if builtin(n) else 'plugin'}")
        return 0
    cases = None
    if a.agg:
        cases = []
        for item in a.agg:
            name, *rest = item.split(":")
            w, g, d = (float(x) for x in rest) if rest else (1800.0, 60.0, 0.0)
            cases.append({"agg": name, "window_s": w, "granularity_s": g, "default": d})
    out = a.out or FIXTURES / ("conformance.json" if cases is None else f"{cases[0]['agg']}.json")
    print(write_fixture(out, cases))
    return 0


if __name__ == "__main__":
    # run in the importable module, so plugins register into the same registry they import
    from realy.features import aggregations as _canonical
    raise SystemExit(_canonical._main())
