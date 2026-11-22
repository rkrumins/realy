"""Python twin hot-path cost on the golden payments set (REALY_BENCH=1 pytest tests/test_bench_hotpath.py -s).
Prints rows per second of the event view and of the verifier's per-event reference loop; recorded in
baselines/perf-bindings.json so the generic codec (Part S) can be compared with today's code."""
from __future__ import annotations

import json
import os
import time

import pyarrow.parquet as pq
import pytest

from realy import golden
from realy.features import load_schema
from realy.features.derived import event_view
from realy.features.engine import ReferenceEngine

pytestmark = pytest.mark.skipif(os.environ.get("REALY_BENCH") != "1", reason="benchmark: set REALY_BENCH=1")


def test_python_hot_path_rows_per_second():
    import cel
    t = pq.read_table(golden.OUT / "events.parquet")
    names = [c for c in t.column_names if c not in ("t_intended_us", "t_intended_offset_us")]
    lists = {k: t[k].to_pylist() for k in names}
    n = t.num_rows
    rows = [{k: lists[k][i] for k in names} for i in range(n)]
    t0 = time.perf_counter()
    for ev in rows:
        event_view(ev)
    view_rps = n / (time.perf_counter() - t0)
    schema = load_schema()
    filt = [cel.compile(x) for x in schema.filters.values()]
    engine = ReferenceEngine(schema)
    buf = [0.0] * len(schema.features)
    t0 = time.perf_counter()
    for ev in rows:
        ctx = cel.Context()
        ctx.add_variable("event", event_view(ev))
        ctx.add_variable("f", {})
        mask = sum(1 << b for b, p in enumerate(filt) if p.execute(ctx))
        engine.process(ev, mask, buf)
    ref_rps = n / (time.perf_counter() - t0)
    print(json.dumps({"bench": "python-hot-path", "events": n, "event_view_rows_s": round(view_rps),
                      "reference_rows_s": round(ref_rps)}))
