"""The `external` topology: your own scorer as a black box (docs/dev/external-scorer.md)."""
from __future__ import annotations

import json

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from realy import topologies as T
from realy.control.topology import build
from realy.infra import check_external_scorer
from realy.scenario import load_scenario
from realy.verify.verify import EXTERNAL_NOTE, render, verify_external


def test_runtime_configures_nothing_on_the_scorers_path():
    rt = T.runtime("external")
    assert rt.external and not rt.uses_flink and not rt.gateway_consumes_kafka and not rt.sync
    caps = T.load()["topologies"]["external"]["capabilities"]
    assert caps["owned_features_exact"]["status"] == "n/a"
    assert caps["audit"]["status"] == "yes"


def test_the_map_shows_your_scorer_between_the_topics():
    g = build("external", False)
    ids = [n["id"] for n in g["nodes"]]
    assert ids[:5] == ["emitter", "events", "scorer", "decisions", "collector"]
    assert {(e["from"], e["to"]) for e in g["edges"]} >= {("events", "scorer"), ("scorer", "decisions")}


def test_preflight_states_the_contract_without_probing():
    c = check_external_scorer()
    assert c["implementation"] == "external" and c["status"] == "pass"
    assert "x-bench-trace" in c["checks"][0]["detail"]


def _run(tmp_path, decided: list[int], outcomes: list[int]):
    d = tmp_path / "external-smoke-x"
    d.mkdir()
    sc = load_scenario("external-smoke")
    (d / "scenario.json").write_text(sc.model_dump_json())
    pq.write_table(pa.table({"event_id": np.arange(1, 6, dtype=np.uint64)}), d / "events-0.parquet")
    pq.write_table(pa.table({"event_id": np.asarray(decided, np.uint64), "revision": np.zeros(len(decided), np.uint8),
                             "outcome": np.asarray(outcomes, np.int8)}), d / "decisions-0.parquet")
    return d, sc


def test_verify_counts_missing_and_duplicate_decisions(tmp_path, monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_URL", "http://127.0.0.1:9")      # nothing listens: the audit says so
    d, sc = _run(tmp_path, [1, 2, 2, 3, 4], [1, 2, 2, 1, 4])
    dec = pq.read_table(d / "decisions-0.parquet")
    v = verify_external(d, sc, dec, 5, None, None)
    assert v["decisions"] == {"expected": 5, "decided_unique": 4, "missing": 1, "duplicates": 1, "invalid_outcomes": 0,
                              "outcomes": {"APPROVE": 2, "REVIEW": 2, "CHALLENGE": 1}}
    assert not v["passed"] and any("1 of 5 events have no decision" in r for r in v["reasons"])
    assert v["rules"]["not_checkable"] == EXTERNAL_NOTE and v["score"]["parity"] is None
    text = render(v)
    assert "features: not checkable (decided by your scorer" in text and "rules parity: not checkable" in text
    assert json.loads((d / "verify.json").read_text())["decisions"]["missing"] == 1


def test_verify_passes_a_complete_run_and_flags_invalid_outcomes(tmp_path, monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_URL", "http://127.0.0.1:9")
    d, sc = _run(tmp_path, [1, 2, 3, 4, 5], [1, 1, 3, 1, 2])
    v = verify_external(d, sc, pq.read_table(d / "decisions-0.parquet"), 5, None, None)
    assert v["passed"], v["reasons"]
    (tmp_path / "b").mkdir()
    d2, _ = _run(tmp_path / "b", [1, 2, 3, 4, 5], [1, 0, 3, 1, 2])
    v2 = verify_external(d2, sc, pq.read_table(d2 / "decisions-0.parquet"), 5, None, None)
    assert not v2["passed"] and v2["decisions"]["invalid_outcomes"] == 1


def test_the_reference_scorer_decides_and_copies_the_trace():
    from realy.external_scorer import decide
    from realy.payments.v1 import transaction_pb2 as epb
    from realy.scoring.v1 import decision_pb2 as dpb
    assert decide(epb.TransactionEvent(amount_usd_minor=1_000), 500_000) == (dpb.APPROVE, [])
    assert decide(epb.TransactionEvent(amount_usd_minor=600_000), 500_000)[0] == dpb.REVIEW


def test_bundled_scoring_jobs_are_stopped_so_they_cannot_compete(monkeypatch):
    from realy.runner import flink as F
    monkeypatch.setattr(F, "_running", lambda c: [{"name": "realy-inline+a2a_transfer", "jid": "a"},
                                                  {"name": "realy-features", "jid": "b"}, {"name": "realy-both", "jid": "c"}])
    stopped: list[str] = []
    monkeypatch.setattr(F, "_stop_with_savepoint", lambda c, jid, log: stopped.append(jid))
    assert F.stop_scoring_jobs("http://flink:8081", lambda m: None) == ["realy-inline+a2a_transfer", "realy-both"]
    assert stopped == ["a", "c"]                     # the features job keeps running: it decides nothing
    assert F.stop_scoring_jobs(None) == []
