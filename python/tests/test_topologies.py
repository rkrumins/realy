"""The topology contract (contracts/topologies.yaml), the advice it gives from measured runs, the flows
variant of the live map, and the console endpoint that serves them."""
from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from realy import topologies as T
from realy.control import app as app_mod
from realy.control.topology import build


def test_the_contract_describes_every_topology_completely():
    assert T.validate() == []
    doc = T.load()
    assert set(doc["topologies"]) == set(T.names())
    # what the scenario model refuses, the contract says it cannot do
    assert doc["topologies"]["gateway-grpc"]["capabilities"]["multi_message"]["status"] == "no"
    assert doc["topologies"]["gateway-grpc"]["capabilities"]["answers_caller"]["status"] == "yes"


def test_the_validator_names_what_is_missing():
    doc = json.loads(json.dumps(T.load()))
    del doc["topologies"]["echo-kafka"]
    doc["topologies"]["flink-inline"]["capabilities"]["audit"]["status"] = True    # an unquoted yes
    doc["advice"]["nowhere"] = {"all": "x"}
    p = T.validate(doc)
    assert any("echo-kafka is not described" in x for x in p)
    assert any("flink-inline: audit: status" in x for x in p)
    assert any("'nowhere'" in x for x in p)


def test_advice_prefers_the_topologys_own_words():
    assert "gateway" in T.advice_for("ingress", "gateway-kafka")
    assert T.advice_for("ingress", "flink-inline") == T.load()["advice"]["ingress"]["all"]
    assert T.advice_for("unknown_stage", "gateway-kafka") == ""


def _report(**over):
    rep = {"topology": "flink-inline", "budget": {"e2e_p99_ms": 150.0},
           "summary": {"verdict": "FAIL", "e2e_p99_ms": 229.0, "achieved_emit_tps": 534, "degraded_fraction": 0.05},
           "tail_attribution": {"threshold_ms": 229.0, "share": {"egress": 0.42, "shuffle": 0.24},
                                "mean_ms_by_stage": {"egress": 90.0, "shuffle": 50.0}},
           "stages": {"schedule_lag": {"p99": 14.0}},
           "resources": {"sources": {"flink": {"utilisation": 0.82, "cpu_cores": 1.6, "cpus_allocated": 2,
                                               "events_per_s": 534, "cpu_ms_per_event": 2.2}}},
           "transactions": {"assembled": 100, "partial_fraction": 0.05, "assembly_wait_ms": {"p99": 145.0}}}
    rep.update(over)
    return rep


def test_run_advice_is_ranked_and_backed_by_evidence():
    adv = T.run_advice(_report(), verify={"features": {"exact_set_agreement": 1.0, "exact_set_target": 0.999},
                                          "rules": {"parity": 1.0}, "score": {"parity": 1.0}, "outcome": {"parity": 1.0}})
    titles = [a["title"] for a in adv]
    assert adv[0]["area"] == "latency" and "egress holds 42%" in adv[0]["title"]
    assert "229 ms against a 150 ms budget" in adv[0]["evidence"] and adv[0]["action"]
    assert any("without the model" in t for t in titles)              # 5% rules-only
    assert any("flink ran at 82%" in t for t in titles)               # capacity
    assert any("without a required part" in t for t in titles)        # flows: 5% partial
    assert not any(a["severity"] == "good" for a in adv)
    # a parity break outranks everything
    bad = T.run_advice(_report(), verify={"features": {}, "rules": {"parity": 0.99}})
    assert bad[0]["area"] == "correctness" and bad[0]["severity"] == "critical"
    # a clean pass says so, and what to do next
    ok = T.run_advice({"topology": "gateway-kafka", "budget": {"e2e_p99_ms": 150}, "summary":
                       {"verdict": "PASS", "e2e_p99_ms": 70, "achieved_emit_tps": 500}})
    assert ok[0]["severity"] == "good" and "step-ramp" in ok[0]["action"]


def test_the_map_shows_the_assembly_hop_when_the_run_has_flows():
    g = build("gateway-kafka", flows=True)
    ids = {n["id"] for n in g["nodes"]}
    assert {"assemble", "assembled"} <= ids
    edges = {(e["from"], e["to"]) for e in g["edges"]}
    assert ("assembled", "gateway") in edges and ("events", "gateway") not in edges
    plain = {(e["from"], e["to"]) for e in build("gateway-kafka")["edges"]}
    assert ("events", "gateway") in plain
    inline = build("flink-inline", flows=True)
    assert ("events", "assemble") in {(e["from"], e["to"]) for e in inline["edges"]}
    assert build("gateway-grpc", flows=True)["flows"] is False          # cannot carry flows


def test_the_console_serves_topologies_with_their_runs_and_advice(tmp_path: Path):
    runs = tmp_path / "runs"
    rd = runs / "r1"
    rd.mkdir(parents=True)
    (rd / "manifest.json").write_text(json.dumps({"run_id": "r1", "scenario": "flows-inline", "topology": "flink-inline",
                                                 "t_start_us": 1_800_000_000_000_000}))
    (rd / "report.json").write_text(json.dumps({**_report(), "run_id": "r1", "scenario": "flows-inline"}))
    scen = tmp_path / "scen"
    scen.mkdir()
    (scen / "flows-inline.yaml").write_text("name: flows-inline\ntopology: flink-inline\ntraffic: {flows: [a2a_transfer]}\n")
    c = TestClient(app_mod.create_app(runs_dir=runs, scenario_dir=scen, kafka=False, background=False,
                                      run_fn=lambda *a, **k: {}, publish_fn=lambda m: None))
    r = c.get("/api/topologies").json()
    assert set(r["capabilities"]) >= {"answers_caller", "multi_message"} and r["hybrid"]
    by = {t["id"]: t for t in r["topologies"]}
    assert set(by) == set(T.names())
    fi = by["flink-inline"]
    assert fi["scenarios"][0]["name"] == "flows-inline" and fi["scenarios"][0]["flows"] == ["a2a_transfer"]
    assert fi["latest"]["run_id"] == "r1" and fi["runs"][0]["verdict"] == "FAIL"
    assert fi["advice"] and fi["advice"][0]["topology"] == "flink-inline"
    assert by["echo-kafka"]["latest"] is None and by["echo-kafka"]["advice"] == []
    g = c.get("/api/topology", params={"topology": "gateway-kafka", "flows": "true"}).json()
    assert any(n["id"] == "assembled" for n in g["nodes"])
