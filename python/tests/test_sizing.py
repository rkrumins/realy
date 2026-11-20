"""`realy size`: capacity plan arithmetic, calibration from a measured run, generated overlay."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from realy.sizing import Inputs, plan, render, write_overlay
from realy.sizing.catalog import GKE
from realy.sizing.plan import erlang_c, gpu_batching, wait_p99_ms


def comp(p: dict, name: str) -> dict:
    return next(c for c in p["components"] if c["component"] == name)


def test_allocatable_leaves_room_for_system_pods():
    m = GKE["c3-standard-8"]
    assert 7.3 < m.alloc_cpu < 7.6
    assert 26 < m.alloc_mem_gib < 29
    assert GKE["g2-standard-8"].alloc_cpu < 8      # an 8-CPU request can never schedule


def test_queueing_helpers():
    assert erlang_c(1, 0.5) == pytest.approx(0.5)                # M/M/1: P(wait) = rho
    assert erlang_c(4, 4.5) == 1.0
    assert wait_p99_ms(4, 1000, 1.0) < wait_p99_ms(4, 3500, 1.0) < float("inf")
    b1, t1, _ = gpu_batching(10_000, 50_000, 1.0, 1.0)
    b2, t2, _ = gpu_batching(40_000, 50_000, 1.0, 1.0)
    assert 1 <= b1 < b2 and t1 < t2                              # batches grow with load
    assert gpu_batching(50_000, 50_000, 1.0, 1.0)[0] == float("inf")


def test_default_plan_for_10k_steady_20k_peak():
    p = plan(Inputs())
    assert p["inputs"]["design_tps"] == 20_000
    gw = comp(p, "scoring gateway")
    assert gw["pods"] >= 4 and gw["nodes"] % 3 == 0             # N+1, whole zones
    assert gw["pod"]["cpu"] <= GKE["c3-standard-8"].alloc_cpu / 2
    assert comp(p, "Kafka brokers")["pods"] % 3 == 0
    assert comp(p, "Kafka brokers")["detail"]["partitions"]["payments.events.raw"] >= 48
    assert comp(p, "model serving (Triton)")["pods"] >= 2        # HA
    fl = comp(p, "Flink feature job")["detail"]
    assert fl["parallelism"] <= comp(p, "Kafka brokers")["detail"]["partitions"]["payments.events.raw"]
    assert comp(p, "Redis online store")["detail"]["primary_shards"] >= 3
    assert p["latency"]["verdict"] == "FEASIBLE"
    assert all(c["source"] == "assumed" for c in p["costs"].values())
    assert "assumed costs" in render(p)


def test_calibration_from_a_measured_run(tmp_path: Path):
    run = tmp_path / "step-ramp-x"
    run.mkdir()
    rep = {"stages": {"hot_path": {"p50": 30.0, "p99": 80, "count": 10}},
           "resources": {"sources": {
               "gateway": {"cpu_ms_per_event": 1.0, "events_per_s": 2000, "utilisation": 0.95, "bytes_per_event": 4000},
               "flink": {"cpu_ms_per_event": 0.5, "events_per_s": 2000},
               "emitter": {"cpu_ms_per_event": 0.05, "bytes_per_event": 260, "events_per_s": 1000}},
               "infra": {"redis": {"ops_per_s": 34_000}}}}
    (run / "report.json").write_text(json.dumps(rep))
    base, cal = plan(Inputs()), plan(Inputs(run=run))
    assert cal["costs"]["gateway_cpu_ms"] == {**cal["costs"]["gateway_cpu_ms"], "value": 1.0, "source": "measured"}
    assert cal["costs"]["redis_ops_per_event"]["value"] == pytest.approx(17)
    assert comp(cal, "scoring gateway")["pods"] > comp(base, "scoring gateway")["pods"]
    assert comp(cal, "Flink feature job")["detail"]["parallelism"] < comp(base, "Flink feature job")["detail"]["parallelism"]
    assert any("gateway ran at 95%" in w for w in cal["warnings"])
    assert any("emitter cost measured at only 1,000 events/s" in w for w in cal["warnings"])
    over = plan(Inputs(run=run, overrides={"gateway_cpu_ms": 0.2}))
    assert over["costs"]["gateway_cpu_ms"]["source"] == "override"


def test_overlay_patches_the_gke_l4_manifests(tmp_path: Path):
    p = plan(Inputs(tps=20_000))
    path = write_overlay(p, tmp_path / "sized")
    k = yaml.safe_load(path.read_text())
    assert k["resources"][0].endswith("overlays/gke-l4")
    kinds = {x["target"]["kind"] for x in k["patches"]}
    assert {"Deployment", "HorizontalPodAutoscaler", "KafkaNodePool", "KafkaTopic", "FlinkDeployment",
            "InferenceService", "StatefulSet"} <= kinds
    flink = [json.loads(x["patch"]) for x in k["patches"] if x["target"]["kind"] == "FlinkDeployment"]
    par = comp(p, "Flink feature job")["detail"]["parallelism"]
    for ops in flink:
        args = next(o["value"] for o in ops if o["path"] == "/spec/job/args")
        assert args[args.index("--parallelism") + 1] == str(par)


def test_step_ramp_calibration_uses_the_level_below_the_knee(tmp_path: Path):
    run = tmp_path / "ramp"
    run.mkdir()
    lv = lambda i, tps, cpu: {"level": i, "target_tps": tps, "decided_tps": tps,  # noqa: E731
                              "sources": {"gateway": {"events_per_s": tps, "cpu_ms_per_event": cpu}}}
    rep = {"summary": {"knee": {"level": 3, "target_tps": 4000}},
           "resources": {"sources": {"gateway": {"cpu_ms_per_event": 0.9, "events_per_s": 2500}},
                         "levels": [lv(0, 1000, 0.9), lv(1, 2000, 0.6), lv(2, 3000, 0.5), lv(3, 4000, 0.7)]}}
    (run / "report.json").write_text(json.dumps(rep))
    c = plan(Inputs(run=run))["costs"]["gateway_cpu_ms"]
    assert c["value"] == 0.5 and c["source"] == "measured"      # level 2: highest before the knee
