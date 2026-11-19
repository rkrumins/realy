"""Resource accounting from live metrics + infra samples (report `resources`, input to `realy size`)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from realy.report.resources import aggregate, render, resources

T0 = 1_800_000_000_000_000


def _snap(src: str, inst: str, t_s: float, counters: dict, stages: list | None = None) -> dict:
    return {"source": src, "instance": inst, "ts_us": T0 + int(t_s * 1e6), "counters": counters, "stages": stages or []}


def _write(run: Path, snaps: list[dict], infra: list[dict] | None = None) -> None:
    run.mkdir(parents=True, exist_ok=True)
    (run / "metrics.jsonl").write_text("\n".join(json.dumps(s) for s in snaps) + "\n")
    if infra:
        (run / "infra.jsonl").write_text("\n".join(json.dumps(s) for s in infra) + "\n")


def test_cpu_per_event_cores_and_utilisation(tmp_path: Path):
    snaps = []
    for t in range(1, 61):
        # two gateway replicas, 1000 decisions/s each, 400 ms CPU per second each (0.4 cores) on 4 CPUs
        for inst in ("gw-a", "gw-b"):
            snaps.append(_snap("gateway", inst, t, {"decisions": 1000, "cpu_ms": 400.0, "cpus": 4, "store_read_bytes": 300_000}))
        snaps.append(_snap("flink", "tm-1", t, {"cpu_ms": 1500.0, "cpus": 4},
                           [{"name": "state_customer", "count": 2000}, {"name": "state_card", "count": 2000}]))
        snaps.append(_snap("emitter", "shard-0", t, {"emitted": 2000, "cpu_ms": 250.0, "cpus": 2, "bytes": 500_000}))
    _write(tmp_path, snaps)
    agg = aggregate(snaps, T0 + 10_000_000, T0 + 60_000_000)
    gw = agg["gateway"]
    assert gw["instances"] == 2
    assert gw["events_per_s"] == pytest.approx(2000, rel=0.03)
    assert gw["cpu_cores"] == pytest.approx(0.8, rel=0.03)
    assert gw["cpu_ms_per_event"] == pytest.approx(0.4)
    assert gw["utilisation"] == pytest.approx(0.1, rel=0.03)
    assert gw["bytes_per_event"] == pytest.approx(300)
    assert agg["flink"]["cpu_ms_per_event"] == pytest.approx(0.75)      # counted once per event (customer stage)
    assert agg["emitter"]["bytes_per_event"] == pytest.approx(250)


def test_levels_and_infra_rates(tmp_path: Path):
    snaps = [_snap("gateway", "gw", t, {"decisions": 500 if t < 30 else 1000, "cpu_ms": 200.0 if t < 30 else 450.0, "cpus": 4})
             for t in range(1, 61)]
    infra = [{"ts_us": T0 + int(t * 1e6), "redis": {"total_commands_processed": 10_000 * t, "used_cpu_sys": 0.1 * t,
                                                     "used_cpu_user": 0.1 * t, "total_net_output_bytes": 2_000_000 * t,
                                                     "total_net_input_bytes": 500_000 * t, "used_memory": 1048576 * 64},
              "kafka": {"payments.events.raw": 1000 * t}} for t in range(0, 61, 5)]
    _write(tmp_path, snaps, infra)
    levels = [{"level": 0, "t_from_s": 0, "target_tps": 500, "decided_tps": 500},
              {"level": 1, "t_from_s": 30, "target_tps": 1000, "decided_tps": 1000}]
    res = resources(tmp_path, {"t_start_us": T0}, levels, duration_s=60, warmup_s=0)
    assert res["levels"][0]["sources"]["gateway"]["cpu_ms_per_event"] == pytest.approx(0.4)
    assert res["levels"][1]["sources"]["gateway"]["cpu_ms_per_event"] == pytest.approx(0.45)
    rd = res["infra"]["redis"]
    assert rd["ops_per_s"] == pytest.approx(10_000)
    assert rd["cpu_cores"] == pytest.approx(0.2)
    assert rd["net_out_mb_s"] == pytest.approx(2.0)
    assert res["infra"]["kafka_msgs_per_s"]["payments.events.raw"] == pytest.approx(1000)
    text = "\n".join(render(res))
    assert "gateway" in text and "redis:" in text


def test_missing_metrics_is_empty_not_an_error(tmp_path: Path):
    tmp_path.mkdir(exist_ok=True)
    assert resources(tmp_path, {"t_start_us": T0}, [], 60, 10)["sources"] == {}
    assert resources(tmp_path, {}, [], 60, 10) == {}
