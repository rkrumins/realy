"""Live topology node states: starting / warming / down / ready instead of 'red' during startup."""
from __future__ import annotations

import time

from realy.control.pipeline_state import node_states
from realy.control.topology import build

NODES = build("gateway-kafka")["nodes"]
NOW = time.time()
OLD_JOB = {"jid": "j", "name": "realy-features", "state": "RUNNING", "start_ms": (NOW - 600) * 1000, "vertices": 7,
           "vertices_running": 7}
HEALTHY = {"kafka": {"ok": True}, "redis": {"ok": True}, "gateway": {"ok": True}, "model": {"ok": True},
           "clickhouse": {"ok": True}, "flink": {"ok": True, "job": OLD_JOB}}


def _steps(**status) -> dict:
    out = {}
    for sid, v in status.items():
        st, prog = (v, None) if isinstance(v, str) else v
        out[sid] = {"id": sid, "status": st, "detail": "", "progress": prog}
    return out


def test_history_replay_is_warming_with_progress_not_red():
    steps = _steps(history=("running", {"done": 412_330, "total": 583_063, "unit": "events", "rate": 9_800, "eta_s": 17}))
    s = node_states(NODES, {"phase": "BACKFILL", "steps": steps, "probes": HEALTHY, "now": NOW, "uses_flink": True})
    assert s["flink"]["state"] == "warming" and "71%" in s["flink"]["reason"]
    assert abs(s["flink"]["progress"] - 412_330 / 583_063) < 1e-9
    assert s["redis"]["state"] == "warming" and "bulk load" in s["redis"]["reason"]
    assert s["emitter"]["state"] == "warming"
    assert s["gateway"]["state"] == "ready"


def test_catch_up_then_ready():
    steps = _steps(history="done", catchup=("running", {"done": 500_000, "total": 583_063, "unit": "events"}))
    s = node_states(NODES, {"phase": "BACKFILL", "steps": steps, "probes": HEALTHY, "now": NOW, "uses_flink": True})
    assert s["flink"]["state"] == "warming" and s["flink"]["reason"].startswith("catching up")
    steps = _steps(history="done", catchup="done", warmup="running")
    s = node_states(NODES, {"phase": "WARMUP", "steps": steps, "probes": HEALTHY, "now": NOW, "uses_flink": True})
    assert s["flink"]["state"] == "ready" and s["redis"]["state"] == "ready"


def test_job_submission_and_young_job_are_starting():
    s = node_states(NODES, {"phase": "PREPARE", "steps": _steps(flink="running"), "probes": HEALTHY, "now": NOW,
                            "uses_flink": True})
    assert s["flink"]["state"] == "starting"
    young = dict(HEALTHY, flink={"ok": True, "job": dict(OLD_JOB, start_ms=(NOW - 5) * 1000)})
    s = node_states(NODES, {"phase": "WARMUP", "steps": {}, "probes": young, "now": NOW, "uses_flink": True})
    assert s["flink"]["state"] == "starting" and "5 s ago" in s["flink"]["reason"]
    restarting = dict(HEALTHY, flink={"ok": True, "job": dict(OLD_JOB, state="RESTARTING")})
    assert node_states(NODES, {"phase": "MEASURE", "steps": {}, "probes": restarting, "now": NOW,
                               "uses_flink": True})["flink"]["state"] == "starting"


def test_down_when_unreachable_or_job_missing():
    gone = dict(HEALTHY, flink={"ok": True, "job": None}, redis={"ok": False, "error": "redis unreachable (ConnectionError)"})
    s = node_states(NODES, {"phase": "MEASURE", "steps": {}, "probes": gone, "now": NOW, "uses_flink": True})
    assert s["flink"] == {"state": "down", "reason": "no Flink job running", "progress": None}
    assert s["redis"]["state"] == "down"
    # no run active: a missing job is not an outage
    s = node_states(NODES, {"phase": None, "steps": {}, "probes": gone, "now": NOW, "uses_flink": False})
    assert s["flink"]["state"] == "ready"


def test_backpressure_degrades_only_while_measuring():
    bp = dict(HEALTHY, flink={"ok": True, "job": OLD_JOB, "backpressure": "high"})
    assert node_states(NODES, {"phase": "MEASURE", "steps": {}, "probes": bp, "now": NOW,
                               "uses_flink": True})["flink"]["state"] == "degraded"
    assert node_states(NODES, {"phase": "BACKFILL", "steps": {}, "probes": bp, "now": NOW,
                               "uses_flink": True})["flink"]["state"] == "ready"


def test_every_topology_gets_a_state_per_node():
    for topo in ("gateway-kafka", "gateway-grpc", "flink-inline", "echo-kafka"):
        nodes = build(topo)["nodes"]
        s = node_states(nodes, {"phase": "MEASURE", "steps": {}, "probes": HEALTHY, "now": NOW, "uses_flink": True})
        assert set(s) == {n["id"] for n in nodes}
