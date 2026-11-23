"""The run's health watch turns a broken dependency into an abort with its reason (online store full,
evicting, restarted or flushed) and records Flink job restarts with their cause."""
from __future__ import annotations

from pathlib import Path

from realy.runner.flink import latest_retained_checkpoint
from realy.runner.infra import HealthWatch


def _watch():
    logs, aborts, notes = [], [], {}
    w = HealthWatch(logs.append, aborts.append, notes.__setitem__)
    return w, logs, aborts, notes


def _redis(used, cap=1000, **kw):
    return {"redis": {"used_memory": used, "maxmemory": cap, "maxmemory_policy": "noeviction", "evicted_keys": 0,
                      "uptime_in_seconds": kw.get("up", 100), "profiles_marker": kw.get("marker", True)}}


def test_store_filling_warns_then_aborts():
    w, logs, aborts, notes = _watch()
    w.check(_redis(500))
    assert not logs and not aborts
    w.check(_redis(900))
    assert any("90%" in x for x in logs) and not aborts
    w.check(_redis(980))
    assert aborts and "online store full" in aborts[0] and "REDIS_MAXMEMORY" in aborts[0]
    assert notes["store_memory_peak"] == 0.98


def test_restarted_or_flushed_store_aborts():
    w, _, aborts, _ = _watch()
    w.check(_redis(10, up=500))
    w.check(_redis(10, up=3))
    assert aborts and "restarted" in aborts[0]
    w2, _, aborts2, _ = _watch()
    w2.check(_redis(10, marker=True))
    w2.check(_redis(10, marker=False, up=200))
    assert aborts2 and "pf:_world" in aborts2[0]


def test_evictions_abort():
    w, _, aborts, notes = _watch()
    rec = _redis(10)
    w.check(rec)
    rec["redis"]["evicted_keys"] = 42
    rec["redis"]["maxmemory_policy"] = "allkeys-lru"
    w.check(rec)
    assert aborts and "evicted 42 keys" in aborts[0] and notes["store_evictions"] == 42


def test_flink_restarts_are_recorded_with_their_cause():
    w, logs, aborts, notes = _watch()
    job = {"jid": "j1", "state": "RUNNING", "restarts": 2.0, "last_failure": ""}
    w.check({"flink": {"_job:realy-features": dict(job)}})          # restarts before the run: baseline
    assert "flink_restarts" not in notes
    w.check({"flink": {"_job:realy-features": {**job, "state": "RESTARTING", "restarts": 3.0,
                                                "last_failure": "java.util.concurrent.TimeoutException: Heartbeat of TaskManager timed out"}}})
    assert notes["flink_restarts"] == 1 and "Heartbeat" in notes["flink_last_failure"]
    assert any("restarted" in x for x in logs) and not aborts


def test_latest_retained_checkpoint(tmp_path: Path):
    jid = "abc123"
    for n, complete in ((7, True), (12, True), (13, False)):
        d = tmp_path / jid / f"chk-{n}"
        d.mkdir(parents=True)
        if complete:
            (d / "_metadata").write_bytes(b"x")
    (tmp_path / jid / "shared").mkdir()
    assert latest_retained_checkpoint(jid, str(tmp_path), "file:///checkpoints") == f"file:///checkpoints/{jid}/chk-12"
    assert latest_retained_checkpoint("nope", str(tmp_path)) is None
    assert latest_retained_checkpoint(jid, "") is None


def test_a_lost_job_with_a_retained_checkpoint_keeps_its_state(tmp_path, monkeypatch):
    import realy.runner.flink as fl
    jid = "deadbeef00"
    (tmp_path / jid / "chk-41").mkdir(parents=True)
    (tmp_path / jid / "chk-41" / "_metadata").write_bytes(b"x")
    monkeypatch.setattr(fl, "CHECKPOINTS_LOCAL", str(tmp_path))
    monkeypatch.setattr(fl, "_running", lambda c: [])
    rec = {"jid": jid, "group": "realy-flink", "world": "world/fs-abc", "lineage": {"runs": ["r1"], "count": 1}}
    monkeypatch.setattr(fl, "job_record", lambda r=None: rec)
    monkeypatch.setattr(fl, "_store_has_stream_blobs", lambda r=None: True)
    p = fl.state_provenance("http://flink:8081")
    assert p["world"] == "world/fs-abc" and p["recoverable"].endswith(f"{jid}/chk-41") and p["lineage"]["count"] == 1
    monkeypatch.setattr(fl, "_store_has_stream_blobs", lambda r=None: False)
    p = fl.state_provenance("http://flink:8081")
    assert p["world"] is None and "lost its blobs" in p["why"]
