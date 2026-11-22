"""The startup sequence: structured steps, consistent log lines, and catch-up measured from Flink's own
progress (not from committed offsets, which only move at checkpoints)."""
from __future__ import annotations

import json

from realy.runner import flink as flink_mod
from realy.runner.startup import STEPS, Startup, fmt_progress


def test_steps_log_consistently_and_persist(tmp_path):
    lines: list[str] = []
    st = Startup(tmp_path, lines.append, write_every_s=0, log_progress_every_s=0)
    st.start("history", "bulk replay")
    st.progress("history", 412_330, 583_063, "events", 9_800, 17.4)
    st.finish("history", "583,063 events sent")
    st.skip("prime", "scoring path already warm")
    n = len(STEPS)
    pos = {sid: i + 1 for i, (sid, _) in enumerate(STEPS)}
    h, p = pos["history"], pos["prime"]
    assert lines[0] == f"[startup {h}/{n}] history replay: bulk replay"
    assert lines[1] == f"[startup {h}/{n}] history replay: 71% (412,330/583,063 events), 9,800/s, ETA 17 s · bulk replay"
    assert lines[2].startswith(f"[startup {h}/{n}] history replay done in ") and lines[2].endswith(": 583,063 events sent")
    assert lines[3] == f"[startup {p}/{n}] scoring-path prime skipped in 0.0 s: scoring path already warm"
    snap = json.loads((tmp_path / "startup.json").read_text())
    by = {s["id"]: s for s in snap["steps"]}
    assert [s["id"] for s in snap["steps"]] == [sid for sid, _ in STEPS]
    assert by["history"]["status"] == "done" and by["prime"]["status"] == "skipped"
    assert by["world"]["status"] == "pending" and snap["active"] is None


def test_abort_marks_the_running_step_failed(tmp_path):
    st = Startup(tmp_path, lambda _m: None, write_every_s=0)
    st.start("catchup")
    st.fail_running("aborted (operator): stop")
    idx = [sid for sid, _ in STEPS].index("catchup")
    assert st.snapshot()["steps"][idx]["status"] == "failed"


def test_fmt_progress():
    assert fmt_progress(None) == ""
    assert fmt_progress({"done": 5, "total": 10, "unit": "", "rate": None, "eta_s": None}) == "50% (5/10)"


def test_catchup_follows_flink_progress_not_committed_offsets(monkeypatch):
    """Committed offsets stay at the whole backlog until a checkpoint; Flink's processed count moves."""
    from realy.common import kafka as kafka_mod
    from realy.runner.run import wait_features_caught_up
    progress = iter([100_000, 300_000, 583_063, 583_063, 583_063])
    monkeypatch.setattr(flink_mod, "job_progress",
                        lambda rest, jid=None: {"jid": "J", "processed": 1_000 + next(progress), "state": "RUNNING"})
    lags = iter([583_063, 583_063, 0, 0])
    monkeypatch.setattr(kafka_mod, "group_lag", lambda g, t, since_ms=None: next(lags))
    monkeypatch.setattr("time.sleep", lambda s: None)
    lines: list[str] = []
    st = Startup(None, lines.append, log_progress_every_s=0)
    res = wait_features_caught_up("http://flink", "realy-flink-r1", 583_063, {"jid": "J", "processed": 1_000, "start_ms": 1},
                                  2_915, 60, st, lines.append, poll_s=0)
    assert res["ok"] and res["mode"] == "flink" and res["processed"] == 583_063
    assert any("17%" in x or "51%" in x for x in lines)          # progress moved while committed lag did not
    assert st.steps["catchup"]["status"] == "done"


def test_catchup_falls_back_to_committed_offsets_when_the_job_restarts(monkeypatch):
    from realy.common import kafka as kafka_mod
    from realy.runner.run import wait_features_caught_up
    monkeypatch.setattr(flink_mod, "job_progress", lambda rest, jid=None: {"jid": "OTHER", "processed": 5, "state": "RUNNING"})
    lags = iter([50_000, 400])
    monkeypatch.setattr(kafka_mod, "group_lag", lambda g, t, since_ms=None: next(lags))
    clock = iter(range(0, 10_000, 6))
    monkeypatch.setattr("time.time", lambda: float(next(clock)))
    monkeypatch.setattr("time.sleep", lambda s: None)
    st = Startup(None, lambda _m: None)
    res = wait_features_caught_up("http://flink", "g", 100_000, {"jid": "J", "processed": 0}, 500, 600, st,
                                  lambda _m: None, poll_s=0)
    assert res["ok"] and res["mode"] == "committed" and res["lag"] == 400


def test_group_lag_counts_uncommitted_partitions_from_the_consumer_start(monkeypatch):
    """A new group must not inherit every older event the topic retains."""
    from realy.common import kafka as kafka_mod

    class TP:
        def __init__(self, topic, partition, offset=-1001):
            self.topic, self.partition, self.offset = topic, partition, offset

    class Fut:
        def __init__(self, v):
            self.v = v

        def result(self):
            return self.v

    class Admin:
        def __init__(self, _cfg):
            pass

        def list_topics(self, topic, timeout):
            class M:
                topics = {topic: type("T", (), {"partitions": {0: None, 1: None}})()}
            return M()

        def list_consumer_group_offsets(self, req):
            g = req[0]
            return {g.group_id: Fut(type("R", (), {"topic_partitions": [TP("t", 0, 900), TP("t", 1, -1001)]})())}

    class Cons:
        def __init__(self, _cfg):
            pass

        def get_watermark_offsets(self, tp, timeout):
            return (0, 1_000)                       # 1,000 retained per partition

        def offsets_for_times(self, tps, timeout):
            return [TP(t.topic, t.partition, 980) for t in tps]   # the job started at offset 980

        def close(self):
            pass

    import confluent_kafka
    monkeypatch.setattr(confluent_kafka, "ConsumerGroupTopicPartitions",
                        lambda g, parts: type("G", (), {"group_id": g})(), raising=False)
    monkeypatch.setattr(confluent_kafka, "TopicPartition", TP)
    monkeypatch.setattr(kafka_mod, "AdminClient", Admin)
    monkeypatch.setattr(kafka_mod, "Consumer", Cons)
    monkeypatch.setattr(kafka_mod, "bootstrap", lambda: "k:9092")
    assert kafka_mod.group_lag("g", "t") == 100 + 1_000                 # old behaviour: from the earliest offset
    assert kafka_mod.group_lag("g", "t", since_ms=123) == 100 + 20      # from where the consumer started
