"""Training on labels as production knows them, the in-/out-of-sample leakage guard, and the AUC metrics."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from realy.report.report import _evaluation, average_precision, roc_auc
from realy.train import proxy

DAY = 86_400_000_000


def _run(tmp: Path, name: str = "r", world_hash: str = "w1") -> Path:
    d = tmp / name
    d.mkdir()
    ids = np.arange(1, 9, dtype=np.uint64)
    t = np.full(8, 100 * DAY, np.int64)
    # 0: fraud reported in 5 d | 1: fraud reported in 40 d (late) | 2: fraud never reported
    # 3: legit with a friendly-fraud chargeback | 4..7: legit
    is_fraud = np.array([1, 1, 1, 0, 0, 0, 0, 0], bool)
    delay = np.array([5 * DAY, 40 * DAY, -1, 10 * DAY, -1, -1, -1, -1], np.int64)
    label = np.array([1, 1, 0, 1, 0, 0, 0, 0], bool)
    pq.write_table(pa.table({"event_id": ids, "event_time_us": t, "is_fraud": is_fraud, "fraud_type": np.zeros(8, np.int32),
                             "label_delay_us": delay, "label_is_fraud": label}), d / "events-0.parquet")
    pq.write_table(pa.table({"event_id": ids, "values": [[float(i)] * 3 for i in range(8)]}), d / "features-0.parquet")
    (d / "manifest.json").write_text(json.dumps({"scenario": "fraud-train", "world_hash": world_hash}))
    (d / "scenario.json").write_text(json.dumps({"seed": 7}))
    return d


def test_observed_labels_are_what_the_bank_knows(tmp_path):
    d = _run(tmp_path)
    _, y, _, st = proxy.load_training_set([d], "observed", maturity_days=30)
    assert y.tolist() == [1, 0, 0, 1, 0, 0, 0, 0]          # late + unreported fraud missing, friendly fraud in
    assert st["true_fraud"] == 3 and st["labelled_fraud"] == 2
    assert st["true_fraud_labelled"] == pytest.approx(1 / 3)
    assert st["labelled_fraud_actually_legit"] == pytest.approx(0.5)
    _, y60, _, _ = proxy.load_training_set([d], "observed", maturity_days=60)
    assert y60.tolist() == [1, 1, 0, 1, 0, 0, 0, 0]         # waiting longer recovers the late chargeback
    _, yt, _, st_t = proxy.load_training_set([d], "ground-truth")
    assert yt.tolist() == [1, 1, 1, 0, 0, 0, 0, 0] and st_t["labels"] == "ground-truth"


def test_leakage_guard(tmp_path, monkeypatch):
    models = tmp_path / "models"
    (models / "proxy" / "versions").mkdir(parents=True)
    meta = {"version": "proxy-obs-100-42", "label_stats": {"labels": "observed"},
            "provenance": [{"run_id": "fraud-train-1", "scenario": "fraud-train", "seed": 7, "world_hash": "w-train"}]}
    (models / "proxy" / "versions" / "proxy-obs-100-42.training.json").write_text(json.dumps(meta))
    monkeypatch.setattr(proxy, "MODELS", models)
    dec = pa.table({"model_version": ["proxy-obs-100-42"] * 3})
    rd = tmp_path / "run"
    rd.mkdir()
    (rd / "manifest.json").write_text(json.dumps({"world_hash": "w-eval"}))
    ev = _evaluation(rd, dec)
    assert ev["sample"] == "out-of-sample" and ev["labels"] == "observed"
    (rd / "manifest.json").write_text(json.dumps({"world_hash": "w-train"}))
    assert _evaluation(rd, dec)["sample"] == "in-sample"
    assert _evaluation(rd, pa.table({"model_version": ["default-1"]}))["sample"] == "not-trained"
    assert _evaluation(rd, pa.table({"model_version": ["proxy-gt-9-1"]}))["sample"] == "unknown"


def test_auc_metrics_match_definitions():
    y = np.array([0, 0, 1, 1, 0, 1], bool)
    s = np.array([0.1, 0.4, 0.35, 0.8, 0.2, 0.9])
    # 9 fraud/legit pairs, 8 ordered correctly (0.35 < 0.4)
    assert roc_auc(y, s) == pytest.approx(8 / 9)
    assert average_precision(y, s) == pytest.approx((1 + 1 + 0.75) / 3)
    assert roc_auc(y, np.ones(6)) == pytest.approx(0.5)


def test_clickhouse_exception_after_200_is_a_clear_error(monkeypatch):
    """ClickHouse streams results: an exception raised mid-query arrives in a 200 body."""
    import httpx

    from realy.verify import verify as v
    body = '{"n": 1}\nCode: 241. DB::Exception: Memory limit exceeded: while reading column features_raw'

    def fake_post(*a, **k):
        return httpx.Response(200, text=body, request=httpx.Request("POST", "http://ch"))
    monkeypatch.setattr(httpx, "post", fake_post)
    monkeypatch.setattr("time.sleep", lambda s: None)
    with pytest.raises(v.ClickHouseError, match="Memory limit"):
        v._q("http://ch", "SELECT 1")


def test_verifier_finds_hot_deployed_model_versions(tmp_path, monkeypatch):
    from realy.verify.verify import ProxyScorer
    src = json.loads((Path(proxy.REPO_ROOT) / "models" / "proxy" / "default.weights.json").read_text())
    (tmp_path / "proxy" / "versions").mkdir(parents=True)
    (tmp_path / "proxy" / "versions" / "proxy-obs-9-1.weights.json").write_text(json.dumps({**src, "version": "proxy-obs-9-1"}))
    monkeypatch.setattr(proxy, "MODELS", tmp_path)
    assert ProxyScorer.for_version("proxy-obs-9-1").version == "proxy-obs-9-1"


def test_world_switch_starts_state_fresh():
    from realy.runner.run import world_switch_decision
    assert world_switch_decision("aaa", "aaa", "warm", None) == ("warm", None, None)           # same world
    st, bf, why = world_switch_decision("aaa", "bbb", "warm", None, 1.0)
    assert (st, bf) == ("fresh", 1.0) and "aaa" in why                                         # switched: fresh + history
    assert world_switch_decision("aaa", "bbb", "warm", 2.0)[1] == 2.0                          # keeps its own backfill
    assert world_switch_decision("aaa", "bbb", "fresh", None)[2] is None                       # already fresh
    # unknown provenance is NOT "the same world": that assumption let a run measure over another
    # world's state (no record yet, or a record left by a run that never reset the state)
    st, bf, why = world_switch_decision(None, "aaa", "warm", None, 1.0, "no feature job running")
    assert (st, bf) == ("fresh", 1.0) and "unknown" in why and "no feature job" in why


class _KV:
    def __init__(self):
        self.kv = {}

    def get(self, k):
        return self.kv.get(k)

    def set(self, k, v):
        self.kv[k] = v.encode() if isinstance(v, str) else v


def test_state_provenance_is_bound_to_the_running_job(monkeypatch):
    from realy.runner import flink
    r = _KV()
    running = [{"jid": "job-A", "name": "realy-features"}]
    monkeypatch.setattr(flink, "_running", lambda c: running)
    assert flink.state_provenance("http://flink", r=r)["world"] is None                         # no record
    flink.record_job("job-A", "realy-flink-run1", "world-1", r=r)
    p = flink.state_provenance("http://flink", r=r)
    assert (p["world"], p["group"]) == ("world-1", "realy-flink-run1")
    running[0]["jid"] = "job-B"                                                                # restarted by hand
    p = flink.state_provenance("http://flink", r=r)
    assert p["world"] is None and "job-A" in p["why"]
    running.clear()
    assert "no feature job" in flink.state_provenance("http://flink", r=r)["why"]


def test_backfill_waits_on_the_group_the_running_job_commits_to(monkeypatch):
    """A job started fresh by an earlier run commits as realy-flink-<that run>; a later warm run that
    keeps it must wait on that group, not on realy-flink (which nothing consumes: a 900 s stall)."""
    from realy.runner import flink
    from realy.runner.run import feature_group
    from realy.scenario import load_scenario
    sc = load_scenario("smoke")
    sc.flink_state = "warm"
    monkeypatch.setattr(flink, "job_record", lambda r=None: {"jid": "j", "group": "realy-flink-fraud-eval-1"})
    assert feature_group(sc, "run2", log=lambda m: None) == "realy-flink-fraud-eval-1"
    monkeypatch.setattr(flink, "job_record", lambda r=None: None)
    assert feature_group(sc, "run2", log=lambda m: None) == "realy-flink"
    sc.flink_state = "fresh"
    assert feature_group(sc, "run2", log=lambda m: None) == "realy-flink-run2"


def test_profile_seed_marker_names_the_current_world():
    from realy.runner import seed

    class R:
        def __init__(self):
            self.kv = {}

        def get(self, k):
            return self.kv.get(k)

        def set(self, k, v):
            self.kv[k] = v.encode() if isinstance(v, str) else v

        def pipeline(self, transaction=False):
            outer = self

            class P:
                def set(self, k, v):
                    outer.kv[k] = v

                def execute(self):
                    return []
            return P()

    class W:
        def __init__(self, h):
            self.content_hash = h

        def customer_profile_arrays(self):
            return {"customer_id": np.array([1, 2]), "tenure_days": np.array([1, 2]), "segment": np.array([0, 1]),
                    "avg_ticket_usd": np.array([10, 20]), "home_lat": np.zeros(2), "home_lon": np.zeros(2),
                    "home_country_idx": np.zeros(2), "tz_offset_min": np.zeros(2)}

        def merchant_profile_arrays(self):
            return {"merchant_id": np.array([1]), "risk_tier": np.array([1]), "fraud_rate_30d": np.array([0.0])}
    r = R()
    assert seed.seed_profiles(W("A"), r=r) > 0 and seed.seed_profiles(W("A"), r=r) == 0
    assert seed.seed_profiles(W("B"), r=r) > 0          # another world overwrote pf:* ...
    assert seed.seed_profiles(W("A"), r=r) > 0          # ... so switching back re-seeds (used to be skipped)


def test_state_lineage_records_which_runs_built_the_state(monkeypatch):
    """Every run that sends events into the feature state is added to the running job's record
    before it sends any; note_run returns what the state held BEFORE this run (what its features
    also see). A fresh job starts empty; records from before lineage existed read as unknown."""
    from realy.runner import flink
    r = _KV()
    monkeypatch.setattr(flink, "_running", lambda c: [{"jid": "job-A", "name": "realy-features"}])
    flink.record_job("job-A", "g", "w", r=r, lineage={"runs": [], "count": 0})
    assert flink.note_run("run-1", r=r) == {"runs": [], "count": 0}                 # fresh: nothing inherited
    before = flink.note_run("run-2", r=r)
    assert before == {"runs": ["run-1"], "count": 1}
    p = flink.state_provenance("http://flink", r=r)
    assert p["lineage"] == {"runs": ["run-1", "run-2"], "count": 2}
    for i in range(3, flink.LINEAGE_KEEP + 10):
        flink.note_run(f"run-{i}", r=r)
    lin = flink.state_provenance("http://flink", r=r)["lineage"]
    assert len(lin["runs"]) == flink.LINEAGE_KEEP and lin["count"] == flink.LINEAGE_KEEP + 9 and lin["runs"][-1] == f"run-{flink.LINEAGE_KEEP + 9}"
    # a record written before lineage existed: which runs built the state is unknown
    r.set(flink._job_key(), json.dumps({"jid": "job-A", "group": "g", "world": "w"}))
    assert flink.state_provenance("http://flink", r=r)["lineage"]["unknown"] is True
    assert flink.note_run("run-x", r=r)["unknown"] is True


def test_a_verified_run_starts_fresh_when_the_warm_state_holds_other_runs():
    """Exact verification replays the run's own events: warm state that earlier runs of the same world
    fed (same simulated window) cannot be reproduced (measured: 74% exact-set agreement), so a run
    that will be verified starts fresh, and says why."""
    from realy.runner.run import verify_needs_fresh
    from realy.scenario import load_scenario
    sc = load_scenario("smoke")
    sc.flink_state = "warm"
    inherited = {"lineage": {"runs": ["mixed-realistic-1", "smoke-2"], "count": 2}}
    why = verify_needs_fresh(sc, inherited, True)
    assert why and "2 earlier run(s)" in why and "smoke-2" in why
    assert verify_needs_fresh(sc, inherited, False) is None                                   # not verified: keep warm
    assert verify_needs_fresh(sc, {"lineage": {"runs": [], "count": 0}}, True) is None        # nothing inherited
    assert "not recorded" in verify_needs_fresh(sc, {"lineage": None}, True)                  # unknown: fresh
    sc.flink_state = "fresh"
    assert verify_needs_fresh(sc, inherited, True) is None                                    # already fresh
    echo = load_scenario("echo-calibration")
    assert verify_needs_fresh(echo, inherited, True) is None                                  # no feature job


def test_verify_does_not_judge_features_read_from_inherited_state():
    from realy.verify.verify import NOT_COMPARABLE, features_comparable, judge
    assert features_comparable(None) == (True, None)                                          # older runs: as before
    assert features_comparable({"mode": "none"}) == (True, None)
    assert features_comparable({"mode": "fresh", "inherited_runs": 0, "inherited": []}) == (True, None)
    ok, why = features_comparable({"mode": "warm", "inherited_runs": 3, "inherited": ["a", "b"]})
    assert not ok and "3 earlier run(s)" in why and "--verify" in why
    assert features_comparable({"mode": "warm", "unknown": True})[0] is False

    def v(comparable=True, exact=0.999, parity=1.0, complete=True):
        return {"rules": {"parity": parity}, "score": {"parity": 1.0}, "outcome": {"parity": 1.0},
                "features": {"comparable": comparable, "exact_set_ok": exact >= 0.98, "exact_set_agreement": exact,
                             "exact_set_target": 0.98},
                "audit": {"complete": complete, "audited_unique": 9, "emitted": 10}}
    assert judge(v()) == (True, [])
    assert judge(v(comparable=False, exact=0.74)) == (False, [NOT_COMPARABLE])                # inconclusive, not "74% wrong"
    passed, reasons = judge(v(exact=0.74, parity=0.99, complete=False))
    assert not passed and len(reasons) == 3 and any("74.0000%" in x for x in reasons)
    assert any("rules parity" in x for x in reasons) and any("audit incomplete" in x for x in reasons)


def test_p99_drift_is_judged_only_where_the_load_is_constant():
    """A live rate change (or a scheduled step) raises p99 by design; drift is a rise at constant load."""
    from realy.report.report import _constant_rate_segments, _window_medians
    from realy.scenario import load_scenario
    sc = load_scenario("smoke")
    t0 = 1_000_000_000_000
    assert _constant_rate_segments(sc, {}, t0, 5, 40) == [(5, 40)]
    run = {"rate_changes": [{"at_us": t0 + 12_000_000, "mult": 2.0}]}
    assert _constant_rate_segments(sc, run, t0, 5, 40) == [(5, 12), (22, 40)]          # 10 s settle after the change
    at_start = {"rate_changes": [{"at_us": t0 + 5_975_000, "mult": 2.0}]}                # measured: right at warm-up end
    assert _constant_rate_segments(sc, at_start, t0, 5, 40) == [(5, 5), (16, 40)][1:]
    early = {"rate_changes": [{"at_us": t0 + 2_000_000, "mult": 2.0}]}                   # during warm-up: settle spills over
    assert _constant_rate_segments(sc, early, t0, 5, 40) == [(12, 40)]
    sc.traffic.profile.kind = "ramp"
    assert _constant_rate_segments(sc, run, t0, 5, 40) == []                            # load varies by design
    sc.traffic.profile.kind, sc.traffic.profile.step_duration_s = "step", 20
    assert _constant_rate_segments(sc, {}, t0, 0, 40) == [(4, 20), (24, 40)]           # each level after its 20% transition
    s = [10.0] * 10 + [20.0] * 10 + [30.0] * 5
    assert _window_medians(s, 0, 25, 10) == [10.0, 20.0]                               # trailing partial window dropped


STEP_UP = """
version: t
thresholds: {review: 0.5, challenge: 0.6, decline: 0.9}
challenge:
  when: "!event.recurring && event.entry_mode in ['ECOM_3DS', 'ECOM_NON_3DS', 'CHIP']"
rules:
  - {id: D, action: DECLINE, when: "f.x >= 3.0"}
  - {id: C, action: CHALLENGE, when: "f.x >= 2.0"}
  - {id: R, action: REVIEW, when: "f.x >= 1.0"}
"""


def test_step_up_precedence_mirrors_java():
    """Same cases as RuleSetTest.stepUpPrecedenceAndChallengeability (Java): the two must agree."""
    import math

    from realy.policy import APPROVE, CHALLENGE, DECLINE, REVIEW, RuleSet
    r = RuleSet(STEP_UP)
    assert r.uses_challenge
    assert r.challengeable({"recurring": False, "entry_mode": "ECOM_3DS"})
    assert not r.challengeable({"recurring": True, "entry_mode": "ECOM_3DS"})
    assert not r.challengeable({"recurring": False, "entry_mode": "MAGSTRIPE"})
    C, R, DC = [("C", "CHALLENGE")], [("R", "REVIEW")], [("D", "DECLINE"), ("C", "CHALLENGE")]
    assert r.decide(DC, 0.1, True) == DECLINE
    assert r.decide(C, 0.1, True) == CHALLENGE and r.decide(C, 0.1, False) == REVIEW
    assert r.decide(C, 0.95, True) == DECLINE
    assert r.decide([], 0.7, True) == CHALLENGE and r.decide([], 0.7, False) == REVIEW
    assert r.decide(R, 0.7, True) == CHALLENGE and r.decide(R, 0.1, True) == REVIEW
    assert r.decide([], 0.55, True) == REVIEW
    assert r.decide(C, math.nan, True) == CHALLENGE and r.decide([], math.nan, True) == APPROVE
    assert r.with_thresholds(challenge=0.8).decide([], 0.7, True) == REVIEW
    from realy.scenario import REPO_ROOT
    plain = RuleSet((REPO_ROOT / "rules" / "baseline-v3.yaml").read_text())
    assert not plain.uses_challenge and plain.decide([], 0.7, True) == REVIEW


def test_step_up_results_follow_the_population_and_end_the_payment():
    import numpy as np

    from realy import challenge as C
    ids = np.arange(1, 100_001, dtype=np.uint64)
    fr = np.zeros(ids.size, bool)
    fr[::2] = True
    ft = np.where(fr, 2, 0)                                   # half the events: account takeover (pass 0.35)
    r = C.draw(ids, fr, ft, 42, (0.90, 0.08), {2: (0.35, 0.0)})
    assert abs((r[~fr] == C.PASS).mean() - 0.90) < 0.01 and abs((r[~fr] == C.ABANDON).mean() - 0.08) < 0.01
    assert abs((r[fr] == C.PASS).mean() - 0.35) < 0.01 and (r[fr] == C.ABANDON).sum() == 0
    assert (C.draw(ids, fr, ft, 42, (0.90, 0.08), {2: (0.35, 0.0)}) == r).all()        # deterministic
    assert (C.draw(ids[::-1], fr[::-1], ft[::-1], 42, (0.90, 0.08), {2: (0.35, 0.0)}) == r[::-1]).all()  # order-free
    assert (C.draw(ids, fr, ft, 43, (0.90, 0.08), {2: (0.35, 0.0)}) != r).any()       # salted by the seed
    out = np.array([1, 2, 3, 4, 4, 4])
    res = np.array([C.FAIL, C.FAIL, C.PASS, C.PASS, C.FAIL, C.ABANDON])
    assert C.final(out, res).tolist() == [1, 2, 3, C.APPROVED, C.DECLINED, C.ABANDONED]
