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
