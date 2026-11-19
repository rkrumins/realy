"""`realy train`: structural proxy models trained on synthetic data.

Point-in-time-correct training set: the feature vectors exactly as SERVED during a run (logged in the
decision records) joined with ground-truth labels — no train/serve skew by construction. Two
structural proxies are exported so the GPU executes real math and memory transfers of the right
shape before Data Science has finalised the real models:

  fraud_mlp   StandardScaler + MLP (hidden sizes configurable)   -> ONNX (Triton onnxruntime / TensorRT)
                                                                   + weights JSON (mock-model-server)
  fraud_gbdt  XGBoost GBDT (trees/depth configurable)           -> Triton FIL backend (xgboost_json)

`--input-dim` pads the tensor with zero columns to the production model's width (e.g. 512) so
host->device transfers and GEMMs match production; the scoring clients pad to MODEL_INPUT_DIM.
"""
from __future__ import annotations

import glob
import json
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ..features import load_schema
from ..scenario import REPO_ROOT

import os

MODELS = Path(os.environ.get("REALY_MODELS") or REPO_ROOT / "models")


LABEL_MODES = ("observed", "ground-truth")


def load_training_set(run_dirs: list[Path], labels: str = "observed", maturity_days: float = 30.0,
                      log=print) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, Any]]:
    """Served feature vectors + labels from finished runs. Returns X, y, true fraud_type, label stats.

    labels="observed" uses what the institution would KNOW when it trains, `maturity_days` after the
    last event of the training window: a row is fraud only if a fraud label was published
    (chargeback / customer report / investigation) and arrived by then. Never-reported fraud trains as
    legitimate; a friendly-fraud chargeback on a legitimate purchase trains as fraud. That is what
    a production model learns from. labels="ground-truth" uses the generator's truth (`is_fraud`):
    an upper bound no real institution has.
    """
    if labels not in LABEL_MODES:
        raise ValueError(f"labels must be one of {LABEL_MODES}")
    Xs, ys, ts, truth = [], [], [], []
    seen_ids: set[int] = set()
    have_observed = True
    for rd in run_dirs:
        feats = [pq.read_table(f) for f in sorted(glob.glob(str(rd / "features-*.parquet")))]
        cols = ["event_id", "is_fraud", "fraud_type", "event_time_us", "label_delay_us", "label_is_fraud", "backfill"]
        evs = []
        for f in sorted(glob.glob(str(rd / "events-*.parquet"))):
            names = pq.read_schema(f).names
            evs.append(pq.read_table(f, columns=[c for c in cols if c in names]))
        if not feats or not evs:
            continue
        f = pa.concat_tables(feats)
        e = pa.concat_tables(evs, promote_options="permissive")
        fid = f["event_id"].to_numpy()
        eid = e["event_id"].to_numpy()
        order = np.argsort(eid)
        pos = np.clip(np.searchsorted(eid[order], fid), 0, eid.size - 1)
        ok = eid[order][pos] == fid
        idx = order[pos[ok]]
        X = np.asarray(f["values"].to_pylist(), np.float32)[ok]
        # Benchmark event ids are deterministic per world+seed: the same event replayed in several runs
        # must not appear on both sides of the train/test split (leakage). Keep its first occurrence.
        ids = fid[ok]
        fresh = np.asarray([int(i) not in seen_ids for i in ids.tolist()], bool)
        seen_ids.update(int(i) for i in ids[fresh].tolist())
        y_true = e["is_fraud"].to_numpy()[idx][fresh].astype(np.int32)
        if "label_delay_us" in e.column_names:
            et = e["event_time_us"].to_numpy()
            delay = e["label_delay_us"].to_numpy()
            cut = int(et.max()) + int(maturity_days * 86_400e6)        # the day the model is trained
            known = (delay >= 0) & (et + delay <= cut)
            y_obs = (known & e["label_is_fraud"].to_numpy())[idx][fresh].astype(np.int32)
        else:
            have_observed = False
            y_obs = y_true
        Xs.append(X[fresh])
        truth.append(y_true)
        ys.append(y_obs if labels == "observed" else y_true)
        ts.append(e["fraud_type"].to_numpy()[idx][fresh])
    if not Xs:
        raise ValueError("no training data: runs need features-*.parquet and events-*.parquet")
    y, yt = np.concatenate(ys), np.concatenate(truth)
    if labels == "observed" and not have_observed:
        log("warn: these runs predate observed-label columns: training on ground truth instead")
    stats = {"labels": labels if have_observed else "ground-truth", "label_maturity_days": maturity_days,
             "rows": int(y.size), "true_fraud": int(yt.sum()), "labelled_fraud": int(y.sum()),
             # how far the training labels are from the truth: the noise a production model lives with
             "true_fraud_labelled": float((y & yt).sum() / max(1, yt.sum())),
             "labelled_fraud_actually_legit": float((y & (1 - yt)).sum() / max(1, y.sum()))}
    return np.concatenate(Xs), y, np.concatenate(ts), {**stats, "_truth": yt}


def _dedupe(run_dirs: list[Path]) -> list[Path]:
    return list(dict.fromkeys(Path(r) for r in run_dirs))


def _provenance(run_dirs: list[Path]) -> list[dict[str, Any]]:
    """What each training run was: lets a report tell whether it scores the data the model learnt from."""
    out = []
    for rd in run_dirs:
        rd = Path(rd)
        try:
            man = json.loads((rd / "manifest.json").read_text())
            sc = json.loads((rd / "scenario.json").read_text())
        except (OSError, ValueError):
            man, sc = {}, {}
        out.append({"run_id": rd.name, "scenario": man.get("scenario"), "seed": sc.get("seed"),
                    "world_hash": man.get("world_hash"), "world": sc.get("world")})
    return out


def train(run_dirs: list[Path], hidden: tuple[int, ...] = (64, 32), trees: int = 300, depth: int = 6,
          input_dim: int | None = None, seed: int = 42, out: Path = MODELS, log=print,
          labels: str = "observed", maturity_days: float = 30.0) -> dict[str, Any]:
    from sklearn.metrics import average_precision_score, roc_auc_score
    from sklearn.model_selection import train_test_split
    from sklearn.neural_network import MLPClassifier
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    schema = load_schema()
    X, y, _, lstats = load_training_set(run_dirs, labels, maturity_days, log)
    y_truth = lstats.pop("_truth")
    log(f"labels: {lstats['labels']} (maturity {maturity_days:g} d): {lstats['labelled_fraud']:,} labelled fraud vs "
        f"{lstats['true_fraud']:,} true; {lstats['true_fraud_labelled']:.1%} of true fraud labelled, "
        f"{lstats['labelled_fraud_actually_legit']:.1%} of fraud labels are legit purchases (friendly fraud)")
    n_feat = X.shape[1]
    dim = max(input_dim or n_feat, n_feat)
    if dim > n_feat:
        X = np.pad(X, ((0, 0), (0, dim - n_feat)))
    log(f"training set: {X.shape[0]:,} served vectors x {dim} features, fraud rate {y.mean():.3%}")
    Xtr, Xte, ytr, yte, _, yte_truth = train_test_split(X, y, y_truth, test_size=0.25, random_state=seed,
                                                       stratify=y if y.sum() >= 4 else None)
    # class imbalance: oversample fraud in the training split (MLPClassifier has no class_weight)
    pos = np.flatnonzero(ytr == 1)
    if pos.size:
        rep = max(1, int(0.2 * ytr.size / max(1, pos.size)))
        Xtr = np.concatenate([Xtr] + [Xtr[pos]] * (rep - 1))
        ytr = np.concatenate([ytr] + [ytr[pos]] * (rep - 1))
    mlp = Pipeline([("scale", StandardScaler()), ("mlp", MLPClassifier(hidden_layer_sizes=hidden, activation="relu",
                                                                         max_iter=60, early_stopping=True,
                                                                         random_state=seed))])
    mlp.fit(Xtr, ytr)
    p_mlp = mlp.predict_proba(Xte)[:, 1]
    import xgboost as xgb
    spw = float((ytr == 0).sum() / max(1, (ytr == 1).sum()))
    gbdt = xgb.XGBClassifier(n_estimators=trees, max_depth=depth, learning_rate=0.1, subsample=0.9,
                             colsample_bytree=0.9, scale_pos_weight=min(spw, 50.0), random_state=seed,
                             tree_method="hist", n_jobs=4, eval_metric="aucpr")
    gbdt.fit(Xtr, ytr)
    p_gb = gbdt.predict_proba(Xte)[:, 1]
    metrics = {}
    for name, p in (("fraud_mlp", p_mlp), ("fraud_gbdt", p_gb)):
        # holdout of the TRAINING world: in-sample by construction (same customers, cards, campaigns).
        # The honest number is an out-of-sample run (another seed / world) scored by the served model.
        metrics[name] = {"roc_auc": float(roc_auc_score(yte, p)) if yte.any() else None,
                         "pr_auc": float(average_precision_score(yte, p)) if yte.any() else None,
                         "roc_auc_vs_truth": float(roc_auc_score(yte_truth, p)) if yte_truth.any() else None,
                         "pr_auc_vs_truth": float(average_precision_score(yte_truth, p)) if yte_truth.any() else None,
                         "evaluated_on": "random holdout of the training runs (in-sample world)"}
    tag = "obs" if lstats["labels"] == "observed" else "gt"
    version = f"proxy-{tag}-{X.shape[0]}-{seed}"
    out = Path(out)
    files = export_mlp(mlp, dim, n_feat, version, out, schema.names)
    files += export_gbdt(gbdt, dim, version, out)
    # parity: weights JSON (what mock-model-server computes) vs sklearn vs ONNX runtime
    from ..verify.verify import ProxyScorer
    w_scores = ProxyScorer(out / "proxy" / "fraud_mlp.weights.json").score(Xte[:2000])
    import onnxruntime as ort
    sess = ort.InferenceSession(str(out / "triton" / "fraud_mlp" / "1" / "model.onnx"), providers=["CPUExecutionProvider"])
    o_scores = sess.run(["output__0"], {"input__0": Xte[:2000].astype(np.float32)})[0][:, 0]
    parity = {"weights_json_vs_sklearn_max_abs": float(np.abs(w_scores - p_mlp[:2000]).max()),
              "onnx_vs_sklearn_max_abs": float(np.abs(o_scores - p_mlp[:2000]).max())}
    meta = {"version": version, "rows": int(X.shape[0]), "fraud_rate": float(y.mean()), "input_dim": dim,
            "features": n_feat, "hidden": list(hidden), "trees": trees, "depth": depth, "metrics": metrics,
            "parity": parity, "files": [_rel(f) for f in files], "label_stats": lstats,
            "runs": [str(Path(r).name) for r in run_dirs], "provenance": _provenance(run_dirs)}
    (out / "proxy" / "training.json").write_text(json.dumps(meta, indent=1))
    # versioned copies: several trained models can be kept, compared and (re)deployed by version
    vdir = out / "proxy" / "versions"
    vdir.mkdir(parents=True, exist_ok=True)
    import shutil
    shutil.copy(out / "proxy" / "fraud_mlp.weights.json", vdir / f"{version}.weights.json")
    (vdir / f"{version}.training.json").write_text(json.dumps(meta, indent=1))
    meta["weights_path"] = str(vdir / f"{version}.weights.json")
    return meta


def _rel(p: Path) -> str:
    try:
        return str(Path(p).relative_to(REPO_ROOT))
    except ValueError:
        return str(p)


def training_meta(model_version: str | None, models: Path | None = None) -> dict[str, Any] | None:
    """Training metadata (incl. provenance) of a served model version, if this install trained it."""
    if not model_version:
        return None
    base = Path(models or MODELS) / "proxy"
    for p in (base / "versions" / f"{model_version}.training.json", base / "training.json"):
        try:
            meta = json.loads(p.read_text())
        except (OSError, ValueError):
            continue
        if meta.get("version") == model_version:
            return meta
    return None


def export_mlp(pipe, dim: int, n_feat: int, version: str, out: Path, names: list[str]) -> list[Path]:
    from skl2onnx import convert_sklearn
    from skl2onnx.common.data_types import FloatTensorType
    import onnx
    from onnx import helper, numpy_helper

    scaler, mlp = pipe.named_steps["scale"], pipe.named_steps["mlp"]
    layers = []
    acts = ["relu"] * (len(mlp.coefs_) - 1) + ["sigmoid"]
    for W, b, act in zip(mlp.coefs_, mlp.intercepts_, acts):
        layers.append({"W": W.T.astype(np.float32).tolist(), "b": b.astype(np.float32).tolist(), "activation": act})
    weights = {"name": "fraud_mlp", "version": version, "n_features": dim, "feature_names": names + [f"pad_{i}" for i in range(dim - n_feat)],
               "mean": scaler.mean_.astype(np.float32).tolist(), "std": np.where(scaler.scale_ == 0, 1, scaler.scale_).astype(np.float32).tolist(),
               "layers": layers}
    wp = out / "proxy" / "fraud_mlp.weights.json"
    wp.parent.mkdir(parents=True, exist_ok=True)
    wp.write_text(json.dumps(weights))
    model = convert_sklearn(pipe, initial_types=[("input__0", FloatTensorType([None, dim]))],
                            options={id(mlp): {"zipmap": False}}, target_opset=17)
    # Triton/KServe contract: FP32 output__0 [B, 1] = P(fraud). Slice probabilities[:, 1:2].
    g = model.graph
    prob = [o.name for o in g.output if "prob" in o.name][0]
    for nm, val in (("sl_start", [1]), ("sl_end", [2]), ("sl_axes", [1])):
        g.initializer.append(numpy_helper.from_array(np.asarray(val, np.int64), nm))
    g.node.append(helper.make_node("Slice", [prob, "sl_start", "sl_end", "sl_axes"], ["output__0"], name="p_fraud"))
    del g.output[:]
    g.output.append(helper.make_tensor_value_info("output__0", onnx.TensorProto.FLOAT, [None, 1]))
    onnx.checker.check_model(model)
    mp = out / "triton" / "fraud_mlp" / "1" / "model.onnx"
    mp.parent.mkdir(parents=True, exist_ok=True)
    mp.write_bytes(model.SerializeToString())
    cfg = out / "triton" / "fraud_mlp" / "config.pbtxt"
    cfg.write_text(f'''name: "fraud_mlp"
platform: "onnxruntime_onnx"
max_batch_size: 2048
input [ {{ name: "input__0", data_type: TYPE_FP32, dims: [ {dim} ] }} ]
output [ {{ name: "output__0", data_type: TYPE_FP32, dims: [ 1 ] }} ]
# Server-side dynamic batching: trade a bounded queue delay for GPU efficiency (L4).
dynamic_batching {{
  preferred_batch_size: [ 64, 256, 1024 ]
  max_queue_delay_microseconds: 500
}}
instance_group [ {{ count: 2, kind: KIND_GPU }} ]
optimization {{
  execution_accelerators {{
    gpu_execution_accelerator: [ {{ name: "tensorrt", parameters {{ key: "precision_mode" value: "FP16" }}
                                   parameters {{ key: "max_workspace_size_bytes" value: "1073741824" }} }} ]
  }}
}}
version_policy: {{ latest: {{ num_versions: 1 }} }}
parameters {{ key: "realy_version" value: {{ string_value: "{version}" }} }}
''')
    return [wp, mp, cfg]


def export_gbdt(model, dim: int, version: str, out: Path) -> list[Path]:
    d = out / "triton" / "fraud_gbdt" / "1"
    d.mkdir(parents=True, exist_ok=True)
    mp = d / "xgboost.json"
    model.get_booster().save_model(str(mp))
    cfg = out / "triton" / "fraud_gbdt" / "config.pbtxt"
    cfg.write_text(f'''name: "fraud_gbdt"
backend: "fil"
max_batch_size: 8192
input [ {{ name: "input__0", data_type: TYPE_FP32, dims: [ {dim} ] }} ]
output [ {{ name: "output__0", data_type: TYPE_FP32, dims: [ 1 ] }} ]
dynamic_batching {{ max_queue_delay_microseconds: 300 }}
instance_group [ {{ count: 1, kind: KIND_GPU }} ]
parameters [
  {{ key: "model_type" value: {{ string_value: "xgboost_json" }} }},
  {{ key: "output_class" value: {{ string_value: "false" }} }},
  {{ key: "predict_proba" value: {{ string_value: "false" }} }},
  {{ key: "storage_type" value: {{ string_value: "AUTO" }} }},
  {{ key: "use_experimental_optimizations" value: {{ string_value: "true" }} }},
  {{ key: "realy_version" value: {{ string_value: "{version}" }} }}
]
''')
    return [mp, cfg]
