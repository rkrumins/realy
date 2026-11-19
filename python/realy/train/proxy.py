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

MODELS = REPO_ROOT / "models"


def load_training_set(run_dirs: list[Path]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Served feature vectors + labels from finished runs. Returns X, y, fraud_type."""
    Xs, ys, ts = [], [], []
    seen_ids: set[int] = set()
    for rd in run_dirs:
        feats = [pq.read_table(f) for f in sorted(glob.glob(str(rd / "features-*.parquet")))]
        evs = [pq.read_table(f, columns=["event_id", "is_fraud", "fraud_type"]) for f in sorted(glob.glob(str(rd / "events-*.parquet")))]
        if not feats or not evs:
            continue
        f = pa.concat_tables(feats)
        e = pa.concat_tables(evs)
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
        Xs.append(X[fresh])
        ys.append(e["is_fraud"].to_numpy()[idx][fresh].astype(np.int32))
        ts.append(e["fraud_type"].to_numpy()[idx][fresh])
    if not Xs:
        raise ValueError("no training data: runs need features-*.parquet and events-*.parquet")
    return np.concatenate(Xs), np.concatenate(ys), np.concatenate(ts)


def _dedupe(run_dirs: list[Path]) -> list[Path]:
    return list(dict.fromkeys(Path(r) for r in run_dirs))


def train(run_dirs: list[Path], hidden: tuple[int, ...] = (64, 32), trees: int = 300, depth: int = 6,
          input_dim: int | None = None, seed: int = 42, out: Path = MODELS, log=print) -> dict[str, Any]:
    from sklearn.metrics import average_precision_score, roc_auc_score
    from sklearn.model_selection import train_test_split
    from sklearn.neural_network import MLPClassifier
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    schema = load_schema()
    X, y, _ = load_training_set(run_dirs)
    n_feat = X.shape[1]
    dim = max(input_dim or n_feat, n_feat)
    if dim > n_feat:
        X = np.pad(X, ((0, 0), (0, dim - n_feat)))
    log(f"training set: {X.shape[0]:,} served vectors x {dim} features, fraud rate {y.mean():.3%}")
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.25, random_state=seed, stratify=y if y.sum() >= 4 else None)
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
        metrics[name] = {"roc_auc": float(roc_auc_score(yte, p)) if yte.any() else None,
                         "pr_auc": float(average_precision_score(yte, p)) if yte.any() else None}
    version = f"proxy-{X.shape[0]}-{seed}"
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
            "parity": parity, "files": [str(f.relative_to(REPO_ROOT)) for f in files],
            "runs": [str(Path(r).name) for r in run_dirs]}
    (out / "proxy" / "training.json").write_text(json.dumps(meta, indent=1))
    return meta


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
