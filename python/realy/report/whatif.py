"""`realy rules-whatif`: replay alternative rule sets / thresholds over a finished run, offline.

Every decision record carries the feature vector exactly as served and the model score, so a change to
the policy can be measured on the same traffic without re-running the pipeline: CEL rules are
re-evaluated (the same Python CEL used by `realy verify`, parity-checked against the Java engine) and the
outcome recomputed with the policy precedence. Ground truth comes from the run's event log.

Use it to tune on a TRAINING run and confirm on the evaluation run; tuning on the run you report is
fitting the test set.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa

from ..features import load_schema
from ..features.derived import event_view
from ..verify.verify import Rules, _read

OUT = {1: "APPROVE", 2: "REVIEW", 3: "DECLINE"}


def load(run_dir: Path, sample_mod: int = 1) -> dict[str, Any]:
    run_dir = Path(run_dir)
    events, feats, dec = _read(run_dir, "events-*.parquet"), _read(run_dir, "features-*.parquet"), _read(run_dir, "decisions-*.parquet")
    if events is None or feats is None or dec is None:
        raise ValueError(f"{run_dir}: needs events, features and decisions parquet")
    dec = dec.filter(pa.compute.equal(dec["revision"], 0))
    d_id = dec["event_id"].to_numpy()
    _, first = np.unique(d_id, return_index=True)
    dec = dec.take(pa.array(first))
    d_id = dec["event_id"].to_numpy()
    f_id = feats["event_id"].to_numpy()
    e_id = events["event_id"].to_numpy()
    ids = np.intersect1d(np.intersect1d(d_id, f_id), e_id)
    if sample_mod > 1:
        ids = ids[(ids % np.uint64(sample_mod)) == 0]
    pick = lambda arr: np.searchsorted(arr, ids, sorter=np.argsort(arr))  # noqa: E731
    di = np.argsort(d_id)[pick(d_id)]
    fi = np.argsort(f_id)[pick(f_id)]
    ei = np.argsort(e_id)[pick(e_id)]
    cols = [c for c in events.column_names if c not in ("t_intended_us", "backfill")]
    ev_cols = {c: events[c].to_numpy(zero_copy_only=False)[ei].tolist() for c in cols}
    vals = feats["values"].to_pylist()
    return {
        "ids": ids, "events": [{c: ev_cols[c][i] for c in cols} for i in range(ids.size)],
        "features": [vals[j] for j in fi.tolist()],
        "score": dec["score"].to_numpy()[di].astype(np.float64), "path": dec["path"].to_numpy()[di],
        "outcome": dec["outcome"].to_numpy()[di], "is_fraud": events["is_fraud"].to_numpy()[ei].astype(bool),
        "amount": events["amount_usd_minor"].to_numpy()[ei], "fraud_type": events["fraud_type"].to_numpy()[ei],
    }


def rescore(data: dict[str, Any], weights: str | Path) -> dict[str, Any]:
    """The same served feature vectors scored by another model (offline, the verifier's exact
    implementation of the proxy MLP). Every event counts as model-scored: this measures model + policy
    on identical traffic without the run's deadline fallbacks."""
    from ..verify.verify import ProxyScorer
    sc = ProxyScorer(weights)
    X = np.asarray(data["features"], np.float32)
    out = dict(data)
    out["score"] = sc.score(X).astype(np.float64)
    out["path"] = np.zeros(X.shape[0], np.int8)
    out["model_version"] = sc.version
    return out


def evaluate(data: dict[str, Any], rules_yaml: str, review: float | None = None, decline: float | None = None) -> dict[str, Any]:
    schema = load_schema()
    rules = Rules(rules_yaml)
    if review:
        rules.review = review
    if decline:
        rules.decline = decline
    n = data["ids"].size
    outcome = np.zeros(n, np.int8)
    per_rule: dict[str, list[int]] = {}
    names = [f.name for f in schema.features]
    for i in range(n):
        fmap = dict(zip(names, (float(x) for x in data["features"][i])))
        hits = rules.evaluate(fmap, event_view(data["events"][i]))
        score = float(data["score"][i]) if int(data["path"][i]) == 0 else math.nan
        outcome[i] = rules.decide(hits, score)
        for rid, _ in hits:
            per_rule.setdefault(rid, []).append(i)
    y = data["is_fraud"]
    flagged, declined = outcome >= 2, outcome == 3
    legit = ~y
    amt = data["amount"]
    res = {"rules": rules.version, "model": data.get("model_version", "as served"), "review": rules.review, "decline": rules.decline, "events": int(n),
           "fraud_events": int(y.sum()),
           "recall_flagged": float(flagged[y].mean()) if y.any() else None,
           "recall_declined": float(declined[y].mean()) if y.any() else None,
           "precision_flagged": float(y[flagged].mean()) if flagged.any() else None,
           "precision_declined": float(y[declined].mean()) if declined.any() else None,
           "false_positive_rate": float(flagged[legit].mean()) if legit.any() else None,
           "legit_declined_rate": float(declined[legit].mean()) if legit.any() else None,
           "fraud_usd_approved": float(amt[y & ~flagged].sum() / 100), "fraud_usd_total": float(amt[y].sum() / 100),
           "by_rule": {}, "by_type": {}}
    ft = data["fraud_type"]
    for t in sorted(set(int(x) for x in ft[y])):
        m = y & (ft == t)
        res["by_type"][int(t)] = {"events": int(m.sum()), "recall_flagged": float(flagged[m].mean()),
                                  "recall_declined": float(declined[m].mean()),
                                  "usd_approved": float(amt[m & ~flagged].sum() / 100)}
    for rid, idx in sorted(per_rule.items()):
        ix = np.asarray(idx)
        res["by_rule"][rid] = {"fires": int(ix.size), "fraud": int(y[ix].sum()), "precision": float(y[ix].mean()),
                               "legit_hit_rate": float((~y[ix]).sum() / max(1, legit.sum()))}
    return res


def render(rows: list[tuple[str, dict[str, Any]]]) -> str:
    keys = ("recall_flagged", "recall_declined", "precision_flagged", "precision_declined", "false_positive_rate",
            "legit_declined_rate", "fraud_usd_approved")
    out = [f"{'variant':34s} " + " ".join(f"{k[:16]:>16s}" for k in keys)]
    for name, r in rows:
        out.append(f"{name[:34]:34s} " + " ".join(
            f"{r[k]:16.4f}" if isinstance(r.get(k), float) and k != "fraud_usd_approved" else f"{r.get(k) or 0:16,.0f}"
            for k in keys))
    changed = sorted({k for _, r in rows for k in r["by_rule"]})
    out.append("")
    out.append(f"{'rule':34s} " + " ".join(f"{n[:22]:>22s}" for n, _ in rows) + "   (fires / precision)")
    for k in changed:
        cells = []
        for _, r in rows:
            v = r["by_rule"].get(k)
            cells.append(f"{v['fires']:>8d} / {v['precision']:.3f}".rjust(22) if v else "-".rjust(22))
        out.append(f"{k[:34]:34s} " + " ".join(cells))
    return "\n".join(out)
