"""`realy verify`: independent correctness checks of a finished run.

  1. features   recompute every feature from the ground-truth event log with the Python reference
                engine (feature DSL mirror) and compare with what the pipeline SERVED to the model
                (exact match expected for customer/card features in the Flink-inline topology; in
                gateway topologies the difference measures online-store staleness)
  2. rules      re-evaluate the CEL rule set (Python CEL) on the served vectors -> identical hits
  3. score      recompute the proxy-model score from the served vector (numpy float32)
  4. outcome    re-derive the decision from hits + score
  5. audit      ClickHouse: completeness for the run, duplicate collapse, audit visibility lag,
                replay of a sample from the stored inputs (features_raw)
"""
from __future__ import annotations

import glob
import json
import math
import os
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from ..features import load_schema
from ..features.derived import compute_derived, event_view
from ..features.engine import ReferenceEngine
from ..scenario import REPO_ROOT, Scenario

OUTCOME = {1: "APPROVE", 2: "REVIEW", 3: "DECLINE"}
EXACT_ENTITIES = ("customer", "card")


def _read(run_dir: Path, pattern: str) -> pa.Table | None:
    files = sorted(glob.glob(str(run_dir / pattern)))
    return pa.concat_tables([pq.read_table(f) for f in files], promote_options="permissive") if files else None


class ProxyScorer:
    @classmethod
    def for_version(cls, version: str | None) -> "ProxyScorer":
        """Weights file whose `version` matches the model version recorded in the decisions."""
        if version:
            for p in sorted((REPO_ROOT / "models" / "proxy").glob("*.weights.json")):
                try:
                    if json.loads(p.read_text()).get("version") == version:
                        return cls(p)
                except ValueError:
                    continue
        return cls()

    def __init__(self, path: str | Path | None = None) -> None:
        path = path or os.environ.get("MODEL_WEIGHTS") or REPO_ROOT / "models" / "proxy" / "default.weights.json"
        w = json.loads(Path(path).read_text())
        self.mean = np.asarray(w["mean"], np.float32)
        self.std = np.asarray(w["std"], np.float32)
        self.std[self.std == 0] = 1
        self.layers = [(np.asarray(l["W"], np.float32), np.asarray(l["b"], np.float32), l.get("activation", "relu"))
                       for l in w["layers"]]
        self.version = w.get("version")

    def score(self, x: np.ndarray) -> np.ndarray:
        h = ((x.astype(np.float32) - self.mean) / self.std).astype(np.float32)
        for W, b, act in self.layers:
            h = (h @ W.T + b).astype(np.float32)
            if act == "relu":
                h = np.maximum(h, 0)
            elif act == "sigmoid":
                h = (1.0 / (1.0 + np.exp(-h.astype(np.float64)))).astype(np.float32)
            elif act == "tanh":
                h = np.tanh(h)
        return h[:, 0]


class Rules:
    def __init__(self, yaml_text: str) -> None:
        import cel
        doc = yaml.safe_load(yaml_text)
        self.version = doc.get("version")
        self.review = float(doc["thresholds"]["review"])
        self.decline = float(doc["thresholds"]["decline"])
        self.rules = [(r["id"], r["action"], cel.compile(r["when"])) for r in doc["rules"]]

    def evaluate(self, f: dict[str, float], ev: dict[str, Any]) -> list[tuple[str, str]]:
        import cel
        ctx = cel.Context()
        ctx.add_variable("f", f)
        ctx.add_variable("event", ev)
        return [(rid, act) for rid, act, p in self.rules if p.execute(ctx)]

    def decide(self, hits: list[tuple[str, str]], score: float) -> int:
        acts = {a for _, a in hits}
        if "DECLINE" in acts:
            return 3
        if "ALLOW" in acts or "APPROVE" in acts:
            return 1
        has = not math.isnan(score)
        if has and score >= self.decline:
            return 3
        if "REVIEW" in acts:
            return 2
        if has and score >= self.review:
            return 2
        return 1


def verify_run(run_dir: Path | str, clickhouse: str | None = None, max_events: int = 1_000_000,
               log=print) -> dict[str, Any]:
    import cel
    from ..world import World
    run_dir = Path(run_dir)
    sc = Scenario.model_validate(json.loads((run_dir / "scenario.json").read_text()))
    schema = load_schema()
    nf = len(schema.features)
    out: dict[str, Any] = {"run_id": run_dir.name}
    events = _read(run_dir, "events-*.parquet")
    feats = _read(run_dir, "features-*.parquet")
    dec = _read(run_dir, "decisions-*.parquet")
    if events is None or dec is None:
        out["error"] = "run has no events or decisions"
        return out
    n_run_events = int(events.num_rows)
    if "backfill" in events.column_names:
        n_run_events = int(events.num_rows - pa.compute.sum(events["backfill"]).as_py())
    dec = dec.filter(pa.compute.equal(dec["revision"], 0))
    d_ids = dec["event_id"].to_numpy()
    _, first = np.unique(d_ids, return_index=True)
    dec = dec.take(pa.array(first))
    d_ids = dec["event_id"].to_numpy()

    # ------------------------------------------------------------------ 1. features
    world = World.load(sc.world_dir)
    cp = world.customer_profile_arrays()
    mp = world.merchant_profile_arrays()
    cols = {c: events[c].to_numpy(zero_copy_only=False) for c in events.column_names
            if c not in ("t_intended_us", "t_intended_offset_us")}
    order = np.lexsort((cols["event_id"], cols["event_time_us"]))
    n_all = order.size
    mod = max(1, int(math.ceil(n_all / max_events)))
    entities = schema.stream_entities() if mod == 1 else list(EXACT_ENTITIES)
    if mod > 1:
        keep = (cols["customer_id"][order] % np.uint64(mod)) == 0
        order = order[keep]
        log(f"verify: {n_all:,} events > {max_events:,}: checking customers with id % {mod} == 0 "
            f"({order.size:,} events; customer/card features only)")
    served: dict[int, np.ndarray] = {}
    missing: dict[int, np.ndarray] = {}
    if feats is not None:
        fids = feats["event_id"].to_numpy()
        fvals = feats["values"].to_pylist()
        served = {int(i): np.asarray(v, np.float32) for i, v in zip(fids, fvals)}
        for i, mb in zip(fids, feats["missing"].to_pylist()):
            bits = np.unpackbits(np.frombuffer(mb or b"", np.uint8), bitorder="little")[:nf] if mb else np.zeros(0, np.uint8)
            if bits.size and bits.any():
                missing[int(i)] = np.pad(bits, (0, nf - bits.size)).astype(bool)
    engine = ReferenceEngine(schema, entities)
    filt = [cel.compile(expr) for expr in schema.filters.values()]
    cust_feats = schema.profile_features("customer")
    merch_feats = schema.profile_features("merchant")
    c_rows = world.customer_row(cols["customer_id"])
    m_ids = cols["merchant_id"]
    names = list(cols)
    lists = {k: cols[k].tolist() for k in names}
    ref: dict[int, np.ndarray] = {}
    buf = [0.0] * nf
    for i in order.tolist():
        ev = {k: lists[k][i] for k in names}
        view = event_view(ev)
        ctx = cel.Context()
        ctx.add_variable("event", view)
        ctx.add_variable("f", {})
        mask = 0
        for b, p in enumerate(filt):
            if p.execute(ctx):
                mask |= 1 << b
        engine.process(ev, mask, buf)
        eid = int(ev["event_id"])
        if eid not in served:
            continue
        v = np.asarray(buf, dtype=np.float64).astype(np.float32)
        r = int(c_rows[i])
        attrs = None
        mb = missing.get(eid)
        cust_missing = mb is not None and any(mb[s.index] for s in cust_feats)
        merch_missing = mb is not None and any(mb[s.index] for s in merch_feats)
        if cust_missing:
            r = -1  # the pipeline served defaults (lookup deadline/error): mirror it
        if r >= 0:
            for s in cust_feats:
                v[s.index] = np.float32(cp[s.field][r])
            attrs = {a: np.float32(cp[a][r]) for a in schema.profile_attributes.get("customer", [])}
        else:
            for s in cust_feats:
                v[s.index] = np.float32(s.default)
        mid = int(m_ids[i])
        mr = int(world.merchant_row(np.asarray([mid], np.uint64))[0]) if mid and not merch_missing else -1
        for s in merch_feats:
            v[s.index] = np.float32(mp[s.field][mr]) if mr >= 0 else np.float32(s.default)
        compute_derived(schema, ev, attrs, v)
        ref[eid] = v
    common = sorted(set(ref) & set(served))
    per_feature: dict[str, dict] = {}
    if common:
        S = np.stack([served[i] for i in common])
        R = np.stack([ref[i] for i in common])
        close = np.isclose(S, R, rtol=1e-4, atol=1e-3)
        # Features the pipeline flagged as missing (lookup deadline/error -> default served) are a
        # declared degradation, not a computation error: they must equal the declared default.
        M = np.stack([missing.get(i, np.zeros(nf, bool)) for i in common])
        defaults = np.asarray([f.default for f in schema.features], np.float32)
        close = np.where(M, np.isclose(S, defaults[None, :]), close)
        defaulted = int(M.any(axis=1).sum())
        for f in schema.features:
            if f.source == "stream" and f.entity not in entities:
                continue
            per_feature[f.name] = {"agreement": float(close[:, f.index].mean()),
                                   "max_abs_diff": float(np.abs(S[:, f.index] - R[:, f.index]).max()),
                                   "entity": f.entity, "source": f.source}
    exact = [k for k, v in per_feature.items() if v["source"] != "stream" or v["entity"] in EXACT_ENTITIES]
    out["features"] = {
        "compared_events": len(common), "sampling_mod": mod, "entities_checked": entities,
        "events_with_defaulted_features": defaulted if common else 0,
        "exact_set_agreement": float(np.mean([per_feature[k]["agreement"] for k in exact])) if exact else None,
        "per_feature": per_feature,
        "worst": sorted(((k, v["agreement"]) for k, v in per_feature.items()), key=lambda x: x[1])[:8],
        "note": ("Flink-inline: customer/card features must match exactly" if sc.topology == "flink-inline" else
                 "gateway topology: customer/card features are re-windowed from online-store state blobs at each "
                 "event's time (target >= 99%); the remainder is store lag - events of the same entity not yet "
                 "written to Redis when the gateway read it"),
    }

    # ------------------------------------------------------------------ 2-4. rules / score / outcome parity
    rules = Rules(sc.scoring_config()["rule_set_yaml"])
    mv = None
    if "model_version" in dec.column_names:
        vals = [v for v in dec["model_version"].to_pylist() if v]
        mv = max(set(vals), key=vals.count) if vals else None
    scorer = ProxyScorer.for_version(mv)
    d_pos = {int(e): j for j, e in enumerate(d_ids)}
    dd = {c: dec[c].to_numpy(zero_copy_only=False) for c in ("outcome", "score", "path", "rule_hits")}
    ev_idx = {int(e): j for j, e in enumerate(cols["event_id"].tolist())}
    sample = [e for e in served if e in d_pos][:20000]
    rule_ok = score_ok = outcome_ok = n_full = 0
    rule_mismatch: list[dict] = []
    vecs = np.stack([served[e] for e in sample]) if sample else np.zeros((0, nf), np.float32)
    py_scores = scorer.score(vecs) if sample else np.zeros(0)
    for k, eid in enumerate(sample):
        j = d_pos[eid]
        ev = {c: lists[c][ev_idx[eid]] for c in names}
        fmap = {f.name: float(served[eid][f.index]) for f in schema.features}
        hits = rules.evaluate(fmap, event_view(ev))
        got = set(x for x in (dd["rule_hits"][j] or "").split(",") if x)
        if {h for h, _ in hits} == got:
            rule_ok += 1
        elif len(rule_mismatch) < 5:
            rule_mismatch.append({"event_id": eid, "python": sorted(h for h, _ in hits), "pipeline": sorted(got)})
        full = int(dd["path"][j]) == 0
        sc_ = float(py_scores[k]) if full else float("nan")
        if full:
            n_full += 1
            if abs(sc_ - float(dd["score"][j])) <= 1e-5:
                score_ok += 1
        if rules.decide(hits, sc_) == int(dd["outcome"][j]):
            outcome_ok += 1
    m = max(1, len(sample))
    out["rules"] = {"checked": len(sample), "parity": rule_ok / m, "mismatches": rule_mismatch}
    out["score"] = {"checked": n_full, "parity": score_ok / max(1, n_full), "model_version": scorer.version}
    out["outcome"] = {"checked": len(sample), "parity": outcome_ok / m}

    # ------------------------------------------------------------------ 5. ClickHouse audit
    ch = clickhouse or os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")
    try:
        out["audit"] = audit_checks(ch, run_dir.name, n_run_events, scorer, rules, schema)
    except Exception as e:  # noqa: BLE001
        out["audit"] = {"error": f"ClickHouse unavailable or not ingesting: {e}"}
    passed = (out["rules"]["parity"] == 1.0 and out["score"]["parity"] == 1.0 and out["outcome"]["parity"] == 1.0
              and (sc.topology != "flink-inline" or (out["features"]["exact_set_agreement"] or 0) >= 0.999))
    a = out.get("audit", {})
    if "complete" in a:
        passed = passed and a["complete"]
    out["passed"] = bool(passed)
    (run_dir / "verify.json").write_text(json.dumps(out, indent=1, default=float))
    return out


def _q(ch: str, sql: str) -> list[dict]:
    import httpx
    r = httpx.post(ch, params={"default_format": "JSONEachRow"}, content=sql, timeout=60)
    r.raise_for_status()
    return [json.loads(line) for line in r.text.splitlines() if line.strip()]


def audit_checks(ch: str, run_id: str, emitted: int, scorer: ProxyScorer, rules: Rules, schema) -> dict[str, Any]:
    import time
    # wait for asynchronous ingestion to catch up (bounded)
    got = 0
    for _ in range(60):
        got = int(_q(ch, f"SELECT uniqExact(event_id) AS n FROM realy.decisions WHERE run_id = '{run_id}' AND revision = 0")[0]["n"])
        if got >= emitted:
            break
        time.sleep(1)
    rows = _q(ch, f"""SELECT count() AS rows, uniqExact(event_id) AS uniq,
        quantileExact(0.5)(dateDiff('microsecond', decided_at, ingested_at)) / 1000 AS vis_p50_ms,
        quantileExact(0.99)(dateDiff('microsecond', decided_at, ingested_at)) / 1000 AS vis_p99_ms
        FROM realy.decisions WHERE run_id = '{run_id}' AND revision = 0""")[0]
    final_rows = int(_q(ch, f"SELECT count() AS n FROM realy.decisions FINAL WHERE run_id = '{run_id}' AND revision = 0")[0]["n"])
    labels = int(_q(ch, f"SELECT count() AS n FROM realy.labels WHERE run_id = '{run_id}'")[0]["n"])
    gaps = int(_q(ch, f"SELECT count() AS n FROM realy.v_audit_gaps WHERE run_id = '{run_id}'")[0]["n"])
    raw = int(_q(ch, f"SELECT uniqExact(event_id) AS n FROM realy.events_raw WHERE run_id = '{run_id}'")[0]["n"])
    train = _q(ch, f"""SELECT count() AS n, sum(label_is_fraud) AS fraud FROM realy.v_training_dataset
        WHERE run_id = '{run_id}'""")[0]
    # replay a sample from the stored inputs
    sample = _q(ch, f"""SELECT event_id, score, outcome, path, features_raw FROM realy.decisions
        WHERE run_id = '{run_id}' AND revision = 0 AND path = 'FULL' AND length(features_raw) = {len(schema.features)}
        ORDER BY cityHash64(event_id) LIMIT 2000""")
    replay_ok = 0
    if sample:
        X = np.asarray([s["features_raw"] for s in sample], np.float32)
        py = scorer.score(X)
        replay_ok = int(sum(abs(float(p) - float(s["score"])) <= 1e-5 for p, s in zip(py, sample)))
    return {"emitted": emitted, "audited_unique": int(rows["uniq"]), "rows_including_duplicates": int(rows["rows"]),
            "rows_after_replacing_merge": final_rows, "complete": int(rows["uniq"]) >= emitted,
            "audit_gaps": max(0, emitted - int(rows["uniq"])), "events_raw_unique": raw, "gap_view_rows": gaps,
            "visibility_p50_ms": float(rows["vis_p50_ms"]), "visibility_p99_ms": float(rows["vis_p99_ms"]),
            "labels_total": labels, "training_rows": int(train["n"]), "training_fraud_labels": int(train["fraud"] or 0),
            "replay_checked": len(sample), "replay_score_parity": replay_ok / max(1, len(sample))}


def render(v: dict[str, Any]) -> str:
    if "error" in v:
        return f"verify: {v['error']}"
    f = v["features"]
    lines = [f"=== verify {v['run_id']}: {'PASS' if v['passed'] else 'FAIL'} ===",
             f"features: {f['compared_events']:,} served vectors vs reference; exact-set agreement "
             f"{(f['exact_set_agreement'] or 0):.4%} ({f['note']})",
             "  lowest agreement: " + ", ".join(f"{k} {a:.1%}" for k, a in f["worst"]),
             f"rules parity (Java CEL vs Python CEL): {v['rules']['parity']:.4%} of {v['rules']['checked']:,}",
             f"score parity (proxy model recompute): {v['score']['parity']:.4%} of {v['score']['checked']:,}",
             f"outcome parity: {v['outcome']['parity']:.4%}"]
    a = v.get("audit", {})
    if "error" in a:
        lines.append(f"audit: {a['error']}")
    elif a:
        lines.append(f"audit (ClickHouse): {a['audited_unique']:,}/{a['emitted']:,} decisions, gaps {a['audit_gaps']}, "
                     f"rows {a['rows_including_duplicates']:,} -> {a['rows_after_replacing_merge']:,} after merge, "
                     f"visibility p50 {a['visibility_p50_ms']:.0f} ms p99 {a['visibility_p99_ms']:.0f} ms, "
                     f"training rows {a['training_rows']:,} ({a['training_fraud_labels']} fraud labels), "
                     f"replay parity {a['replay_score_parity']:.2%} of {a['replay_checked']}")
    return "\n".join(lines)
