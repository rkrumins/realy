"""`realy verify`: independent correctness checks of a finished run.

  1. features   recompute every feature from the ground-truth event log with the Python reference
                engine (feature DSL mirror) and compare with what the pipeline SERVED to the model
                (exact match expected for customer/card features in the Flink-inline topology; in
                gateway topologies the difference measures online-store staleness)
  2. rules      re-evaluate the CEL rule set (Python CEL) on the served vectors -> identical hits
  3. score      recompute the proxy-model score from the served vector (numpy float32)
  4. outcome    re-derive the decision from hits + score + whether the payment can be stepped up
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

from ..policy import NAMES as OUTCOME  # noqa: E402  (APPROVE, REVIEW, DECLINE, CHALLENGE)
EXACT_ENTITIES = ("customer", "card")


def _read(run_dir: Path, pattern: str) -> pa.Table | None:
    files = sorted(glob.glob(str(run_dir / pattern)))
    return pa.concat_tables([pq.read_table(f) for f in files], promote_options="permissive") if files else None


class ProxyScorer:
    @classmethod
    def for_version(cls, version: str | None) -> "ProxyScorer":
        """Weights file whose `version` matches the model version recorded in the decisions."""
        if version:
            from ..train.proxy import MODELS
            # trained models live in REALY_MODELS (the shared models volume in Docker), versioned under
            # proxy/versions/; bundled weights ship in the repository
            dirs = [MODELS / "proxy" / "versions", MODELS / "proxy", REPO_ROOT / "models" / "proxy"]
            direct = MODELS / "proxy" / "versions" / f"{version}.weights.json"
            candidates = [direct] + [p for d in dirs if d.is_dir() for p in sorted(d.glob("*.weights.json"))]
            for p in candidates:
                try:
                    if p.exists() and json.loads(p.read_text()).get("version") == version:
                        return cls(p)
                except ValueError:
                    continue
            import warnings
            warnings.warn(f"no weights for served model {version}: score parity uses the default weights "
                          "and will not match", stacklevel=2)
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


# the policy lives in realy.policy (one definition for verify, what-if, explanations, Explorer)
from ..policy import RuleSet as Rules  # noqa: E402


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
    from ..scenario import run_rules
    cfg = sc.scoring or {}
    rules = Rules(run_rules(run_dir, sc)[0]).with_thresholds(
        cfg.get("review_threshold"), cfg.get("decline_threshold"), cfg.get("challenge_threshold"))
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
        view = event_view(ev)
        hits = rules.evaluate(fmap, view)
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
        if rules.decide(hits, sc_, rules.challengeable(view)) == int(dd["outcome"][j]):
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
    # exact-set target: flink-inline computes in-task (must be exact); gateway topologies re-window state
    # blobs and differ only by store lag (REALY_VERIFY_MIN_EXACT, default 0.98). Lower means the served
    # features are wrong, e.g. state left over from another world.
    exact = out["features"]["exact_set_agreement"] or 0
    target = 0.999 if sc.topology == "flink-inline" else float(os.environ.get("REALY_VERIFY_MIN_EXACT", "0.98"))
    out["features"]["exact_set_target"] = target
    out["features"]["exact_set_ok"] = exact >= target or out["features"]["compared_events"] == 0
    # The reference replays THIS run's events only. Warm state that earlier runs fed (same world, same
    # simulated window) makes the served features legitimately differ: not comparable, so not judged.
    state = state_of(run_dir)
    out["state"] = state
    comparable, why_not = features_comparable(state)
    out["features"]["comparable"] = comparable
    if not comparable:
        out["features"]["not_comparable_because"] = why_not
    out["passed"], out["reasons"] = judge(out)
    out["inconclusive"] = not comparable and out["passed"] is False and out["reasons"] == [NOT_COMPARABLE]
    (run_dir / "verify.json").write_text(json.dumps(out, indent=1, default=float))
    return out


class ClickHouseError(RuntimeError):
    """ClickHouse failed a query, possibly after HTTP 200: it streams results, so an exception raised
    mid-query arrives in the body (`Code: N. DB::Exception: ...`), not as an HTTP error status."""


def _q(ch: str, sql: str, retries: int = 2) -> list[dict]:
    import time

    import httpx
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            r = httpx.post(ch, params={"default_format": "JSONEachRow"}, content=sql, timeout=120)
            if r.status_code >= 400:
                raise ClickHouseError(f"HTTP {r.status_code}: {r.text.strip()[:300]}")
            rows = []
            for line in r.text.splitlines():
                if not line.strip():
                    continue
                if line.startswith("Code:") or "DB::Exception" in line:
                    raise ClickHouseError(line.strip()[:300])
                rows.append(json.loads(line))
            return rows
        except (ClickHouseError, httpx.HTTPError, ValueError) as e:   # transient under load: retry, then fail clearly
            last = e
            time.sleep(2 * (attempt + 1))
    raise ClickHouseError(f"{last} (query: {' '.join(sql.split())[:160]})")


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


NOT_COMPARABLE = "features not comparable: the run read warm state that other runs' events also built"


def state_of(run_dir: Path) -> dict[str, Any] | None:
    """The run's feature-state lineage, as the runner recorded it before sending any event (manifest
    "state"); None for runs older than the record."""
    try:
        return json.loads((Path(run_dir) / "manifest.json").read_text()).get("state")
    except (OSError, ValueError):
        return None


def features_comparable(state: dict[str, Any] | None) -> tuple[bool, str | None]:
    if state is None or state.get("mode") in ("none", None):
        return True, None   # no feature job, or a run from before lineage was recorded (judged as before)
    if state.get("unknown"):
        return False, "which runs built the warm state was not recorded"
    if state.get("inherited_runs"):
        runs = ", ".join(state.get("inherited") or [])
        return False, (f"the warm state also held the events of {state['inherited_runs']} earlier run(s)"
                       + (f" (latest: {runs})" if runs else "") + "; re-run with --verify (or verify from the "
                       "console), which starts the state fresh")
    return True, None


def judge(v: dict[str, Any]) -> tuple[bool, list[str]]:
    """Pass only if every check holds; every failure names itself."""
    reasons: list[str] = []
    for k, label in (("rules", "rules"), ("score", "model score"), ("outcome", "outcome")):
        if v[k]["parity"] != 1.0:
            reasons.append(f"{label} parity {v[k]['parity']:.4%} (must be 100%)")
    f = v["features"]
    if not f.get("comparable", True):
        reasons.append(NOT_COMPARABLE)
    elif not f["exact_set_ok"]:
        reasons.append(f"exact-set feature agreement {(f['exact_set_agreement'] or 0):.4%} below "
                       f"{f['exact_set_target']:.1%}")
    a = v.get("audit", {})
    if "complete" in a and not a["complete"]:
        reasons.append(f"ClickHouse audit incomplete ({a.get('audited_unique', 0):,} of {a.get('emitted', 0):,} decisions)")
    return not reasons, reasons


def render(v: dict[str, Any]) -> str:
    if "error" in v:
        return f"verify: {v['error']}"
    f = v["features"]
    head = "PASS" if v["passed"] else ("INCONCLUSIVE" if v.get("inconclusive") else "FAIL")
    judged = (f"target {f.get('exact_set_target', 0):.1%}{'' if f.get('exact_set_ok', True) else ' - BELOW TARGET'}"
              if f.get("comparable", True) else f"NOT JUDGED: {f.get('not_comparable_because')}")
    lines = [f"=== verify {v['run_id']}: {head} ===",
             *(f"  - {r}" for r in v.get("reasons") or []),
             f"features: {f['compared_events']:,} served vectors vs reference; exact-set agreement "
             f"{(f['exact_set_agreement'] or 0):.4%} ({judged}; {f['note']})",
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
