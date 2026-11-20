"""Run report: per-step latency vs throughput, throughput funnel, tail attribution, budget and
sustainability verdict, correctness (loss/dups) and detection KPIs.

Inputs (runs/<run_id>/): manifest.json, scenario.json, emit-*.parquet, decisions-*.parquet,
events-*.parquet (ground truth), metrics.jsonl (service-side live stats).
"""
from __future__ import annotations

import glob
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

PCTS = (50, 90, 99, 99.9)
OUTCOME = {1: "APPROVE", 2: "REVIEW", 3: "DECLINE"}
FRAUD_TYPES = {0: "none", 1: "card_testing", 2: "account_takeover", 3: "lost_stolen", 4: "counterfeit",
               5: "merchant_breach", 6: "atm_cashout", 7: "app_scam_mule", 8: "bust_out", 9: "bot_ecom",
               10: "friendly_fraud"}
STEP_ORDER = ["schedule_lag", "produce_ack", "ingress", "queue_wait", "deserialize", "shuffle", "state_features",
              "async_wait", "feature_lookup", "feature_assemble", "model_call", "model_batch_wait",
              "model_server_queue", "model_server_compute", "rules", "policy", "audit_ack", "hot_path", "egress", "e2e"]
STEP_LABEL = {
    "schedule_lag": "1 on-time publish (sent - intended)", "produce_ack": "2 produce ack (broker write)",
    "ingress": "3 ingress transit + pickup", "queue_wait": "  service queue wait", "deserialize": "  deserialize",
    "shuffle": "4 Flink keyBy shuffles (sum)", "state_features": "4 Flink keyed state (sum)",
    "async_wait": "4 Flink async I/O wait", "feature_lookup": "5 feature lookup", "feature_assemble": "5 feature assemble",
    "model_call": "5 model call (client)", "model_batch_wait": "6 model micro-batch wait",
    "model_server_queue": "6 model server queue", "model_server_compute": "6 model server compute",
    "rules": "5 rules (CEL)", "policy": "5 policy", "audit_ack": "5 audit ack (Kafka)", "hot_path": "= hot path (ingest->decision)",
    "egress": "7 egress transit / response", "e2e": "8 END-TO-END (intended->collected|response)",
}


def _read(run_dir: Path, pattern: str) -> pa.Table | None:
    files = sorted(glob.glob(str(run_dir / pattern)))
    if not files:
        return None
    return pa.concat_tables([pq.read_table(f) for f in files], promote_options="permissive")


def _pct(a: np.ndarray) -> dict[str, float]:
    if a.size == 0:
        return {"count": 0}
    p = np.percentile(a, PCTS)
    return {"count": int(a.size), "p50": float(p[0]), "p90": float(p[1]), "p99": float(p[2]), "p999": float(p[3]),
            "max": float(a.max()), "mean": float(a.mean())}


def _ms(us: np.ndarray) -> np.ndarray:
    return us.astype(np.float64) / 1000.0


def build_report(run_dir: Path | str) -> dict[str, Any]:
    run_dir = Path(run_dir)
    man = json.loads((run_dir / "manifest.json").read_text())
    sc = json.loads((run_dir / "scenario.json").read_text())
    from ..scenario import Scenario
    scenario = Scenario.model_validate(sc)
    budget = scenario.budget
    run = json.loads((run_dir / "run.json").read_text()) if (run_dir / "run.json").exists() else {}
    abort = _abort_info(run)
    rep: dict[str, Any] = {"run_id": man["run_id"], "scenario": scenario.name, "topology": scenario.topology,
                           "manifest": man, "budget": budget.model_dump()}
    if abort:
        rep["abort"] = abort
    t0 = man.get("t_start_us")
    warm = man["warmup_s"]
    emit = _read(run_dir, "emit-*.parquet") if t0 else None
    dec = _read(run_dir, "decisions-*.parquet") if t0 else None
    if emit is None or emit.num_rows == 0:
        if abort:
            why = f"aborted in {abort.get('phase') or 'PREPARE'} before any event was measured: {abort['reason']}"
            verdict = "FAIL" if abort.get("kind") in FAIL_KINDS else "ABORTED"
        else:
            why, verdict = (run.get("error") and f"error: {run['error']}") or "no events emitted", "ERROR"
        rep["summary"] = {"verdict": verdict, "reasons": [why], "emitted": 0, "decided": 0, "missing": 0,
                          "duplicates": 0, "aborted": abort, "measured_s": 0.0}
        rep["text"] = f"{verdict}: {why}"
        _write(run_dir, rep)
        return rep

    e_id = emit["event_id"].to_numpy()
    e_int = emit["t_intended_us"].to_numpy()
    e_sent = emit["t_sent_us"].to_numpy()
    e_ack = emit["ack_us"].to_numpy()
    n_emit = e_id.size

    # ---------------------------------------------------------------- correctness: completeness
    if dec is not None and dec.num_rows:
        rev = dec["revision"].to_numpy()
        d = dec.filter(pa.compute.equal(dec["revision"], 0))
        d_id = d["event_id"].to_numpy()
        uniq, first_idx = np.unique(d_id, return_index=True)
        dups_in_files = int(d_id.size - uniq.size)
        d = d.take(pa.array(first_idx))
        d_id = uniq
        finals = int((rev > 0).sum())
    else:
        d, d_id, dups_in_files, finals = None, np.zeros(0, np.uint64), 0, 0
    coll = run.get("collectors", {})
    dup_total = dups_in_files + sum(int(c.get("duplicates", 0)) for c in coll.values() if isinstance(c, dict))
    present = np.isin(e_id, d_id)
    missing = int((~present).sum())
    unknown = int((~np.isin(d_id, e_id)).sum()) if d_id.size else 0

    # ---------------------------------------------------------------- join emit <-> decisions
    order = np.argsort(e_id)
    e_sorted = e_id[order]
    lat: dict[str, np.ndarray] = {}
    sync = "t_response_us" in emit.column_names
    if sync:
        # Synchronous rail: latency is what the CLIENT observed (intended -> response), from the
        # load emitter's per-event results; the audit log (decisions) is used for completeness.
        st_ = emit["status"].to_numpy()
        okr = st_ == 0
        t_resp = emit["t_response_us"].to_numpy()
        t_ing = emit["t_ingest_us"].to_numpy()
        t_dec = emit["t_decision_us"].to_numpy()
        ei = np.arange(n_emit)
        intended = e_int
        measured = (e_int >= t0 + int(warm * 1e6)) & okr
        lat["e2e"] = np.where(okr, t_resp - e_int, -1)
        lat["ingress"] = np.where(okr & (t_ing > 0), t_ing - e_sent, -1)
        lat["hot_path"] = np.where(okr & (t_ing > 0) & (t_dec > 0), t_dec - t_ing, -1)
        lat["egress"] = np.where(okr & (t_dec > 0), t_resp - t_dec, -1)
        for k in emit.column_names:
            if k.startswith("rs_"):
                lat[k[3:]] = emit[k].to_numpy().astype(np.int64)
        outcome = emit["outcome"].to_numpy()
        degraded = emit["degraded"].to_numpy().astype(bool)
        stale = np.full(n_emit, -1.0)
        if d is not None and d.num_rows:
            dpos = np.clip(np.searchsorted(d_id, e_id), 0, d_id.size - 1)
            hit = d_id[dpos] == e_id
            stale = np.where(hit, d["feature_staleness_ms"].to_numpy()[dpos], -1.0)
        dd = {"t_decision_us": np.where(okr, t_dec, -1), "t_collected_us": np.where(okr, t_resp, -1)}
        rep["sync"] = {"responses_ok": int(okr.sum()), "deadline": int((st_ == 1).sum()),
                       "errors": int((st_ == 2).sum()), "no_response": int((st_ == 3).sum()),
                       "audit_records": int(d_id.size)}
    elif d is not None and d.num_rows:
        pos = np.searchsorted(e_sorted, d_id)
        pos = np.clip(pos, 0, e_sorted.size - 1)
        ok = e_sorted[pos] == d_id
        ei = order[pos[ok]]
        dd = {name: d[name].to_numpy()[ok] for name in d.column_names if name not in ("reason_codes", "rule_hits", "model_version") and not name.startswith("span_")}
        intended = e_int[ei]
        sent = e_sent[ei]
        measured = intended >= t0 + int(warm * 1e6)
        t_ing, t_dec, t_col = dd["t_ingest_us"], dd["t_decision_us"], dd["t_collected_us"]
        lat["e2e"] = t_col - intended
        lat["ingress"] = np.where(t_ing > 0, t_ing - sent, -1)
        lat["hot_path"] = np.where((t_ing > 0) & (t_dec > 0), t_dec - t_ing, -1)
        lat["egress"] = np.where(t_dec > 0, t_col - t_dec, -1)
        for k in dd:
            if k.startswith("st_"):
                lat[k[3:]] = dd[k].astype(np.int64)
        outcome = dd["outcome"]
        degraded = dd["degraded"]
        stale = dd["feature_staleness_ms"]
    else:
        measured = np.zeros(0, bool)
        intended = np.zeros(0, np.int64)
        outcome = degraded = stale = np.zeros(0)
        ei = np.zeros(0, np.int64)
    m_emit = e_int >= t0 + int(warm * 1e6)
    lat_emit = {"schedule_lag": e_sent - e_int, "produce_ack": e_ack}

    def step_stats(mask_d: np.ndarray, mask_e: np.ndarray) -> dict[str, dict]:
        out = {}
        for k in STEP_ORDER:
            if k in lat_emit:
                v = lat_emit[k][mask_e]
            elif k in lat:
                v = lat[k][mask_d]
            else:
                continue
            v = v[v >= 0]
            if v.size:
                out[k] = _pct(_ms(v))
        return out

    overall = step_stats(measured, m_emit)
    e2e_m = _ms(lat["e2e"][measured & (lat["e2e"] >= 0)]) if "e2e" in lat else np.zeros(0)
    hot_m = _ms(lat["hot_path"][measured]) if "hot_path" in lat else np.zeros(0)
    hot_m = hot_m[hot_m >= 0]

    # ---------------------------------------------------------------- per TPS level (step / ramp)
    prof = scenario.traffic.profile
    levels = []
    if prof.kind in ("step", "ramp", "spike", "sine"):
        if prof.kind == "step":
            width = prof.step_duration_s or 60.0
        else:
            width = max(10.0, scenario.traffic.duration_s / 10)
        nlev = int(math.ceil(scenario.traffic.duration_s / width))
        end_us = run.get("emission_end_us") if abort else None
        for li in range(nlev):
            lo = t0 + int(li * width * 1e6)
            hi = t0 + int((li + 1) * width * 1e6)
            # skip the first 20% of each level (transition)
            lo_m = lo + int(0.2 * width * 1e6)
            partial = bool(end_us and end_us < hi)
            if end_us and end_us <= lo_m:
                break                          # the run was aborted before this level
            if partial:
                hi = int(end_us)
            md = (intended >= lo_m) & (intended < hi) & measured if intended.size else intended.astype(bool)
            me = (e_int >= lo_m) & (e_int < hi) & m_emit
            secs = (hi - lo_m) / 1e6
            # decided rate by ARRIVAL time (not intended time): falls behind the emitted rate when a backlog builds
            if d is not None and d.num_rows or sync:
                tc = dd["t_collected_us"]
                n_dec = int(((tc >= lo_m) & (tc < hi)).sum())
            else:
                n_dec = 0
            lv = {"level": li, "t_from_s": li * width, "target_tps": round(prof.rate_at(li * width + width / 2), 1),
                  "emitted_tps": round(int(me.sum()) / secs, 1), "decided_tps": round(n_dec / secs, 1),
                  "stages": step_stats(md, me)}
            if partial:
                lv["partial"] = True           # aborted mid-level: reported, never used for the knee
            if "e2e" in lv["stages"]:
                lv["within_budget"] = float((_ms(lat["e2e"][md]) <= budget.e2e_p99_ms).mean())
            levels.append(lv)

    # ---------------------------------------------------------------- per-second funnel
    dur = int(scenario.traffic.duration_s) + 1
    def per_sec(ts_us: np.ndarray) -> list[int]:
        s = ((ts_us - t0) // 1_000_000).astype(np.int64)
        s = s[(s >= 0) & (s < dur + 120)]
        return np.bincount(s, minlength=dur).tolist()
    funnel = {"emitted": per_sec(e_sent), "acked": per_sec(e_sent[e_ack >= 0] + e_ack[e_ack >= 0])}
    series_p99: list[float | None] = []
    series_p50: list[float | None] = []
    if (d is not None and d.num_rows) or sync:
        funnel["decided"] = per_sec(dd["t_decision_us"][dd["t_decision_us"] > 0])
        funnel["responded" if sync else "collected"] = per_sec(dd["t_collected_us"][dd["t_collected_us"] > 0])
        sec_of = ((intended - t0) // 1_000_000).astype(np.int64)
        e2e_all = np.where(lat["e2e"] >= 0, _ms(lat["e2e"]), np.nan)
        idx_sort = np.argsort(sec_of, kind="stable")
        bounds = np.searchsorted(sec_of[idx_sort], np.arange(dur + 1))
        for s in range(dur):
            v = e2e_all[idx_sort[bounds[s]:bounds[s + 1]]]
            v = v[~np.isnan(v)]
            series_p99.append(float(np.percentile(v, 99)) if v.size else None)
            series_p50.append(float(np.percentile(v, 50)) if v.size else None)
    cum_e = np.cumsum(funnel["emitted"])
    cum_c = np.cumsum(funnel.get("collected", funnel.get("responded", [0] * len(cum_e)))[: len(cum_e)])
    backlog = (cum_e[: len(cum_c)] - cum_c).tolist()

    # ---------------------------------------------------------------- tail attribution
    tail = {}
    if e2e_m.size >= 100:
        thr = np.percentile(e2e_m, 99)
        tmask = measured & (_ms(lat["e2e"]) >= thr)
        parts = {}
        for k in ("schedule_lag", "ingress", "queue_wait", "shuffle", "state_features", "async_wait", "feature_lookup",
                  "model_call", "rules", "audit_ack", "egress"):
            if k == "schedule_lag":
                v = (e_sent[ei] - e_int[ei])[tmask]
            elif k in lat:
                v = lat[k][tmask]
            else:
                continue
            v = v[v >= 0]
            if v.size:
                parts[k] = float(_ms(v).mean())
        tot = sum(parts.values()) or 1.0
        tail = {"threshold_ms": float(thr), "events": int(tmask.sum()),
                "mean_ms_by_stage": parts, "share": {k: v / tot for k, v in parts.items()},
                "dominant": max(parts, key=parts.get) if parts else None}

    # ---------------------------------------------------------------- sustainability
    sust = {}
    if series_p99:
        mins = []
        w = 60 if scenario.traffic.duration_s >= 180 else 10
        for i in range(int(warm), len(series_p99), w):
            vals = [v for v in series_p99[i:i + w] if v is not None]
            if vals:
                mins.append(float(np.median(vals)))
        slope = float(np.polyfit(np.arange(len(mins)), mins, 1)[0]) if len(mins) >= 3 else 0.0
        half = len(backlog) // 2
        tail_backlog = backlog[half:int(scenario.traffic.duration_s)] if backlog else []
        growing = bool(len(tail_backlog) > 5 and tail_backlog[-1] > tail_backlog[0] + max(1000, 0.05 * cum_e[-1]))
        drift = abs(slope) > 0.1 * (np.median(mins) if mins else 1) and slope > 0
        sust = {"window_s": w, "p99_by_window_ms": mins, "p99_slope_ms_per_window": slope,
                "backlog_growing": growing, "max_backlog": int(max(backlog)) if backlog else 0,
                "sustainable": not growing and not drift}

    # ---------------------------------------------------------------- knee
    knee = None
    for lv in levels:
        st = lv["stages"].get("e2e")
        if lv.get("partial"):
            continue
        if st and (st["p99"] > budget.e2e_p99_ms or lv["decided_tps"] < 0.95 * lv["emitted_tps"]):
            knee = {"level": lv["level"], "target_tps": lv["target_tps"], "e2e_p99_ms": st["p99"],
                    "decided_tps": lv["decided_tps"], "emitted_tps": lv["emitted_tps"]}
            break

    # ---------------------------------------------------------------- detection KPIs (ground truth)
    detection = _detection(run_dir, d, d_id) if d is not None else {}

    # ---------------------------------------------------------------- service-side live stats
    service = _service_stats(run_dir, scenario.traffic.warmup_s)
    from .resources import resources as _resources
    try:
        res = _resources(run_dir, man, levels, scenario.traffic.duration_s, scenario.traffic.warmup_s)
    except Exception as e:  # noqa: BLE001 - resource accounting must never break a report
        res = {"error": f"{type(e).__name__}: {e}"}

    # ---------------------------------------------------------------- verdict
    reasons = []
    compliance = float((e2e_m <= budget.e2e_p99_ms).mean()) if e2e_m.size else 0.0
    e2e_p99 = float(np.percentile(e2e_m, 99)) if e2e_m.size else float("nan")
    hot_p99 = float(np.percentile(hot_m, 99)) if hot_m.size else float("nan")
    if missing / max(1, n_emit) > scenario.guardrails.max_missing_fraction:
        reasons.append(f"missing decisions {missing} ({missing / n_emit:.2%})"
                       + (" in the audit log" if sync else "")
                       + (" - includes events still in flight when the run was aborted; the ClickHouse audit"
                          " (realy verify) is authoritative" if abort else ""))
    if sync and rep["sync"]["responses_ok"] < n_emit:
        bad = n_emit - rep["sync"]["responses_ok"]
        if bad / n_emit > scenario.guardrails.max_missing_fraction:
            reasons.append(f"{bad} requests without a successful response ({rep['sync']})")
    if not (e2e_p99 <= budget.e2e_p99_ms):
        reasons.append(f"e2e p99 {e2e_p99:.1f} ms > budget {budget.e2e_p99_ms:.0f} ms")
    if hot_m.size and not (hot_p99 <= budget.hot_path_p99_ms):
        reasons.append(f"hot-path p99 {hot_p99:.1f} ms > budget {budget.hot_path_p99_ms:.0f} ms")
    if compliance < budget.min_compliance:
        reasons.append(f"only {compliance:.2%} of events within {budget.e2e_p99_ms:.0f} ms (< {budget.min_compliance:.0%})")
    if sust and not sust.get("sustainable", True):
        reasons.append("not sustainable (backlog growing or p99 drifting)")
    if (man.get("gates") or {}).get("flink_running") is False:
        reasons.append("the Flink feature job was not fully RUNNING when the measured window started")
    if man.get("backfill_caught_up") is False:
        lag = man.get("backfill_lag")
        reasons.append(f"feature state did not catch up after the backfill (lag {lag if lag is not None and lag >= 0 else 'unknown'}):"
                       " features in the measured window were stale")
    if abort:
        reasons.insert(0, f"aborted ({abort.get('kind')}, in {abort.get('phase')}): {abort['reason']}")
    # An operator / instrument / watchdog abort leaves no verdict on the system under test (ABORTED);
    # a guardrail abort IS a verdict: the pipeline failed under this load (FAIL).
    if abort and abort.get("kind") not in FAIL_KINDS:
        verdict = "ABORTED"
    else:
        verdict = "PASS" if not reasons else "FAIL"
    # measured window actually reached (an aborted run ends early)
    reached = run.get("measured_s")
    measured_secs = max(1e-9, float(reached) if reached is not None else scenario.traffic.duration_s - warm)
    summary = {
        "verdict": verdict, "reasons": reasons,
        "emitted": int(n_emit), "decided": int(d_id.size), "missing": missing, "duplicates": int(dup_total),
        "unknown": unknown, "finals_after_provisional": finals,
        "achieved_emit_tps": round(int(m_emit.sum()) / measured_secs, 1),
        "e2e_p50_ms": overall.get("e2e", {}).get("p50"), "e2e_p99_ms": e2e_p99, "e2e_p999_ms": overall.get("e2e", {}).get("p999"),
        "hot_path_p99_ms": hot_p99, "within_budget": compliance,
        "schedule_lag_p99_ms": overall.get("schedule_lag", {}).get("p99"),
        "degraded_fraction": float(degraded[measured].mean()) if degraded.size and measured.any() else 0.0,
        "outcomes": {OUTCOME.get(int(k), str(k)): int(v) for k, v in zip(*np.unique(outcome, return_counts=True))} if outcome.size else {},
        # Online-store freshness (gateway topologies): Flink's Kafka-pickup -> Redis-write lag, i.e. how
        # long a new event is invisible to the gateway. (Blob age is NOT staleness: an idle entity's
        # old blob is still exact.)
        "online_store_update_lag_p99_ms": (service.get("stages", {}).get("flink.store_update_lag", {}) or {}).get("p99_median_ms"),
        "knee": knee, "sustainable": sust.get("sustainable"),
        "measured_s": round(measured_secs, 2), "aborted": abort,
    }
    rep.update({"summary": summary, "stages": overall, "levels": levels, "funnel": funnel, "backlog": backlog,
                "e2e_p99_by_second": series_p99, "e2e_p50_by_second": series_p50, "tail_attribution": tail,
                "sustainability": sust, "detection": detection, "service": service, "resources": res})
    from ..runner.startup import read as read_startup
    startup = read_startup(run_dir)
    if startup:
        rep["startup"] = startup
        rep["summary"]["prepare_s"] = round(sum(s_.get("duration_s") or 0 for s_ in startup.get("steps", [])
                                                if s_.get("id") != "measure"), 1)
    expect = scenario.expect.get("verdict")
    if expect:
        rep["summary"]["expected_verdict"] = expect
        rep["summary"]["matches_expectation"] = expect == verdict
    rep["text"] = render_text(rep)
    _write(run_dir, rep)
    return rep


FAIL_KINDS = ("guardrail",)


def roc_auc(y: np.ndarray, s: np.ndarray) -> float:
    """ROC-AUC = P(score of a random fraud > score of a random legit), ties counted half (Mann-Whitney)."""
    y = np.asarray(y, bool)
    order = np.argsort(s, kind="mergesort")
    ranks = np.empty(s.size, np.float64)
    ss = np.asarray(s)[order]
    # average ranks for ties
    _, first, counts = np.unique(ss, return_index=True, return_counts=True)
    avg = first + (counts - 1) / 2.0 + 1
    ranks[order] = np.repeat(avg, counts)
    npos, nneg = int(y.sum()), int((~y).sum())
    return float((ranks[y].sum() - npos * (npos + 1) / 2) / (npos * nneg))


def average_precision(y: np.ndarray, s: np.ndarray) -> float:
    """PR-AUC as average precision (sum over thresholds of precision x recall increment)."""
    y = np.asarray(y, bool)
    order = np.argsort(-np.asarray(s), kind="mergesort")
    yt, st = y[order], np.asarray(s)[order]
    last = np.r_[np.flatnonzero(np.diff(st) != 0), yt.size - 1]   # end of each run of tied scores
    tp = np.cumsum(yt)[last]
    prec = tp / (last + 1)
    recall_inc = np.diff(np.r_[0, tp]) / max(1, yt.sum())
    return float((prec * recall_inc).sum())


def _abort_info(run: dict[str, Any]) -> dict[str, Any] | None:
    """run.json `abort` (reason, kind, phase, after_s); older runs only carry `aborted` (the reason)."""
    a = run.get("abort")
    if a and a.get("reason"):
        return a
    if run.get("aborted"):
        r = str(run["aborted"])
        guard = any(k in r for k in ("schedule lag", "backlog", "error rate"))
        return {"reason": r, "kind": "guardrail" if guard else "operator", "phase": None}
    return None


def _detection(run_dir: Path, d: pa.Table, d_id: np.ndarray) -> dict[str, Any]:
    ev = _read(run_dir, "events-*.parquet")
    if ev is None or d is None or not d.num_rows:
        return {}
    cols = ["event_id", "is_fraud", "fraud_type", "campaign_id", "amount_usd_minor", "event_time_us"]
    ev = ev.select([c for c in cols if c in ev.column_names])
    eid = ev["event_id"].to_numpy()
    order = np.argsort(d_id)
    pos = np.searchsorted(d_id[order], eid)
    pos = np.clip(pos, 0, max(0, d_id.size - 1))
    have = d_id[order][pos] == eid if d_id.size else np.zeros(eid.size, bool)
    out_all = d["outcome"].to_numpy()[order]
    outcome = np.where(have, out_all[pos] if d_id.size else 0, 0)
    rule_col = d["rule_hits"].to_pylist()
    rule_hits = [rule_col[order[p]] if h else "" for p, h in zip(pos.tolist(), have.tolist())] if d_id.size else []
    reason_col = d["reason_codes"].to_pylist()
    reasons = [reason_col[order[p]] if h else "" for p, h in zip(pos.tolist(), have.tolist())] if d_id.size else []
    fraud = ev["is_fraud"].to_numpy().astype(bool) & have
    ftype = ev["fraud_type"].to_numpy()
    amt = ev["amount_usd_minor"].to_numpy()
    legit = (~ev["is_fraud"].to_numpy().astype(bool)) & have
    flagged = outcome >= 2
    declined = outcome == 3
    res: dict[str, Any] = {
        "fraud_events": int(fraud.sum()), "legit_events": int(legit.sum()),
        "recall_flagged": float(flagged[fraud].mean()) if fraud.any() else None,
        "recall_declined": float(declined[fraud].mean()) if fraud.any() else None,
        "precision_flagged": float(fraud[flagged].mean()) if flagged.any() else None,
        "false_positive_rate": float(flagged[legit].mean()) if legit.any() else None,
        "fraud_usd_total": float(amt[fraud].sum() / 100), "fraud_usd_approved": float(amt[fraud & ~flagged].sum() / 100),
        "by_type": {},
    }
    # per-rule / per-reason precision and false-positive contribution
    per_rule: dict[str, dict[str, int]] = {}
    for i, (rh, rc) in enumerate(zip(rule_hits, reasons)):
        if not have[i]:
            continue
        keys = set(x for x in rh.split(",") if x) | {x for x in rc.split(",") if x.startswith("SCORE_")}
        for k in keys:
            st = per_rule.setdefault(k, {"hits": 0, "fraud": 0, "legit": 0})
            st["hits"] += 1
            st["fraud" if fraud[i] else "legit"] += 1
    n_legit = max(1, int(legit.sum()))
    res["by_rule"] = {k: {**v, "precision": v["fraud"] / v["hits"], "legit_hit_rate": v["legit"] / n_legit}
                      for k, v in sorted(per_rule.items(), key=lambda kv: -kv[1]["legit"])}
    # the served model on its own: ranking quality of the score where the model actually answered
    # (rules-only fallbacks carry no model score and are reported separately)
    if "path" in d.column_names and "score" in d.column_names and d_id.size:
        path = np.where(have, d["path"].to_numpy()[order][pos], -1)
        score = np.where(have, d["score"].to_numpy()[order][pos], np.nan)
        scored = have & (path == 0)
        fb = have & (path == 1)
        y = ev["is_fraud"].to_numpy().astype(bool)
        model: dict[str, Any] = {"scored": int(scored.sum()), "rules_only_fallback": int(fb.sum()),
                                 "fallback_recall_flagged": float(flagged[fb & y].mean()) if (fb & y).any() else None}
        if scored.any() and y[scored].any() and (~y[scored]).any():
            model["roc_auc"] = roc_auc(y[scored], score[scored])
            model["pr_auc"] = average_precision(y[scored], score[scored])
            model["fraud_base_rate"] = float(y[scored].mean())
        res["model"] = model
    res["evaluation"] = _evaluation(run_dir, d)
    for t, name in FRAUD_TYPES.items():
        m = fraud & (ftype == t)
        if not m.any():
            continue
        camp = ev["campaign_id"].to_numpy()[m]
        oc = outcome[m]
        tt = ev["event_time_us"].to_numpy()[m]
        # time to detect: fraud events approved before the first decline, per campaign
        ttd = []
        for c in np.unique(camp):
            cm = camp == c
            o = oc[cm][np.argsort(tt[cm])]
            first = np.flatnonzero(o == 3)
            ttd.append(int(first[0]) if first.size else int(o.size))
        res["by_type"][name] = {"events": int(m.sum()), "campaigns": int(np.unique(camp).size),
                                "recall_flagged": float((oc >= 2).mean()), "recall_declined": float((oc == 3).mean()),
                                "approved_before_first_decline_mean": float(np.mean(ttd)),
                                "usd_approved": float(amt[m][oc == 1].sum() / 100)}
    return res


def _evaluation(run_dir: Path, d: pa.Table) -> dict[str, Any]:
    """Leakage guard: is this run scored by a model that was trained on this very world? Benchmark
    worlds are deterministic per seed, so training and evaluating on the same seed means the model has
    seen these customers, cards, devices and campaigns: its detection numbers are in-sample (optimistic)."""
    if "model_version" not in d.column_names or not d.num_rows:
        return {"sample": "unknown", "reason": "no model version recorded"}
    vs = [v for v in d["model_version"].to_pylist() if v]
    if not vs:
        return {"sample": "unknown", "reason": "no model version recorded"}
    version = max(set(vs), key=vs.count)
    try:
        man = json.loads((run_dir / "manifest.json").read_text())
    except (OSError, ValueError):
        man = {}
    from ..train.proxy import training_meta
    meta = training_meta(version)
    if meta is None:
        trained = version.startswith("proxy-")
        return {"sample": "unknown" if trained else "not-trained", "model_version": version,
                "reason": ("trained elsewhere: no training metadata on this install" if trained
                           else "hand-set default weights: not fitted to any data")}
    prov = meta.get("provenance") or []
    if not prov:
        # trained before provenance was recorded: resolve the training runs it names, if still on disk
        for name in meta.get("runs") or []:
            try:
                m = json.loads((run_dir.parent / name / "manifest.json").read_text())
                prov.append({"run_id": name, "scenario": m.get("scenario"), "world_hash": m.get("world_hash")})
            except (OSError, ValueError):
                pass
        if not prov or not any(p.get("world_hash") for p in prov):
            return {"sample": "unknown", "model_version": version,
                    "reason": "no provenance for this model's training data: cannot rule out in-sample"}
    same = [p for p in prov if p.get("world_hash") and p.get("world_hash") == man.get("world_hash")]
    out = {"model_version": version, "labels": (meta.get("label_stats") or {}).get("labels", "ground-truth"),
           "trained_on": [{k: p.get(k) for k in ("run_id", "scenario", "seed")} for p in prov]}
    if same:
        return {**out, "sample": "in-sample",
                "reason": f"the model was trained on this world (seed/world hash of {same[0].get('run_id')}): "
                          "detection is optimistic; evaluate on another seed"}
    return {**out, "sample": "out-of-sample", "reason": "different world (seed) from every training run"}


def _service_stats(run_dir: Path, warmup_s: float) -> dict[str, Any]:
    p = run_dir / "metrics.jsonl"
    if not p.exists():
        return {}
    agg: dict[str, dict[str, list[float]]] = {}
    counters: dict[str, float] = {}
    first_ts = None
    for line in p.read_text().splitlines():
        try:
            s = json.loads(line)
        except ValueError:
            continue
        if s.get("source") in ("emitter", "collector"):
            continue
        ts = int(s.get("ts_us", 0))
        first_ts = first_ts or ts
        if (ts - first_ts) / 1e6 < warmup_s:
            continue
        src = s["source"]
        for st in s.get("stages", []):
            a = agg.setdefault(f"{src}.{st['name']}", {"p50": [], "p99": [], "count": []})
            a["p50"].append(float(st.get("p50_ms", 0))); a["p99"].append(float(st.get("p99_ms", 0)))
            a["count"].append(int(st.get("count", 0)))
        for k, v in s.get("counters", {}).items():
            if not k.endswith("_total"):
                counters[f"{src}.{k}"] = counters.get(f"{src}.{k}", 0) + float(v)
    return {"stages": {k: {"p50_median_ms": float(np.median(v["p50"])), "p99_median_ms": float(np.median(v["p99"])),
                           "p99_max_ms": float(np.max(v["p99"])), "count": int(np.sum(v["count"]))}
                       for k, v in agg.items()}, "counters": counters}


# ------------------------------------------------------------------------------------- rendering
def _f(v: Any, d: int = 2) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "-"
    if isinstance(v, float):
        return f"{v:.{d}f}"
    return str(v)


def _startup_item(x: dict[str, Any]) -> str:
    took = "skipped" if x["status"] == "skipped" else f"{x.get('duration_s') or 0:.1f} s"
    flag = "" if x["status"] in ("done", "skipped") else f" ({x['status']})"
    return f"{x['label'].lower()} {took}{flag}"


def render_text(rep: dict[str, Any]) -> str:
    s = rep["summary"]
    lines = [f"=== {rep['run_id']} ({rep['topology']}) : {s['verdict']} ===",
             f"emitted {s['emitted']:,}  decided {s['decided']:,}  missing {s['missing']:,}  dups {s['duplicates']:,}  "
             f"achieved {s['achieved_emit_tps']:,.0f} TPS",
             f"e2e p50 {_f(s['e2e_p50_ms'])} ms  p99 {_f(s['e2e_p99_ms'])} ms  p99.9 {_f(s['e2e_p999_ms'])} ms  "
             f"hot-path p99 {_f(s['hot_path_p99_ms'])} ms  within budget {s['within_budget']:.2%}"]
    if s["reasons"]:
        lines.append("reasons: " + "; ".join(s["reasons"]))
    if rep.get("startup"):
        steps = [x for x in rep["startup"].get("steps", []) if x.get("id") != "measure" and x.get("status") != "pending"]
        lines.append(f"startup: prepared in {s.get('prepare_s', 0):.1f} s: " + ", ".join(_startup_item(x) for x in steps))
    lines.append(f"{'step':40s} {'p50':>8s} {'p90':>8s} {'p99':>8s} {'p99.9':>8s} {'max':>8s}  (ms)")
    for k in STEP_ORDER:
        st = rep["stages"].get(k)
        if st and st.get("count"):
            lines.append(f"{STEP_LABEL.get(k, k):40s} {st['p50']:8.2f} {st['p90']:8.2f} {st['p99']:8.2f} "
                         f"{st['p999']:8.2f} {st['max']:8.2f}")
    if rep.get("levels"):
        lines.append("latency vs throughput:")
        lines.append(f"  {'target':>8s} {'emitted':>8s} {'decided':>8s} {'e2e p50':>8s} {'e2e p99':>8s} {'hot p99':>8s} {'model p99':>9s} {'feat p99':>8s}")
        for lv in rep["levels"]:
            st = lv["stages"]
            g = lambda k, q="p99": _f(st.get(k, {}).get(q))  # noqa: E731
            lines.append(f"  {lv['target_tps']:8.0f} {lv['emitted_tps']:8.0f} {lv['decided_tps']:8.0f} {g('e2e', 'p50'):>8s} "
                         f"{g('e2e'):>8s} {g('hot_path'):>8s} {g('model_call'):>9s} {g('feature_lookup'):>8s}")
    if s.get("knee"):
        lines.append(f"knee: {s['knee']}")
    t = rep.get("tail_attribution")
    if t:
        lines.append(f"tail (>= p99 {t['threshold_ms']:.1f} ms) dominated by {t['dominant']}: "
                     + ", ".join(f"{k} {v:.0%}" for k, v in sorted(t['share'].items(), key=lambda x: -x[1])[:5]))
    from .resources import render as _render_resources
    lines.extend(_render_resources(rep.get("resources") or {}))
    det = rep.get("detection")
    if det and det.get("fraud_events"):
        lines.append(f"detection: recall(flagged) {_f(det['recall_flagged'])}  precision {_f(det['precision_flagged'])}  "
                     f"FPR {_f(det['false_positive_rate'], 4)}  fraud $ approved {det['fraud_usd_approved']:,.0f} / {det['fraud_usd_total']:,.0f}")
        ev_ = det.get("evaluation") or {}
        if ev_:
            lines.append(f"evaluation: {ev_.get('sample')} (model {ev_.get('model_version')}): {ev_.get('reason')}")
        md = det.get("model") or {}
        if md.get("roc_auc") is not None:
            lines.append(f"model alone: ROC-AUC {md['roc_auc']:.4f}  PR-AUC {md['pr_auc']:.4f}  "
                         f"(scored {md['scored']:,}, rules-only fallbacks {md['rules_only_fallback']:,})")
    return "\n".join(lines)


def _write(run_dir: Path, rep: dict[str, Any]) -> None:
    from .html import render_html, render_markdown
    (run_dir / "report.json").write_text(json.dumps(rep, indent=1, default=float))
    (run_dir / "report.md").write_text(render_markdown(rep))
    (run_dir / "report.html").write_text(render_html(rep))
