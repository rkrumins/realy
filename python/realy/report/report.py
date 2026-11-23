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
from ..policy import NAMES as OUTCOME  # noqa: E402  (APPROVE, REVIEW, DECLINE, CHALLENGE)
# typology names come from the run's use-case snapshot (python/realy/usecases.py), not a fixed table
STEP_ORDER = ["schedule_lag", "produce_ack", "ingress", "queue_wait", "deserialize", "txn_assembly", "shuffle", "state_features",
              "async_wait", "feature_lookup", "feature_assemble", "model_call", "model_batch_wait",
              "model_server_queue", "model_server_compute", "rules", "policy", "audit_ack", "hot_path", "egress", "e2e"]
STEP_LABEL = {
    "schedule_lag": "1 on-time publish (sent - intended)", "produce_ack": "2 produce ack (broker write)",
    "ingress": "3 ingress transit + pickup", "queue_wait": "  service queue wait", "deserialize": "  deserialize",
    "txn_assembly": "4 transaction assembly (wait for its parts)",
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


def transactions_summary(run_dir: Path) -> dict[str, Any] | None:
    """Multi-message transactions of a run: per contract, how many were assembled, how many without a
    required part, the assembly wait, the decisions taken again at scored stages, and the decisions broken
    down by the context values the contract names (`report.by`: a transfer's payee check, a card payment's
    3DS status), with ground truth."""
    import json as _json
    dec = _read(run_dir, "decisions-*.parquet")
    if dec is None or "txn" not in dec.column_names:
        return None
    dec = dec.filter(pa.compute.equal(dec["revision"], 0))
    names = dec["txn"].to_pylist()
    if not any(names):
        return None
    fraud = {}
    ev = _read(run_dir, "events-*.parquet")
    if ev is not None and "is_fraud" in ev.column_names:
        fraud = dict(zip(ev["event_id"].to_numpy().tolist(), ev["is_fraud"].to_numpy(zero_copy_only=False).tolist()))
    from ..bindings.transactions import transactions as _contracts
    known = _contracts()
    has_wait = "st_txn_assembly" in dec.column_names
    by_contract: dict[str, Any] = {}
    all_waits = []
    for cname in sorted({n.split("/", 1)[0] for n in names if n}):
        idx_all = [i for i, n in enumerate(names) if n and n.split("/", 1)[0] == cname]
        stages: dict[str, int] = {}
        for i in idx_all:
            st = names[i].split("/", 1)[1] if "/" in names[i] else ""
            stages[st] = stages.get(st, 0) + 1
        # the transaction's own decision (at assembly); re-scored stages are counted under `stages`
        idx = [i for i in idx_all if names[i].endswith("/assembled")] or idx_all
        sub = dec.take(pa.array(idx))
        partial = sub["txn_partial"].to_numpy(zero_copy_only=False)
        wait = sub["st_txn_assembly"].to_numpy() if has_wait else np.zeros(0)
        wait = wait[wait >= 0]
        all_waits.append(wait)
        ctx = [(_json.loads(x) if x else {}) for x in sub["txn_context"].to_pylist()]
        oc = sub["outcome"].to_numpy().tolist()
        eids = sub["event_id"].to_numpy().tolist()
        breakdown: dict[str, dict[str, dict[str, int]]] = {}
        contract = known.get(cname)
        for key in (contract.report_by if contract else ()):
            spec = next(c for c in contract.context if c.name == key)
            dflt = contract.context_default(spec)        # a context recorded without it (its part never came)
            b_: dict[str, dict[str, int]] = {}
            for c, o, e in zip(ctx, oc, eids):
                b = b_.setdefault(str(c.get(key, dflt)), {"decided": 0, "fraud": 0, "stopped": 0, "fraud_stopped": 0})
                b["decided"] += 1
                stopped = OUTCOME.get(int(o)) in ("DECLINE", "REVIEW", "CHALLENGE")
                b["stopped"] += int(stopped)
                if fraud.get(int(e)):
                    b["fraud"] += 1
                    b["fraud_stopped"] += int(stopped)
            breakdown[key] = dict(sorted(b_.items()))
        by_contract[cname] = {"assembled": len(idx), "stages": stages, "partial": int(partial.sum()),
                              "partial_fraction": float(partial.mean()) if partial.size else 0.0,
                              "assembly_wait_ms": _pct(_ms(wait)) if wait.size else None, "by": breakdown}
    parts_sent = 0
    for f in glob.glob(str(run_dir / "emit-*.json")):
        try:
            parts_sent += int(_json.loads(Path(f).read_text()).get("parts_sent") or 0)
        except (OSError, ValueError):
            pass
    wait = np.concatenate(all_waits) if all_waits else np.zeros(0)
    stages_all: dict[str, int] = {}
    for c in by_contract.values():
        for k, v in c["stages"].items():
            stages_all[k] = stages_all.get(k, 0) + v
    assembled = sum(c["assembled"] for c in by_contract.values())
    partial_n = sum(c["partial"] for c in by_contract.values())
    return {"contracts": sorted(by_contract), "assembled": assembled, "stages": stages_all,
            "partial": partial_n, "partial_fraction": partial_n / assembled if assembled else 0.0,
            "parts_sent": parts_sent, "assembly_wait_ms": _pct(_ms(wait)) if wait.size else None,
            "by_contract": by_contract}


def message_mix(run_dir: Path, decided: int) -> dict[str, Any] | None:
    """The standalone event types the emitters sent (logins, device telemetry...) besides the decided
    payments: each type's count and messages per decided payment, the extra load to size for."""
    sent: dict[str, int] = {}
    for f in glob.glob(str(run_dir / "emit-*.json")):
        try:
            for k, v in (json.loads(Path(f).read_text()).get("typed_sent") or {}).items():
                sent[k] = sent.get(k, 0) + int(v)
        except (OSError, ValueError):
            pass
    from ..bindings.pipeline import pipeline
    try:
        pipe = pipeline()
        sent = {k: v for k, v in sent.items() if pipe.standalone(k)}
    except KeyError:
        pass
    if not sent:
        return None
    total = sum(sent.values())
    return {"decided": decided, "messages": total, "per_payment": total / decided if decided else None,
            "by_type": {k: {"sent": v, "per_payment": v / decided if decided else None} for k, v in sorted(sent.items())}}


def _render_mix(mx: dict[str, Any]) -> list[str]:
    per = ", ".join(f"{k} {v['sent']:,} ({v['per_payment']:.2f}/payment)" if v["per_payment"] is not None else f"{k} {v['sent']:,}"
                    for k, v in mx["by_type"].items())
    return [f"message mix: {mx['messages']:,} messages besides {mx['decided']:,} decided payments: {per}"]


def _timing(run_dir: Path, rep: dict[str, Any]) -> None:
    """The run's startup steps and its timeline (runner/timeline.py): `prepare_s` is everything before
    traffic, inside a step or not; warm-up belongs to the traffic window, not to the preparation."""
    from ..runner.startup import read as read_startup
    from ..runner.timeline import timeline
    startup = read_startup(run_dir)
    if startup:
        rep["startup"] = startup
    tl = timeline(run_dir, live=False)
    rep["timeline"] = tl
    if tl["durations"]["prepare_s"] is not None:
        rep["summary"]["prepare_s"] = round(tl["durations"]["prepare_s"], 1)


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
        _timing(run_dir, rep)     # a run aborted while preparing still shows where its time went
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
        w = 60 if scenario.traffic.duration_s >= 180 else 10
        mins = _window_medians(series_p99, int(warm), len(series_p99), w)
        slope = float(np.polyfit(np.arange(len(mins)), mins, 1)[0]) if len(mins) >= 3 else 0.0
        half = len(backlog) // 2
        tail_backlog = backlog[half:int(scenario.traffic.duration_s)] if backlog else []
        growing = bool(len(tail_backlog) > 5 and tail_backlog[-1] > tail_backlog[0] + max(1000, 0.05 * cum_e[-1]))
        # p99 drift is judged where the offered load is constant: a scheduled step / ramp, or an operator's
        # live rate change, raises p99 by design (that is the knee analysis' job, not drift)
        segs = _constant_rate_segments(scenario, run, t0, int(warm), len(series_p99))
        judged = []
        for a, b in segs:
            sm = _window_medians(series_p99, a, b, w)
            if len(sm) >= 3:
                sl = float(np.polyfit(np.arange(len(sm)), sm, 1)[0])
                judged.append({"from_s": a, "to_s": b, "p99_by_window_ms": sm, "slope_ms_per_window": sl,
                               "drift": bool(sl > 0 and sl > 0.1 * float(np.median(sm)))})
        drift = any(x["drift"] for x in judged)
        sust = {"window_s": w, "p99_by_window_ms": mins, "p99_slope_ms_per_window": slope,
                "backlog_growing": growing, "max_backlog": int(max(backlog)) if backlog else 0,
                "drift_segments": judged, "rate_changes": len((run or {}).get("rate_changes") or []),
                "drift_judged": bool(judged) or None,
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
    txn = transactions_summary(run_dir)
    if txn:
        rep["transactions"] = txn
    mix = message_mix(run_dir, int(summary.get("decided") or summary.get("events") or 0))
    if mix:
        rep["message_mix"] = mix
    _timing(run_dir, rep)
    expect = scenario.expect.get("verdict")
    if expect:
        rep["summary"]["expected_verdict"] = expect
        rep["summary"]["matches_expectation"] = expect == verdict
    try:
        from ..usecases import for_run
        from .insights import build_insights, load_config, previous_report
        cfg = load_config()
        rep["insights_config"] = cfg
        rep["insights"] = build_insights(rep, cfg, previous_report(run_dir, scenario.name), for_run(run_dir))
    except Exception as e:  # noqa: BLE001 - insights must never break a report
        rep["insights"] = [{"severity": "info", "area": "report", "title": f"insights unavailable: {type(e).__name__}: {e}",
                            "detail": "", "evidence": {}, "link": None, "action": ""}]
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


def _label_coverage(ev: pa.Table, fraud: np.ndarray, ftype: np.ndarray, run_dir: Path) -> dict[str, Any] | None:
    """Of this run's fraud, the share a training set cut `label_maturity_days` (contracts/insights.yaml)
    after the window would see labelled, overall and per typology. Late and unreported fraud trains
    as legitimate."""
    if "label_delay_us" not in ev.column_names or not fraud.any():
        return None
    from ..usecases import for_run
    from .insights import load_config
    days = float(load_config().get("label_maturity_days", 30))
    et = ev["event_time_us"].to_numpy()
    delay = ev["label_delay_us"].to_numpy()
    cut = int(et[fraud].max()) + int(days * 86_400_000_000)
    seen = fraud & (delay >= 0) & (et + np.maximum(delay, 0) <= cut)
    names = for_run(run_dir).fraud_type_names()
    by_type = {names.get(int(t), str(t)): round(float(seen[fraud & (ftype == t)].mean()), 4)
               for t in np.unique(ftype[fraud])}
    return {"maturity_days": days, "fraud_events": int(fraud.sum()),
            "reported": round(float((delay[fraud] >= 0).mean()), 4), "coverage": round(float(seen[fraud].mean()), 4),
            "by_type": by_type}


def _detection(run_dir: Path, d: pa.Table, d_id: np.ndarray) -> dict[str, Any]:
    ev = _read(run_dir, "events-*.parquet")
    if ev is None or d is None or not d.num_rows:
        return {}
    from .insights import load_config
    cols = ["event_id", "is_fraud", "fraud_type", "campaign_id", "amount_usd_minor", "event_time_us", "label_delay_us",
            "payment_type"]
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
    flagged = outcome >= 2          # any intervention: REVIEW, DECLINE or CHALLENGE (step-up)
    declined = outcome == 3
    # what became of each payment after a step-up: the instrument's latent result (realy.challenge)
    from .. import challenge as stepup
    latent = stepup.for_run(run_dir, eid, ev["is_fraud"].to_numpy().astype(bool), ftype)
    holds = stepup.review_holds(ev["payment_type"].to_numpy()) if "payment_type" in ev.column_names else None
    fin = stepup.final(outcome, latent, holds)
    let_through = have & (fin == stepup.APPROVED)            # approved outright or after the step-up
    paid = have & np.isin(fin, stepup.PAID)                  # ... or reviewed after the money moved (card auths)
    stopped = have & np.isin(fin, stepup.STOPPED)            # declined, failed / abandoned the step-up, held review
    res: dict[str, Any] = {
        "fraud_events": int(fraud.sum()), "legit_events": int(legit.sum()),
        "recall_flagged": float(flagged[fraud].mean()) if fraud.any() else None,
        "recall_declined": float(declined[fraud].mean()) if fraud.any() else None,
        "recall_stopped": float(stopped[fraud].mean()) if fraud.any() else None,
        "precision_flagged": float(fraud[flagged].mean()) if flagged.any() else None,
        "false_positive_rate": float(flagged[legit].mean()) if legit.any() else None,
        "legit_flagged": int((flagged & legit).sum()), "legit_declined": int((declined & legit).sum()),
        "fraud_flagged": int((flagged & fraud).sum()), "fraud_declined": int((declined & fraud).sum()),
        "fraud_usd_total": float(amt[fraud].sum() / 100), "fraud_usd_approved": float(amt[fraud & let_through].sum() / 100),
        "fraud_usd_paid": float(amt[fraud & paid].sum() / 100),
        "review_holds": None if holds is None else sorted(set(load_config().get("review_holds", []))),
        "by_type": {},
        "stepup": _stepup(run_dir, outcome, latent, fin, fraud, legit, ftype, amt),
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
    res["labels"] = _label_coverage(ev, fraud, ftype, run_dir)
    from ..usecases import for_run
    for t, name in for_run(run_dir).fraud_type_names().items():
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
                                "recall_stopped": float(stopped[m].mean()),
                                "approved_before_first_decline_mean": float(np.mean(ttd)),
                                "usd_approved": float(amt[m & let_through].sum() / 100),
                                "usd_paid": float(amt[m & paid].sum() / 100)}
    return res


def _window_medians(series: list[float | None], a: int, b: int, w: int) -> list[float]:
    out = []
    for i in range(a, b, w):
        if i + w > b and out:
            break                      # a trailing partial window is noise, not a trend
        vals = [v for v in series[i:min(b, i + w)] if v is not None]
        if vals:
            out.append(float(np.median(vals)))
    return out


def _constant_rate_segments(scenario: Scenario, run: dict[str, Any] | None, t0: int | None, warm: int,
                            n: int) -> list[tuple[int, int]]:
    """[from_s, to_s) stretches of the measured window where the offered load is constant: the whole
    window for a constant profile, each level of a step profile (after its 20% transition), and none
    for ramp / spike / sine; every operator live rate change splits a stretch (10 s settle after it)."""
    prof = scenario.traffic.profile
    if prof.kind == "constant":
        segs = [(warm, n)]
    elif prof.kind == "step":
        width = prof.step_duration_s or 60.0
        segs = [(max(warm, int(li * width + 0.2 * width)), min(n, int((li + 1) * width)))
                for li in range(int(math.ceil(n / width)))]
    else:
        return []
    cuts = sorted((c["at_us"] - t0) / 1e6 for c in ((run or {}).get("rate_changes") or []) if t0 and c.get("at_us"))
    for c in cuts:
        nxt = []
        for a, b in segs:
            if c < b and c + 10 > a:           # the change, or its 10 s settle, overlaps this stretch
                if c > a:
                    nxt.append((a, int(c)))
                nxt.append((max(a, int(math.ceil(c + 10))), b))
            else:
                nxt.append((a, b))
        segs = nxt
    return [(a, b) for a, b in segs if b - a > 0]


def _stepup(run_dir: Path, outcome: np.ndarray, latent: np.ndarray, fin: np.ndarray, fraud: np.ndarray,
            legit: np.ndarray, ftype: np.ndarray, amt: np.ndarray) -> dict[str, Any] | None:
    """Step-up economics: who was challenged, who passed, failed or gave up, and what it cost. A
    challenge stops most automated fraud (no OTP, no PIN) but not a phished account takeover or a
    scam victim authorising their own payment, and every challenged good customer is friction, some
    of it a lost sale. None when the run issued no CHALLENGE."""
    from .. import challenge as stepup
    ch = outcome == stepup.CHALLENGED
    if not ch.any():
        return None
    from ..usecases import for_run

    def split(m: np.ndarray) -> dict[str, int]:
        return {name: int((m & ch & (latent == code)).sum()) for code, name in stepup.RESULTS.items()}
    lg, fr = legit & ch, fraud & ch
    names = for_run(run_dir).fraud_type_names()
    by_type = {}
    for t in np.unique(ftype[fr]):
        m = fr & (ftype == t)
        by_type[names.get(int(t), str(t))] = {"challenged": int(m.sum()), **split(fraud & (ftype == t)),
                                              "usd_passed": float(amt[m & (latent == stepup.PASS)].sum() / 100)}
    return {
        "challenged": int(ch.sum()), "legit_challenged": int(lg.sum()), "fraud_challenged": int(fr.sum()),
        "legit_challenge_rate": float(lg.sum() / max(1, legit.sum())),
        "fraud_challenge_rate": float(fr.sum() / max(1, fraud.sum())),
        "legit": split(legit), "fraud": split(fraud),
        "fraud_stopped": int((fr & (latent != stepup.PASS)).sum()),
        "fraud_passed": int((fr & (latent == stepup.PASS)).sum()),
        "fraud_usd_passed": float(amt[fr & (latent == stepup.PASS)].sum() / 100),
        "legit_abandoned": int((lg & (latent == stepup.ABANDON)).sum()),
        "lost_sales_usd": float(amt[lg & (latent == stepup.ABANDON)].sum() / 100),
        "legit_failed": int((lg & (latent == stepup.FAIL)).sum()),
        "by_type": dict(sorted(by_type.items(), key=lambda kv: -kv[1]["challenged"])),
        "note": "results are the instrument's simulated step-up outcomes (scenario fraud.challenge and each "
                "typology's challenge rates); the system under test never sees them",
    }


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



def _render_transactions(tx: dict[str, Any]) -> list[str]:
    """Text lines for the transactions section (one block per contract; old reports' single block too)."""
    def one(name: str, t: dict[str, Any], parts: str) -> list[str]:
        w = t.get("assembly_wait_ms") or {}
        again = [f"{k} {v:,}" for k, v in (t.get("stages") or {}).items() if k != "assembled"]
        out = [f"transactions ({name}): {t['assembled']:,} assembled{parts}; {t['partial']:,} without a required part "
               f"({t['partial_fraction']:.2%}); assembly wait p50 {_f(w.get('p50'))} ms p99 {_f(w.get('p99'))} ms"
               + (f"; decided again at {', '.join(again)}" if again else "")]
        by = dict(t.get("by") or {})
        if "by_payee_check" in t:                       # reports written before per-contract breakdowns
            by["payee_check"] = t["by_payee_check"]
        for key, vals in by.items():
            for k, b in vals.items():
                out.append(f"  {key.replace('_', ' ')} {k:24s} {b['decided']:6,} decided, {b['stopped']:5,} stopped; "
                           f"fraud {b['fraud']:4,} ({b['fraud_stopped']:,} stopped)")
        return out
    sent = f" from {tx['parts_sent']:,} more messages"
    per = tx.get("by_contract")
    if not per:
        return one(", ".join(tx["contracts"]), tx, sent)
    lines = []
    for name, t in per.items():
        lines += one(name, t, sent if len(per) == 1 else "")
    if len(per) > 1:
        lines.append(f"  {tx['parts_sent']:,} messages sent besides the payments' triggers")
    return lines

def render_text(rep: dict[str, Any]) -> str:
    s = rep["summary"]
    lines = [f"=== {rep['run_id']} ({rep['topology']}) : {s['verdict']} ===",
             f"emitted {s['emitted']:,}  decided {s['decided']:,}  missing {s['missing']:,}  dups {s['duplicates']:,}  "
             f"achieved {s['achieved_emit_tps']:,.0f} TPS",
             f"e2e p50 {_f(s['e2e_p50_ms'])} ms  p99 {_f(s['e2e_p99_ms'])} ms  p99.9 {_f(s['e2e_p999_ms'])} ms  "
             f"hot-path p99 {_f(s['hot_path_p99_ms'])} ms  within budget {s['within_budget']:.2%}"]
    if s["reasons"]:
        lines.append("reasons: " + "; ".join(s["reasons"]))
    ins = [x for x in rep.get("insights") or [] if x.get("severity") != "good"][:6]
    if ins:
        mark = {"critical": "!!", "warning": " !", "info": " -"}
        lines.append("insights:")
        lines += [f"  {mark.get(x['severity'], ' -')} {x['title']}" for x in ins]
    if rep.get("startup"):
        steps = [x for x in rep["startup"].get("steps", []) if x.get("id") not in ("warmup", "measure")
                 and x.get("group", "startup") == "startup" and x.get("status") != "pending"]
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
    tx = rep.get("transactions")
    if tx:
        lines.extend(_render_transactions(tx))
    if rep.get("message_mix"):
        lines.extend(_render_mix(rep["message_mix"]))
    det = rep.get("detection")
    if det and det.get("fraud_events"):
        lines.append(f"detection: recall(flagged) {_f(det['recall_flagged'])}  precision {_f(det['precision_flagged'])}  "
                     f"FPR {_f(det['false_positive_rate'], 4)}  fraud $ approved {det['fraud_usd_approved']:,.0f} / {det['fraud_usd_total']:,.0f}"
                     + (f"  stopped {_f(det['recall_stopped'], 3)}  fraud $ paid {det['fraud_usd_paid']:,.0f}"
                        if det.get("fraud_usd_paid") is not None else ""))
        su = det.get("stepup")
        if su:
            lines.append(f"step-up: {su['challenged']:,} challenged ({su['legit_challenge_rate']:.2%} of good customers); "
                         f"fraud stopped {su['fraud_stopped']:,}, passed {su['fraud_passed']:,} (${su['fraud_usd_passed']:,.0f}); "
                         f"good customers gave up {su['legit_abandoned']:,} (${su['lost_sales_usd']:,.0f} of sales)")
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
