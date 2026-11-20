"""Event & audit explorer: look up any event / decision / customer / fraud campaign of a finished run from
its Parquet artefacts (events-*, decisions-*, features-*, emit-*). Single-decision lookups use Parquet
row-group statistics (filters on event_id) so they stay fast on multi-million-event runs; search uses a
small cached projection per run."""
from __future__ import annotations

import collections
import glob
import json
import threading
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from fastapi import APIRouter, HTTPException

from ..features import load_schema
from ..features.derived import CARD_TYPE, CHANNEL, ENTRY_MODE, PAYMENT_TYPE

OUTCOME = {0: None, 1: "APPROVE", 2: "REVIEW", 3: "DECLINE", 4: "CHALLENGE"}
PATH = {0: "FULL", 1: "RULES_ONLY_FALLBACK", 2: "PROVISIONAL", 3: "FINAL_AFTER_PROVISIONAL", 4: "ECHO"}
INGRESS = {0: None, 1: "SYNC_GRPC", 2: "SYNC_HTTP", 3: "ASYNC_KAFKA_GATEWAY", 4: "ASYNC_FLINK_INLINE"}
def _fraud_names(run_dir=None) -> dict[int, str]:
    """Typology names by id: the run's use-case snapshot, else the current registry (contracts/usecases)."""
    from ..usecases import for_run
    return for_run(run_dir).fraud_type_names()
STAGE_NAMES = {10: "INGEST", 11: "QUEUE_WAIT", 12: "DESERIALIZE", 20: "SHUFFLE", 21: "STATE_FEATURES",
               22: "FEATURE_STORE_WRITE", 30: "FEATURE_LOOKUP", 31: "FEATURE_ASSEMBLE", 40: "MODEL_CALL",
               41: "MODEL_BATCH_WAIT", 42: "MODEL_SERVER_QUEUE", 43: "MODEL_SERVER_COMPUTE", 50: "RULES",
               51: "POLICY", 60: "AUDIT_ACK", 61: "SINK", 70: "DECISION", 71: "RESPONSE", 80: "FALLBACK",
               81: "ASYNC_WAIT"}
ENTITY_NAMES = {1: "customer", 2: "card", 3: "device", 4: "ip", 5: "merchant", 6: "terminal", 7: "payee"}
ROW_EVENT_COLS = ["event_id", "customer_id", "card_id", "event_time_us", "payment_type", "channel",
                  "amount_usd_minor", "merchant_country", "is_fraud", "fraud_type", "campaign_id"]


def _enum(names: list[str], v: Any) -> str:
    try:
        return names[int(v)]
    except (IndexError, TypeError, ValueError):
        return str(v)


def _files(run_dir: Path, pattern: str) -> list[str]:
    return sorted(glob.glob(str(run_dir / pattern)))


def _read(run_dir: Path, pattern: str, columns: list[str] | None = None, filters=None) -> pa.Table | None:
    files = _files(run_dir, pattern)
    if not files:
        return None
    tables = []
    for f in files:
        schema = pq.read_schema(f)
        cols = [c for c in columns if c in schema.names] if columns else None
        flt = filters if filters is None or all(c[0] in schema.names for c in filters) else None
        tables.append(pq.read_table(f, columns=cols, filters=flt))
    return pa.concat_tables(tables, promote_options="permissive")


class _Slim:
    """Cached search projection of one run (events joined with first-revision decisions)."""

    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.names = _fraud_names(run_dir)
        ev = _read(run_dir, "events-*.parquet", ROW_EVENT_COLS + ["backfill"])
        if ev is None:
            raise HTTPException(404, "run has no event log")
        if "backfill" in ev.column_names:
            ev = ev.filter(pc.invert(ev["backfill"]))
        self.ev = {c: ev[c].to_numpy(zero_copy_only=False) for c in ROW_EVENT_COLS if c in ev.column_names}
        n = len(self.ev["event_id"])
        self.outcome = np.zeros(n, np.int8)
        self.score = np.full(n, np.nan, np.float32)
        self.e2e = np.full(n, np.nan, np.float64)
        dec = _read(run_dir, "decisions-*.parquet", ["event_id", "revision", "outcome", "score", "t_intended_us",
                                                     "t_collected_us"])
        if dec is not None and dec.num_rows:
            dec = dec.filter(pc.equal(dec["revision"], 0))
            did = dec["event_id"].to_numpy()
            order = np.argsort(did)
            did = did[order]
            pos = np.clip(np.searchsorted(did, self.ev["event_id"]), 0, max(0, did.size - 1))
            hit = did[pos] == self.ev["event_id"] if did.size else np.zeros(n, bool)
            src = order[pos]
            self.outcome = np.where(hit, dec["outcome"].to_numpy()[src], 0).astype(np.int8)
            self.score = np.where(hit, dec["score"].to_numpy()[src], np.nan)
            e2e = (dec["t_collected_us"].to_numpy() - dec["t_intended_us"].to_numpy()) / 1000.0
            self.e2e = np.where(hit, e2e[src], np.nan)
        self.order = np.argsort(self.ev["event_time_us"], kind="stable")

    def rows(self, idx: np.ndarray) -> list[dict[str, Any]]:
        e = self.ev
        out = []
        for i in idx.tolist():
            sc = float(self.score[i])
            e2e = float(self.e2e[i])
            out.append({"event_id": str(int(e["event_id"][i])), "customer_id": str(int(e["customer_id"][i])),
                        "card_id": str(int(e["card_id"][i])), "event_time_us": int(e["event_time_us"][i]),
                        "payment_type": _enum(PAYMENT_TYPE, e["payment_type"][i]),
                        "channel": _enum(CHANNEL, e["channel"][i]),
                        "amount_usd": int(e["amount_usd_minor"][i]) / 100.0,
                        "merchant_country": str(e["merchant_country"][i]),
                        "outcome": OUTCOME.get(int(self.outcome[i])), "score": None if np.isnan(sc) else sc,
                        "is_fraud": bool(e["is_fraud"][i]), "fraud_type": self.names.get(int(e["fraud_type"][i]), "?"),
                        "campaign_id": str(int(e["campaign_id"][i])), "e2e_ms": None if np.isnan(e2e) else e2e})
        return out


class Explorer:
    def __init__(self, runs_dir: Path) -> None:
        self.runs_dir = runs_dir
        self._cache: collections.OrderedDict[str, _Slim] = collections.OrderedDict()
        self._lock = threading.Lock()
        self._worlds: dict[str, Any] = {}

    def run_dir(self, run_id: str) -> Path:
        d = self.runs_dir / run_id
        if "/" in run_id or ".." in run_id or not d.is_dir():
            raise HTTPException(404, "no such run")
        return d

    def slim(self, run_id: str) -> _Slim:
        with self._lock:
            hit = self._cache.get(run_id)
            if hit is not None:
                self._cache.move_to_end(run_id)
                return hit
        s = _Slim(self.run_dir(run_id))
        with self._lock:
            self._cache[run_id] = s
            while len(self._cache) > 3:
                self._cache.popitem(last=False)
        return s

    # ------------------------------------------------------------------ search
    def search(self, run_id: str, q: str, kind: str, limit: int) -> list[dict[str, Any]]:
        s = self.slim(run_id)
        e = s.ev
        q = (q or "").strip()
        if ":" in q and kind == "auto":
            kind, q = q.split(":", 1)
            kind, q = kind.strip().lower(), q.strip()
        if not q:
            fraud = np.flatnonzero(e["is_fraud"])
            recent = s.order[-max(1, limit // 2):]
            pick = np.unique(np.concatenate([recent, fraud[-max(1, limit // 2):]]))
            pick = pick[np.argsort(e["event_time_us"][pick])][::-1][:limit]
            return s.rows(pick)
        if kind == "fraud":
            code = {v: k for k, v in _fraud_names(self.run_dir(run_id)).items()}.get(q.lower())
            mask = e["fraud_type"] == code if code is not None else np.zeros(len(e["event_id"]), bool)
        elif kind == "outcome":
            code = {v: k for k, v in OUTCOME.items() if v}.get(q.upper())
            mask = s.outcome == code if code is not None else np.zeros(len(e["event_id"]), bool)
        else:
            try:
                v = np.uint64(int(q))
            except ValueError:
                raise HTTPException(400, "search by id, or use customer:/card:/campaign:/event:/fraud:/outcome:") from None
            cols = {"event": ["event_id"], "customer": ["customer_id"], "card": ["card_id"],
                    "campaign": ["campaign_id"]}.get(kind, ["event_id", "customer_id", "card_id", "campaign_id"])
            mask = np.zeros(len(e["event_id"]), bool)
            for c in cols:
                mask |= e[c].astype(np.uint64) == v
        idx = np.flatnonzero(mask)
        idx = idx[np.argsort(e["event_time_us"][idx], kind="stable")][:limit]
        return s.rows(idx)

    # ------------------------------------------------------------------ one decision
    def decision(self, run_id: str, event_id: int, maturity_days: float = 30.0) -> dict[str, Any]:
        d = self.run_dir(run_id)
        flt = [("event_id", "=", event_id)]
        ev = _read(d, "events-*.parquet", None, flt)
        if ev is None or ev.num_rows == 0:
            raise HTTPException(404, "event not in this run")
        er = ev.slice(0, 1).to_pylist()[0]
        event = {k: v for k, v in er.items() if k not in ("is_fraud", "fraud_type", "campaign_id", "t_intended_us",
                                                         "t_intended_offset_us", "backfill")}
        for k in ("event_id", "customer_id", "account_id", "card_id", "merchant_id", "terminal_id", "device_id",
                  "payee_id"):
            if k in event:
                event[k] = str(event[k])
        for k, names in (("payment_type", PAYMENT_TYPE), ("channel", CHANNEL), ("entry_mode", ENTRY_MODE),
                         ("card_type", CARD_TYPE)):
            if k in event:
                event[k] = _enum(names, event[k])
        event["amount_usd"] = er.get("amount_usd_minor", 0) / 100.0
        label = {"is_fraud": bool(er.get("is_fraud")), "fraud_type": _fraud_names(self.run_dir(run_id)).get(int(er.get("fraud_type") or 0), "?"),
                 "campaign_id": str(er.get("campaign_id") or 0)}
        dec = _read(d, "decisions-*.parquet", None, flt)
        decision = None
        spans: list[dict[str, Any]] = []
        timing: dict[str, Any] = {}
        if dec is not None and dec.num_rows:
            rows = sorted(dec.to_pylist(), key=lambda r: r.get("revision", 0))
            r = rows[0]
            decision = {"outcome": OUTCOME.get(r.get("outcome")), "score": r.get("score"),
                        "path": PATH.get(r.get("path"), str(r.get("path"))), "degraded": bool(r.get("degraded")),
                        "ingress_mode": INGRESS.get(r.get("ingress_mode")),
                        "reason_codes": [x for x in (r.get("reason_codes") or "").split(",") if x],
                        "rule_hits": [x for x in (r.get("rule_hits") or "").split(",") if x],
                        "model_version": r.get("model_version"), "revision": r.get("revision", 0),
                        "revisions": len(rows)}
            if r.get("span_stage"):
                for st, at, dm, s0, du in zip(r["span_stage"], r["span_attr"], r["span_domain"], r["span_start_us"],
                                              r["span_dur_us"]):
                    spans.append({"stage": int(st), "stage_name": STAGE_NAMES.get(int(st), str(st)),
                                  "attr": int(at), "entity": ENTITY_NAMES.get(int(at)) if st in (20, 21) else None,
                                  "domain": int(dm), "start_us": int(s0), "dur_us": int(du)})
            else:  # runs recorded before full spans were collected: durations only
                for k, v in r.items():
                    if k.startswith("st_") and v is not None and v >= 0:
                        spans.append({"stage": None, "stage_name": k[3:].upper(), "attr": 0, "entity": None,
                                      "domain": None, "start_us": None, "dur_us": int(v)})
            timing = {"t_intended_us": r.get("t_intended_us"), "t_sent_us": r.get("t_sent_us"),
                      "t_ingest_us": r.get("t_ingest_us"), "t_decision_us": r.get("t_decision_us"),
                      "t_collected_us": r.get("t_collected_us")}
        em = _read(d, "emit-*.parquet", None, flt)
        if em is not None and em.num_rows:
            row = em.slice(0, 1).to_pylist()[0]
            timing.setdefault("t_intended_us", row.get("t_intended_us"))
            timing.setdefault("t_sent_us", row.get("t_sent_us"))
            timing["ack_us"] = row.get("ack_us")
            if row.get("t_response_us"):
                timing["t_response_us"] = row["t_response_us"]
        end = timing.get("t_response_us") or timing.get("t_collected_us")
        if end and timing.get("t_intended_us"):
            timing["e2e_ms"] = (end - timing["t_intended_us"]) / 1000.0
        features = None
        ft = _read(d, "features-*.parquet", None, flt)
        if ft is not None and ft.num_rows:
            fr = ft.slice(0, 1).to_pylist()[0]
            schema = load_schema()
            mb = fr.get("missing") or b""
            bits = np.unpackbits(np.frombuffer(mb, np.uint8), bitorder="little") if mb else np.zeros(0, np.uint8)
            features = [{"index": f.index, "name": f.name, "entity": f.entity or "derived", "source": f.source,
                         "agg": f.agg, "value": float(v), "default": f.default,
                         "missing": bool(bits[f.index]) if f.index < bits.size else False}
                        for f, v in zip(schema.features, fr["values"])]
        try:
            label.update(self._label_story(run_id, d, er, maturity_days))
        except Exception as e:  # noqa: BLE001 - an explanation must never break the event view
            label["story_error"] = f"{type(e).__name__}: {e}"
        explanation = None
        if features is not None and decision is not None:
            try:
                explanation = self._explain(d, er, features, decision)
            except Exception as e:  # noqa: BLE001
                explanation = {"error": f"{type(e).__name__}: {e}"}
        return {"event": event, "decision": decision, "features": features, "spans": spans, "timing": timing,
                "label": label, "explanation": explanation}

    def _label_story(self, run_id: str, d: Path, er: dict[str, Any], maturity_days: float) -> dict[str, Any]:
        from ..explain import label_story
        from ..usecases import for_run
        reg = for_run(d)
        step = None
        cid = int(er.get("campaign_id") or 0)
        if cid and er.get("campaign_role"):
            members = _read(d, "events-*.parquet", ["event_id", "event_time_us", "campaign_role"],
                            [("campaign_id", "=", cid)])
            if members is not None and members.num_rows:
                m = members.to_pydict()
                same = sorted((t, e) for t, e, r in zip(m["event_time_us"], m["event_id"], m["campaign_role"])
                              if r == er["campaign_role"])
                ids = [e for _, e in same]
                if int(er["event_id"]) in ids:
                    step = (ids.index(int(er["event_id"])) + 1, len(ids))
        run_end = int(np.max(self.slim(run_id).ev["event_time_us"])) if len(self.slim(run_id).ev["event_time_us"]) else 0
        return {"story": label_story(er, reg, run_end, maturity_days, step)}

    def _explain(self, d: Path, er: dict[str, Any], features: list[dict[str, Any]], decision: dict[str, Any]) -> dict[str, Any]:
        """Policy path, every rule with the values it saw, and the model's attribution (realy.explain)."""
        import json as _json

        from ..explain import explain_rules, integrated_gradients, policy_path, top_attributions
        from ..features.derived import event_view
        from ..scenario import Scenario
        from ..policy import RuleSet as Rules
        from ..verify.verify import ProxyScorer
        sc = Scenario.model_validate(_json.loads((d / "scenario.json").read_text()))
        cfg = sc.scoring_config()
        from ..scenario import run_rules
        rules_yaml, rules_source = run_rules(d, sc)
        rules = Rules(rules_yaml).with_thresholds(cfg.get("review_threshold"), cfg.get("decline_threshold"),
                                                  cfg.get("challenge_threshold"))
        fmap = {f["name"]: float(f["value"]) for f in features}
        view = event_view(er)
        results = explain_rules(rules_yaml, fmap, view)
        score = decision.get("score")
        out: dict[str, Any] = {"rule_set": rules.version, "rules_source": rules_source, "rules": results,
                               "policy": policy_path(results, score, rules.review, rules.decline, decision.get("outcome"),
                                                     decision.get("path"), rules.challenge, rules.challengeable(view),
                                                     rules.challenge_when),
                               "served_rule_hits": decision.get("rule_hits")}
        # the step-up's fate (instrument-only latent result): what happens if this payment is challenged
        from .. import challenge as stepup
        r = int(stepup.for_run(d, np.asarray([int(er["event_id"])], np.uint64), np.asarray([bool(er["is_fraud"])]),
                               np.asarray([int(er["fraud_type"])]))[0])
        out["stepup"] = {"result": stepup.RESULTS[r], "challenged": decision.get("outcome") == "CHALLENGE",
                         "final": (None if decision.get("outcome") != "CHALLENGE" else
                                   {stepup.PASS: "APPROVED after passing the step-up", stepup.FAIL: "DECLINED: failed the step-up",
                                    stepup.ABANDON: "ABANDONED at the step-up"}[r])}
        if score is not None and decision.get("path") in ("FULL", None):
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                scorer = ProxyScorer.for_version(decision.get("model_version"))
            # attribute in the model's own input space (inputs bound by name: a model may use fewer
            # features than the schema has)
            x = scorer.to_model(np.asarray([[f["value"] for f in features]], np.float32))[0].astype(np.float64)
            names = scorer.names or [f["name"] for f in features][:x.shape[0]]
            ig = integrated_gradients(scorer, x)
            out["attribution"] = {"model_version": scorer.version, "score": ig["score"],
                                  "served_score": score, "baseline_score": ig["baseline_score"],
                                  "logit": ig["logit"], "baseline_logit": ig["baseline_logit"],
                                  "top": top_attributions(ig, names, x, scorer.mean),
                                  "method": "integrated gradients from the training mean (64 steps)"}
        return out

    # ------------------------------------------------------------------ customer / campaigns
    def customer(self, run_id: str, cid: int) -> dict[str, Any]:
        rows = self.search(run_id, str(cid), "customer", 500)
        profile = None
        try:
            from ..scenario import Scenario
            sc = Scenario.model_validate(json.loads((self.run_dir(run_id) / "scenario.json").read_text()))
            key = str(sc.world_dir)
            if key not in self._worlds:
                from ..world import World
                self._worlds = {key: World.load(sc.world_dir)}
            w = self._worlds[key]
            r = int(w.customer_row(np.asarray([cid], np.uint64))[0])
            if r >= 0:
                p = w.customer_profile_arrays()
                countries = load_schema().countries
                ci = int(p["home_country_idx"][r])
                profile = {"customer_id": str(cid), "segment": ["student", "mass", "affluent", "sme"][int(p["segment"][r]) % 4],
                           "home_country": countries[ci] if 0 <= ci < len(countries) else None,
                           "tenure_days": float(p["tenure_days"][r]), "avg_ticket_usd": float(p["avg_ticket_usd"][r])}
        except Exception:  # noqa: BLE001 - profile is optional context
            profile = None
        return {"customer": profile, "events": rows}

    def campaigns(self, run_id: str) -> list[dict[str, Any]]:
        s = self.slim(run_id)
        e = s.ev
        camp = e["campaign_id"].astype(np.uint64)
        idx = np.flatnonzero((camp > 0) & e["is_fraud"].astype(bool))
        out = []
        groups: dict[int, list[int]] = {}
        for i in idx.tolist():
            groups.setdefault(int(camp[i]), []).append(i)
        for cid, members in groups.items():
            m = np.asarray(members)
            m = m[np.argsort(e["event_time_us"][m], kind="stable")]
            oc = s.outcome[m]
            first_decl = np.flatnonzero(oc == 3)
            amt = e["amount_usd_minor"][m] / 100.0
            out.append({"campaign_id": str(cid), "fraud_type": _fraud_names(self.run_dir(run_id)).get(int(e["fraud_type"][m].max()), "?"),
                        "events": int(m.size), "flagged": int((oc >= 2).sum()), "declined": int((oc == 3).sum()),
                        "first_event_us": int(e["event_time_us"][m[0]]), "last_event_us": int(e["event_time_us"][m[-1]]),
                        "approved_before_first_decline": int(first_decl[0]) if first_decl.size else int(m.size),
                        "usd_total": float(amt.sum()), "usd_approved": float(amt[oc == 1].sum())})
        return sorted(out, key=lambda c: c["first_event_us"])

    def campaign_events(self, run_id: str, cid: int) -> list[dict[str, Any]]:
        return self.search(run_id, str(cid), "campaign", 2000)


def make_router(runs_dir: Path) -> APIRouter:
    ex = Explorer(runs_dir)
    r = APIRouter(prefix="/api/runs/{run_id}", tags=["explorer"])

    @r.get("/search")
    def search(run_id: str, q: str = "", kind: str = "auto", limit: int = 50) -> dict[str, Any]:
        return {"results": ex.search(run_id, q, kind, max(1, min(limit, 1000)))}

    @r.get("/decisions/{event_id}")
    def decision(run_id: str, event_id: str, maturity_days: float = 30.0) -> dict[str, Any]:
        try:
            eid = int(event_id)
        except ValueError:
            raise HTTPException(400, "event_id must be an integer") from None
        return ex.decision(run_id, eid, maturity_days)

    @r.get("/customers/{cid}")
    def customer(run_id: str, cid: str) -> dict[str, Any]:
        return ex.customer(run_id, int(cid))

    @r.get("/campaigns")
    def campaigns(run_id: str) -> dict[str, Any]:
        return {"campaigns": ex.campaigns(run_id)}

    @r.get("/campaigns/{cid}")
    def campaign(run_id: str, cid: str) -> dict[str, Any]:
        return {"events": ex.campaign_events(run_id, int(cid))}

    r.explorer = ex  # type: ignore[attr-defined]
    return r
