"""Event & audit explorer: look up any event, decision, entity (customer, card, account, device, IP,
merchant, terminal, payee) or fraud campaign of a finished run from its Parquet artefacts (events-*,
decisions-*, features-*, emit-*).

- One decision is read with Parquet row-group statistics (a filter on event_id), so it stays fast on
  multi-million-event runs.
- Search, entity lists and trails read a compact index of the run's measured events (ids, enums,
  outcome, a usable score, rule hits, the step-up fate), built once per run and cached under a byte
  budget (REALY_EXPLORER_CACHE_MB). Per-entity groupings are built on first use and cached with it.
- Trail columns are data: contracts/explorer.yaml says which columns each entity and each kind of payment
  shows (a transfer its payee and transfer velocity, an online card payment its device, IP country, 3DS
  and CVV), so a new typology, entity column or feature needs no code here or in the UI.
"""
from __future__ import annotations

import collections
import glob
import json
import os
import threading
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import yaml
from fastapi import APIRouter, HTTPException

from ..features import load_schema
from ..features.derived import CARD_TYPE, CHANNEL, ENTRY_MODE, PAYMENT_TYPE, THREE_DS
from ..scenario import REPO_ROOT

OUTCOME = {0: None, 1: "APPROVE", 2: "REVIEW", 3: "DECLINE", 4: "CHALLENGE"}
PATH = {0: "FULL", 1: "RULES_ONLY_FALLBACK", 2: "PROVISIONAL", 3: "FINAL_AFTER_PROVISIONAL", 4: "ECHO"}
SCORED_PATHS = (0, 3)       # decisions that carry a model score; a fallback / provisional / echo stores 0.0
INGRESS = {0: None, 1: "SYNC_GRPC", 2: "SYNC_HTTP", 3: "ASYNC_KAFKA_GATEWAY", 4: "ASYNC_FLINK_INLINE"}
STAGE_NAMES = {10: "INGEST", 11: "QUEUE_WAIT", 12: "DESERIALIZE", 20: "SHUFFLE", 21: "STATE_FEATURES",
               22: "FEATURE_STORE_WRITE", 23: "TXN_ASSEMBLY", 30: "FEATURE_LOOKUP", 31: "FEATURE_ASSEMBLE", 40: "MODEL_CALL",
               41: "MODEL_BATCH_WAIT", 42: "MODEL_SERVER_QUEUE", 43: "MODEL_SERVER_COMPUTE", 50: "RULES",
               51: "POLICY", 60: "AUDIT_ACK", 61: "SINK", 70: "DECISION", 71: "RESPONSE", 80: "FALLBACK",
               81: "ASYNC_WAIT"}
ENTITY_NAMES = {1: "customer", 2: "card", 3: "device", 4: "ip", 5: "merchant", 6: "terminal", 7: "payee"}
CPP_MIN_CARDS = 3          # later-fraud cards an entity needs before its common-point lift is ranked
VIEWS_PATH = Path(os.environ.get("REALY_EXPLORER", REPO_ROOT / "contracts" / "explorer.yaml"))

# ---------------------------------------------------------------------------------------- the index
# event columns held per measured event (dtype, default for runs recorded before the column existed)
def _entity_fields() -> dict[str, str]:
    """Entity -> event field of the payment binding (contracts/events/payment.yaml), in registry order."""
    from ..bindings import event_type, registry
    ents = event_type("payment").entities
    return {e: ents[e] for e in registry().names if e in ents}


ENTITY_FIELDS = _entity_fields()
ID_COLS = ["event_id", *ENTITY_FIELDS.values(), "campaign_id"]
NUM_COLS: dict[str, tuple[Any, Any]] = {
    **{c: (np.uint64, 0) for c in ID_COLS}, "ip_v4": (np.uint32, 0),
    "event_time_us": (np.int64, 0), "amount_usd_minor": (np.int64, 0), "payment_type": (np.int8, 0),
    "channel": (np.int8, 0), "entry_mode": (np.int8, 0), "card_present": (np.bool_, False), "mcc": (np.int16, 0),
    "is_fraud": (np.bool_, False), "fraud_type": (np.int16, 0), "campaign_role": (np.int16, 0),
    "label_delay_us": (np.int64, -1), "label_is_fraud": (np.bool_, False)}
STR_COLS = ["merchant_country", "ip_country"]
# id search kinds -> event column; the entity kinds are the same names
ID_KINDS = {"event": "event_id", **ENTITY_FIELDS, "campaign": "campaign_id"}
ENUM_KINDS = {"type": ("payment_type", PAYMENT_TYPE), "channel": ("channel", CHANNEL), "entry": ("entry_mode", ENTRY_MODE)}
OTHER_KINDS = ["fraud", "outcome", "path", "rule", "country", "ipcountry", "role", "label"]
KINDS = list(ID_KINDS) + list(ENUM_KINDS) + OTHER_KINDS
# fields a context can match on / a column can show (besides the ids)
MATCH_FIELDS = {"payment_type": PAYMENT_TYPE, "channel": CHANNEL, "entry_mode": ENTRY_MODE, "card_present": None}
FIELD_COLS = set(ID_COLS) | {"entry_mode", "channel", "payment_type", "merchant_country", "ip_country", "mcc"}


def _enum(names: list[str], v: Any) -> str:
    try:
        return names[int(v)]
    except (IndexError, TypeError, ValueError):
        return str(v)


def ipv4(v: int) -> str:
    v = int(v)
    return f"{(v >> 24) & 255}.{(v >> 16) & 255}.{(v >> 8) & 255}.{v & 255}"


def parse_ipv4(s: str) -> int:
    if "." not in s:
        return int(s)
    parts = [int(p) for p in s.split(".")]
    if len(parts) != 4 or not all(0 <= p <= 255 for p in parts):
        raise ValueError(s)
    return (parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]


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


def _np(t: pa.Table, col: str, dtype: Any, default: Any) -> np.ndarray:
    if col not in t.column_names:
        return np.full(t.num_rows, default, dtype)
    a = t[col]
    if a.null_count:
        a = pc.fill_null(a, default)
    return a.to_numpy().astype(dtype, copy=False)


def _codes(t: pa.Table, col: str) -> tuple[np.ndarray, list[str]]:
    """Dictionary-encode a string column: (int32 codes, vocabulary)."""
    if col not in t.column_names or t.num_rows == 0:
        return np.zeros(t.num_rows, np.int32), [""]
    d = pc.dictionary_encode(pc.fill_null(t[col], "")).combine_chunks()
    return d.indices.to_numpy(zero_copy_only=False).astype(np.int32), [str(x) for x in d.dictionary.to_pylist()]


def _by_event(ids_sorted: np.ndarray, ids: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Positions of `ids` in the sorted `ids_sorted` and whether each was found."""
    if not ids_sorted.size:
        return np.zeros(ids.size, np.int64), np.zeros(ids.size, bool)
    pos = np.clip(np.searchsorted(ids_sorted, ids), 0, ids_sorted.size - 1)
    return pos, ids_sorted[pos] == ids


def _txn_roles(name: str, parts: int | None) -> list[dict[str, Any]]:
    """The roles of a transaction contract and whether each was present when it was assembled."""
    try:
        from ..bindings.transactions import transaction
        t = transaction(name)
    except Exception:  # noqa: BLE001 - a contract removed since the run: show the mask only
        return []
    p = int(parts or 0)
    return [{"role": r, "type": ty, "present": bool(p & t.bit(r)),
             "part": ("trigger" if r == t.trigger else "required" if r in t.require else
                      "optional" if r in t.optional else "stage")} for r, ty in t.roles.items()]


class _RunMeta:
    """What a run snapshotted, parsed once: scenario, use-case registry, insights config."""

    def __init__(self, run_dir: Path) -> None:
        from ..scenario import Scenario
        from ..usecases import for_run
        self.dir = run_dir
        try:
            self.scenario = Scenario.model_validate(json.loads((run_dir / "scenario.json").read_text()))
        except Exception:  # noqa: BLE001 - an unparseable scenario only costs the context it gives
            self.scenario = None
        self.registry = for_run(run_dir)
        self.names = self.registry.fraud_type_names()
        cfg = None
        try:
            cfg = (json.loads((run_dir / "report.json").read_text()) or {}).get("insights_config")
        except (OSError, ValueError):
            pass
        if not cfg:
            from ..report.insights import load_config
            cfg = load_config()
        self.insights = cfg
        self.sample_permille = int(getattr(self.scenario, "feature_sample_permille", 1000) or 1000) \
            if self.scenario is not None else 1000


class _Index:
    """Compact projection of one run's measured events joined with their first-revision decision."""

    def __init__(self, run_dir: Path, meta: _RunMeta) -> None:
        self.dir, self.meta = run_dir, meta
        cols = list(NUM_COLS) + STR_COLS + ["backfill"]
        ev = _read(run_dir, "events-*.parquet", cols, [("backfill", "=", False)])
        if ev is None:
            raise HTTPException(404, "run has no event log")
        if "backfill" in ev.column_names:        # a file without the statistics the filter needs
            ev = ev.filter(pc.invert(pc.fill_null(ev["backfill"], False)))
        self.c: dict[str, np.ndarray] = {k: _np(ev, k, dt, df) for k, (dt, df) in NUM_COLS.items()}
        self.vocab: dict[str, list[str]] = {}
        for k in STR_COLS:
            self.c[k], self.vocab[k] = _codes(ev, k)
        n = self.n = ev.num_rows
        self.c["outcome"] = np.zeros(n, np.int8)
        self.c["path"] = np.full(n, -1, np.int8)
        self.c["score"] = np.full(n, np.nan, np.float32)
        self.c["e2e"] = np.full(n, np.nan, np.float32)
        self.c["rules"] = np.zeros(n, np.int32)
        self.c["model"] = np.zeros(n, np.int32)
        self.rule_vocab: list[list[str]] = [[]]
        self.model_vocab: list[str] = [""]
        self.decided = 0
        dec = _read(run_dir, "decisions-*.parquet", ["event_id", "revision", "outcome", "score", "path",
                                                     "t_intended_us", "t_collected_us", "rule_hits", "model_version"])
        if dec is not None and dec.num_rows:
            self._join(dec)
        self._fate()
        self.order = np.argsort(self.c["event_time_us"], kind="stable")
        self.groups: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self.features = _Features(run_dir)

    def _join(self, dec: pa.Table) -> None:
        eid = self.c["event_id"]
        rev = _np(dec, "revision", np.int16, 0)
        did = _np(dec, "event_id", np.uint64, 0)
        path = _np(dec, "path", np.int8, 0)
        first = rev == 0
        f_ids = did[first]
        o = np.argsort(f_ids)
        f_ids = f_ids[o]
        pos, hit = _by_event(f_ids, eid)
        src = np.flatnonzero(first)[o][pos]
        self.decided = int(hit.sum())
        self.c["outcome"] = np.where(hit, _np(dec, "outcome", np.int8, 0)[src], 0).astype(np.int8)
        self.c["path"] = np.where(hit, path[src], -1).astype(np.int8)
        e2e = (_np(dec, "t_collected_us", np.int64, 0) - _np(dec, "t_intended_us", np.int64, 0)) / 1000.0
        self.c["e2e"] = np.where(hit, e2e[src], np.nan).astype(np.float32)
        codes, vocab = _codes(dec, "rule_hits")
        self.c["rules"] = np.where(hit, codes[src], 0).astype(np.int32)
        self.rule_vocab = [[x for x in s.split(",") if x] for s in vocab]
        # the model score: from the decision that carries one (a hedged event's final revision); a
        # rules-only fallback, provisional or echo decision stores 0.0, which is not a score
        scored = np.isin(path, SCORED_PATHS)
        s_ids, s_rev = did[scored], rev[scored]
        o2 = np.lexsort((s_rev, s_ids))[::-1]                 # last revision first
        s_ids_r = s_ids[o2]
        u, first_of = np.unique(s_ids_r, return_index=True)
        src2 = np.flatnonzero(scored)[o2][first_of]
        pos2, hit2 = _by_event(u, eid)
        self.c["score"] = np.where(hit2, _np(dec, "score", np.float32, np.nan)[src2[pos2]] if u.size else np.nan,
                                   np.nan).astype(np.float32)
        mcodes, self.model_vocab = _codes(dec, "model_version")
        self.c["model"] = np.where(hit2, mcodes[src2[pos2]] if u.size else 0, 0).astype(np.int32)

    def _fate(self) -> None:
        """The step-up's latent result and the payment's fate (challenge.final), for every event."""
        from .. import challenge as stepup
        self.c["stepup"] = np.zeros(self.n, np.int8)
        self.c["fate"] = self.c["outcome"].copy()
        if not self.n or self.meta.scenario is None:
            return
        try:
            res = stepup.for_run(self.dir, self.c["event_id"], self.c["is_fraud"], self.c["fraud_type"])
            self.c["stepup"] = res.astype(np.int8)
            holds = stepup.review_holds(self.c["payment_type"], self.meta.insights)
            self.c["fate"] = stepup.final(self.c["outcome"], res, holds).astype(np.int8)
        except Exception:  # noqa: BLE001 - fates are context; the decisions stand without them
            pass

    @property
    def nbytes(self) -> int:
        return int(sum(a.nbytes for a in self.c.values()) + self.order.nbytes + self.features.nbytes
                   + sum(sum(a.nbytes for a in g.values() if isinstance(a, np.ndarray)) for g in self.groups.values()))

    def common_points(self, key: str) -> dict[str, np.ndarray]:
        """Per merchant / terminal: distinct cards used there (history included) and how many of them
        were used fraudulently afterwards, the visit being genuine and before the card's first fraud -
        the common-point-of-purchase analysis behind a breach or a skimmer. Cached per run."""
        name = f"cpp:{key}"
        with self._lock:
            hit = self.groups.get(name)
        if hit is not None:
            return hit
        c = self.c
        fr = c["is_fraud"] & (c["card_id"] != 0)
        fcard, ft = c["card_id"][fr], c["event_time_us"][fr]
        o = np.lexsort((ft, fcard))
        fcard, ft = fcard[o], ft[o]
        u_fc, first = np.unique(fcard, return_index=True)
        first_fraud = ft[first]
        ev = _read(self.dir, "events-*.parquet", [key, "card_id", "event_time_us", "is_fraud"])
        ent = _np(ev, key, np.uint64, 0)
        card = _np(ev, "card_id", np.uint64, 0)
        t = _np(ev, "event_time_us", np.int64, 0)
        ok = (ent != 0) & (card != 0) & ~_np(ev, "is_fraud", np.bool_, False)
        ent, card, t = ent[ok], card[ok], t[ok]
        pos, found = _by_event(u_fc, card)
        later = found & (t < np.where(found, first_fraud[pos], 0))

        def per_entity(e: np.ndarray, cd: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
            if not e.size:
                return np.zeros(0, np.uint64), np.zeros(0, np.int64)
            k = np.lexsort((cd, e))
            e, cd = e[k], cd[k]
            new = np.ones(e.size, bool)
            new[1:] = (e[1:] != e[:-1]) | (cd[1:] != cd[:-1])
            return np.unique(e[new], return_counts=True)

        ids, seen = per_entity(ent, card)
        l_ids, l_cnt = per_entity(ent[later], card[later])
        lp, lf = _by_event(l_ids, ids)
        out = {"ids": ids, "seen": seen.astype(np.int64),
               "later": np.where(lf, l_cnt[lp] if l_ids.size else 0, 0).astype(np.int64),
               "baseline": np.asarray([u_fc.size / max(1, np.unique(c["card_id"][c["card_id"] != 0]).size)])}
        with self._lock:
            self.groups[name] = out
        return out

    # ------------------------------------------------------------------ rows
    def str_of(self, col: str, i: int) -> str:
        return self.vocab[col][int(self.c[col][i])]

    def field(self, col: str, i: int) -> Any:
        """A display value of an event field (None when the payment has no such entity)."""
        v = self.c[col][i]
        if col in ID_COLS:
            v = int(v)
            return None if v == 0 else (ipv4(v) if col == "ip_v4" else str(v))
        if col in STR_COLS:
            return self.str_of(col, i) or None
        if col in MATCH_FIELDS and MATCH_FIELDS[col] is not None:
            return _enum(MATCH_FIELDS[col], v)
        if col == "mcc":
            return int(v) or None
        return v.item() if hasattr(v, "item") else v

    def rows(self, idx: np.ndarray) -> list[dict[str, Any]]:
        from ..challenge import FATE, RESULTS
        c = self.c
        names, reg = self.meta.names, self.meta.registry
        out = []
        for i in np.asarray(idx).tolist():
            sc, e2e, path = float(c["score"][i]), float(c["e2e"][i]), int(c["path"][i])
            oc = int(c["outcome"][i])
            role = int(c["campaign_role"][i])
            delay = int(c["label_delay_us"][i])
            out.append({
                "event_id": str(int(c["event_id"][i])), "event_time_us": int(c["event_time_us"][i]),
                **{k: self.field(k, i) for k in ("customer_id", "account_id", "card_id", "device_id", "merchant_id",
                                                 "terminal_id", "payee_id")},
                "ip": self.field("ip_v4", i), "ip_v4": int(c["ip_v4"][i]) or None,
                "ip_country": self.field("ip_country", i),
                "payment_type": _enum(PAYMENT_TYPE, c["payment_type"][i]), "channel": _enum(CHANNEL, c["channel"][i]),
                "entry_mode": _enum(ENTRY_MODE, c["entry_mode"][i]), "card_present": bool(c["card_present"][i]),
                "amount_usd": int(c["amount_usd_minor"][i]) / 100.0, "merchant_country": self.field("merchant_country", i),
                "outcome": OUTCOME.get(oc), "path": PATH.get(path) if path >= 0 else None,
                "score": None if np.isnan(sc) else sc,
                "model_version": self.model_vocab[int(c["model"][i])] or None,
                "rule_hits": self.rule_vocab[int(c["rules"][i])],
                "fate": FATE.get(int(c["fate"][i])) if oc else None,
                "stepup": RESULTS.get(int(c["stepup"][i])) if oc == 4 else None,
                "is_fraud": bool(c["is_fraud"][i]), "fraud_type": names.get(int(c["fraud_type"][i]), "?"),
                "campaign_id": str(int(c["campaign_id"][i])), "role": reg.role_name(role)[1] if role else None,
                "label": None if delay < 0 else {"fraud": bool(c["label_is_fraud"][i]), "after_days": round(delay / 86_400e6, 1)},
                "e2e_ms": None if np.isnan(e2e) else round(e2e, 2)})
        return out

    # ------------------------------------------------------------------ search
    def mask(self, q: str) -> tuple[np.ndarray, dict[str, int]]:
        """Events matching a query: whitespace-separated terms are ANDed; `kind:a,b` matches a OR b; a
        bare number matches any id column (the counts per id kind come back, since ids of different
        kinds can collide)."""
        c, n = self.c, self.n
        m = np.ones(n, bool)
        matched: dict[str, int] = {}
        for term in q.split():
            if ":" in term:
                kind, val = term.split(":", 1)
                kind = kind.strip().lower()
            else:
                kind, val = "", term
            vals = [v for v in val.split(",") if v.strip()]
            if not vals:
                continue
            if kind == "":
                tm = np.zeros(n, bool)
                for v in vals:
                    for k, col in ID_KINDS.items():
                        try:
                            x = parse_ipv4(v) if k == "ip" else int(v)
                        except ValueError:
                            raise HTTPException(400, f"'{v}' is not an id; search by id, or use kind:value with one of "
                                                     f"{', '.join(KINDS)}") from None
                        hit = c[col].astype(np.uint64) == np.uint64(x) if 0 <= x < 2 ** 64 else np.zeros(n, bool)
                        cnt = int((hit & m).sum())
                        if cnt:
                            matched[k] = matched.get(k, 0) + cnt
                        tm |= hit
                m &= tm
            else:
                m &= self._term(kind, vals)
        return m, matched

    def _term(self, kind: str, vals: list[str]) -> np.ndarray:
        c, n = self.c, self.n
        tm = np.zeros(n, bool)
        if kind in ID_KINDS:
            col = c[ID_KINDS[kind]].astype(np.uint64)
            for v in vals:
                try:
                    x = parse_ipv4(v) if kind == "ip" else int(v)
                except ValueError:
                    raise HTTPException(400, f"{kind}: expects an id{' or a dotted address' if kind == 'ip' else ''}, "
                                             f"got '{v}'") from None
                tm |= col == np.uint64(x)
            return tm
        if kind in ENUM_KINDS:
            col, names = ENUM_KINDS[kind]
            for v in vals:
                code = _lookup(names, v)
                if code is None:
                    raise HTTPException(400, f"{kind}: one of {', '.join(x for x in names if 'UNSPECIFIED' not in x)}")
                tm |= c[col] == code
            return tm
        if kind == "fraud":
            by_name = {v: k for k, v in self.meta.names.items()}
            for v in vals:
                v = v.lower()
                if v in ("any", "all", "yes"):
                    tm |= c["is_fraud"]
                elif v in ("none", "no", "genuine"):
                    tm |= ~c["is_fraud"]
                elif v in by_name:
                    tm |= c["fraud_type"] == by_name[v]
                else:
                    raise HTTPException(400, f"fraud: one of any, none, {', '.join(sorted(n for n in by_name if n != 'none'))}")
            return tm
        if kind == "outcome":
            codes = {v: k for k, v in OUTCOME.items() if v}
            for v in vals:
                if v.upper() not in codes:
                    raise HTTPException(400, f"outcome: one of {', '.join(codes)}")
                tm |= c["outcome"] == codes[v.upper()]
            return tm
        if kind == "path":
            codes = {v: k for k, v in PATH.items()}
            for v in vals:
                code = codes.get(v.upper()) if not v.upper().startswith("RULES") else 1
                if code is None:
                    raise HTTPException(400, f"path: one of {', '.join(codes)}")
                tm |= c["path"] == code
            return tm
        if kind == "rule":
            for v in vals:
                hits = [j for j, rs in enumerate(self.rule_vocab) if v.upper() in rs]
                tm |= np.isin(c["rules"], hits)
            return tm
        if kind in ("country", "ipcountry"):
            col = "merchant_country" if kind == "country" else "ip_country"
            voc = self.vocab[col]
            for v in vals:
                hits = [j for j, s in enumerate(voc) if s.upper() == v.upper()]
                tm |= np.isin(c[col], hits)
            return tm
        if kind == "role":
            reg = self.meta.registry
            codes = {code: role for t in reg.all for role in t.roles for code in [t.role_code(role)]}
            for v in vals:
                want = [code for code, role in codes.items() if role == v.lower()]
                if not want:
                    raise HTTPException(400, f"role: one of {', '.join(sorted(set(codes.values())))}")
                tm |= np.isin(c["campaign_role"], want)
            return tm
        if kind == "label":
            known = c["label_delay_us"] >= 0
            for v in vals:
                v = v.lower()
                if v in ("fraud", "yes"):
                    tm |= known & c["label_is_fraud"]
                elif v in ("genuine", "no", "chargeback_free"):
                    tm |= known & ~c["label_is_fraud"]
                elif v in ("none", "never", "unlabelled"):
                    tm |= ~known
                else:
                    raise HTTPException(400, "label: fraud | genuine | none")
            return tm
        raise HTTPException(400, f"unknown search kind '{kind}'; use one of {', '.join(KINDS)}")

    # ------------------------------------------------------------------ entity groupings
    def group(self, entity: str, key: str, shared_by: list[str], sel: np.ndarray | None = None) -> dict[str, Any]:
        """Per-entity aggregates over the measured events (or a subset `sel` of them)."""
        cached = sel is None
        if cached:
            with self._lock:
                g = self.groups.get(entity)
            if g is not None:
                return g
        c = self.c
        kcol = c[key]
        sel = np.flatnonzero(kcol != 0) if sel is None else sel[kcol[sel] != 0]
        k = kcol[sel]
        uniq, inv, counts = np.unique(k, return_inverse=True, return_counts=True)
        m = uniq.size
        order = sel[np.argsort(inv, kind="stable")]
        starts = np.concatenate([[0], np.cumsum(counts)]).astype(np.int64)

        def tally(mask: np.ndarray) -> np.ndarray:
            return np.bincount(inv, weights=mask[sel].astype(np.float64), minlength=m).astype(np.int64)

        oc = c["outcome"]
        smax = np.full(m, -1.0)
        if m:
            np.maximum.at(smax, inv, np.nan_to_num(c["score"][sel].astype(np.float64), nan=-1.0))
        t = c["event_time_us"][sel]
        first = np.full(m, np.iinfo(np.int64).max)
        last = np.full(m, np.iinfo(np.int64).min)
        if m:
            np.minimum.at(first, inv, t)
            np.maximum.at(last, inv, t)
        g = {"uniq": uniq, "order": order, "starts": starts, "events": counts.astype(np.int64),
             "fraud": tally(c["is_fraud"]), "flagged": tally(oc >= 2), "declined": tally(oc == 3),
             "challenged": tally(oc == 4),
             "amount": np.bincount(inv, weights=c["amount_usd_minor"][sel].astype(np.float64), minlength=m) / 100.0,
             "max_score": smax, "first": first, "last": last}
        for col in shared_by:
            g[f"distinct:{col}"] = _distinct(inv, c[col][sel], m)
        if cached:
            with self._lock:
                self.groups[entity] = g
        return g

    def members(self, g: dict[str, Any], i: int) -> np.ndarray:
        m = g["order"][g["starts"][i]:g["starts"][i + 1]]
        return m[np.lexsort((self.c["event_id"][m], self.c["event_time_us"][m]))]


def _lookup(names: list[str], v: str) -> int | None:
    v = v.strip().upper()
    if v.isdigit():
        return int(v) if int(v) < len(names) else None
    for i, n in enumerate(names):
        if n == v or n.endswith("_" + v) or n.replace("_", "") == v.replace("_", ""):
            return i
    return None


def _distinct(inv: np.ndarray, vals: np.ndarray, m: int) -> np.ndarray:
    """Distinct non-zero values of `vals` per group."""
    keep = vals != 0
    g, v = inv[keep], vals[keep]
    if not g.size:
        return np.zeros(m, np.int64)
    o = np.lexsort((v, g))
    g, v = g[o], v[o]
    new = np.ones(g.size, bool)
    new[1:] = (g[1:] != g[:-1]) | (v[1:] != v[:-1])
    return np.bincount(g[new], minlength=m).astype(np.int64)


class _Features:
    """The decisions' feature vectors, by event id. Held as one float32 matrix when it fits
    REALY_EXPLORER_FEATURES_MB, else read per row group on demand."""

    def __init__(self, run_dir: Path) -> None:
        self.dir = run_dir
        self._ready = False
        self._lock = threading.Lock()
        self.matrix: np.ndarray | None = None

    @property
    def nbytes(self) -> int:
        return 0 if not self._ready else int(self.ids.nbytes * 3 + (self.matrix.nbytes if self.matrix is not None else 0)
                                              + (len(self.missing) * 40 if self.missing else 0))

    def _load(self) -> None:
        files = _files(self.dir, "features-*.parquet")
        self.files = [pq.ParquetFile(f) for f in files]
        ids, fidx, ridx = [], [], []
        for fi, pf in enumerate(self.files):
            e = pf.read(columns=["event_id"])["event_id"].to_numpy()
            ids.append(e.astype(np.uint64))
            fidx.append(np.full(e.size, fi, np.int16))
            ridx.append(np.arange(e.size, dtype=np.int64))
        self.ids = np.concatenate(ids) if ids else np.zeros(0, np.uint64)
        o = np.argsort(self.ids, kind="stable")
        self.ids, self.fi = self.ids[o], (np.concatenate(fidx) if fidx else np.zeros(0, np.int16))[o]
        self.ri = (np.concatenate(ridx) if ridx else np.zeros(0, np.int64))[o]
        self.width = 0
        self.matrix: np.ndarray | None = None
        self.missing: list[bytes] | None = None
        if self.files and self.files[0].metadata.num_rows:
            first = self.files[0].read_row_group(0, columns=["values"])["values"]
            self.width = len(first[0].as_py() or [])
        budget = float(os.environ.get("REALY_EXPLORER_FEATURES_MB", "256")) * 1e6
        if self.width and self.ids.size * self.width * 4 <= budget:
            mats, miss = [], []
            for pf in self.files:
                t = pf.read(columns=["values", "missing"])
                flat = pc.list_flatten(t["values"]).to_numpy(zero_copy_only=False).astype(np.float32)
                w = flat.size // max(1, t.num_rows)
                mats.append(flat.reshape(t.num_rows, w) if w == self.width else
                            np.asarray([np.pad(np.asarray(r or [], np.float32), (0, self.width))[:self.width]
                                        for r in t["values"].to_pylist()], np.float32))
                miss.extend(t["missing"].to_pylist() if "missing" in t.column_names else [b""] * t.num_rows)
            offsets = np.concatenate([[0], np.cumsum([m.shape[0] for m in mats])])
            self.matrix = np.concatenate(mats) if mats else np.zeros((0, self.width), np.float32)
            self.missing = miss
            self.gpos = offsets[self.fi] + self.ri       # global row of each sorted id
        self._ready = True

    def get(self, event_ids: list[int]) -> dict[int, tuple[np.ndarray, np.ndarray]]:
        """{event_id: (values, missing flags)} for the events whose vector was recorded."""
        with self._lock:
            if not self._ready:
                self._load()
        if not self.ids.size or not event_ids:
            return {}
        want = np.asarray(event_ids, np.uint64)
        pos, hit = _by_event(self.ids, want)
        out: dict[int, tuple[np.ndarray, np.ndarray]] = {}
        if self.matrix is not None:
            for e, p, h in zip(event_ids, pos.tolist(), hit.tolist()):
                if h:
                    g = int(self.gpos[p])
                    out[int(e)] = (self.matrix[g], _bits(self.missing[g], self.width))
            return out
        need: dict[tuple[int, int], list[tuple[int, int]]] = collections.defaultdict(list)
        for e, p, h in zip(event_ids, pos.tolist(), hit.tolist()):
            if not h:
                continue
            fi, ri = int(self.fi[p]), int(self.ri[p])
            pf = self.files[fi]
            rg, base = 0, 0
            while base + pf.metadata.row_group(rg).num_rows <= ri:
                base += pf.metadata.row_group(rg).num_rows
                rg += 1
            need[(fi, rg)].append((int(e), ri - base))
        for (fi, rg), items in need.items():
            t = self.files[fi].read_row_group(rg, columns=["values", "missing"])
            vals, miss = t["values"], t["missing"] if "missing" in t.column_names else None
            for e, r in items:
                v = np.asarray(vals[r].as_py() or [], np.float32)
                out[e] = (v, _bits(miss[r].as_py() if miss is not None else b"", v.size))
        return out


def _bits(mb: bytes | None, n: int) -> np.ndarray:
    if not mb:
        return np.zeros(n, bool)
    b = np.unpackbits(np.frombuffer(mb, np.uint8), bitorder="little").astype(bool)
    return np.pad(b, (0, max(0, n - b.size)))[:n]


# ---------------------------------------------------------------------------------------- views
_views_cache: dict[str, Any] = {}


def load_views(path: Path | None = None) -> dict[str, Any]:
    """contracts/explorer.yaml, validated against the feature schema and the indexable fields, with
    `features: auto` resolved. Cached; reloads when the file changes."""
    p = Path(path or VIEWS_PATH)
    stamp = (str(p), p.stat().st_mtime_ns if p.exists() else 0)
    if _views_cache.get("stamp") == stamp:
        return _views_cache["views"]
    doc = yaml.safe_load(p.read_text()) if p.exists() else {}
    doc = doc or {}
    schema = load_schema()
    feats = {f.name: f for f in schema.features}
    labels, formats = doc.get("labels") or {}, doc.get("formats") or {}
    problems: list[str] = []

    def ffmt(name: str, given: str | None) -> str | None:
        """Display format; money aggregates over amount_usd_minor are cents, whatever the file says."""
        f = feats.get(name)
        if f is not None and f.source == "stream" and getattr(f, "value", None) == "amount_usd_minor" \
                and f.agg in ("sum", "max", "avg"):
            return "usd_minor"
        return given or formats.get(name)

    def flabel(name: str) -> str:
        if name in labels:
            return str(labels[name])
        f = feats.get(name)
        pre = {"customer": "cust_", "card": "card_", "device": "dev_", "merchant": "merch_", "terminal": "term_",
               "payee": "payee_", "ip": "ip_"}.get(f.entity or "", "") if f else ""
        return (name[len(pre):] if pre and name.startswith(pre) else name).replace("_", " ")

    entities = {}
    for name, e in (doc.get("entities") or {}).items():
        key = e.get("key")
        if key not in ID_COLS:
            problems.append(f"entity {name}: key {key} is not an id column")
            continue
        shared = [s for s in e.get("shared_by") or [] if s in ID_COLS and s != key]
        fl = e.get("features", "auto")
        if fl == "auto":
            fl = [f.name for f in schema.features if f.source == "stream" and f.entity == name]
        fl = [f for f in fl or []]
        problems += [f"entity {name}: unknown feature {f}" for f in fl if f not in feats]
        entities[name] = {"name": name, "key": key, "title": e.get("title", name.title()), "format": e.get("format"),
                          "shared_by": shared, "shared_min": int(e.get("shared_min", 2)),
                          "profile": e.get("profile"), "common_point": bool(e.get("common_point")),
                          "features": [{"key": f, "label": flabel(f), "format": ffmt(f, None)} for f in fl if f in feats]}
    contexts = []
    for cx in doc.get("contexts") or []:
        match = {}
        for fld, allowed in (cx.get("match") or {}).items():
            if fld not in MATCH_FIELDS:
                problems.append(f"context {cx.get('name')}: cannot match on {fld}")
                continue
            names = MATCH_FIELDS[fld]
            if names is None:
                match[fld] = [bool(a) for a in allowed]
            else:
                codes = [_lookup(names, str(a)) for a in allowed]
                problems += [f"context {cx.get('name')}: unknown {fld} {a}" for a, k in zip(allowed, codes) if k is None]
                match[fld] = [k for k in codes if k is not None]
        cols = []
        for col in cx.get("columns") or []:
            if "field" in col:
                if col["field"] not in FIELD_COLS:
                    problems.append(f"context {cx.get('name')}: {col['field']} is not an indexed event field")
                    continue
                key, kind = col["field"], "field"
            else:
                key, kind = col.get("feature"), "feature"
                if key not in feats:      # kept: a run of another schema may have it; shown n/a otherwise
                    problems.append(f"context {cx.get('name')}: unknown feature {key}")
            link = col.get("link")
            if link and link not in entities:
                problems.append(f"context {cx.get('name')}: link to unknown entity {link}")
                link = None
            cols.append({"key": key, "kind": kind, "label": col.get("label") or (flabel(key) if kind == "feature" else key),
                         "link": link, "format": ffmt(key, col.get("format")) if kind == "feature" else col.get("format")})
        contexts.append({"name": cx["name"], "title": cx.get("title", cx["name"]), "match": match, "columns": cols})
    lenses = [dict(x) for x in doc.get("lenses") or []]
    for ln in lenses:
        if ln.get("entity") and ln["entity"] not in entities:
            problems.append(f"lens {ln.get('name')}: unknown entity {ln['entity']}")
    views = {"version": doc.get("version", 1), "entities": entities, "contexts": contexts, "lenses": lenses,
             "kinds": KINDS, "problems": problems}
    _views_cache.update(stamp=stamp, views=views)
    return views


def _context_of(idx: _Index, i: int, contexts: list[dict[str, Any]]) -> dict[str, Any] | None:
    c = idx.c
    for cx in contexts:
        if all((bool(c[f][i]) if MATCH_FIELDS[f] is None else int(c[f][i])) in allowed
               for f, allowed in cx["match"].items()):
            return cx
    return None


# ---------------------------------------------------------------------------------------- explorer
class Explorer:
    def __init__(self, runs_dir: Path) -> None:
        self.runs_dir = runs_dir
        self._cache: collections.OrderedDict[str, _Index] = collections.OrderedDict()
        self._meta: dict[str, _RunMeta] = {}
        self._lock = threading.Lock()
        self._building: dict[str, threading.Lock] = {}
        self.budget = float(os.environ.get("REALY_EXPLORER_CACHE_MB", "768")) * 1e6

    def run_dir(self, run_id: str) -> Path:
        d = self.runs_dir / run_id
        if "/" in run_id or ".." in run_id or not d.is_dir():
            raise HTTPException(404, "no such run")
        return d

    def meta(self, run_id: str) -> _RunMeta:
        d = self.run_dir(run_id)
        m = self._meta.get(run_id)
        if m is None:
            m = self._meta[run_id] = _RunMeta(d)
        return m

    def index(self, run_id: str) -> _Index:
        with self._lock:
            hit = self._cache.get(run_id)
            if hit is not None:
                self._cache.move_to_end(run_id)
                return hit
            build = self._building.setdefault(run_id, threading.Lock())
        with build:                       # one build per run, however many requests arrive at once
            with self._lock:
                hit = self._cache.get(run_id)
            if hit is not None:
                return hit
            s = _Index(self.run_dir(run_id), self.meta(run_id))
            with self._lock:
                self._cache[run_id] = s
                while len(self._cache) > 1 and sum(x.nbytes for x in self._cache.values()) > self.budget:
                    self._cache.popitem(last=False)
        return s

    slim = index    # older name

    def cache_info(self) -> dict[str, Any]:
        with self._lock:
            return {"runs": list(self._cache), "mb": round(sum(x.nbytes for x in self._cache.values()) / 1e6, 1),
                    "budget_mb": round(self.budget / 1e6)}

    # ------------------------------------------------------------------ search
    def search(self, run_id: str, q: str, kind: str = "auto", limit: int = 50) -> dict[str, Any]:
        s = self.index(run_id)
        q = (q or "").strip()
        if kind and kind != "auto" and q and ":" not in q:
            q = f"{kind}:{q}"
        if not q:
            fraud = np.flatnonzero(s.c["is_fraud"])
            recent = s.order[-max(1, limit // 2):]
            pick = np.unique(np.concatenate([recent, fraud[-max(1, limit // 2):]]))
            pick = pick[np.argsort(s.c["event_time_us"][pick])][::-1][:limit]
            return {"results": s.rows(pick), "total": int(s.n), "matched": {}}
        m, matched = s.mask(q)
        idx = np.flatnonzero(m)
        idx = idx[np.lexsort((s.c["event_id"][idx], s.c["event_time_us"][idx]))]
        return {"results": s.rows(idx[:limit]), "total": int(idx.size), "matched": matched}

    # ------------------------------------------------------------------ one decision
    def decision(self, run_id: str, event_id: int, maturity_days: float = 30.0) -> dict[str, Any]:
        d = self.run_dir(run_id)
        meta = self.meta(run_id)
        flt = [("event_id", "=", event_id)]
        ev = _read(d, "events-*.parquet", None, flt)
        if ev is None or ev.num_rows == 0:
            raise HTTPException(404, "event not in this run")
        er = ev.slice(0, 1).to_pylist()[0]
        event = {k: v for k, v in er.items() if k not in ("is_fraud", "fraud_type", "campaign_id", "t_intended_us",
                                                         "t_intended_offset_us", "backfill", "label_delay_us",
                                                         "label_is_fraud", "label_type", "label_source", "campaign_role")}
        for k in ("event_id", "customer_id", "account_id", "card_id", "merchant_id", "terminal_id", "device_id",
                  "payee_id"):
            if k in event:
                event[k] = str(event[k]) if event[k] else None
        for k, names in (("payment_type", PAYMENT_TYPE), ("channel", CHANNEL), ("entry_mode", ENTRY_MODE),
                         ("card_type", CARD_TYPE), ("three_ds", THREE_DS)):
            if k in event and event[k] is not None:
                event[k] = None if (int(event[k]) == 0 and k != "three_ds") else _enum(names, event[k])
        if event.get("mcc") == 0:
            event["mcc"] = None
        if event.get("ip_v4"):
            event["ip"] = ipv4(event["ip_v4"])
        event["event_id"] = str(er["event_id"])
        event["amount_usd"] = er.get("amount_usd_minor", 0) / 100.0
        role = int(er.get("campaign_role") or 0)
        label = {"is_fraud": bool(er.get("is_fraud")), "fraud_type": meta.names.get(int(er.get("fraud_type") or 0), "?"),
                 "campaign_id": str(er.get("campaign_id") or 0),
                 "role": meta.registry.role_name(role)[1] if role else None, "backfill": bool(er.get("backfill"))}
        warnings: list[str] = []
        try:
            dec = _read(d, "decisions-*.parquet", None, flt)
        except Exception as e:  # noqa: BLE001 - every section degrades on its own; the event still shows
            dec, _ = None, warnings.append(f"decision: {type(e).__name__}: {e}")
        decision = None
        spans: list[dict[str, Any]] = []
        timing: dict[str, Any] = {}
        if dec is not None and dec.num_rows:
            rows = sorted(dec.to_pylist(), key=lambda r: r.get("revision", 0))
            r = rows[0]
            scored = [x for x in rows if x.get("path") in SCORED_PATHS]
            path = PATH.get(r.get("path"), str(r.get("path")))
            decision = {"outcome": OUTCOME.get(r.get("outcome")),
                        "score": scored[-1].get("score") if scored else None,
                        "score_note": None if scored else _no_score(path),
                        "path": path, "degraded": bool(r.get("degraded")),
                        "ingress_mode": INGRESS.get(r.get("ingress_mode")),
                        "reason_codes": [x for x in (r.get("reason_codes") or "").split(",") if x],
                        "rule_hits": [x for x in (r.get("rule_hits") or "").split(",") if x],
                        "model_version": (scored[-1] if scored else r).get("model_version"),
                        "revision": r.get("revision", 0), "revisions": len(rows)}
            if r.get("txn"):
                # a transaction assembled from several messages: which one, its stage, the parts it had
                tname, _, tstage = str(r["txn"]).partition("/")
                try:
                    tctx = json.loads(r.get("txn_context") or "{}")
                except ValueError:
                    tctx = {}
                decision["transaction"] = {"name": tname, "stage": tstage, "parts_present": r.get("txn_parts"),
                                           "partial": bool(r.get("txn_partial")), "context": tctx,
                                           "roles": _txn_roles(tname, r.get("txn_parts"))}
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
        elif er.get("backfill"):
            warnings.append("history replay: this event built feature state before the measured window and was "
                            "not scored")
        try:
            em = _read(d, "emit-*.parquet", None, flt)
        except Exception as e:  # noqa: BLE001
            em, _ = None, warnings.append(f"emitter timing: {type(e).__name__}: {e}")
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
        features_note = None
        try:
            ft = _read(d, "features-*.parquet", None, flt)
            if ft is not None and ft.num_rows:
                fr = ft.slice(0, 1).to_pylist()[0]
                schema = load_schema()
                vals = fr["values"] or []
                bits = _bits(fr.get("missing") or b"", len(vals))
                # a run of an earlier (narrower) schema has fewer values: features are only ever appended
                features = [{"index": f.index, "name": f.name, "entity": f.entity or "derived", "source": f.source,
                             "agg": f.agg, "value": float(v), "default": f.default, "missing": bool(bits[f.index])}
                            for f, v in zip(schema.features, vals)]
            elif decision is not None:
                features_note = _features_note(meta)
        except Exception as e:  # noqa: BLE001
            features, _ = None, warnings.append(f"features: {type(e).__name__}: {e}")
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
        return {"event": event, "decision": decision, "features": features, "features_note": features_note,
                "spans": spans, "timing": timing, "label": label, "explanation": explanation,
                **({"warnings": warnings} if warnings else {})}

    def _label_story(self, run_id: str, d: Path, er: dict[str, Any], maturity_days: float) -> dict[str, Any]:
        from ..explain import label_story
        reg = self.meta(run_id).registry
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
        t = self.index(run_id).c["event_time_us"]
        run_end = int(t.max()) if t.size else 0
        return {"story": label_story(er, reg, run_end, maturity_days, step)}

    def _explain(self, d: Path, er: dict[str, Any], features: list[dict[str, Any]], decision: dict[str, Any]) -> dict[str, Any]:
        """Policy path, every rule with the values it saw, and the model's attribution (realy.explain)."""
        from ..explain import explain_rules, integrated_gradients, policy_path, top_attributions
        from ..features.derived import event_view
        from ..policy import RuleSet as Rules
        from ..scenario import run_rules
        from ..verify.verify import ProxyScorer
        sc = self.meta(d.name).scenario
        if sc is None:
            raise ValueError("the run's scenario.json could not be read")
        cfg = sc.scoring_config()
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
        if score is not None and decision.get("path") in ("FULL", "FINAL_AFTER_PROVISIONAL", None):
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

    # ------------------------------------------------------------------ entities
    def _entity_def(self, entity: str) -> dict[str, Any]:
        v = load_views()
        e = v["entities"].get(entity)
        if e is None:
            raise HTTPException(404, f"unknown entity '{entity}'; one of {', '.join(v['entities'])}")
        return e

    def entities(self, run_id: str, entity: str, sort: str = "events", show: str = "all", limit: int = 100,
                 q: str = "") -> dict[str, Any]:
        """Entities of a run with their decision counts, to find the ones worth reading: many events, a
        mix of genuine and fraudulent activity, shared by many customers or cards (mules, bot devices,
        breached merchants), high scores. `q` restricts the events counted (a search query)."""
        e = self._entity_def(entity)
        s = self.index(run_id)
        sel = None
        if q.strip():
            m, _ = s.mask(q)
            sel = np.flatnonzero(m)
        g = s.group(entity, e["key"], e["shared_by"], sel)
        m = g["uniq"].size
        empty_stats = {"entities": 0, "events": 0}
        if not m:
            return {"entity": entity, "rows": [], "stats": empty_stats, **({"customers": []} if entity == "customer" else {})}
        counts, fraud = g["events"], g["fraud"]
        legit = counts - fraud
        primary = g.get(f"distinct:{e['shared_by'][0]}") if e["shared_by"] else np.zeros(m, np.int64)
        shared = primary >= e["shared_min"]
        keep = {"all": np.ones(m, bool), "fraud": fraud > 0, "mixed": (fraud > 0) & (legit > 0), "shared": shared,
                "multi_card": shared if entity == "customer" else shared, "flagged": g["flagged"] > 0}.get(show)
        if keep is None and not (show == "common_point" and e["common_point"]):
            raise HTTPException(400, "show: all | fraud | mixed | shared | flagged" + (" | common_point" if e["common_point"] else ""))
        if keep is None:
            keep = np.ones(m, bool)
        cpp = None
        if e["common_point"]:
            cp = s.common_points(e["key"])
            pos, f = _by_event(cp["ids"], g["uniq"])
            cpp = {"seen": np.where(f, cp["seen"][pos] if cp["ids"].size else 0, 0),
                   "later": np.where(f, cp["later"][pos] if cp["ids"].size else 0, 0), "baseline": float(cp["baseline"][0])}
        sorts = {"events": counts, "fraud": fraud, "flagged": g["flagged"], "max_score": g["max_score"],
                 "amount": g["amount"], "shared": primary, "recent": g["last"]}
        if cpp is not None:
            # lift over the run's base rate, among entities with enough later-fraud cards to mean something
            with np.errstate(divide="ignore", invalid="ignore"):
                lift = np.where(cpp["seen"] > 0, cpp["later"] / np.maximum(cpp["seen"], 1) / max(cpp["baseline"], 1e-9), 0.0)
            sorts["common_point"] = np.where(cpp["later"] >= CPP_MIN_CARDS, lift, -1.0)
            if show == "common_point":
                show = "all"
                keep_cpp = cpp["later"] >= CPP_MIN_CARDS
        key = sorts.get(sort)
        if key is None:
            raise HTTPException(400, f"sort: {' | '.join(sorts)}")
        if cpp is not None and "keep_cpp" in locals():
            keep = keep & keep_cpp
        cand = np.flatnonzero(keep)
        top = cand[np.lexsort((-counts[cand], -key[cand]))][:limit]
        names = s.meta.names
        rows = []
        for i in top.tolist():
            types: list[str] = []
            if fraud[i]:
                mem = g["order"][g["starts"][i]:g["starts"][i + 1]]
                ft = s.c["fraud_type"][mem][s.c["is_fraud"][mem]]
                types = sorted({names.get(int(x), str(int(x))) for x in np.unique(ft)})
            ident = int(g["uniq"][i])
            row = {"id": str(ident), "display": ipv4(ident) if e["format"] == "ipv4" else str(ident),
                   "events": int(counts[i]), "fraud_events": int(fraud[i]), "fraud_types": types,
                   "flagged": int(g["flagged"][i]), "declined": int(g["declined"][i]),
                   "challenged": int(g["challenged"][i]),
                   "max_score": None if g["max_score"][i] < 0 else round(float(g["max_score"][i]), 6),
                   "amount_usd": round(float(g["amount"][i]), 2), "first_us": int(g["first"][i]),
                   "last_us": int(g["last"][i]),
                   "distinct": {col.removesuffix("_id").replace("ip_v4", "ip"): int(g[f"distinct:{col}"][i])
                                for col in e["shared_by"]}}
            if cpp is not None:
                seen_i, later_i = int(cpp["seen"][i]), int(cpp["later"][i])
                row["common_point"] = {"cards_seen": seen_i, "cards_later_fraud": later_i,
                                       "lift": round((later_i / seen_i) / cpp["baseline"], 2) if seen_i and cpp["baseline"] else None}
            if entity == "customer":           # older field names of the customers list
                row.update(customer_id=row["id"], cards=row["distinct"].get("card", 0))
            rows.append(row)
        stats = {"entities": int(m), "events": int(counts.sum()), "with_2_plus": int((counts >= 2).sum()),
                 "with_10_plus": int((counts >= 10).sum()), "median_events": float(np.median(counts)),
                 "max_events": int(counts.max()), "with_fraud": int((fraud > 0).sum()),
                 "mixed": int(((fraud > 0) & (legit > 0)).sum()), "shared": int(shared.sum()),
                 "shared_by": e["shared_by"][0].removesuffix("_id") if e["shared_by"] else None,
                 "shared_min": e["shared_min"],
                 "note": _ENTITY_NOTES.get(entity, "measured events of the run (history replay excluded)")}
        out = {"entity": entity, "rows": rows, "stats": stats}
        if entity == "customer":
            stats.update(customers=stats["entities"], multi_card=stats["shared"])
            out["customers"] = rows
        return out

    def customers(self, run_id: str, sort: str = "events", show: str = "all", limit: int = 100) -> dict[str, Any]:
        return self.entities(run_id, "customer", sort, show, limit)

    def entity(self, run_id: str, entity: str, eid: int, offset: int = 0, limit: int = 500) -> dict[str, Any]:
        """One entity's events in time order with what each decision saw and did, a summary over all its
        events, the entities it is connected to (pivots) and its world profile."""
        e = self._entity_def(entity)
        s = self.index(run_id)
        v = load_views()
        g = s.group(entity, e["key"], e["shared_by"])
        i = int(np.searchsorted(g["uniq"], eid)) if g["uniq"].size else 0
        found = i < g["uniq"].size and int(g["uniq"][i]) == eid
        mem = s.members(g, i) if found else np.zeros(0, np.int64)
        profile = None
        try:
            profile = self._profile(run_id, entity, eid)
        except Exception as ex:  # noqa: BLE001 - the world profile is optional context
            profile = {"error": f"{type(ex).__name__}: {ex}"}
        out = {"entity": entity, "id": str(eid), "display": ipv4(eid) if e["format"] == "ipv4" else str(eid),
               "title": e["title"], "profile": profile, "summary": self._summary(s, mem),
               "related": self._related(s, mem, e["key"]), "total": int(mem.size), "offset": offset,
               "columns": {"state": e["features"], "contexts": [{k: cx[k] for k in ("name", "title", "columns")}
                                                                for cx in v["contexts"]]},
               "features_note": _features_note(s.meta)}
        if e["common_point"] and found:
            try:
                out["common_point"] = self._common_point(s, e["key"], eid)
            except Exception as ex:  # noqa: BLE001
                out["common_point"] = {"error": f"{type(ex).__name__}: {ex}"}
        page = mem[offset:offset + limit]
        out["events"] = self._trail(s, page, e, v["contexts"])
        out["truncated"] = bool(offset + limit < mem.size)
        if not found:
            out["note"] = f"no measured events of this {entity} in the run (it may appear only in the history replay)"
        if entity == "customer":
            out["customer"] = profile if profile and "error" not in profile else None
            out["features"] = [f["key"] for f in e["features"]]
        return out

    def customer(self, run_id: str, cid: int) -> dict[str, Any]:
        return self.entity(run_id, "customer", cid)

    def _summary(self, s: _Index, mem: np.ndarray) -> dict[str, Any]:
        from ..challenge import PAID, STOPPED
        c = s.c
        if not mem.size:
            return {"events": 0}
        oc = c["outcome"][mem]
        fate = c["fate"][mem]
        fraud = c["is_fraud"][mem]
        amt = c["amount_usd_minor"][mem] / 100.0
        sc = c["score"][mem]
        out = {"events": int(mem.size), "fraud_events": int(fraud.sum()), "amount_usd": round(float(amt.sum()), 2),
               "first_us": int(c["event_time_us"][mem].min()), "last_us": int(c["event_time_us"][mem].max()),
               "cards": int(np.unique(c["card_id"][mem][c["card_id"][mem] != 0]).size),
               "max_score": None if np.all(np.isnan(sc)) else float(np.nanmax(sc)),
               "unscored": int(np.isnan(sc).sum()),
               "fraud_usd_paid": round(float(amt[fraud & np.isin(fate, PAID)].sum()), 2),
               "fraud_usd_stopped": round(float(amt[fraud & np.isin(fate, STOPPED)].sum()), 2),
               "good_stopped": int((~fraud & np.isin(fate, STOPPED)).sum())}
        for code, name in OUTCOME.items():
            if name:
                out[name.lower()] = int((oc == code).sum())
        types = collections.Counter(_enum(PAYMENT_TYPE, x) for x in c["payment_type"][mem].tolist())
        out["payment_types"] = dict(types.most_common())
        return out

    def _related(self, s: _Index, mem: np.ndarray, key: str) -> dict[str, Any]:
        """The entities this one's events touch: distinct count and the most frequent (pivots)."""
        out: dict[str, Any] = {}
        if not mem.size:
            return out
        fraud = s.c["is_fraud"][mem]
        for name, col in ID_KINDS.items():
            if col in (key, "event_id", "campaign_id"):
                continue
            vals = s.c[col][mem]
            nz = vals != 0
            if not nz.any():
                continue
            u, inv, cnt = np.unique(vals[nz], return_inverse=True, return_counts=True)
            fr = np.bincount(inv, weights=fraud[nz].astype(np.float64), minlength=u.size)
            top = np.lexsort((-fr, -cnt))[:5]
            out[name] = {"distinct": int(u.size),
                         "top": [{"id": str(int(u[j])), "display": ipv4(int(u[j])) if col == "ip_v4" else str(int(u[j])),
                                  "events": int(cnt[j]), "fraud": int(fr[j])} for j in top.tolist()]}
        return out

    def _trail(self, s: _Index, page: np.ndarray, e: dict[str, Any], contexts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = s.rows(page)
        if not rows:
            return rows
        schema = load_schema()
        fidx = {f.name: f.index for f in schema.features}
        feats = s.features.get([int(x) for x in s.c["event_id"][page]])

        def fval(fv: tuple[np.ndarray, np.ndarray] | None, name: str) -> float | None:
            if fv is None or name not in fidx:
                return None
            j = fidx[name]
            vals, miss = fv
            if j >= vals.size or miss[j]:
                return None          # a run of a narrower schema, or the entity was absent (defaulted)
            return float(vals[j])

        for r, i in zip(rows, page.tolist()):
            fv = feats.get(int(s.c["event_id"][i]))
            cx = _context_of(s, i, contexts)
            r["context"] = cx["name"] if cx else None
            r["features_recorded"] = fv is not None
            ctx: dict[str, Any] = {}
            for col in (cx["columns"] if cx else []):
                ctx[col["key"]] = s.field(col["key"], i) if col["kind"] == "field" else fval(fv, col["key"])
            r["ctx"] = ctx
            r["state"] = {f["key"]: fval(fv, f["key"]) for f in e["features"]}
            r["features"] = r["state"]          # older field name of the customer trail
        return rows

    def _common_point(self, s: _Index, key: str, eid: int) -> dict[str, Any]:
        """Cards used at this merchant / terminal (history included, genuine visits) that were later used
        fraudulently: the common point of purchase behind a breach or a skimmer, against the share of all
        active cards that saw fraud."""
        cp = s.common_points(key)
        pos, f = _by_event(cp["ids"], np.asarray([eid], np.uint64))
        seen = int(cp["seen"][pos[0]]) if f[0] else 0
        later = int(cp["later"][pos[0]]) if f[0] else 0
        base = float(cp["baseline"][0])
        share = later / seen if seen else 0.0
        # what the later fraud on those cards was
        d = s.dir
        visits = _read(d, "events-*.parquet", ["card_id", "event_time_us", "is_fraud"], [(key, "=", eid)])
        by_type: collections.Counter = collections.Counter()
        if visits is not None and visits.num_rows:
            vc = _np(visits, "card_id", np.uint64, 0)
            vt = _np(visits, "event_time_us", np.int64, 0)
            ok = (vc != 0) & ~_np(visits, "is_fraud", np.bool_, False)
            vc, vt = vc[ok], vt[ok]
            o = np.lexsort((vt, vc))
            u, first = np.unique(vc[o], return_index=True)
            first_visit = vt[o][first]
            c = s.c
            fr = c["is_fraud"] & (c["card_id"] != 0)
            p2, f2 = _by_event(u, c["card_id"][fr])
            after = f2 & (c["event_time_us"][fr] > np.where(f2, first_visit[p2], 0))
            by_type.update(s.meta.names.get(int(x), str(int(x))) for x in c["fraud_type"][fr][after].tolist())
        return {"cards_seen": seen, "cards_later_fraud": later, "share": share, "baseline_share": base,
                "lift": (share / base) if base and seen else None, "by_type": dict(by_type.most_common()),
                "note": "cards used here genuinely (history included) whose first fraud came afterwards, against the "
                        "share of all cards active in the measured window that saw fraud"}

    def _profile(self, run_id: str, entity: str, eid: int) -> dict[str, Any] | None:
        """The entity's world record: who owns a card / device / account, a customer's cards, devices and
        regular payees, a merchant's category and risk tier, a payee's country."""
        from ..world.build import (ACCOUNT_BASE, CARD_BASE, DEVICE_BASE, FRAUD_DEVICE_BASE, MERCHANT_BASE,
                                   NEW_DEVICE_BASE, PAYEE_BASE, SEGMENTS, TERMINAL_BASE)
        sc = self.meta(run_id).scenario
        if sc is None:
            return None
        wd = Path(sc.world_dir)
        if not (wd / "world.json").exists():
            return None
        countries = load_schema().countries

        def row(table: str, col: str, val: int, cols: list[str] | None = None) -> dict[str, Any] | None:
            files = sorted(wd.glob(f"{table}*.parquet"))
            for f in files:
                t = pq.read_table(f, columns=cols, filters=[(col, "=", val)])
                if t.num_rows:
                    return t.slice(0, 1).to_pylist()[0]
            return None

        def country(i: Any) -> str | None:
            return countries[int(i)] if i is not None and 0 <= int(i) < len(countries) else None

        if entity == "customer":
            r = row("customers-", "customer_id", eid)
            if not r:
                return None
            cards = pq.read_table(wd / "cards.parquet", columns=["card_id", "card_type"]).slice(int(r["card_offset"]), int(r["n_cards"]))
            devs = pq.read_table(wd / "devices.parquet", columns=["device_id", "os"]).slice(int(r["device_offset"]), int(r["n_devices"]))
            payees = [str(PAYEE_BASE + int(r[f"payee{k}"])) for k in range(3) if f"payee{k}" in r and int(r[f"payee{k}"]) >= 0]
            habits = [str(MERCHANT_BASE + int(r[f"hab{k}"])) for k in range(6) if f"hab{k}" in r and int(r[f"hab{k}"]) >= 0]
            return {"customer_id": str(eid), "segment": SEGMENTS[int(r["segment"]) % len(SEGMENTS)],
                    "home_country": country(r["home_country"]), "tenure_days": float(r["tenure_days"]),
                    "avg_ticket_usd": float(r["avg_ticket_usd"]), "activity_per_day": float(r.get("activity_per_day") or 0),
                    "home_ip": ipv4(int(r["home_ip"])) if r.get("home_ip") else None,
                    "accounts": [str(ACCOUNT_BASE + (eid - 1) * 4)],
                    "cards": [{"id": str(c), "type": _enum(CARD_TYPE, t)} for c, t in
                              zip(cards["card_id"].to_pylist(), cards["card_type"].to_pylist())],
                    "devices": [{"id": str(dv), "os": int(o)} for dv, o in zip(devs["device_id"].to_pylist(), devs["os"].to_pylist())],
                    "payees": payees, "habit_merchants": habits}
        if entity == "card":
            r = row("cards", "card_id", eid)
            if not r:
                return {"note": "not a card of the world"}
            return {"card_id": str(eid), "owner": str(int(r["customer_row"]) + 1), "type": _enum(CARD_TYPE, r["card_type"]),
                    "credit_limit_usd": float(r.get("credit_limit_usd") or 0), "primary": bool(r.get("primary"))}
        if entity == "account":
            if eid < ACCOUNT_BASE or eid >= CARD_BASE:
                return {"note": "not an account of the world"}
            return {"account_id": str(eid), "owner": str((eid - ACCOUNT_BASE) // 4 + 1), "slot": (eid - ACCOUNT_BASE) % 4}
        if entity == "device":
            if eid >= FRAUD_DEVICE_BASE:
                return {"device_id": str(eid), "note": "not a customer's device: outside the world's device registry "
                                                       "(fraudster infrastructure)"}
            if eid >= NEW_DEVICE_BASE:
                return {"device_id": str(eid), "note": "a new device of a genuine customer (phone upgrade)"}
            r = row("devices", "device_id", eid)
            return {"device_id": str(eid), "owner": str(int(r["customer_row"]) + 1), "os": int(r["os"])} if r else None
        if entity == "merchant":
            r = row("merchants", "merchant_id", eid)
            if not r:
                return None
            from ..world import mcc as M
            cat = int(r["cat"])
            return {"merchant_id": str(eid), "category": M.NAMES[cat] if 0 <= cat < len(M.NAMES) else str(cat),
                    "mcc": int(r["mcc"]), "country": country(r["country"]), "online": bool(r["online"]),
                    "atm": bool(r["atm"]), "risk_tier": int(r["risk_tier"]), "fraud_rate_30d": float(r["fraud_rate_30d"]),
                    "terminals": int(r["n_terminals"]), "popularity": float(r["popularity"])}
        if entity == "terminal":
            m = pq.read_table(wd / "merchants.parquet", columns=["merchant_id", "terminal_offset", "n_terminals"])
            off = m["terminal_offset"].to_numpy().astype(np.int64)
            k = eid - TERMINAL_BASE
            j = int(np.searchsorted(off, k, side="right")) - 1
            if j < 0 or k >= off[j] + int(m["n_terminals"][j].as_py()):
                return {"note": "not a terminal of the world"}
            return {"terminal_id": str(eid), "merchant": str(int(m["merchant_id"][j].as_py())), "index": int(k - off[j])}
        if entity == "payee":
            r = row("payees", "payee_id", eid)
            if not r:
                return None
            return {"payee_id": str(eid), "country": country(r["country"]), "mule": bool(r["is_mule"])}
        if entity == "ip":
            return {"ip": ipv4(eid)}
        return None

    # ------------------------------------------------------------------ campaigns
    def campaigns(self, run_id: str) -> list[dict[str, Any]]:
        """Fraud campaigns of the run: how far each got before it was stopped, with the step-up's fate
        counted (a passed step-up pays out, a failed or abandoned one stops it; a held review stops it)."""
        from ..challenge import PAID, STOPPED
        s = self.index(run_id)
        c = s.c
        names = s.meta.names
        camp = c["campaign_id"]
        idx = np.flatnonzero(camp > 0)
        if not idx.size:
            return []
        idx = idx[np.lexsort((c["event_time_us"][idx], camp[idx]))]
        u, starts = np.unique(camp[idx], return_index=True)
        ends = np.append(starts[1:], idx.size)
        out = []
        for cid, a, b in zip(u.tolist(), starts.tolist(), ends.tolist()):
            m = idx[a:b]
            fm = m[c["is_fraud"][m]]
            if not fm.size:
                continue
            oc, fate = c["outcome"][fm], c["fate"][fm]
            stopped = np.isin(fate, STOPPED)
            first_stop = np.flatnonzero(stopped)
            first_decl = np.flatnonzero(oc == 3)
            amt = c["amount_usd_minor"][fm] / 100.0
            out.append({"campaign_id": str(cid), "fraud_type": names.get(int(c["fraud_type"][fm].max()), "?"),
                        "events": int(fm.size), "story_events": int(m.size - fm.size),
                        "customers": int(np.unique(c["customer_id"][fm]).size),
                        "flagged": int((oc >= 2).sum()), "declined": int((oc == 3).sum()),
                        "challenged": int((oc == 4).sum()), "stopped": int(stopped.sum()),
                        "first_event_us": int(c["event_time_us"][fm[0]]), "last_event_us": int(c["event_time_us"][fm[-1]]),
                        "approved_before_first_stop": int(first_stop[0]) if first_stop.size else int(fm.size),
                        "approved_before_first_decline": int(first_decl[0]) if first_decl.size else int(fm.size),
                        "usd_total": float(amt.sum()), "usd_paid": float(amt[np.isin(fate, PAID)].sum()),
                        "usd_approved": float(amt[oc == 1].sum())})
        return sorted(out, key=lambda x: x["first_event_us"])

    def campaign_events(self, run_id: str, cid: int) -> list[dict[str, Any]]:
        return self.search(run_id, f"campaign:{cid}", "auto", 5000)["results"]


def _no_score(path: str) -> str:
    return {"RULES_ONLY_FALLBACK": "no model score: the model missed its deadline and the rules decided alone",
            "PROVISIONAL": "no model score yet: a provisional rules-only decision (the final revision carries the score)",
            "ECHO": "no model score: echo mode (the pipeline returns without scoring)"}.get(path, "no model score")


def _features_note(meta: _RunMeta) -> str | None:
    p = meta.sample_permille
    if p >= 1000:
        return None
    return f"feature vectors were recorded for {p / 10:g}% of decisions (feature_sample_permille={p})"


_ENTITY_NOTES = {
    "customer": "measured events of the run (history replay excluded); a customer's events share one Kafka key, so "
                "they are processed in order and every one sees the state the earlier ones built",
    "device": "a device seen with many customers or cards is the bot / fraud-farm shape",
    "ip": "an address seen with many cards or customers is a proxy, bot or fraud-farm shape",
    "payee": "a payee paid by many senders is the mule shape (fan-in)",
    "merchant": "a merchant whose cards later turn up in fraud elsewhere is a common point of purchase (breach)",
    "terminal": "a terminal whose cards later turn up in fraud is a skimming suspect",
}


def make_router(runs_dir: Path) -> APIRouter:
    ex = Explorer(runs_dir)
    r = APIRouter(tags=["explorer"])
    base = "/api/runs/{run_id}"

    @r.get("/api/explorer/views")
    def views() -> dict[str, Any]:
        """Entities, context columns and use-case lenses of the Explorer (contracts/explorer.yaml)."""
        return {**load_views(), "cache": ex.cache_info()}

    @r.get("/api/usecases")
    def usecases() -> dict[str, Any]:
        """The typology registry (contracts/usecases + user typologies)."""
        from ..usecases import registry
        return {"typologies": [{k: v for k, v in t.model_dump(mode="json", by_alias=True).items()
                                if k in ("id", "name", "title", "description", "pattern", "mix_weight", "roles",
                                         "signals", "rules", "disabled", "source", "labels")} for t in registry().all]}

    @r.get(base + "/search")
    def search(run_id: str, q: str = "", kind: str = "auto", limit: int = 50) -> dict[str, Any]:
        """Events matching a query. Terms are ANDed (`channel:ECOM fraud:account_takeover`), commas mean OR
        (`outcome:DECLINE,CHALLENGE`); a bare number matches any id. Kinds: see /api/explorer/views."""
        return ex.search(run_id, q, kind, max(1, min(limit, 5000)))

    @r.get(base + "/decisions/{event_id}")
    def decision(run_id: str, event_id: str, maturity_days: float = 30.0) -> dict[str, Any]:
        try:
            eid = int(event_id)
        except ValueError:
            raise HTTPException(400, "event_id must be an integer") from None
        return ex.decision(run_id, eid, maturity_days)

    @r.get(base + "/entities/{entity}")
    def entities(run_id: str, entity: str, sort: str = "events", show: str = "all", limit: int = 100,
                 q: str = "") -> dict[str, Any]:
        """Entities of a run (customer, card, account, device, ip, merchant, terminal, payee) with per-entity
        decision counts. sort: events | fraud | flagged | max_score | amount | shared | recent;
        show: all | fraud | mixed | shared | flagged; q: count only the events matching a search."""
        return ex.entities(run_id, entity, sort, show, max(1, min(limit, 1000)), q)

    @r.get(base + "/entities/{entity}/{eid}")
    def entity(run_id: str, entity: str, eid: str, offset: int = 0, limit: int = 500) -> dict[str, Any]:
        """One entity's decision trail (paged), summary, connected entities and world profile."""
        try:
            ident = parse_ipv4(eid) if entity == "ip" else int(eid)
        except ValueError:
            raise HTTPException(400, f"{entity} id must be an integer{' or a dotted address' if entity == 'ip' else ''}") from None
        return ex.entity(run_id, entity, ident, max(0, offset), max(1, min(limit, 2000)))

    @r.get(base + "/customers")
    def customers(run_id: str, sort: str = "events", show: str = "all", limit: int = 100) -> dict[str, Any]:
        """Alias of /entities/customer."""
        return ex.customers(run_id, sort, show, max(1, min(limit, 1000)))

    @r.get(base + "/customers/{cid}")
    def customer(run_id: str, cid: str) -> dict[str, Any]:
        """Alias of /entities/customer/{cid}."""
        try:
            return ex.customer(run_id, int(cid))
        except ValueError:
            raise HTTPException(400, "customer id must be an integer") from None

    @r.get(base + "/campaigns")
    def campaigns(run_id: str) -> dict[str, Any]:
        return {"campaigns": ex.campaigns(run_id)}

    @r.get(base + "/campaigns/{cid}")
    def campaign(run_id: str, cid: str) -> dict[str, Any]:
        try:
            return {"events": ex.campaign_events(run_id, int(cid))}
        except ValueError:
            raise HTTPException(400, "campaign id must be an integer") from None

    r.explorer = ex  # type: ignore[attr-defined]
    return r
