"""The topologies a scenario can run (contracts/topologies.yaml): how each is built, what it can and cannot
do, its known limits and the enhancements that would lift them, and advice from measured runs.

One source for the generated reference (docs/reference/topologies.md), the console's Topologies page and
Launch explainer, and the run report's latency insight. `run_advice` turns a finished run's report,
summary and verify result into ranked, evidence-backed next steps for the platform being built.
"""
from __future__ import annotations

import functools
import os
from pathlib import Path
from typing import Any, get_args

import yaml

from .scenario import REPO_ROOT, Topology

PATH = REPO_ROOT / "contracts" / "topologies.yaml"
STATUSES = ("yes", "partial", "no", "n/a")
ENHANCEMENT_STATUSES = ("available", "planned", "idea")
# stages the report's tail attribution can name (report.report tail_attribution)
TAIL_STAGES = ("schedule_lag", "ingress", "queue_wait", "shuffle", "state_features", "async_wait", "feature_lookup",
               "model_call", "rules", "audit_ack", "egress")


@functools.lru_cache(maxsize=4)
def _load(path: str) -> dict[str, Any]:
    return yaml.safe_load(Path(path).read_text()) or {}


def load(path: Path | None = None) -> dict[str, Any]:
    return _load(str(path or os.environ.get("REALY_TOPOLOGIES") or PATH))


def names() -> tuple[str, ...]:
    return tuple(get_args(Topology))


def validate(doc: dict[str, Any] | None = None) -> list[str]:
    """Problems with the contract: every topology the scenario model accepts is described, with every
    capability, a path, and valid statuses; advice names only stages the report attributes."""
    doc = doc if doc is not None else load()
    p: list[str] = []
    caps = list((doc.get("capabilities") or {}).keys())
    tops = doc.get("topologies") or {}
    for t in names():
        if t not in tops:
            p.append(f"topology {t} is not described")
    for t, d in tops.items():
        if t not in names():
            p.append(f"{t}: not a topology the scenario model accepts ({', '.join(names())})")
        for k in ("title", "tagline", "decides", "built", "capabilities", "choose_when"):
            if not d.get(k):
                p.append(f"{t}: missing {k}")
        for c in caps:
            v = (d.get("capabilities") or {}).get(c)
            if v is None:
                p.append(f"{t}: capability {c} not stated")
            elif v.get("status") not in STATUSES:
                p.append(f"{t}: {c}: status must be one of {STATUSES}")
            elif not v.get("note"):
                p.append(f"{t}: {c}: a status needs a note saying why")
        for c in (d.get("capabilities") or {}):
            if c not in caps:
                p.append(f"{t}: unknown capability {c}")
        for e in d.get("enhancements") or []:
            if e.get("status") not in ENHANCEMENT_STATUSES:
                p.append(f"{t}: enhancement {e.get('title')!r}: status must be one of {ENHANCEMENT_STATUSES}")
    for stage, per in (doc.get("advice") or {}).items():
        if stage not in TAIL_STAGES:
            p.append(f"advice for {stage!r}: not a stage the report attributes ({', '.join(TAIL_STAGES)})")
        for k in per:
            if k != "all" and k not in names():
                p.append(f"advice for {stage}: {k!r} is not a topology")
    return p


def advice_for(stage: str, topology: str | None) -> str:
    """What to try when `stage` dominates the slowest 1% of events on `topology`."""
    per = (load().get("advice") or {}).get(stage) or {}
    return per.get(topology or "", per.get("all", ""))


def _pct(x: float) -> str:
    return f"{x * 100:.0f}%"


def run_advice(rep: dict[str, Any], summary: dict[str, Any] | None = None, verify: dict[str, Any] | None = None,
               topology: str | None = None) -> list[dict[str, Any]]:
    """Next steps from one finished run, most important first. Each item says what was measured (the
    evidence, with numbers) and what to change; `area` groups them (latency, capacity, correctness,
    flows, instrument)."""
    rep = rep or {}
    s = rep.get("summary") or {}
    topology = topology or rep.get("topology") or (summary or {}).get("topology")
    t = (load().get("topologies") or {}).get(topology or "", {})
    out: list[dict[str, Any]] = []

    def add(severity: str, area: str, title: str, evidence: str, action: str, rank: float, **extra: Any) -> None:
        out.append({"severity": severity, "area": area, "title": title, "evidence": evidence, "action": action,
                    "rank": rank, **extra})

    budget = (rep.get("budget") or {}).get("e2e_p99_ms") or s.get("budget_e2e_p99_ms")
    p99 = s.get("e2e_p99_ms")
    over = bool(budget and p99 and p99 > budget)
    tail = rep.get("tail_attribution") or {}
    share = tail.get("share") or {}
    ranked = sorted(share.items(), key=lambda kv: -kv[1])
    if ranked and ranked[0][1] >= 0.25:
        st, sh = ranked[0]
        second = f"; then {ranked[1][0].replace('_', ' ')} {_pct(ranked[1][1])}" if len(ranked) > 1 else ""
        add("warning" if over else "info", "latency",
            f"{st.replace('_', ' ')} holds {_pct(sh)} of the slowest 1% of events",
            f"events at or above p99 {tail.get('threshold_ms', 0):.0f} ms spend {(tail.get('mean_ms_by_stage') or {}).get(st, 0):.0f} ms "
            f"there on average{second}" + (f"; e2e p99 {p99:.0f} ms against a {budget:.0f} ms budget" if over else ""),
            advice_for(st, topology), (3 if over else 1) * 100 + sh * 100, stage=st)
    fb = s.get("degraded_fraction")
    if fb is not None and fb > 0.01:
        add("warning", "latency", f"{_pct(fb)} of decisions were taken without the model",
            "a feature or model deadline expired, so rules alone decided (flagged degraded)",
            "look at the model-call and feature-lookup p99 against their deadlines; hedging (hedge_after_ms) and "
            "model-server capacity cut deadline misses; raise the deadlines only if the budget allows", 250 + fb * 100)
    lag = ((rep.get("stages") or {}).get("schedule_lag") or {}).get("p99")
    if lag is not None and lag > 50:
        add("warning", "instrument", f"the load generator was late by up to {lag:.0f} ms (p99)",
            "events left the emitter later than intended, so it, not the pipeline, was the limit at this rate",
            advice_for("schedule_lag", topology), 300)
    for name, src in ((rep.get("resources") or {}).get("sources") or {}).items():
        u = src.get("utilisation")
        if u is not None and u >= 0.7:
            add("warning", "capacity", f"{name} ran at {_pct(u)} of its CPU allocation",
                f"{src.get('cpu_cores', 0):.2f} of {src.get('cpus_allocated') or '?'} cores for "
                f"{src.get('events_per_s', 0):,.0f} events/s "
                f"({src.get('cpu_ms_per_event', 0):.2f} ms CPU per event)",
                f"scale {name} out (more replicas / parallelism) or cut its cost per event before raising the rate; "
                f"`pulsebench size` turns the measured cost into a node count", 200 + u * 100, component=name)
    if verify:
        f = verify.get("features") or {}
        exact, target = f.get("exact_set_agreement"), f.get("exact_set_target")
        if exact is not None and target is not None and exact < target:
            add("critical", "correctness", f"served features agree with the reference on {exact * 100:.2f}% of events",
                f"below the {target * 100:.1f}% target for {topology}",
                "the gateway read state that was not yet written (store lag): check the Flink job's store-write "
                "lag and backpressure; for exact features on this channel, flink-inline", 900)
        for k in ("rules", "score", "outcome"):
            v = (verify.get(k) or {}).get("parity") if isinstance(verify.get(k), dict) else None
            if v is not None and v < 1.0:
                add("critical", "correctness", f"{k} parity {v * 100:.2f}%",
                    "Java and Python disagree on what was decided", "open the verify report: a twin has diverged",
                    1000)
        low = sorted(((n, (x or {}).get("agreement")) for n, x in (f.get("per_feature") or {}).items()
                      if (x or {}).get("entity") not in ("customer", "card") and (x or {}).get("agreement") is not None
                      and x["agreement"] < 0.9), key=lambda kv: kv[1])[:3]
        if low and topology in ("gateway-kafka", "gateway-grpc"):
            add("info", "correctness", "shared-entity features lag the stream",
                ", ".join(f"{n} {a * 100:.0f}%" for n, a in low) + " agreement with the reference",
                "these are read as of the entity's last Redis write; coalescing Flink's writes helps at high TPS, and "
                "flink-inline computes them in-stream where they must be fresh", 120)
    tx = rep.get("transactions") or {}
    if tx.get("assembled"):
        pf = tx.get("partial_fraction") or 0.0
        w = (tx.get("assembly_wait_ms") or {}).get("p99")
        if pf > 0.02:
            add("warning", "flows", f"{_pct(pf)} of transactions were decided without a required part",
                "the part did not arrive within the contract's wait_ms",
                "find which part is late (the Explorer's transaction view) and raise wait_ms if the budget allows, or "
                "make the part optional and let a later stage decide again", 220 + pf * 100)
        if w is not None and budget and w > 0.4 * budget:
            add("info", "flows", f"waiting for a transaction's parts takes up to {w:.0f} ms (p99)",
                f"{_pct(w / budget)} of the {budget:.0f} ms budget is spent before scoring starts",
                "the customer waits for the payee check too: a shorter wait_ms trades completeness for latency"
                + ("; assembling in the gateway would also remove the assembled topic's hop" if topology == "gateway-kafka" else ""),
                150, stage="txn_assembly")
    if s.get("verdict") == "PASS" and not any(i["severity"] in ("warning", "critical") for i in out):
        add("good", "latency", f"within budget at {s.get('achieved_emit_tps', 0):,.0f} TPS: e2e p99 {p99 or 0:.0f} ms",
            "no stage stands out", "raise the rate (step-ramp) to find this topology's knee on this host", 10)
    out.sort(key=lambda i: -i["rank"])
    for i in out:
        i.pop("rank", None)
    if t:
        for i in out:
            i["topology"] = topology
    return out
