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
from typing import Any

import yaml

from dataclasses import dataclass

# the repository root, as realy.scenario computes it (not imported: the scenario model takes its allowed
# topologies from this contract)
REPO_ROOT = Path(os.environ.get("REALY_HOME") or Path(__file__).resolve().parents[2])
PATH = REPO_ROOT / "contracts" / "topologies.yaml"
# what the runner implements, per runtime choice (a new value is a new component: docs/dev/topologies.md)
RUNTIME_VALUES = {
    "ingress": ("kafka", "grpc"),                 # how events reach the scorer
    "scorer": ("gateway", "flink", "echo"),       # who decides
    "flink_mode": ("features", "both", "none"),   # the Flink job's mode (none: Flink is not used)
    "flows": ("assembled_topic", "in_job", "none"),   # multi-message transactions: how they reach the scorer
}
COMPONENT_KINDS = ("driver", "kafka", "flink", "store", "service", "model", "sink")
# Freshness of the online store (Flink pickup -> blob durable in Redis) is not a hot-path latency: it decides
# how stale the gateway's features can be. It gets its own SLO on the map (warn at 25%, hot at 100%) instead
# of a share of the e2e budget, which painted every replay red.
FRESHNESS_SLO_MS = float(os.environ.get("REALY_FRESHNESS_SLO_MS", "1000"))
SLO_STAGES = {"store_update_lag": FRESHNESS_SLO_MS}
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
    """Every topology the contract defines (the scenario model accepts exactly these)."""
    return tuple((load().get("topologies") or {}).keys())


@dataclass(frozen=True)
class Runtime:
    """What the runner sets up for a topology."""
    ingress: str
    scorer: str
    flink_mode: str
    flows: str
    exact_target: float | None

    @property
    def uses_flink(self) -> bool:
        return self.flink_mode != "none"

    @property
    def gateway_consumes_kafka(self) -> bool:
        """The gateway reads the events (or assembled) topic: the async rail and echo."""
        return self.scorer in ("gateway", "echo") and self.ingress == "kafka"

    @property
    def sync(self) -> bool:
        return self.ingress == "grpc"

    @property
    def echo(self) -> bool:
        return self.scorer == "echo"

    @property
    def supports_flows(self) -> bool:
        return self.flows != "none"


def runtime(topology: str) -> Runtime:
    t = (load().get("topologies") or {}).get(topology)
    if t is None:
        raise KeyError(f"no topology {topology!r} in contracts/topologies.yaml ({', '.join(names())})")
    r = t.get("runtime") or {}
    return Runtime(r.get("ingress", "kafka"), r.get("scorer", "gateway"), r.get("flink_mode", "features"),
                   r.get("flows", "none"), r.get("exact_target"))


def _metric(ref: str | None, entity: str | None = None) -> dict[str, str] | None:
    """"counter:src.name" / "stage:src.name" (rate) or "src.stage" (p99) -> the map's metric reference."""
    if not ref:
        return None
    ref = ref.replace("{entity}", entity or "")
    kind, _, rest = ref.partition(":") if ":" in ref else ("stage", "", ref)
    src, _, name = rest.partition(".")
    return {"source": src, kind: name}


def steps(topology: str, flows: bool = False) -> list[dict[str, Any]]:
    """The steps of `topology` as written (the flows variant when `flows` and the topology has one)."""
    t = (load().get("topologies") or {})[topology]
    return list((t.get("steps_with_flows") if flows and t.get("steps_with_flows") else t.get("steps")) or [])


def graph(topology: str, flows: bool = False, stream_entities: list[str] | None = None) -> dict[str, Any]:
    """The live map of `topology`: nodes on a column / row grid with the metrics that feed them, and the
    hops between them, built from its steps (`for_each: stream_entities` expanded, chained)."""
    if stream_entities is None:
        from .features import load_schema
        stream_entities = load_schema().stream_entities()
    comps = load().get("components") or {}
    flows = bool(flows) and runtime(topology).supports_flows
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    first_of: dict[str, str] = {}      # a step id -> the node an edge into it reaches
    last_of: dict[str, str] = {}       # a step id -> the node its edges leave from
    in_edge: dict[str, dict] = {}      # a for_each step's edge template into each of its nodes
    max_col, next_col = -1, 0

    def col_of(c: Any) -> int:
        nonlocal max_col, next_col
        if c == "next":
            next_col = max_col + 1
            return next_col
        if c == "same":
            return next_col
        next_col = int(c)
        return int(c)

    def node(sid: str, st: dict[str, Any], col: int, row: int, entity: str | None = None) -> None:
        nonlocal max_col
        comp = comps.get(st["component"]) or {}
        fill = (lambda x: x.replace("{entity}", entity) if isinstance(x, str) and entity else x)
        n = {"id": sid, "label": fill(st.get("label") or comp.get("label") or sid), "kind": comp.get("kind", "service"),
             "column": col, "row": row, "rate": _metric(st.get("rate"), entity), "p99": _metric(st.get("p99"), entity),
             "subtitle": fill(st.get("sub")), "component": st["component"], "does": fill(st.get("does"))}
        if n["p99"] and n["p99"].get("stage") in SLO_STAGES:
            n["slo_ms"] = SLO_STAGES[n["p99"]["stage"]]
        nodes.append(n)
        max_col = max(max_col, col)

    plan = steps(topology, flows)
    for st in plan:
        c, r = (st.get("at") or [0, 0])
        if st.get("for_each") == "stream_entities":
            c0, wrap = col_of(c), int(st.get("wrap", 4))
            ids = [st["id"].replace("{entity}", e) for e in stream_entities]
            for i, e in enumerate(stream_entities):
                node(ids[i], st, c0 + i // wrap, int(r) + i % wrap, e)
                if i:
                    tmpl = st.get("in") or {}
                    edges.append({"from": ids[i - 1], "to": ids[i], "label": tmpl.get("label", ""),
                                  "rate": _metric(tmpl.get("rate"), e), "p99": _metric(tmpl.get("p99"), e)})
            next_col = c0 + (len(stream_entities) - 1) // wrap
            first_of[st["id"]], last_of[st["id"]] = ids[0], ids[-1]
            in_edge[st["id"]] = {**(st.get("in") or {}), "_entity": stream_entities[0]}
        else:
            node(st["id"], st, col_of(c), int(r))
            first_of[st["id"]] = last_of[st["id"]] = st["id"]
    for st in plan:
        for h in st.get("to") or []:
            to = h["id"]
            tmpl = in_edge.get(to)
            e = {"from": last_of[st["id"]], "to": first_of.get(to, to), "label": h.get("label", ""),
                 "rate": _metric(h.get("rate")), "p99": _metric(h.get("p99"))}
            if tmpl:                    # into a for_each block: its own first hop (the first shuffle)
                e.update({"label": tmpl.get("label", e["label"]), "rate": _metric(tmpl.get("rate"), tmpl["_entity"]),
                          "p99": _metric(tmpl.get("p99"), tmpl["_entity"])})
            if h.get("bulk"):
                e["bulk"] = _metric(h["bulk"])
            edges.append(e)
    return {"topology": topology, "flows": flows, "nodes": nodes, "edges": edges}


def validate(doc: dict[str, Any] | None = None) -> list[str]:
    """Problems with the contract: every topology the scenario model accepts is described, with every
    capability, a path, and valid statuses; advice names only stages the report attributes."""
    doc = doc if doc is not None else load()
    p: list[str] = []
    caps = list((doc.get("capabilities") or {}).keys())
    tops = doc.get("topologies") or {}
    for t in ("gateway-kafka", "gateway-grpc", "flink-inline", "echo-kafka"):   # the bundled scenarios use these
        if t not in tops:
            p.append(f"topology {t} is not described")
    for t, d in tops.items():
        if not t.replace("-", "").isalnum() or t != t.lower():
            p.append(f"{t}: a topology name is lowercase letters, digits and dashes")
        for k in ("title", "tagline", "decides", "runtime", "steps", "capabilities", "choose_when"):
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
    comps = doc.get("components") or {}
    for cid, c in comps.items():
        if c.get("kind") not in COMPONENT_KINDS:
            p.append(f"component {cid}: kind must be one of {COMPONENT_KINDS}")
        for k in ("label", "what", "code", "extend"):
            if not c.get(k):
                p.append(f"component {cid}: missing {k}")
        for i, cfg in enumerate(c.get("config") or []):
            for k in ("key", "in", "effect"):
                if not cfg.get(k):
                    p.append(f"component {cid}: config[{i}] needs {k}")
    for t, d in tops.items():
        r = d.get("runtime") or {}
        for k, allowed in RUNTIME_VALUES.items():
            if r.get(k) not in allowed:
                p.append(f"{t}: runtime.{k} must be one of {allowed} (a new value needs a component: docs/dev/topologies.md)")
        et = r.get("exact_target")
        if et is not None and not (isinstance(et, (int, float)) and 0 < et <= 1):
            p.append(f"{t}: runtime.exact_target must be a share in (0, 1] or null")
        if r.get("flows") != "none" and not d.get("steps_with_flows"):
            p.append(f"{t}: runtime.flows is {r.get('flows')} but there are no steps_with_flows")
        for key in ("steps", "steps_with_flows"):
            plan = d.get(key) or []
            if key == "steps" and not plan:
                p.append(f"{t}: no steps")
            ids = [x.get("id") for x in plan]
            if len(ids) != len(set(ids)):
                p.append(f"{t}: {key}: step ids repeat")
            for x in plan:
                if x.get("component") not in comps:
                    p.append(f"{t}: {key}: step {x.get('id')} uses unknown component {x.get('component')!r}")
                if not x.get("does"):
                    p.append(f"{t}: {key}: step {x.get('id')} needs `does`")
                for h in x.get("to") or []:
                    if h.get("id") not in ids:
                        p.append(f"{t}: {key}: step {x.get('id')} goes to unknown step {h.get('id')!r}")
                for ref in [x.get("rate"), *(h.get("rate") for h in x.get("to") or []), *(h.get("bulk") for h in x.get("to") or [])]:
                    if ref and not (ref.startswith("counter:") or ref.startswith("stage:")):
                        p.append(f"{t}: {key}: step {x.get('id')}: rate {ref!r} must start with counter: or stage:")
                if x.get("for_each") not in (None, "stream_entities"):
                    p.append(f"{t}: {key}: step {x.get('id')}: for_each must be stream_entities")
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
        from .bindings.transactions import transactions as _contracts
        known = _contracts()
        per = tx.get("by_contract") or {",".join(tx.get("contracts") or []): tx}
        for name, c in per.items():
            pf = c.get("partial_fraction") or 0.0
            w = (c.get("assembly_wait_ms") or {}).get("p99")
            waits = bool(getattr(known.get(name), "require", ()))   # does the contract wait for a part at all
            if pf > 0.02:
                add("warning", "flows", f"{name}: {_pct(pf)} of transactions were decided without a required part",
                    "the part did not arrive within the contract's wait_ms",
                    "find which part is late (the Explorer's transaction view) and raise wait_ms if the budget allows, "
                    "or make the part optional and let a later stage decide again", 220 + pf * 100)
            if w is not None and budget and w > 0.4 * budget:
                if waits:
                    add("info", "flows", f"{name}: waiting for a transaction's parts takes up to {w:.0f} ms (p99)",
                        f"{_pct(w / budget)} of the {budget:.0f} ms budget is spent before scoring starts",
                        "the customer waits for the required part too: a shorter wait_ms trades completeness for latency"
                        + ("; assembling in the gateway would also remove the assembled topic's hop"
                           if topology == "gateway-kafka" else ""), 150, stage="txn_assembly")
                else:
                    add("warning", "flows", f"{name}: {w:.0f} ms (p99) pass before a transaction is assembled, although "
                        "it waits for no part", f"{_pct(w / budget)} of the {budget:.0f} ms budget: the messages queue in "
                        "front of the Flink assembler (source, deserialisation), not in the wait",
                        "the assembler's input is behind: more Flink source parallelism (up to the partitions), fewer "
                        "messages per payment (send only the parts the decision needs), or a larger host", 210,
                        stage="txn_assembly")
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
