"""`realy budget <scenario>`: Monte Carlo feasibility of a latency budget BEFORE running anything.

The P99 of a sum is not the sum of P99s, and a sequential chain can be infeasible by construction
(e.g. a fixed 100 ms feature build + a 30-100 ms model call can never fit 150 ms). This samples every
stage of the topology's critical path from the scenario's own distributions (same parameterisation
as the Java LatencyModel), applies per-stage deadlines and the fallback policy, and predicts the
end-to-end distribution, the fallback rate and which stage owns the tail.

Infrastructure hops (Kafka, Redis, network) use overridable baselines measured on the local stack;
pass --infra to replace them with numbers from your own cluster.
"""
from __future__ import annotations

from typing import Any

import numpy as np

from ..scenario import Scenario

Z99 = 2.3263478740408408
# Baselines (ms) for infrastructure hops, lognormal median/p99 (from the local compose stack, warm).
INFRA_DEFAULTS: dict[str, dict] = {
    "produce_ack": {"lognormal": {"median_ms": 2.6, "p99_ms": 25}},
    "ingress_transit": {"lognormal": {"median_ms": 2.8, "p99_ms": 30}},
    "redis_rtt": {"lognormal": {"median_ms": 0.6, "p99_ms": 6}},
    "model_network": {"lognormal": {"median_ms": 1.0, "p99_ms": 8}},
    "rules_policy": {"lognormal": {"median_ms": 0.05, "p99_ms": 0.5}},
    "audit_ack": {"lognormal": {"median_ms": 2.2, "p99_ms": 35}},
    "egress_transit": {"lognormal": {"median_ms": 2.2, "p99_ms": 30}},
    "flink_shuffles": {"lognormal": {"median_ms": 5.0, "p99_ms": 60}},
    "flink_state": {"lognormal": {"median_ms": 0.5, "p99_ms": 3.5}},
}


def sample(dist: dict | None, n: int, rng: np.random.Generator) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return (ms, error_mask, timeout_mask) for a proto3-JSON LatencyDist."""
    dist = dist or {}
    err = np.zeros(n, bool)
    tmo = np.zeros(n, bool)
    er, tr = float(dist.get("error_rate", 0)), float(dist.get("timeout_rate", 0))
    if er or tr:
        u = rng.random(n)
        err = u < er
        tmo = (u >= er) & (u < er + tr)
    if "fixed" in dist:
        v = np.full(n, float(dist["fixed"].get("ms", 0)))
    elif "uniform" in dist:
        d = dist["uniform"]
        v = rng.uniform(float(d.get("min_ms", 0)), float(d.get("max_ms", 0)), n)
    elif "normal" in dist:
        d = dist["normal"]
        v = rng.normal(float(d.get("mean_ms", 0)), float(d.get("sd_ms", 0)), n)
        v = np.clip(v, float(d.get("min_ms", 0)), float(d.get("max_ms", np.inf)) or np.inf)
    elif "lognormal" in dist or "bimodal" in dist:
        d = dist.get("lognormal") or dist.get("bimodal")
        med = max(float(d.get("median_ms", 1)), 1e-6)
        p99 = max(float(d.get("p99_ms", med)), med)
        sigma = (np.log(p99) - np.log(med)) / Z99
        v = np.exp(np.log(med) + sigma * rng.standard_normal(n))
        if "lognormal" in dist and float(d.get("max_ms", 0)) > 0:
            v = np.minimum(v, float(d["max_ms"]))
        if "bimodal" in dist:
            tail = rng.random(n) < float(d.get("tail_prob", 0))
            v = np.where(tail, rng.uniform(float(d.get("tail_min_ms", 0)), float(d.get("tail_max_ms", 0)), n), v)
    elif "empirical" in dist:
        d = dist["empirical"]
        v = np.interp(rng.random(n), d["quantiles"], d["ms"])
    else:
        v = np.zeros(n)
    return np.maximum(v, 0), err, tmo


def check(sc: Scenario, n: int = 200_000, infra: dict[str, dict] | None = None, seed: int = 7) -> dict[str, Any]:
    rng = np.random.default_rng(seed)
    cfg = sc.scoring_config()
    inf = {**INFRA_DEFAULTS, **(infra or {})}
    ms = sc.model_server_config()
    budget = float(cfg.get("budget_ms", 100))
    fdl = float(cfg.get("feature_deadline_ms", 30)) or np.inf
    mdl = float(cfg.get("model_deadline_ms", 80)) or np.inf
    S: dict[str, np.ndarray] = {}

    def s(name: str, dist: dict | None) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        v, e, t = sample(dist, n, rng)
        return v, e, t

    topo = sc.topology
    if cfg.get("echo"):
        S["produce_ack"] = s("", inf["produce_ack"])[0]
        S["ingress"] = s("", inf["ingress_transit"])[0]
        S["egress"] = s("", inf["egress_transit"])[0]
        hot = np.zeros(n)
        fb = np.zeros(n, bool)
    else:
        # ---- feature lookup (store RTT + injected delay), bounded by its deadline and the remaining budget
        rtt = s("", inf["redis_rtt"])[0]
        fdel, fe, ft = s("", cfg.get("feature_lookup_delay"))
        feat = rtt + fdel
        f_to = ft | (feat > fdl)
        feat = np.where(f_to, fdl, np.where(fe, rtt, feat))
        pre = np.zeros(n)
        if topo == "flink-inline":
            pre = s("", inf["flink_shuffles"])[0] + s("", inf["flink_state"])[0]
            S["flink_shuffles+state"] = pre
        remaining = budget - pre - feat
        # ---- model (extra delay + network + service time; capacity/queueing not modelled here)
        xdel, xe, xt = s("", cfg.get("model_extra_delay"))
        svc, se, st = s("", ms.get("service_time"))
        per_item = float(ms.get("per_item_us", 0)) / 1000.0
        net = s("", inf["model_network"])[0]
        model = xdel + net + svc + per_item
        m_cap = np.minimum(mdl, np.maximum(remaining, 0))
        m_to = xt | st | (model > m_cap)
        fb = f_to | fe | m_to | xe | se
        model = np.where(m_to, m_cap, np.where(xe | se, net, model))
        rules = s("", inf["rules_policy"])[0]
        S["feature_lookup"] = feat
        S["model_call"] = model
        S["rules_policy"] = rules
        hot = pre + feat + model + rules
        S["produce_ack"] = s("", inf["produce_ack"])[0]
        if topo != "gateway-grpc":
            S["ingress"] = s("", inf["ingress_transit"])[0]
        if topo != "flink-inline":
            S["audit_ack"] = s("", inf["audit_ack"])[0]
        S["egress"] = s("", inf["egress_transit"])[0]
    e2e = hot + sum(v for k, v in S.items() if k not in ("feature_lookup", "model_call", "rules_policy",
                                                          "flink_shuffles+state"))
    if topo == "gateway-grpc":
        e2e = e2e - S["produce_ack"]  # sync: no emitter->Kafka hop; audit ack is on the path instead
    pct = lambda a: {f"p{q}": float(np.percentile(a, q)) for q in (50, 90, 99, 99.9)}  # noqa: E731
    thr = np.percentile(e2e, 99)
    tail = e2e >= thr
    share = {k: float(v[tail].mean()) for k, v in S.items()}
    tot = sum(share.values()) or 1
    within = float((e2e <= sc.budget.e2e_p99_ms).mean())
    p99 = float(np.percentile(e2e, 99))
    hot_p99 = float(np.percentile(hot, 99))
    verdict = ("FEASIBLE" if p99 <= sc.budget.e2e_p99_ms and hot_p99 <= sc.budget.hot_path_p99_ms
               else "MARGINAL" if p99 <= 1.15 * sc.budget.e2e_p99_ms else "INFEASIBLE")
    advice = []
    if fb.mean() > 0.01:
        advice.append(f"{fb.mean():.1%} of events would hit a stage deadline and fall back to rules only; "
                      "raise deadlines, cut injected delays, or parallelise lookups")
    dom = max(share, key=share.get) if share else None
    if dom:
        advice.append(f"the slowest 1% are dominated by {dom} ({share[dom] / tot:.0%} of tail time)")
    if cfg.get("feature_lookup_delay", {}).get("fixed", {}).get("ms", 0) >= 0.5 * budget:
        advice.append("a fixed feature-build delay of >= half the budget leaves no room for the model: infeasible "
                      "as a sequential chain")
    return {"scenario": sc.name, "topology": topo, "samples": n, "budget": sc.budget.model_dump(),
            "e2e": pct(e2e), "hot_path": pct(hot), "within_budget": within, "fallback_rate": float(fb.mean()),
            "stages_p99": {k: float(np.percentile(v, 99)) for k, v in S.items()},
            "tail_share": {k: v / tot for k, v in share.items()}, "verdict": verdict, "advice": advice,
            "note": "capacity/queueing (workers, batching, CPU saturation) is not modelled; the run measures it"}


def render(r: dict[str, Any]) -> str:
    b = r["budget"]
    lines = [f"budget check {r['scenario']} ({r['topology']}): {r['verdict']}  "
             f"[e2e p99 budget {b['e2e_p99_ms']:.0f} ms, hot path {b['hot_path_p99_ms']:.0f} ms]",
             f"  predicted e2e p50 {r['e2e']['p50']:.1f}  p99 {r['e2e']['p99']:.1f}  p99.9 {r['e2e']['p99.9']:.1f} ms; "
             f"hot path p99 {r['hot_path']['p99']:.1f} ms; within budget {r['within_budget']:.2%}; "
             f"fallback {r['fallback_rate']:.2%}",
             "  stage p99 (ms): " + ", ".join(f"{k} {v:.1f}" for k, v in r["stages_p99"].items())]
    lines += [f"  - {a}" for a in r["advice"]]
    return "\n".join(lines)
