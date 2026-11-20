"""Why a decision came out the way it did, and why the event is (or is not) labelled fraud.

Offline and read-only: everything is recomputed from what the run recorded (the served feature
vector, the rule set and model the run used, the event log), so the pipeline is unchanged and the
explanation matches what was served.

  * policy path:  which branch of the precedence decided (DECLINE rule > ALLOW > score >= decline >
                  REVIEW rule > score >= review > approve), in words;
  * rules:        every rule re-evaluated with the run's CEL, with the values of the features and
                  event fields its condition references;
  * attribution:  integrated gradients of the served model's logit, from an "average customer"
                  baseline (the training mean) to this event: which features pushed the score up or
                  down, and by how much (attributions sum to logit(x) - logit(baseline));
  * label story:  ground truth (typology, campaign role and step), and what the bank learns and when
                  (label source, delay, and whether a training set cut N days after the window sees it).
"""
from __future__ import annotations

import math
import re
from typing import Any

import numpy as np

IDENT = re.compile(r"\b(f|event)\.([a-zA-Z_][a-zA-Z0-9_]*)")
LABEL_SOURCE = {1: "chargeback", 2: "customer report", 3: "investigation"}
DAY_US = 86_400_000_000


# ------------------------------------------------------------------------------------- model
def _forward(scorer: Any, z: np.ndarray) -> tuple[np.ndarray, list[np.ndarray]]:
    """Logit (pre-sigmoid output) for standardised inputs z [n, d] and the ReLU masks per hidden layer."""
    h, masks = z.astype(np.float64), []
    layers = scorer.layers
    for li, (W, b, act) in enumerate(layers):
        h = h @ W.T.astype(np.float64) + b.astype(np.float64)
        if li == len(layers) - 1:
            break                       # last layer: stop before its activation (the logit)
        if act == "relu":
            masks.append(h > 0)
            h = np.maximum(h, 0)
        elif act == "tanh":
            masks.append(1.0 - np.tanh(h) ** 2)
            h = np.tanh(h)
        else:
            masks.append(np.ones_like(h))
    return h[:, 0], masks


def _grad(scorer: Any, z: np.ndarray) -> np.ndarray:
    """d logit / d z for each row of z (manual backprop through the MLP)."""
    _, masks = _forward(scorer, z)
    layers = scorer.layers
    g = np.repeat(layers[-1][0][0:1].astype(np.float64), z.shape[0], axis=0)   # [n, units of last hidden]
    for li in range(len(layers) - 2, -1, -1):
        g = g * masks[li]
        g = g @ layers[li][0].astype(np.float64)
    return g


def integrated_gradients(scorer: Any, x: np.ndarray, steps: int = 64) -> dict[str, Any]:
    """Attribution of the model's logit to each input feature, baseline = the training mean."""
    z = ((np.asarray(x, np.float64) - scorer.mean) / scorer.std)[None, :]
    z0 = np.zeros_like(z)
    alphas = (np.arange(steps, dtype=np.float64) + 0.5) / steps
    path = z0 + alphas[:, None] * (z - z0)
    avg_grad = _grad(scorer, path).mean(axis=0)
    attr = (z - z0)[0] * avg_grad
    lx, _ = _forward(scorer, z)
    lb, _ = _forward(scorer, z0)
    return {"attribution": attr, "logit": float(lx[0]), "baseline_logit": float(lb[0]),
            "score": 1.0 / (1.0 + math.exp(-float(lx[0]))), "baseline_score": 1.0 / (1.0 + math.exp(-float(lb[0])))}


def top_attributions(ig: dict[str, Any], names: list[str], values: np.ndarray, baseline: np.ndarray,
                     k: int = 8) -> list[dict[str, Any]]:
    a = ig["attribution"]
    order = np.argsort(-np.abs(a))[:k]
    return [{"feature": names[i], "value": float(values[i]), "typical": float(baseline[i]),
             "contribution": float(a[i]), "direction": "towards fraud" if a[i] > 0 else "away from fraud"}
            for i in order if abs(a[i]) > 1e-6]


# ------------------------------------------------------------------------------------- rules
def explain_rules(rules_yaml: str, fmap: dict[str, float], ev_view: dict[str, Any]) -> list[dict[str, Any]]:
    import cel
    import yaml
    doc = yaml.safe_load(rules_yaml)
    ctx = cel.Context()
    ctx.add_variable("f", fmap)
    ctx.add_variable("event", ev_view)
    out = []
    for r in doc.get("rules", []):
        try:
            hit = bool(cel.compile(r["when"]).execute(ctx))
        except Exception as e:  # noqa: BLE001
            hit, err = False, str(e)
        else:
            err = None
        refs = {}
        for scope, name in IDENT.findall(r["when"]):
            src = fmap if scope == "f" else ev_view
            if name in src:
                refs[f"{scope}.{name}"] = src[name]
        out.append({"id": r["id"], "action": r.get("action", "REVIEW"), "when": r["when"], "hit": hit,
                    "reason": r.get("reason", ""), "values": refs, **({"error": err} if err else {})})
    return out


def policy_path(rule_results: list[dict[str, Any]], score: float | None, review: float, decline: float,
                outcome: str | None, path: str | None) -> dict[str, Any]:
    """The precedence branch that decided, mirroring RuleSet.decide (Java) / verify.Rules.decide."""
    hits = [r for r in rule_results if r["hit"]]
    by = lambda a: [r["id"] for r in hits if r["action"] == a]  # noqa: E731
    has_score = score is not None and not (isinstance(score, float) and math.isnan(score))
    if by("DECLINE"):
        branch, why = "decline rule", f"DECLINE rule {', '.join(by('DECLINE'))} fired: DECLINE rules win over everything"
    elif by("ALLOW") or by("APPROVE"):
        branch, why = "allow rule", f"ALLOW rule {', '.join(by('ALLOW') + by('APPROVE'))} fired"
    elif has_score and score >= decline:
        branch, why = "score decline", f"model score {score:.3f} >= decline threshold {decline:.2f}"
    elif by("REVIEW"):
        branch, why = "review rule", f"REVIEW rule {', '.join(by('REVIEW'))} fired" + (
            f" (score {score:.3f} below the decline threshold {decline:.2f})" if has_score else "")
    elif has_score and score >= review:
        branch, why = "score review", f"model score {score:.3f} >= review threshold {review:.2f}"
    else:
        branch = "approve"
        why = "no rule fired" + (f" and the score {score:.3f} is below the review threshold {review:.2f}" if has_score
                                 else " and there was no model score")
    if not has_score and path and path != "FULL":
        why += f" (decided without the model: {path.lower().replace('_', ' ')})"
    return {"outcome": outcome, "branch": branch, "why": why, "thresholds": {"review": review, "decline": decline}}


# ------------------------------------------------------------------------------------- labels
def label_story(er: dict[str, Any], reg: Any, run_end_us: int, maturity_days: float = 30.0,
                step: tuple[int, int] | None = None) -> dict[str, Any]:
    """Ground truth, what the bank learns and when, and what a training set cut `maturity_days`
    after the run's last event makes of this event."""
    is_fraud = bool(er.get("is_fraud"))
    names = reg.fraud_type_names()
    typ = names.get(int(er.get("fraud_type") or 0), "none")
    cid = int(er.get("campaign_id") or 0)
    role_typ, role = reg.role_name(int(er.get("campaign_role") or 0))
    delay = int(er.get("label_delay_us") if er.get("label_delay_us") is not None else -1)
    lab_fraud = bool(er.get("label_is_fraud"))
    src = LABEL_SOURCE.get(int(er.get("label_source") or 0))
    et = int(er.get("event_time_us") or 0)
    cut = run_end_us + int(maturity_days * DAY_US)
    out: dict[str, Any] = {"truth": "fraud" if is_fraud else "legitimate", "typology": typ if is_fraud else None,
                           "campaign_id": str(cid) if cid else None, "role": role,
                           "role_typology": role_typ, "step": {"index": step[0], "of": step[1]} if step else None,
                           "label_source": src, "label_delay_days": round(delay / DAY_US, 2) if delay >= 0 else None,
                           "maturity_days": maturity_days}
    if is_fraud:
        title = f"fraud: {typ}" + (f", {role.replace('_', ' ')}" if role else "") + (
            f" ({step[0]} of {step[1]})" if step else "")
    elif cid and role:
        title = f"legitimate, part of a {role_typ} campaign's story ({role.replace('_', ' ')})"
    else:
        title = "legitimate"
    if lab_fraud and delay >= 0:
        days = delay / DAY_US
        known = et + delay <= cut
        if not is_fraud:
            observed = (f"friendly fraud: the customer disputed this genuine payment ({src}, after {days:.1f} days); "
                        "it trains as fraud (label noise)")
            training = "fraud (wrongly)"
        elif known:
            observed = f"reported by {src} after {days:.1f} days"
            training = f"fraud: the label arrives inside a training set cut {maturity_days:g} days after the window"
        else:
            observed = f"reported by {src} after {days:.1f} days"
            training = (f"LEGITIMATE: the label arrives {days:.1f} days after the payment, past a training set cut "
                        f"{maturity_days:g} days after the window")
    elif is_fraud:
        observed = "never reported: the bank never learns this was fraud"
        training = "LEGITIMATE (never labelled)"
    else:
        observed, training = "no label", "legitimate"
    out.update(title=title, observed=observed, training=training)
    return out
