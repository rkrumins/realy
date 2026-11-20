"""Explicit per-component state for the live page, derived by a pure function (no I/O, `now` passed in).

Colouring a component by its probe latency alone cannot tell a component that is *busy with a job step*
(warming) or *just booted* (starting) from one that is slow, so the live page would flash red for no
reason. Each component gets one explicit state instead:

    down      unreachable or failing (the reason says why)
    starting  booting / warming up its process
    warming   busy with a running job step (with that step's progress and label)
    degraded  reachable but impaired: a slow or degraded probe, held for STICKY_S (hysteresis)
    ready     healthy

`sticky` is the only memory: {component_id: last time it was seen degraded}. The caller owns it (one dict
for the process) and passes it in, so tests drive time explicitly.
"""
from __future__ import annotations

from typing import Any

SLOW_MS = 250.0     # a probe slower than this counts as degraded
STICKY_S = 30.0     # ...and the component stays degraded this long after the last bad probe


def fmt_progress(p: dict[str, Any] | None) -> str:
    """Same wording as the UI's fmtProgress: "71% (412,330/583,063 items) · 9,800/s · ETA 17 s"."""
    if not p or not p.get("total"):
        return ""
    pct = min(100.0, 100.0 * (p.get("done") or 0) / max(p["total"], 1))
    out = f"{pct:.0f}% ({p.get('done') or 0:,.0f}/{p['total']:,.0f}{' ' + p['unit'] if p.get('unit') else ''})"
    if p.get("rate"):
        out += f" · {p['rate']:,.0f}/s"
    if p.get("eta_s") is not None:
        out += f" · ETA {round(p['eta_s'])} s"
    return out


def _progress(step: dict[str, Any]) -> float | None:
    p = step.get("progress") or {}
    return max(0.0, min(1.0, (p.get("done") or 0) / p["total"])) if p.get("total") else None


def component_states(components: list[dict[str, Any]], steps: list[dict[str, Any]], now: float,
                     sticky: dict[str, float]) -> dict[str, dict[str, Any]]:
    """components: probe results [{id, status, latency_ms, details}]; steps: the running job's steps."""
    busy = {s["component"]: s for s in steps if s.get("status") == "running" and s.get("component")}
    out: dict[str, dict[str, Any]] = {}
    for c in components:
        cid, details = c["id"], c.get("details") or {}
        lat = c.get("latency_ms")
        if c.get("status") == "down":
            out[cid] = {"state": "down", "reason": details.get("error") or details.get("reason") or "unreachable", "progress": None}
        elif c.get("status") == "starting":
            out[cid] = {"state": "starting", "reason": details.get("reason") or "starting", "progress": None}
        elif cid in busy:
            s = busy[cid]
            out[cid] = {"state": "warming", "reason": f"{s['label']}: {fmt_progress(s.get('progress')) or s.get('detail') or 'running'}",
                        "progress": _progress(s)}
        elif c.get("status") == "degraded" or (lat is not None and lat > SLOW_MS):
            sticky[cid] = now
            why = details.get("reason") or (f"probe took {lat:.0f} ms (> {SLOW_MS:.0f} ms)" if lat and lat > SLOW_MS else "degraded")
            out[cid] = {"state": "degraded", "reason": why, "progress": None}
        elif now - sticky.get(cid, float("-inf")) < STICKY_S:
            out[cid] = {"state": "degraded", "reason": f"recovering: degraded {now - sticky[cid]:.0f} s ago", "progress": None}
        else:
            out[cid] = {"state": "ready", "reason": "", "progress": None}
    return out
