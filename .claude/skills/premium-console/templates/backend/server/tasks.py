"""Job kinds: a registry the UI reads (GET /api/kinds) to build its "new job" form, plus the demo job.

Replace `demo` with real work. Keep the contract: declare steps up front, report progress and one metric
sample per second, honour `handle.wait()` / `handle.aborted`, return a result dict with numbers + insights.
"""
from __future__ import annotations

import math
import random
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable

from .worker import JobHandle


@dataclass
class Param:
    name: str
    label: str
    default: float
    unit: str | None = None
    min: float | None = None
    max: float | None = None
    step: float | None = None
    hint: str | None = None


@dataclass
class Task:
    kind: str
    label: str
    description: str
    params: list[Param]
    fn: Callable[[dict[str, Any], JobHandle], dict[str, Any] | None] = field(repr=False)

    def public(self) -> dict[str, Any]:
        return {"kind": self.kind, "label": self.label, "description": self.description,
                "params": [{k: v for k, v in asdict(p).items() if v is not None} for p in self.params]}

    def validate(self, params: dict[str, Any]) -> dict[str, float]:
        """Unknown names and out-of-range values are rejected with a message the UI can show as is."""
        spec = {p.name: p for p in self.params}
        unknown = sorted(set(params) - set(spec))
        if unknown:
            raise ValueError(f"unknown parameter(s): {', '.join(unknown)}")
        out: dict[str, float] = {}
        for name, p in spec.items():
            v = params.get(name, p.default)
            if not isinstance(v, (int, float)) or isinstance(v, bool) or not math.isfinite(v):
                raise ValueError(f"{name}: must be a number")
            if p.min is not None and v < p.min:
                raise ValueError(f"{name}: must be >= {p.min:g}")
            if p.max is not None and v > p.max:
                raise ValueError(f"{name}: must be <= {p.max:g}")
            out[name] = v
        return out


# ---------------------------------------------------------------------------------- insights
SEVERITY_ORDER = {"critical": 0, "warning": 1, "info": 2, "good": 3}


def rank_insights(result: dict[str, Any], steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Pure: turn a result into ranked findings (most severe first), each with evidence and a next step."""
    out: list[dict[str, Any]] = []
    wb, budget = result.get("within_budget"), result.get("budget_ms")
    if wb is not None and budget:
        if wb < 0.9:
            out.append({"severity": "critical", "area": "latency", "anchor": "timeline",
                        "title": f"p99 over the {budget:g} ms budget in {100 * (1 - wb):.0f}% of seconds",
                        "detail": f"Worst second p99 {result.get('p99_max_ms') or 0:.0f} ms; median p50 {result.get('p50_ms') or 0:.0f} ms.",
                        "action": "Open the latency chart and find when the slow spell started; compare with the log."})
        elif wb < 0.99:
            out.append({"severity": "warning", "area": "latency", "anchor": "timeline",
                        "title": f"p99 over budget in {100 * (1 - wb):.1f}% of seconds",
                        "detail": "Short slow spells; the job mostly met its budget.",
                        "action": "Check whether the spells line up with a step boundary."})
        else:
            out.append({"severity": "good", "area": "latency", "anchor": "timeline",
                        "title": f"p99 within the {budget:g} ms budget for {100 * wb:.1f}% of the job",
                        "detail": f"p50 {result.get('p50_ms') or 0:.0f} ms, p99 {result.get('p99_ms') or 0:.0f} ms."})
    failed, processed = result.get("failed") or 0, result.get("processed") or 0
    if failed:
        share = failed / max(1, processed + failed)
        out.append({"severity": "critical" if share > 0.05 else "warning", "area": "errors", "anchor": "log",
                    "title": f"{failed:,} item(s) failed ({100 * share:.2f}%)",
                    "detail": "Failed items are counted, not retried.",
                    "action": "Read the job log for the first error, then re-run with the same parameters."})
    timed = [s for s in steps if s.get("duration_s")]
    if timed:
        total = sum(s["duration_s"] for s in timed)
        slow = max(timed, key=lambda s: s["duration_s"])
        out.append({"severity": "info", "area": "steps", "anchor": "steps",
                    "title": f"Slowest step: {slow['label']} ({slow['duration_s']:.1f} s, {100 * slow['duration_s'] / max(total, 1e-9):.0f}% of the job)",
                    "detail": f"{len(timed)} steps took {total:.1f} s in total."})
    return sorted(out, key=lambda i: SEVERITY_ORDER[i["severity"]])


# ---------------------------------------------------------------------------------- demo job
def _pct(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    return s[min(len(s) - 1, int(q * len(s)))]


def demo(job: dict[str, Any], h: JobHandle) -> dict[str, Any]:
    """Processes synthetic items at a target rate, with an optional slow spell midway (to show warn/hot)."""
    p = job["params"]
    items, rate, budget = int(p["items"]), float(p["rate"]), float(p["budget_ms"])
    incident, tick = bool(p.get("incident", 0)), float(p.get("tick_s", 1.0))  # tick_s < 1 speeds tests up
    rng = random.Random(job["id"])
    h.budget_ms = budget
    h.planned_s = round(items / rate + 1.8 * tick, 1)
    h.define_steps([("prepare", "Prepare inputs", "store"), ("process", "Process items", "worker"),
                    ("verify", "Verify results", None), ("publish", "Publish report", "store")])

    h.step("prepare", "running", detail="validating parameters and reserving storage")
    h.log(f"prepare: {items:,} items at {rate:g}/s, p99 budget {budget:g} ms")
    if h.wait(1.0 * tick):
        return {}
    h.step("prepare", "done", detail="")

    h.phase = "RUN"
    h.step("process", "running")
    t0, done, failed = time.time(), 0, 0
    series: list[dict[str, Any]] = []
    while done + failed < items and not h.aborted:
        n = min(items - done - failed, max(1, round(rate * tick * rng.uniform(0.9, 1.1))))
        slow = incident and 0.55 <= (done + failed) / items < 0.7
        base = budget * (0.25 if not slow else 0.9)
        lat = [rng.lognormvariate(math.log(base), 0.35 if not slow else 0.5) for _ in range(min(n, 400))]
        errs = sum(1 for _ in range(n) if rng.random() < (0.002 if not slow else 0.01))
        done, failed = done + n - errs, failed + errs
        sample = {"throughput": round(n / max(tick, 1e-6), 1), "p50_ms": round(_pct(lat, 0.5) or 0, 2),
                  "p99_ms": round(_pct(lat, 0.99) or 0, 2), "errors": round(errs / max(tick, 1e-6), 2)}
        h.metric(**sample)
        series.append({"t": round(time.time() - t0, 1), "throughput": sample["throughput"], "p50_ms": sample["p50_ms"],
                       "p99_ms": sample["p99_ms"]})
        h.step("process", done=done + failed, total=items, unit="items")
        if slow and errs:
            h.log(f"warn: {errs} item(s) failed during a slow spell (p99 {sample['p99_ms']:.0f} ms)")
        if h.wait(tick):
            break
    h.step("process", "done" if not h.aborted else "warn", detail="aborted: partial result" if h.aborted else "")

    if not h.aborted:
        h.phase = "VERIFY"
        h.step("verify", "running", detail="recounting outputs")
        h.wait(0.5 * tick)
        h.step("verify", "done", detail="")
        h.step("publish", "running")
        h.wait(0.3 * tick)
        h.step("publish", "done")
    else:
        h.step("verify", "skipped")
        h.step("publish", "skipped")

    p99s = [s["p99_ms"] for s in series]
    elapsed = max(1e-6, (series[-1]["t"] if series else 0) + tick)
    result: dict[str, Any] = {
        "processed": done, "failed": failed, "throughput": round((done + failed) / elapsed, 1),
        "p50_ms": _pct([s["p50_ms"] for s in series], 0.5), "p99_ms": _pct(p99s, 0.99), "p99_max_ms": max(p99s, default=None),
        "budget_ms": budget, "within_budget": round(sum(1 for v in p99s if v <= budget) / len(p99s), 4) if p99s else None,
        "series": series,
    }
    result["insights"] = rank_insights(result, h.steps())
    h.log(f"complete: {done:,} processed, {failed:,} failed" + (" (aborted)" if h.aborted else ""))
    return result


TASKS: dict[str, Task] = {t.kind: t for t in [
    Task("demo", "Demo pipeline", "Synthetic items at a target rate, with a slow spell midway.", [
        Param("items", "Items", 3000, unit="items", min=10, max=10_000_000, step=100),
        Param("rate", "Target rate", 120, unit="/s", min=1, max=100_000, step=10),
        Param("budget_ms", "p99 budget", 120, unit="ms", min=1, max=60_000, step=5),
        Param("incident", "Slow spell", 1, min=0, max=1, step=1, hint="1 = inject one midway, 0 = none"),
        Param("tick_s", "Tick", 1, unit="s", min=0.01, max=10, step=0.5, hint="seconds per sample"),
    ], demo),
    Task("smoke", "Smoke check", "A short, clean run: 20 seconds at 100 items/s, no slow spell.", [
        Param("items", "Items", 2000, unit="items", min=10, max=100_000, step=100),
        Param("rate", "Target rate", 100, unit="/s", min=1, max=100_000, step=10),
        Param("budget_ms", "p99 budget", 120, unit="ms", min=1, max=60_000, step=5),
        Param("incident", "Slow spell", 0, min=0, max=1, step=1),
        Param("tick_s", "Tick", 1, unit="s", min=0.01, max=10, step=0.5),
    ], demo),
]}


def run(job: dict[str, Any], handle: JobHandle) -> dict[str, Any] | None:
    """Default `run_fn`: dispatch on the job's kind."""
    task = TASKS.get(job["kind"])
    if task is None:
        raise ValueError(f"no such job kind: {job['kind']}")
    return task.fn(job, handle)
