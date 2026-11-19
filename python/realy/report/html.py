"""Markdown and self-contained HTML renderings of a run report (inline SVG, no external assets)."""
from __future__ import annotations

import html
import math
from typing import Any

from .report import STEP_LABEL, STEP_ORDER, _f


def render_markdown(rep: dict[str, Any]) -> str:
    s = rep["summary"]
    out = [f"# {rep['run_id']} — {s['verdict']}", "",
           f"Topology `{rep['topology']}` · scenario `{rep['scenario']}` · budget e2e p99 ≤ {rep['budget']['e2e_p99_ms']} ms, "
           f"hot path p99 ≤ {rep['budget']['hot_path_p99_ms']} ms", ""]
    if s["reasons"]:
        out += ["**Why it failed:**", *[f"- {r}" for r in s["reasons"]], ""]
    out += ["| metric | value |", "|---|---|",
            f"| events emitted / decided | {s['emitted']:,} / {s['decided']:,} |",
            f"| missing / duplicates / unknown | {s['missing']} / {s['duplicates']} / {s['unknown']} |",
            f"| achieved TPS (measured window) | {s['achieved_emit_tps']:,.0f} |",
            f"| e2e p50 / p99 / p99.9 | {_f(s['e2e_p50_ms'])} / {_f(s['e2e_p99_ms'])} / {_f(s['e2e_p999_ms'])} ms |",
            f"| hot-path p99 | {_f(s['hot_path_p99_ms'])} ms |",
            f"| within e2e budget | {s['within_budget']:.2%} |",
            f"| on-time publishing p99 (sent − intended) | {_f(s['schedule_lag_p99_ms'])} ms |",
            f"| degraded (fallback) decisions | {s['degraded_fraction']:.2%} |",
            f"| online-store update lag p99 (Flink pickup -> Redis) | {_f(s.get('online_store_update_lag_p99_ms'))} ms |",
            f"| outcomes | {s['outcomes']} |", "",
            "## Latency by step (ms)", "", "| step | p50 | p90 | p99 | p99.9 | max |", "|---|---|---|---|---|---|"]
    for k in STEP_ORDER:
        st = rep["stages"].get(k)
        if st and st.get("count"):
            out.append(f"| {STEP_LABEL.get(k, k)} | {st['p50']:.2f} | {st['p90']:.2f} | {st['p99']:.2f} | {st['p999']:.2f} | {st['max']:.2f} |")
    if rep.get("levels"):
        out += ["", "## Latency vs throughput", "",
                "| target TPS | emitted | decided | e2e p50 | e2e p99 | hot p99 | feature p99 | model p99 | publish lag p99 | within budget |",
                "|---|---|---|---|---|---|---|---|---|---|"]
        for lv in rep["levels"]:
            st = lv["stages"]
            g = lambda k, q="p99": _f(st.get(k, {}).get(q))  # noqa: E731
            out.append(f"| {lv['target_tps']:.0f} | {lv['emitted_tps']:.0f} | {lv['decided_tps']:.0f} | {g('e2e', 'p50')} | {g('e2e')} | "
                       f"{g('hot_path')} | {g('feature_lookup')} | {g('model_call')} | {g('schedule_lag')} | "
                       f"{_f(lv.get('within_budget'), 4)} |")
    if s.get("knee"):
        out += ["", f"**Knee:** {s['knee']}"]
    t = rep.get("tail_attribution")
    if t:
        out += ["", f"## Tail attribution (events ≥ p99 = {t['threshold_ms']:.1f} ms)", "",
                "| stage | mean ms | share |", "|---|---|---|"]
        for k, v in sorted(t["mean_ms_by_stage"].items(), key=lambda x: -x[1]):
            out.append(f"| {k} | {v:.2f} | {t['share'][k]:.0%} |")
    det = rep.get("detection")
    if det and det.get("fraud_events"):
        out += ["", "## Detection (ground truth)", "",
                f"recall (review+decline) {_f(det['recall_flagged'])}, recall (decline) {_f(det['recall_declined'])}, "
                f"precision {_f(det['precision_flagged'])}, FPR {_f(det['false_positive_rate'], 4)}, "
                f"fraud $ approved {det['fraud_usd_approved']:,.0f} of {det['fraud_usd_total']:,.0f}", "",
                "| typology | events | campaigns | recall flagged | recall declined | approved before 1st decline | $ approved |",
                "|---|---|---|---|---|---|---|"]
        for k, v in det["by_type"].items():
            out.append(f"| {k} | {v['events']} | {v['campaigns']} | {v['recall_flagged']:.2f} | {v['recall_declined']:.2f} | "
                       f"{v['approved_before_first_decline_mean']:.1f} | {v['usd_approved']:,.0f} |")
    if det and det.get("by_rule"):
        out += ["", "## Rules and score thresholds (which ones flag legitimate traffic)", "",
                "| rule / reason | hits | fraud | legit | precision | legit hit rate |", "|---|---|---|---|---|---|"]
        for k, v in det["by_rule"].items():
            out.append(f"| {k} | {v['hits']} | {v['fraud']} | {v['legit']} | {v['precision']:.2f} | {v['legit_hit_rate']:.4f} |")
    svc = rep.get("service", {}).get("stages")
    if svc:
        out += ["", "## Service-side live stats (median of 1 s windows)", "", "| source.stage | p50 | p99 | worst 1 s p99 |", "|---|---|---|---|"]
        for k, v in sorted(svc.items()):
            out.append(f"| {k} | {v['p50_median_ms']:.2f} | {v['p99_median_ms']:.2f} | {v['p99_max_ms']:.2f} |")
    sus = rep.get("sustainability")
    if sus:
        out += ["", f"Sustainability: {'sustainable' if sus.get('sustainable') else 'NOT sustainable'} "
                f"(p99 slope {sus['p99_slope_ms_per_window']:.3f} ms/window, max backlog {sus['max_backlog']:,})"]
    return "\n".join(out) + "\n"


def _svg_series(series: dict[str, list[float | None]], width: int = 760, height: int = 200, y_label: str = "",
                hline: float | None = None) -> str:
    vals = [v for s in series.values() for v in s if v is not None and not math.isnan(v)]
    if not vals:
        return ""
    n = max(len(s) for s in series.values())
    ymax = max(vals + ([hline] if hline else [])) * 1.1 or 1
    pad_l, pad_b, pad_t = 48, 22, 10
    w, h = width - pad_l - 10, height - pad_b - pad_t
    colors = ["var(--c1)", "var(--c2)", "var(--c3)", "var(--c4)"]
    parts = [f'<svg viewBox="0 0 {width} {height}" class="chart" role="img" aria-label="{html.escape(y_label)}">']
    for i in range(5):
        y = pad_t + h - h * i / 4
        parts.append(f'<line x1="{pad_l}" x2="{pad_l + w}" y1="{y:.1f}" y2="{y:.1f}" class="grid"/>'
                     f'<text x="{pad_l - 6}" y="{y + 4:.1f}" class="tick" text-anchor="end">{ymax * i / 4:.0f}</text>')
    for i in range(0, n, max(1, n // 8)):
        x = pad_l + w * i / max(1, n - 1)
        parts.append(f'<text x="{x:.1f}" y="{height - 6}" class="tick" text-anchor="middle">{i}s</text>')
    if hline:
        y = pad_t + h - h * hline / ymax
        parts.append(f'<line x1="{pad_l}" x2="{pad_l + w}" y1="{y:.1f}" y2="{y:.1f}" class="budget"/>'
                     f'<text x="{pad_l + w}" y="{y - 4:.1f}" class="tick" text-anchor="end">budget {hline:.0f}</text>')
    for ci, (name, s) in enumerate(series.items()):
        pts = [f"{pad_l + w * i / max(1, n - 1):.1f},{pad_t + h - h * v / ymax:.1f}" for i, v in enumerate(s) if v is not None]
        if pts:
            parts.append(f'<polyline fill="none" stroke="{colors[ci % 4]}" stroke-width="1.6" points="{" ".join(pts)}"/>')
    parts.append("</svg>")
    legend = " ".join(f'<span class="lg" style="--lc:{colors[i % 4]}">{html.escape(k)}</span>' for i, k in enumerate(series))
    return f'<div class="legend">{legend} <span class="unit">{html.escape(y_label)}</span></div>' + "".join(parts)


def render_html(rep: dict[str, Any]) -> str:
    s = rep["summary"]
    md = render_markdown(rep)
    body = []
    for line in md.splitlines():
        if line.startswith("# "):
            body.append(f"<h1>{html.escape(line[2:])}</h1>")
        elif line.startswith("## "):
            body.append(f"<h2>{html.escape(line[3:])}</h2>")
        elif line.startswith("|---"):
            continue
        elif line.startswith("| "):
            cells = [html.escape(c.strip()) for c in line.strip("|").split("|")]
            tag = "th" if not body or not body[-1].startswith("<tr") else "td"
            body.append("<tr>" + "".join(f"<{tag}>{c}</{tag}>" for c in cells) + "</tr>")
        elif line.startswith("- "):
            body.append(f"<li>{html.escape(line[2:])}</li>")
        elif line.strip():
            body.append(f"<p>{html.escape(line).replace('**', '')}</p>")
    # group table rows
    out, in_table = [], False
    for b in body:
        if b.startswith("<tr"):
            if not in_table:
                out.append("<div class='tw'><table>")
                in_table = True
        elif in_table:
            out.append("</table></div>")
            in_table = False
        out.append(b)
    if in_table:
        out.append("</table></div>")
    charts = []
    if rep.get("e2e_p99_by_second"):
        charts.append("<h2>End-to-end latency per second (by intended time)</h2>" + _svg_series(
            {"p99": rep["e2e_p99_by_second"], "p50": rep["e2e_p50_by_second"]}, y_label="ms",
            hline=rep["budget"]["e2e_p99_ms"]))
    f = rep.get("funnel", {})
    if f:
        charts.append("<h2>Throughput funnel (events/s)</h2>" + _svg_series(
            {k: f[k] for k in ("emitted", "acked", "decided", "collected") if k in f}, y_label="events/s"))
    if rep.get("backlog"):
        charts.append("<h2>Backlog (emitted − collected)</h2>" + _svg_series({"backlog": rep["backlog"]}, y_label="events"))
    badge = "pass" if s["verdict"] == "PASS" else "fail"
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>{html.escape(rep['run_id'])}</title>
<style>
:root{{--bg:#fbfbfa;--fg:#1d1d1b;--muted:#6b6b66;--line:#e4e3df;--c1:#2f6fdb;--c2:#d9822b;--c3:#2e9d6b;--c4:#9b4dca;--bad:#c2392f;--good:#2e9d6b}}
@media (prefers-color-scheme: dark){{:root{{--bg:#151514;--fg:#ecebe7;--muted:#9a9993;--line:#2c2c2a;--c1:#6d9ef2;--c2:#f0a35b;--c3:#5cc496;--c4:#c08ae6}}}}
body{{background:var(--bg);color:var(--fg);font:14px/1.5 ui-sans-serif,system-ui,sans-serif;max-width:1000px;margin:0 auto;padding:16px}}
h1{{font-size:22px}} h2{{font-size:16px;margin-top:28px}} .tw{{overflow-x:auto}}
table{{border-collapse:collapse;font-variant-numeric:tabular-nums;width:100%}} th,td{{border-bottom:1px solid var(--line);padding:4px 8px;text-align:left;white-space:nowrap}}
th{{color:var(--muted);font-weight:600}} .badge{{display:inline-block;padding:2px 10px;border-radius:12px;color:#fff;font-weight:700}}
.pass{{background:var(--good)}} .fail{{background:var(--bad)}} .chart{{width:100%;height:auto}} .grid{{stroke:var(--line)}}
.tick{{fill:var(--muted);font-size:10px}} .budget{{stroke:var(--bad);stroke-dasharray:4 3}} .legend{{font-size:12px;color:var(--muted)}}
.lg::before{{content:"";display:inline-block;width:10px;height:3px;background:var(--lc);margin-right:4px;vertical-align:middle}} .lg{{margin-right:12px}}
</style></head><body><span class="badge {badge}">{s['verdict']}</span>
{''.join(out)}{''.join(charts)}</body></html>"""
