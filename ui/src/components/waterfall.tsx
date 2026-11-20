import { cn } from "@/lib/cn";
import { fmtMs, isNum, titleCase } from "@/lib/format";
import type { DecisionDetail, StageStats } from "@/lib/types";
import { SERIES } from "./charts";
import { Tooltip } from "./ui/tooltip";

/** Hot-path order of the decomposed stages (python/realy/report). Sub-stages of model_call are indented. */
export const STAGE_ORDER: { key: string; label: string; sub?: boolean }[] = [
  { key: "schedule_lag", label: "schedule lag" },
  { key: "produce_ack", label: "produce ack" },
  { key: "ingress", label: "ingress transit" },
  { key: "queue_wait", label: "queue wait" },
  { key: "deserialize", label: "deserialize" },
  { key: "shuffle", label: "keyBy shuffles" },
  { key: "state_features", label: "state features" },
  { key: "async_wait", label: "async wait" },
  { key: "feature_lookup", label: "feature lookup" },
  { key: "feature_assemble", label: "feature assemble" },
  { key: "model_call", label: "model call" },
  { key: "model_batch_wait", label: "batch wait", sub: true },
  { key: "model_server_queue", label: "server queue", sub: true },
  { key: "model_server_compute", label: "server compute", sub: true },
  { key: "rules", label: "rules" },
  { key: "policy", label: "policy" },
  { key: "audit_ack", label: "audit ack" },
  { key: "egress", label: "egress transit" },
];

/**
 * Latency by step: each stage's p50 bar starts where the previous stage's p50 ended (sub-stages sit
 * inside their parent); the whisker reaches the stage's p99. Totals (hot path, e2e) underneath.
 */
export function StepWaterfall({ stages, budget }: { stages: Record<string, StageStats>; budget?: number | null }) {
  const rows: { key: string; label: string; sub?: boolean; start: number; p50: number; p99: number; p999: number; count: number }[] = [];
  let t = 0;
  let parentStart = 0;
  for (const s of STAGE_ORDER) {
    const st = stages[s.key];
    if (!st || !st.count) continue;
    if (s.sub) {
      rows.push({ ...s, start: parentStart, p50: st.p50, p99: st.p99, p999: st.p999, count: st.count });
      parentStart += st.p50;
      continue;
    }
    rows.push({ ...s, start: t, p50: st.p50, p99: st.p99, p999: st.p999, count: st.count });
    parentStart = t;
    t += st.p50;
  }
  const e2e = stages.e2e;
  const hot = stages.hot_path;
  const max = Math.max(t, ...rows.map((r) => r.start + r.p99), e2e?.p99 ?? 0, budget ?? 0) * 1.04 || 1;
  const pct = (v: number) => `${(v / max) * 100}%`;
  if (!rows.length) return <div className="py-8 text-center text-[13px] text-subtle">No per-stage timings in this run.</div>;

  const Row = ({ label, start, p50, p99, sub, total, color }: { label: string; start: number; p50: number; p99: number; sub?: boolean; total?: boolean; color: string }) => (
    <div className={cn("grid grid-cols-[88px_minmax(0,1fr)_104px] sm:grid-cols-[132px_minmax(0,1fr)_150px] items-center gap-3 py-[5px] text-[12.5px]", total && "border-t border-border pt-2.5 mt-1")}>
      <div className={cn("truncate", sub ? "pl-4 text-subtle" : total ? "font-medium text-fg" : "text-muted")}>{label}</div>
      <Tooltip content={<span className="num">p50 {fmtMs(p50)} · p99 {fmtMs(p99)} · starts at {fmtMs(start)}</span>}>
        <div className="relative h-4">
          <div className="absolute top-1/2 h-px -translate-y-1/2 bg-[var(--fg-subtle)] opacity-60" style={{ left: pct(start + p50), width: pct(Math.max(0, p99 - p50)) }} />
          <div className="absolute top-1/2 h-2 w-px -translate-y-1/2 bg-[var(--fg-subtle)]" style={{ left: pct(start + p99) }} />
          <div className={cn("absolute top-0 h-4 rounded-[4px]", sub && "top-1 h-2 opacity-70")}
            style={{ left: pct(start), width: `max(2px, ${pct(p50)})`, background: color }} />
        </div>
      </Tooltip>
      <div className="whitespace-nowrap text-right font-mono text-[10.5px] text-muted sm:text-[11.5px]">{fmtMs(p50)} <span className="text-subtle">/ {fmtMs(p99)}</span></div>
    </div>
  );

  return (
    <div className="relative">
      {isNum(budget) && (
        <div className="pointer-events-none absolute inset-y-0 left-[calc(88px+12px)] right-[calc(104px+12px)] sm:left-[calc(132px+12px)] sm:right-[calc(150px+12px)]">
          <div className="absolute inset-y-0 border-l border-dashed border-fail/70" style={{ left: pct(budget) }}>
            <span className="absolute -top-0.5 ml-1 whitespace-nowrap text-[10.5px] text-fail">budget {fmtMs(budget)}</span>
          </div>
        </div>
      )}
      <div className="mb-1 grid grid-cols-[88px_minmax(0,1fr)_104px] sm:grid-cols-[132px_minmax(0,1fr)_150px] gap-3 pt-4 text-[10.5px] uppercase tracking-wider text-subtle">
        <span>stage</span><span className="truncate">p50 bar · p99 whisker</span><span className="text-right">p50 / p99</span>
      </div>
      {rows.map((r, i) => <Row key={r.key} label={r.label} start={r.start} p50={r.p50} p99={r.p99} sub={r.sub} color={SERIES[i % 8]} />)}
      {hot && <Row label="hot path" start={0} p50={hot.p50} p99={hot.p99} total color="var(--accent)" />}
      {e2e && <Row label="end to end" start={0} p50={e2e.p50} p99={e2e.p99} total color="var(--fg-muted)" />}
    </div>
  );
}

/** One decision's spans, positioned in time and coloured by clock domain (host/process). */
export function SpanWaterfall({ detail }: { detail: DecisionDetail }) {
  const spans = detail.spans ?? [];
  if (!spans.length) return <div className="py-6 text-center text-[13px] text-subtle">No span data recorded for this decision.</div>;
  const timed = spans.every((s) => isNum(s.start_us));
  let cursor = 0;
  const laid = spans.map((s) => {
    const start = timed ? (s.start_us as number) : cursor;
    if (!timed) cursor += s.dur_us;
    return { ...s, start };
  });
  const t0 = timed ? Math.min(detail.timing.t_intended_us ?? Infinity, ...laid.map((s) => s.start)) : 0;
  const end = timed
    ? Math.max(...laid.map((s) => s.start + s.dur_us), detail.timing.t_collected_us ?? 0, detail.timing.t_response_us ?? 0)
    : cursor;
  const span = Math.max(1, end - t0);
  const domains = [...new Set(laid.map((s) => String(s.domain ?? "?")))];
  const color = (d: number | null) => SERIES[domains.indexOf(String(d ?? "?")) % 8];
  const marks = [
    { k: "intended", v: detail.timing.t_intended_us }, { k: "sent", v: detail.timing.t_sent_us },
    { k: "decided", v: detail.timing.t_decision_us }, { k: "collected", v: detail.timing.t_collected_us ?? detail.timing.t_response_us },
  ].filter((m) => timed && isNum(m.v)) as { k: string; v: number }[];
  // merge markers that would print on top of each other ("intended · sent")
  const merged: { k: string; v: number }[] = [];
  for (const m of [...marks].sort((a, b) => a.v - b.v)) {
    const last = merged[merged.length - 1];
    if (last && (m.v - last.v) / span < 0.07) last.k = `${last.k} · ${m.k}`;
    else merged.push({ ...m });
  }

  return (
    <div>
      <div className="mb-3 flex flex-wrap items-center gap-x-4 gap-y-1 text-[11.5px] text-muted">
        {domains.map((d, i) => (
          <span key={d} className="inline-flex items-center gap-1.5" title={`clock domain id ${d}`}>
            <span className="size-2 rounded-sm" style={{ background: SERIES[i % 8] }} />{d === "?" ? "unknown clock domain" : `clock domain ${i + 1}`}
          </span>
        ))}
        {!timed && <span className="text-subtle">(durations only: this run predates full span capture)</span>}
      </div>
      <div className="relative">
        {merged.map((m) => (
          <div key={m.k} className="pointer-events-none absolute inset-y-0 z-10 border-l border-dashed border-border-strong" style={{ left: `calc(150px + (100% - 150px - 80px) * ${(m.v - t0) / span})` }}>
            <span className="absolute -top-4 -translate-x-1/2 whitespace-nowrap text-[10px] text-subtle">{m.k}</span>
          </div>
        ))}
        <div className="space-y-1 pt-4">
          {laid.map((s, i) => (
            <div key={i} className="grid grid-cols-[150px_1fr_80px] items-center gap-0 text-[12px]">
              <div className="truncate pr-3 text-muted" title={s.stage_name}>
                {titleCase(s.stage_name.toLowerCase())}{s.entity ? <span className="text-subtle"> · {s.entity}</span> : null}
              </div>
              <Tooltip content={<span className="num">{s.stage_name} · {fmtMs(s.dur_us / 1000)} · domain {domains.indexOf(String(s.domain ?? "?")) + 1}{timed ? ` · +${fmtMs((s.start - t0) / 1000)}` : ""}</span>}>
                <div className="relative h-3.5 rounded bg-surface-2">
                  <div className="absolute top-0 h-3.5 rounded" style={{
                    left: `${((s.start - t0) / span) * 100}%`, width: `max(3px, ${(s.dur_us / span) * 100}%)`, background: color(s.domain),
                  }} />
                </div>
              </Tooltip>
              <div className="text-right font-mono text-[11px] text-muted">{fmtMs(s.dur_us / 1000)}</div>
            </div>
          ))}
        </div>
      </div>
      <div className="mt-3 flex justify-between pl-[150px] pr-[80px] font-mono text-[10.5px] text-subtle">
        <span>0</span><span>{fmtMs(span / 2000)}</span><span>{fmtMs(span / 1000)}</span>
      </div>
    </div>
  );
}

