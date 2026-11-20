/** Builder editors whose previews mirror the engine: LatencyDist (Java LatencyModel) and RateProfile.rate_at. */
import { Plus, Trash2 } from "lucide-react";
import * as React from "react";
import { cn } from "@/lib/cn";
import { defaultDist, distKind, preview, type DistKind, type LatencyDist } from "@/lib/dist";
import { fmtCompact, fmtDuration, fmtMs, fmtPct, isNum } from "@/lib/format";
import { rateCurve, type RateProfile } from "@/lib/rate";
import { DistChart, RateChart } from "./charts";
import { Segmented } from "./ui/controls";
import { Input, NumberField } from "./ui/input";

const KINDS: { value: DistKind; label: string }[] = [
  { value: "none", label: "none" }, { value: "fixed", label: "fixed" }, { value: "uniform", label: "uniform" },
  { value: "normal", label: "normal" }, { value: "lognormal", label: "lognormal" }, { value: "bimodal", label: "bimodal" },
  { value: "empirical", label: "empirical" },
];

type Params = Record<string, number | number[] | undefined>;

function Field({ label, value, onChange, unit = "ms", step = 1, hint }: {
  label: string; value: number | undefined; onChange: (v: number | undefined) => void; unit?: string; step?: number; hint?: string;
}) {
  return <NumberField label={label} value={value} onChange={onChange} unit={unit} step={step} hint={hint} min={0} />;
}

/** Edit one proto3-JSON LatencyDist with a live percentile + histogram preview. */
export function DistributionEditor({ value, onChange, title, allowNone = true, compact }: {
  value: LatencyDist | null | undefined; onChange: (v: LatencyDist | null) => void; title?: React.ReactNode; allowNone?: boolean; compact?: boolean;
}) {
  const kind = distKind(value);
  const pv = React.useMemo(() => preview(value, 12000), [value]);
  const params = (kind !== "none" && value ? (value[kind as Exclude<DistKind, "none">] as Params) : {}) || {};
  const set = (k: string, v: number | number[] | undefined) => {
    if (kind === "none" || !value) return;
    onChange({ ...value, [kind]: { ...params, [k]: v } });
  };
  const setTop = (k: "error_rate" | "timeout_rate", v: number | undefined) => {
    if (!value) return;
    const next = { ...value };
    if (v) next[k] = v;
    else delete next[k];
    onChange(next);
  };
  const p = params as Record<string, number | undefined>;

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        {title && <div className="text-[13px] font-medium text-fg">{title}</div>}
        <Segmented size="sm" value={kind} onChange={(k) => onChange(defaultDist(k, value))}
          options={KINDS.filter((k) => allowNone || k.value !== "none")} className="max-w-full overflow-x-auto" />
      </div>
      {kind === "none" ? (
        <div className="rounded-xl border border-dashed border-border px-4 py-5 text-center text-[12.5px] text-subtle">No injected latency.</div>
      ) : (
        <div className={cn("grid grid-cols-1 gap-4", !compact && "lg:grid-cols-[minmax(0,1fr)_minmax(0,1.1fr)]")}>
          <div className="grid grid-cols-2 gap-3">
            {kind === "fixed" && <Field label="value" value={p.ms} onChange={(v) => set("ms", v)} />}
            {kind === "uniform" && <>
              <Field label="min" value={p.min_ms} onChange={(v) => set("min_ms", v)} />
              <Field label="max" value={p.max_ms} onChange={(v) => set("max_ms", v)} />
            </>}
            {kind === "normal" && <>
              <Field label="mean" value={p.mean_ms} onChange={(v) => set("mean_ms", v)} />
              <Field label="std dev" value={p.sd_ms} onChange={(v) => set("sd_ms", v)} />
              <Field label="clamp min" value={p.min_ms} onChange={(v) => set("min_ms", v)} />
              <Field label="clamp max" value={p.max_ms} onChange={(v) => set("max_ms", v)} hint="0 = none" />
            </>}
            {(kind === "lognormal" || kind === "bimodal") && <>
              <Field label="median" value={p.median_ms} onChange={(v) => set("median_ms", v)} />
              <Field label="p99" value={p.p99_ms} onChange={(v) => set("p99_ms", v)} />
            </>}
            {kind === "lognormal" && <Field label="hard cap" value={p.max_ms} onChange={(v) => set("max_ms", v)} hint="0 = none" />}
            {kind === "bimodal" && <>
              <Field label="tail probability" value={p.tail_prob} onChange={(v) => set("tail_prob", v)} unit="" step={0.001} />
              <div />
              <Field label="tail min" value={p.tail_min_ms} onChange={(v) => set("tail_min_ms", v)} />
              <Field label="tail max" value={p.tail_max_ms} onChange={(v) => set("tail_max_ms", v)} />
            </>}
            {kind === "empirical" && (
              <EmpiricalEditor q={(params.quantiles as number[]) ?? []} ms={(params.ms as number[]) ?? []}
                onChange={(q, ms) => value && onChange({ ...value, empirical: { quantiles: q, ms } })} />
            )}
            <Field label="error rate" value={value?.error_rate} onChange={(v) => setTop("error_rate", v)} unit="" step={0.001} hint="fail fast" />
            <Field label="timeout rate" value={value?.timeout_rate} onChange={(v) => setTop("timeout_rate", v)} unit="" step={0.001} hint="never answers" />
          </div>
          {pv && (
            <div className="rounded-xl border border-border bg-surface-2/50 p-3">
              <div className="grid grid-cols-4 gap-2 text-center">
                {([["p50", pv.p50], ["p90", pv.p90], ["p99", pv.p99], ["p99.9", pv.p999]] as const).map(([k, v]) => (
                  <div key={k}>
                    <div className="text-[10.5px] uppercase tracking-wider text-subtle">{k}</div>
                    <div className="mt-0.5 text-[14px] font-semibold text-fg num">{fmtMs(v)}</div>
                  </div>
                ))}
              </div>
              <DistChart height={compact ? 90 : 110} hist={pv.hist.map((h) => ({ x: h.ms, y: h.n }))}
                markers={[{ x: pv.p50, label: "p50" }, { x: pv.p99, label: "p99" }]} />
              {(pv.errorRate > 0 || pv.timeoutRate > 0) && (
                <div className="mt-1 text-center text-[11.5px] text-warn">
                  {fmtPct(pv.errorRate, 2)} errors · {fmtPct(pv.timeoutRate, 2)} timeouts
                </div>
              )}
              <div className="mt-1 text-center text-[10.5px] text-subtle">preview: 12k samples, same sampler as the Java LatencyModel</div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function EmpiricalEditor({ q, ms, onChange }: { q: number[]; ms: number[]; onChange: (q: number[], ms: number[]) => void }) {
  const rows = q.map((x, i) => ({ q: x, ms: ms[i] ?? 0 }));
  const upd = (i: number, k: "q" | "ms", v: number) => {
    const next = rows.map((r, j) => (j === i ? { ...r, [k]: v } : r));
    onChange(next.map((r) => r.q), next.map((r) => r.ms));
  };
  return (
    <div className="col-span-2 space-y-1.5">
      <div className="grid grid-cols-[1fr_1fr_28px] gap-2 text-[11px] uppercase tracking-wider text-subtle"><span>quantile</span><span>ms</span><span /></div>
      {rows.map((r, i) => (
        <div key={i} className="grid grid-cols-[1fr_1fr_28px] gap-2">
          <Input type="number" step={0.01} min={0} max={1} value={r.q} onChange={(e) => upd(i, "q", Number(e.target.value))} className="h-8" />
          <Input type="number" step={1} min={0} value={r.ms} onChange={(e) => upd(i, "ms", Number(e.target.value))} className="h-8" />
          <button aria-label="remove point" onClick={() => onChange(q.filter((_, j) => j !== i), ms.filter((_, j) => j !== i))}
            className="flex items-center justify-center rounded-lg text-subtle hover:bg-surface-2 hover:text-fail cursor-pointer"><Trash2 className="size-3.5" /></button>
        </div>
      ))}
      <button onClick={() => onChange([...q, Math.min(1, (q[q.length - 1] ?? 0.9) + 0.005)], [...ms, (ms[ms.length - 1] ?? 10) * 1.5])}
        className="inline-flex items-center gap-1 text-[12px] text-accent hover:underline cursor-pointer"><Plus className="size-3.5" />add point</button>
    </div>
  );
}

const RATE_KINDS = [
  { value: "constant", label: "constant" }, { value: "ramp", label: "ramp" }, { value: "step", label: "step" },
  { value: "spike", label: "spike" }, { value: "sine", label: "sine" },
] as const;

/** RateProfile editor with the exact rate_at() curve and event totals. */
export function RateProfileEditor({ value, duration, onChange }: { value: RateProfile; duration: number; onChange: (v: RateProfile) => void }) {
  const kind = value.kind ?? "constant";
  const { curve, events, peak } = React.useMemo(() => rateCurve(value, Math.max(1, duration)), [value, duration]);
  const set = (k: keyof RateProfile, v: number | undefined) => {
    const next = { ...value } as Record<string, unknown>;
    if (v === undefined) delete next[k];
    else next[k] = v;
    onChange(next as RateProfile);
  };
  return (
    <div className="space-y-4">
      <Segmented value={kind} onChange={(k) => onChange({ ...value, kind: k })} options={RATE_KINDS.map((k) => ({ ...k }))} />
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
        <NumberField label={kind === "constant" ? "rate" : "base rate"} value={value.tps} onChange={(v) => set("tps", v)} unit="TPS" min={0} />
        {kind === "ramp" && <>
          <NumberField label="start" value={value.start_tps ?? undefined} onChange={(v) => set("start_tps", v)} unit="TPS" placeholder="0" />
          <NumberField label="end" value={value.end_tps ?? undefined} onChange={(v) => set("end_tps", v)} unit="TPS" placeholder="= rate" />
        </>}
        {kind === "step" && <>
          <NumberField label="start" value={value.start_tps ?? undefined} onChange={(v) => set("start_tps", v)} unit="TPS" placeholder="= rate" />
          <NumberField label="increment" value={value.step_tps ?? undefined} onChange={(v) => set("step_tps", v)} unit="TPS" placeholder="= start" />
          <NumberField label="step length" value={value.step_duration_s ?? undefined} onChange={(v) => set("step_duration_s", v)} unit="s" placeholder="60" />
        </>}
        {kind === "spike" && <>
          <NumberField label="spike at" value={value.spike_at_s ?? undefined} onChange={(v) => set("spike_at_s", v)} unit="s" placeholder="0" />
          <NumberField label="spike length" value={value.spike_duration_s ?? undefined} onChange={(v) => set("spike_duration_s", v)} unit="s" placeholder="30" />
          <NumberField label="multiplier" value={value.spike_multiplier ?? undefined} onChange={(v) => set("spike_multiplier", v)} unit="×" step={0.1} placeholder="3" />
        </>}
        {kind === "sine" && <>
          <NumberField label="period" value={value.sine_period_s ?? undefined} onChange={(v) => set("sine_period_s", v)} unit="s" placeholder="600" />
          <NumberField label="amplitude" value={value.sine_amplitude ?? undefined} onChange={(v) => set("sine_amplitude", v)} step={0.05} placeholder="0.3" />
        </>}
      </div>
      <div className="rounded-xl border border-border bg-surface-2/40 p-3">
        <div className="mb-2 flex flex-wrap gap-x-6 gap-y-1 text-[12px] text-muted">
          <span>peak <b className="font-semibold text-fg num">{fmtCompact(peak)} TPS</b></span>
          <span>events <b className="font-semibold text-fg num">{fmtCompact(events)}</b></span>
          <span>duration <b className="font-semibold text-fg num">{fmtDuration(duration)}</b></span>
          {isNum(peak) && peak > 0 && <span>mean <b className="font-semibold text-fg num">{fmtCompact(events / Math.max(1, duration))} TPS</b></span>}
        </div>
        <RateChart curve={curve} height={150} />
      </div>
    </div>
  );
}
