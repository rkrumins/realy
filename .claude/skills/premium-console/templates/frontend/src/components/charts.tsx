/**
 * Recharts wrappers in the console's visual language: hairline grid, recessive axes, 2px lines, crosshair
 * tooltip, a legend for >= 2 series, an explicit "waiting for data" state. Colours are CSS variables, so
 * both themes just work. Animations are off: live charts must not wobble on every update.
 */
import * as React from "react";
import {
  Area, AreaChart, Bar, BarChart, CartesianGrid, Cell, ComposedChart, Line, ReferenceArea, ReferenceLine, ResponsiveContainer,
  Tooltip, XAxis, YAxis,
} from "recharts";
import { cn } from "@/lib/cn";
import { fmtCompact, isNum } from "@/lib/format";

export const SERIES = Array.from({ length: 8 }, (_, i) => `var(--chart-${i + 1})`);
export const TOKENS = { grid: "var(--grid)", axis: "var(--fg-subtle)", fail: "var(--fail)", pass: "var(--pass)", warn: "var(--warn)", accent: "var(--accent)" };

const axisProps = {
  stroke: "var(--border-strong)",
  tick: { fill: "var(--fg-subtle)", fontSize: 11 },
  tickLine: false,
  axisLine: false,
} as const;

export interface SeriesDef {
  key: string;
  label: string;
  color?: string;
  kind?: "line" | "area";
  dashed?: boolean;
  stack?: string;
  width?: number;
  hidden?: boolean;
}

type Fmt = (v: number) => string;

export function TooltipBox({ active, payload, label, xFormat, yFormat }: {
  active?: boolean; payload?: { name: string; value: number; color: string; dataKey: string }[]; label?: number | string;
  xFormat?: (v: number | string) => string; yFormat: Fmt;
}) {
  if (!active || !payload?.length) return null;
  const rows = payload.filter((p) => isNum(p.value));
  if (!rows.length) return null;
  return (
    <div className="min-w-[150px] rounded-xl border border-border bg-surface/95 px-3 py-2 text-[12px] shadow-xl backdrop-blur">
      {label !== undefined && <div className="mb-1.5 font-medium text-muted num">{xFormat ? xFormat(label) : label}</div>}
      <div className="space-y-1">
        {rows.map((p) => (
          <div key={p.dataKey} className="flex items-center gap-2">
            <span className="h-2 w-2 shrink-0 rounded-full" style={{ background: p.color }} />
            <span className="text-muted">{p.name}</span>
            <span className="ml-auto pl-3 font-medium text-fg num">{yFormat(p.value)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

export function Legend({ series, className }: { series: { label: string; color?: string; dashed?: boolean }[]; className?: string }) {
  if (series.length < 2) return null;
  return (
    <div className={cn("flex flex-wrap items-center gap-x-4 gap-y-1 text-[11.5px] text-muted", className)}>
      {series.map((s, i) => (
        <span key={s.label} className="inline-flex items-center gap-1.5">
          <svg width="14" height="6" aria-hidden>
            <line x1="0" y1="3" x2="14" y2="3" stroke={s.color ?? SERIES[i]} strokeWidth="2.5" strokeLinecap="round" strokeDasharray={s.dashed ? "3 3" : undefined} />
          </svg>
          {s.label}
        </span>
      ))}
    </div>
  );
}

export const clock = (t: number | string) => {
  const n = Number(t);
  if (!Number.isFinite(n)) return String(t);
  return new Date(n * 1000).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
};
export const secs = (t: number | string) => `${Number(t).toFixed(0)}s`;

/** Time-series chart (lines and/or stacked areas) with optional budget line, "good" band and markers. */
export function TimeChart({
  data, x = "t", series, height = 220, yFormat = (v) => fmtCompact(v), xFormat = clock, budget, budgetLabel = "budget",
  band, yDomain, legend = true, className, empty, syncId, markers,
}: {
  data: object[]; x?: string; series: SeriesDef[]; height?: number; yFormat?: Fmt;
  xFormat?: (v: number | string) => string; budget?: number | null; budgetLabel?: string;
  band?: { from: number; to: number } | null; yDomain?: [number | "auto" | "dataMin", number | "auto" | "dataMax"];
  legend?: boolean; className?: string; empty?: React.ReactNode; syncId?: string;
  markers?: { x: number; label: string }[];
}) {
  const visible = series.filter((s) => !s.hidden);
  const hasData = data.some((d) => visible.some((s) => isNum((d as Record<string, unknown>)[s.key])));
  const gid = React.useId().replace(/:/g, "");
  return (
    <div className={cn("w-full", className)}>
      {legend && <Legend className="mb-2" series={visible.map((s, i) => ({ label: s.label, color: s.color ?? SERIES[i], dashed: s.dashed }))} />}
      <div style={{ height }} className="relative w-full">
        {!hasData && (
          <div className="absolute inset-0 z-10 flex items-center justify-center text-[12.5px] text-subtle">{empty ?? "Waiting for data…"}</div>
        )}
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 6, right: 8, bottom: 0, left: 0 }} syncId={syncId}>
            <defs>
              {visible.map((s, i) => (
                <linearGradient key={s.key} id={`${gid}-${s.key}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={s.color ?? SERIES[i]} stopOpacity={s.stack ? 0.55 : 0.28} />
                  <stop offset="100%" stopColor={s.color ?? SERIES[i]} stopOpacity={s.stack ? 0.25 : 0} />
                </linearGradient>
              ))}
            </defs>
            <CartesianGrid stroke={TOKENS.grid} vertical={false} />
            <XAxis dataKey={x} {...axisProps} tickFormatter={xFormat} minTickGap={48} type="number" domain={["dataMin", "dataMax"]} scale="linear" />
            <YAxis {...axisProps} width={60} tickFormatter={yFormat} domain={yDomain ?? [0, "auto"]} allowDataOverflow={false} />
            <Tooltip cursor={{ stroke: "var(--border-strong)", strokeWidth: 1 }} isAnimationActive={false}
              content={(p) => <TooltipBox {...(p as object)} xFormat={xFormat} yFormat={yFormat} />} />
            {band && <ReferenceArea y1={band.from} y2={band.to} fill={TOKENS.pass} fillOpacity={0.06} stroke="none" ifOverflow="hidden" />}
            {isNum(budget) && (
              <ReferenceLine y={budget} stroke={TOKENS.fail} strokeDasharray="5 4" strokeOpacity={0.8} ifOverflow="extendDomain"
                label={{ value: `${budgetLabel} ${yFormat(budget)}`, position: "insideTopRight", fill: "var(--fail)", fontSize: 11 }} />
            )}
            {markers?.map((m) => (
              <ReferenceLine key={m.x} x={m.x} stroke="var(--border-strong)" strokeDasharray="2 3"
                label={{ value: m.label, position: "insideTopLeft", fill: "var(--fg-subtle)", fontSize: 10.5 }} />
            ))}
            {visible.map((s, i) => {
              const color = s.color ?? SERIES[i];
              return s.kind === "area" || s.stack ? (
                <Area key={s.key} dataKey={s.key} name={s.label} stackId={s.stack} type="monotone" stroke={color}
                  strokeWidth={s.width ?? 1.5} fill={`url(#${gid}-${s.key})`} isAnimationActive={false} connectNulls dot={false}
                  activeDot={{ r: 4, strokeWidth: 2, stroke: "var(--card)" }} />
              ) : (
                <Line key={s.key} dataKey={s.key} name={s.label} type="monotone" stroke={color} strokeWidth={s.width ?? 2}
                  strokeDasharray={s.dashed ? "4 4" : undefined} dot={false} isAnimationActive={false} connectNulls
                  activeDot={{ r: 4, strokeWidth: 2, stroke: "var(--card)" }} />
              );
            })}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

/** Tiny area line for KPI tiles and hero cards (no axes, no tooltip). Renders nothing below 2 points. */
export function Sparkline({ data, color = "var(--accent)", height = 40 }: { data: (number | null)[]; color?: string; height?: number }) {
  const pts = data.map((v, i) => ({ i, v }));
  const id = React.useId().replace(/:/g, "");
  if (pts.filter((p) => isNum(p.v)).length < 2) return null;
  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={pts} margin={{ top: 2, right: 0, bottom: 0, left: 0 }}>
        <defs>
          <linearGradient id={id} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={0.35} />
            <stop offset="100%" stopColor={color} stopOpacity={0} />
          </linearGradient>
        </defs>
        <YAxis hide domain={[0, "auto"]} />
        <Area dataKey="v" type="monotone" stroke={color} strokeWidth={1.5} fill={`url(#${id})`} isAnimationActive={false} connectNulls dot={false} />
      </AreaChart>
    </ResponsiveContainer>
  );
}

/** Horizontal bars (category -> value) with value labels; `highlight` marks the dominant one. */
export function HBars({ data, height, format = (v) => fmtCompact(v), color = "var(--chart-1)", highlight }: {
  data: { name: string; value: number }[]; height?: number; format?: Fmt; color?: string; highlight?: string | null;
}) {
  const h = height ?? Math.max(120, data.length * 28 + 20);
  return (
    <ResponsiveContainer width="100%" height={h}>
      <BarChart data={data} layout="vertical" margin={{ top: 0, right: 48, bottom: 0, left: 0 }} barCategoryGap={6}>
        <CartesianGrid stroke={TOKENS.grid} horizontal={false} />
        <XAxis type="number" {...axisProps} tickFormatter={format} />
        <YAxis type="category" dataKey="name" {...axisProps} width={130} tick={{ fill: "var(--fg-muted)", fontSize: 11.5 }} />
        <Tooltip cursor={{ fill: "var(--card-2)" }} isAnimationActive={false}
          content={(p) => <TooltipBox {...(p as object)} yFormat={format} />} />
        <Bar dataKey="value" name="value" radius={[0, 4, 4, 0]} isAnimationActive={false}
          label={{ position: "right", fill: "var(--fg-muted)", fontSize: 11, formatter: (v: unknown) => (isNum(v) ? format(v) : "") }}>
          {data.map((d) => <Cell key={d.name} fill={highlight && d.name === highlight ? "var(--chart-2)" : color} />)}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}
