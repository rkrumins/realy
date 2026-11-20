/** Page-level building blocks shared by every page: header, KPI tile, status badge, empty / error states. */
import { AlertTriangle, ArrowLeft, CircleDashed, Inbox, Loader2 } from "lucide-react";
import * as React from "react";
import { Link } from "react-router";
import { cn } from "@/lib/cn";
import { fmtCompact, fmtPct, isNum } from "@/lib/format";
import type { JobStatus } from "@/lib/types";
import { Badge } from "./ui/badge";

export function PageHeader({ title, subtitle, actions, eyebrow, children }: {
  title: React.ReactNode; subtitle?: React.ReactNode; actions?: React.ReactNode; eyebrow?: React.ReactNode; children?: React.ReactNode;
}) {
  return (
    <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
      <div className="min-w-0">
        {eyebrow && <div className="mb-1.5 text-[11.5px] font-semibold uppercase tracking-[0.12em] text-accent">{eyebrow}</div>}
        <h1 className="text-[26px] font-semibold leading-tight tracking-tight text-fg">{title}</h1>
        {subtitle && <p className="mt-1.5 max-w-3xl text-[14px] text-muted">{subtitle}</p>}
        {children}
      </div>
      {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
    </div>
  );
}

export type Tone = "pass" | "fail" | "warn" | "accent";

/** KPI tile: label (with unit context), big tabular value, a sub-line that explains the number. */
export function Kpi({ label, value, sub, tone, icon, className, spark, title }: {
  label: React.ReactNode; value: React.ReactNode; sub?: React.ReactNode; tone?: Tone;
  icon?: React.ReactNode; className?: string; spark?: React.ReactNode; title?: string;
}) {
  const toneCls = tone ? { pass: "text-pass", fail: "text-fail", warn: "text-warn", accent: "text-accent" }[tone] : "text-fg";
  return (
    <div title={title} className={cn("relative overflow-hidden rounded-2xl border border-border bg-surface p-4 shadow-card", className)}>
      <div className="flex items-center gap-1.5 text-[11.5px] font-medium uppercase tracking-wider text-subtle [&_svg]:size-3.5">
        {icon}
        <span className="truncate">{label}</span>
      </div>
      <div className={cn("mt-2 truncate text-[24px] font-semibold leading-none tracking-tight num", toneCls)}>{value}</div>
      {sub && <div className="mt-1.5 truncate text-[12px] text-muted num">{sub}</div>}
      {spark && <div className="pointer-events-none absolute inset-x-0 bottom-0 h-10 opacity-70">{spark}</div>}
    </div>
  );
}

const STATUS_TONE: Record<JobStatus, "pass" | "fail" | "warn" | "info" | "neutral" | "accent"> = {
  done: "pass", running: "accent", queued: "info", aborted: "warn", failed: "fail", cancelled: "neutral", interrupted: "warn",
};

export function StatusBadge({ status, phase }: { status: JobStatus | string; phase?: string }) {
  const tone = STATUS_TONE[status as JobStatus] ?? "neutral";
  return (
    <Badge tone={tone}>
      {status === "running" && <Loader2 className="animate-spin" />}
      {status}
      {phase && status === "running" && <span className="opacity-75">· {phase.toLowerCase()}</span>}
    </Badge>
  );
}

export function EmptyState({ icon, title, children, action, className }: {
  icon?: React.ReactNode; title: React.ReactNode; children?: React.ReactNode; action?: React.ReactNode; className?: string;
}) {
  return (
    <div className={cn("flex flex-col items-center justify-center rounded-2xl border border-dashed border-border px-6 py-12 text-center", className)}>
      <div className="mb-3 flex size-11 items-center justify-center rounded-xl border border-border bg-surface-2 text-muted [&_svg]:size-5">
        {icon ?? <Inbox />}
      </div>
      <div className="text-[14px] font-medium text-fg">{title}</div>
      {children && <div className="mt-1 max-w-md text-[13px] text-muted">{children}</div>}
      {action && <div className="mt-4">{action}</div>}
    </div>
  );
}

export function ErrorState({ error, title = "Could not load" }: { error: unknown; title?: string }) {
  const msg = error instanceof Error ? error.message : String(error);
  return (
    <div role="alert" className="flex items-start gap-3 rounded-xl border border-fail/25 bg-fail/5 px-4 py-3 text-[13px]">
      <AlertTriangle className="mt-0.5 size-4 shrink-0 text-fail" />
      <div><div className="font-medium text-fail">{title}</div><div className="mt-0.5 text-muted">{msg}</div></div>
    </div>
  );
}

/** For endpoints an older backend does not have (404/405): explain, do not show a red error. */
export function Unavailable({ what }: { what: string }) {
  return (
    <EmptyState icon={<CircleDashed />} title={`${what} not available`}>
      This backend does not expose this endpoint yet (or it is unreachable). Upgrade the service or check its logs.
    </EmptyState>
  );
}

/** Label / value row for definition-style panels. */
export function Stat({ label, value, mono }: { label: React.ReactNode; value: React.ReactNode; mono?: boolean }) {
  return (
    <div className="flex items-baseline justify-between gap-3 border-b border-border py-1.5 text-[13px] last:border-0">
      <span className="text-muted">{label}</span>
      <span className={cn("text-right text-fg num", mono && "font-mono text-[12px]")}>{value}</span>
    </div>
  );
}

/** Inline horizontal bar for ratios inside tables and lists. */
export function MiniBar({ value, max = 1, tone = "accent", className }: { value: number | null | undefined; max?: number; tone?: "accent" | "pass" | "fail" | "warn"; className?: string }) {
  const w = value === null || value === undefined || !Number.isFinite(value) ? 0 : Math.max(0, Math.min(1, value / (max || 1)));
  const c = { accent: "bg-accent", pass: "bg-pass", fail: "bg-fail", warn: "bg-warn" }[tone];
  return (
    <div className={cn("h-1.5 w-full min-w-12 overflow-hidden rounded-full bg-surface-3", className)} role="meter"
      aria-valuemin={0} aria-valuemax={max} aria-valuenow={isNum(value) ? value : undefined}>
      <div className={cn("h-full rounded-full", c)} style={{ width: `${w * 100}%` }} />
    </div>
  );
}

export function SectionTitle({ children, actions, className, id }: { children: React.ReactNode; actions?: React.ReactNode; className?: string; id?: string }) {
  return (
    <div id={id} className={cn("mb-3 mt-8 flex scroll-mt-20 items-center justify-between gap-3 first:mt-0", className)}>
      <h2 className="text-[13px] font-semibold uppercase tracking-[0.1em] text-subtle">{children}</h2>
      {actions}
    </div>
  );
}

/** B relative to A, coloured by whether the change is an improvement. */
export function Delta({ a, b, better, pct = true }: { a: number | null | undefined; b: number | null | undefined; better: "lower" | "higher"; pct?: boolean }) {
  if (!isNum(a) || !isNum(b)) return <span className="text-subtle">—</span>;
  const d = b - a;
  if (Math.abs(d) < 1e-9) return <span className="text-subtle">=</span>;
  const good = better === "lower" ? d < 0 : d > 0;
  const rel = a !== 0 ? d / Math.abs(a) : null;
  return (
    <span className={cn("num font-medium", good ? "text-pass" : "text-fail")}>
      {d > 0 ? "+" : "−"}{pct && rel !== null ? fmtPct(Math.abs(rel), 0) : fmtCompact(Math.abs(d))}
    </span>
  );
}

/** Colour key for anything coloured by state. Every coloured mark on a page needs one. */
export function LegendDots({ items, className }: { items: { label: React.ReactNode; className: string }[]; className?: string }) {
  return (
    <div className={cn("flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] text-subtle", className)}>
      {items.map((it, i) => (
        <span key={i} className="inline-flex items-center gap-1.5"><span className={cn("size-2 rounded-full", it.className)} />{it.label}</span>
      ))}
    </div>
  );
}

export function BackLink({ to, children }: { to: string; children: React.ReactNode }) {
  return (
    <Link to={to} className="mb-4 inline-flex items-center gap-1.5 text-[13px] text-muted hover:text-fg">
      <ArrowLeft className="size-4" />{children}
    </Link>
  );
}
