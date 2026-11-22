import { AlertTriangle, CheckCircle2, CircleDashed, Inbox, Loader2, OctagonX, XCircle } from "lucide-react";
import * as React from "react";
import { cn } from "@/lib/cn";
import type { Verdict } from "@/lib/types";
import { Badge } from "./ui/badge";
import { Tooltip } from "./ui/tooltip";

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

export function Kpi({ label, value, sub, tone, icon, className, spark, title }: {
  label: React.ReactNode; value: React.ReactNode; sub?: React.ReactNode; tone?: "pass" | "fail" | "warn" | "accent";
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

export function VerdictBadge({ verdict, size = "default", expected }: { verdict: Verdict | string; size?: "default" | "lg"; expected?: boolean }) {
  if (!verdict) return <Badge tone="neutral"><CircleDashed />pending</Badge>;
  const pass = verdict === "PASS";
  const aborted = verdict === "ABORTED";
  const lg = size === "lg";
  return (
    <Tooltip content={expected ? "matches the scenario's expected verdict" : undefined}>
      <span>
        <Badge tone={pass ? "pass" : aborted ? "warn" : "fail"} className={cn(lg && "px-3 py-1 text-[13px] [&_svg]:size-4", "font-semibold tracking-wide")}>
          {pass ? <CheckCircle2 /> : aborted ? <OctagonX /> : <XCircle />}
          {verdict}
          {expected && <span className="font-normal opacity-80">· expected</span>}
        </Badge>
      </span>
    </Tooltip>
  );
}

const STATUS_TONE: Record<string, "pass" | "fail" | "warn" | "info" | "neutral" | "accent"> = {
  done: "pass", running: "accent", queued: "info", aborted: "warn", failed: "fail", cancelled: "neutral", incomplete: "neutral",
  interrupted: "warn",
};

export function StatusBadge({ status, phase }: { status: string; phase?: string }) {
  const tone = STATUS_TONE[status] ?? "neutral";
  return (
    <Badge tone={tone === "accent" ? "accent" : tone}>
      {status === "running" && <Loader2 className="animate-spin" />}
      {status}
      {phase && status === "running" && <span className="opacity-75">· {phase.toLowerCase()}</span>}
    </Badge>
  );
}

const TOPO_LABEL: Record<string, string> = {
  "gateway-kafka": "async gateway", "gateway-grpc": "sync gRPC", "flink-inline": "Flink inline", "echo-kafka": "echo",
  external: "your scorer",
};
export function TopologyBadge({ topology }: { topology?: string | null }) {
  if (!topology) return null;
  const tone = topology === "flink-inline" ? "info" : topology === "gateway-grpc" ? "accent" : topology === "external" ? "warn" : "neutral";
  return <Badge tone={tone} className="font-mono text-[11px]">{TOPO_LABEL[topology] ?? topology}</Badge>;
}
export const topologyLabel = (t?: string | null) => (t ? TOPO_LABEL[t] ?? t : "—");

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
    <div className="flex items-start gap-3 rounded-xl border border-fail/25 bg-fail/5 px-4 py-3 text-[13px]">
      <AlertTriangle className="mt-0.5 size-4 shrink-0 text-fail" />
      <div><div className="font-medium text-fail">{title}</div><div className="mt-0.5 text-muted">{msg}</div></div>
    </div>
  );
}

export function Unavailable({ what }: { what: string }) {
  return (
    <EmptyState icon={<CircleDashed />} title={`${what} not available`}>
      This console's backend does not expose this endpoint yet (or it is unreachable). Upgrade the console image or check the logs.
    </EmptyState>
  );
}

export function Stat({ label, value, mono }: { label: React.ReactNode; value: React.ReactNode; mono?: boolean }) {
  return (
    <div className="flex items-baseline justify-between gap-3 border-b border-border py-1.5 text-[13px] last:border-0">
      <span className="text-muted">{label}</span>
      <span className={cn("text-right text-fg num", mono && "font-mono text-[12px]")}>{value}</span>
    </div>
  );
}

/** Horizontal bar used in tables (precision, agreement, lag...). */
export function MiniBar({ value, max = 1, tone = "accent", className }: { value: number | null | undefined; max?: number; tone?: "accent" | "pass" | "fail" | "warn"; className?: string }) {
  const w = value === null || value === undefined || !Number.isFinite(value) ? 0 : Math.max(0, Math.min(1, value / (max || 1)));
  const c = { accent: "bg-accent", pass: "bg-pass", fail: "bg-fail", warn: "bg-warn" }[tone];
  return (
    <div className={cn("h-1.5 w-full min-w-12 overflow-hidden rounded-full bg-surface-3", className)}>
      <div className={cn("h-full rounded-full", c)} style={{ width: `${w * 100}%` }} />
    </div>
  );
}

export function SectionTitle({ children, actions, className }: { children: React.ReactNode; actions?: React.ReactNode; className?: string }) {
  return (
    <div className={cn("mb-3 mt-8 flex items-center justify-between gap-3 first:mt-0", className)}>
      <h2 className="text-[13px] font-semibold uppercase tracking-[0.1em] text-subtle">{children}</h2>
      {actions}
    </div>
  );
}
