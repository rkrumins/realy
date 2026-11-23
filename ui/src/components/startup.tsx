/**
 * The run's startup sequence (runner/startup.py): what it is doing before it measures, with real
 * progress (Flink's processed records, not committed offsets) and timings. Shown on Live while a run
 * prepares, replays history, catches up and primes; collapsed to a summary once it measures.
 */
import { AlertTriangle, CheckCircle2, ChevronDown, Circle, Loader2, MinusCircle, XCircle } from "lucide-react";
import * as React from "react";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/cn";
import { fmtNum, isNum } from "@/lib/format";
import type { StartupProgress, StartupSnapshot, StartupStep } from "@/lib/types";

const ICON: Record<StartupStep["status"], { icon: typeof Circle; cls: string }> = {
  pending: { icon: Circle, cls: "text-subtle" },
  running: { icon: Loader2, cls: "text-accent animate-spin" },
  done: { icon: CheckCircle2, cls: "text-pass" },
  skipped: { icon: MinusCircle, cls: "text-subtle" },
  warn: { icon: AlertTriangle, cls: "text-warn" },
  failed: { icon: XCircle, cls: "text-fail" },
};

export function fmtStartupProgress(p: StartupProgress | null | undefined): string {
  if (!p || !p.total) return "";
  const pct = Math.min(100, (100 * (p.done || 0)) / Math.max(p.total, 1));
  let out = `${pct.toFixed(0)}% (${fmtNum(p.done)}/${fmtNum(p.total)}${p.unit ? ` ${p.unit}` : ""})`;
  if (isNum(p.rate) && p.rate > 0) out += ` · ${fmtNum(p.rate)}/s`;
  if (isNum(p.eta_s)) out += ` · ETA ${Math.round(p.eta_s)} s`;
  return out;
}

function secs(s?: number) {
  if (!isNum(s)) return "";
  return s < 10 ? `${s.toFixed(1)} s` : s < 120 ? `${Math.round(s)} s` : `${Math.floor(s / 60)} min ${Math.round(s % 60)} s`;
}

function StepRow({ s, now }: { s: StartupStep; now?: number }) {
  const { icon: Icon, cls } = ICON[s.status] ?? ICON.pending;
  // a running step's time is counted to now (the record is written only on progress, start and finish)
  const dur = s.status === "running" && s.started_at && now ? Math.max(0, now - s.started_at) : s.duration_s;
  const pct = s.progress?.total ? Math.min(1, (s.progress.done || 0) / s.progress.total) : null;
  return (
    <li className={cn("flex gap-3 py-2", s.status === "pending" && "opacity-55")}>
      <Icon className={cn("mt-0.5 size-4 shrink-0", cls)} aria-label={s.status} />
      <div className="min-w-0 flex-1">
        <div className="flex items-baseline justify-between gap-3">
          <span className="text-[13px] font-medium text-fg"><span className="mr-1.5 font-mono text-[11px] text-subtle">{s.index}</span>{s.label}</span>
          <span className="shrink-0 font-mono text-[11.5px] text-subtle num">
            {s.status === "skipped" ? "skipped" : secs(dur)}
          </span>
        </div>
        {s.status === "running" && pct !== null && (
          <div className="mt-1.5">
            <div className="h-1.5 overflow-hidden rounded-full bg-surface-3">
              <div className="h-full rounded-full bg-accent transition-[width] duration-700" style={{ width: `${pct * 100}%` }} />
            </div>
            <div className="mt-1 font-mono text-[11px] text-muted num">{fmtStartupProgress(s.progress)}</div>
          </div>
        )}
        {s.detail && <div className="mt-0.5 text-[12px] leading-snug text-muted">{s.detail}</div>}
      </div>
    </li>
  );
}

/**
 * @param prepareS everything before traffic (the run's timeline), inside a step or not; without it the
 *   steps before warm-up are summed (an older backend)
 * @param now the server's clock, for the running step's time
 */
export function StartupPanel({ startup, phase, className, prepareS, now }: {
  startup: StartupSnapshot | null | undefined; phase?: string | null; className?: string; prepareS?: number | null; now?: number;
}) {
  const measuring = ["MEASURE", "DRAIN", "COMPLETE", "VERIFY"].includes(phase ?? "");
  const [open, setOpen] = React.useState(!measuring);
  React.useEffect(() => { setOpen(!measuring); }, [measuring]);
  const steps = (startup?.steps ?? []).filter((s) => (s.group ?? "startup") === "startup");
  if (!steps.length) return null;
  const active = steps.find((s) => s.status === "running");
  const failed = steps.filter((s) => s.status === "failed" || s.status === "warn");
  const prepS = isNum(prepareS) ? prepareS
    : steps.filter((s) => s.id !== "measure" && s.id !== "warmup" && isNum(s.duration_s)).reduce((a, s) => a + (s.duration_s ?? 0), 0);
  const half = Math.ceil(steps.length / 2);
  return (
    <Card className={cn("mt-5", className)}>
      <CardHeader title="Startup"
        subtitle={active ? `${active.index}/${steps.length} · ${active.label}${active.progress ? ` · ${fmtStartupProgress(active.progress)}` : ""}`
          : `prepared in ${secs(prepS)} (launch to traffic)`}
        actions={<div className="flex items-center gap-2">
          {failed.length > 0 && <Badge tone="warn">{failed.length} step{failed.length > 1 ? "s" : ""} need attention</Badge>}
          <button type="button" onClick={() => setOpen((o) => !o)} className="inline-flex items-center gap-1 rounded-lg px-2 py-1 text-[12px] text-muted hover:bg-surface-2"
            aria-expanded={open}>{open ? "Hide" : "Show"} steps<ChevronDown className={cn("size-3.5 transition-transform", open && "rotate-180")} /></button>
        </div>} />
      {open && (
        <CardBody>
          <div className="grid grid-cols-1 gap-x-8 md:grid-cols-2">
            <ol className="divide-y divide-border">{steps.slice(0, half).map((s) => <StepRow key={s.id} s={s} now={now} />)}</ol>
            <ol className="divide-y divide-border">{steps.slice(half).map((s) => <StepRow key={s.id} s={s} now={now} />)}</ol>
          </div>
        </CardBody>
      )}
    </Card>
  );
}

/** One line for the Overview's "running now" card. */
export function ActiveStartupStep({ startup }: { startup: StartupSnapshot | null | undefined }) {
  const steps = (startup?.steps ?? []).filter((x) => (x.group ?? "startup") === "startup");
  const s = steps.find((x) => x.status === "running");
  if (!s) return null;
  return (
    <div className="mt-1 text-[12px] text-muted">
      Startup {s.index}/{steps.length}: <span className="text-fg">{s.label}</span>
      {s.progress ? <span className="font-mono num"> · {fmtStartupProgress(s.progress)}</span> : s.detail ? ` · ${s.detail}` : ""}
    </div>
  );
}
