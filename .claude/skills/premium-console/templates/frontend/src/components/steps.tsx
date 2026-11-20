/**
 * Step checklist: what a multi-step job is doing right now, with real progress (done / total, rate, ETA)
 * on the running step and the duration of every finished one. Open while the job prepares; collapses to
 * a one-line summary once nothing is running.
 */
import { AlertTriangle, CheckCircle2, ChevronDown, Circle, Loader2, MinusCircle, XCircle } from "lucide-react";
import * as React from "react";
import { Badge } from "@/components/ui/badge";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { cn } from "@/lib/cn";
import { fmtNum, isNum } from "@/lib/format";
import type { Step, StepProgress } from "@/lib/types";

const ICON: Record<Step["status"], { icon: typeof Circle; cls: string }> = {
  pending: { icon: Circle, cls: "text-subtle" },
  running: { icon: Loader2, cls: "text-accent animate-spin" },
  done: { icon: CheckCircle2, cls: "text-pass" },
  skipped: { icon: MinusCircle, cls: "text-subtle" },
  warn: { icon: AlertTriangle, cls: "text-warn" },
  failed: { icon: XCircle, cls: "text-fail" },
};

/** "71% (412,330/583,063 items) · 9,800/s · ETA 17 s" */
export function fmtProgress(p: StepProgress | null | undefined): string {
  if (!p || !p.total) return "";
  const pct = Math.min(100, (100 * (p.done || 0)) / Math.max(p.total, 1));
  let out = `${pct.toFixed(0)}% (${fmtNum(p.done)}/${fmtNum(p.total)}${p.unit ? ` ${p.unit}` : ""})`;
  if (isNum(p.rate) && p.rate > 0) out += ` · ${fmtNum(p.rate)}/s`;
  if (isNum(p.eta_s)) out += ` · ETA ${Math.round(p.eta_s)} s`;
  return out;
}

export function fmtStepDuration(s?: number | null) {
  if (!isNum(s)) return "";
  return s < 10 ? `${s.toFixed(1)} s` : s < 120 ? `${Math.round(s)} s` : `${Math.floor(s / 60)} min ${Math.round(s % 60)} s`;
}

function StepRow({ s }: { s: Step }) {
  const { icon: Icon, cls } = ICON[s.status] ?? ICON.pending;
  const pct = s.progress?.total ? Math.min(1, (s.progress.done || 0) / s.progress.total) : null;
  return (
    <li className={cn("flex gap-3 py-2", s.status === "pending" && "opacity-55")}>
      <Icon className={cn("mt-0.5 size-4 shrink-0", cls)} aria-label={s.status} />
      <div className="min-w-0 flex-1">
        <div className="flex items-baseline justify-between gap-3">
          <span className="text-[13px] font-medium text-fg"><span className="mr-1.5 font-mono text-[11px] text-subtle">{s.index}</span>{s.label}</span>
          <span className="shrink-0 font-mono text-[11.5px] text-subtle num">{s.status === "skipped" ? "skipped" : fmtStepDuration(s.duration_s)}</span>
        </div>
        {s.status === "running" && pct !== null && (
          <div className="mt-1.5">
            <div className="h-1.5 overflow-hidden rounded-full bg-surface-3" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(pct * 100)}>
              <div className="h-full rounded-full bg-accent transition-[width] duration-700" style={{ width: `${pct * 100}%` }} />
            </div>
            <div className="mt-1 font-mono text-[11px] text-muted num">{fmtProgress(s.progress)}</div>
          </div>
        )}
        {s.detail && <div className="mt-0.5 text-[12px] leading-snug text-muted">{s.detail}</div>}
      </div>
    </li>
  );
}

export function StepChecklist({ steps, title = "Steps", className, defaultOpen }: {
  steps: Step[] | null | undefined; title?: string; className?: string; defaultOpen?: boolean;
}) {
  const active = steps?.find((s) => s.status === "running");
  const [open, setOpen] = React.useState(defaultOpen ?? Boolean(active));
  const wasActive = React.useRef(Boolean(active));
  React.useEffect(() => {
    // auto-collapse when the last step finishes, auto-open when one starts
    if (Boolean(active) !== wasActive.current) setOpen(Boolean(active));
    wasActive.current = Boolean(active);
  }, [active]);
  if (!steps?.length) return null;
  const attention = steps.filter((s) => s.status === "failed" || s.status === "warn");
  const total = steps.filter((s) => isNum(s.duration_s)).reduce((a, s) => a + (s.duration_s ?? 0), 0);
  const half = Math.ceil(steps.length / 2);
  return (
    <Card className={className}>
      <CardHeader title={title}
        subtitle={active ? `${active.index}/${steps.length} · ${active.label}${active.progress ? ` · ${fmtProgress(active.progress)}` : ""}`
          : `${steps.filter((s) => s.status === "done").length} of ${steps.length} done in ${fmtStepDuration(total) || "0 s"}`}
        actions={<div className="flex items-center gap-2">
          {attention.length > 0 && <Badge tone="warn">{attention.length} step{attention.length > 1 ? "s" : ""} need attention</Badge>}
          <button type="button" onClick={() => setOpen((o) => !o)} aria-expanded={open}
            className="inline-flex items-center gap-1 rounded-lg px-2 py-1 text-[12px] text-muted hover:bg-surface-2 cursor-pointer">
            {open ? "Hide" : "Show"} steps<ChevronDown className={cn("size-3.5 transition-transform", open && "rotate-180")} />
          </button>
        </div>} />
      {open && (
        <CardBody>
          <div className="grid grid-cols-1 gap-x-8 md:grid-cols-2">
            <ol className="divide-y divide-border">{steps.slice(0, half).map((s) => <StepRow key={s.id} s={s} />)}</ol>
            <ol className="divide-y divide-border">{steps.slice(half).map((s) => <StepRow key={s.id} s={s} />)}</ol>
          </div>
        </CardBody>
      )}
    </Card>
  );
}

/** One line for a hero card: "Step 2/4: Load · 40% (…)". */
export function ActiveStep({ steps }: { steps: Step[] | null | undefined }) {
  const s = steps?.find((x) => x.status === "running");
  if (!s || !steps) return null;
  return (
    <div className="mt-1 text-[12px] text-muted">
      Step {s.index}/{steps.length}: <span className="text-fg">{s.label}</span>
      {s.progress ? <span className="font-mono num"> · {fmtProgress(s.progress)}</span> : s.detail ? ` · ${s.detail}` : ""}
    </div>
  );
}
