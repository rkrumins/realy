/**
 * A run's whole lifecycle as one bar: preparing, warm-up, measuring, draining, the report and the
 * verification, each segment as long as it took (or is expected to take). Times come from the server's
 * timeline and clock (lib/timeline.ts), so a run that prepares for minutes shows exactly that, and the
 * traffic window is the scenario's duration (warm-up inside it, not added to it).
 */
import { cn } from "@/lib/cn";
import { fmtDuration, fmtTime } from "@/lib/format";
import { STAGE_LABEL, segments, totals, type StageId } from "@/lib/timeline";
import type { RunEta, RunTimeline, StartupStep } from "@/lib/types";

const FILL: Record<StageId, string> = {
  prepare: "bg-info/70",
  warmup: "bg-warn/60",
  measure: "bg-[linear-gradient(90deg,#6366f1,#22d3ee)]",
  drain: "bg-info/45",
  report: "bg-accent/50",
  verify: "bg-pass/70",
};
const SHORT: Record<StageId, string> = {
  prepare: "prepare", warmup: "warm-up", measure: "measure", drain: "drain", report: "report", verify: "verify",
};

export function RunLifecycle({ tl, eta, now, verify, step, nSteps, className }: {
  tl: RunTimeline; eta?: RunEta | null; now: number; verify?: boolean;
  /** the startup step running now (while preparing) */
  step?: StartupStep | null; nSteps?: number; className?: string;
}) {
  const segs = segments(tl, eta, now, verify);
  const { elapsed_s, remaining_s } = totals(segs);
  const shown = segs.filter((s) => s.state !== "skipped");
  const span = Math.max(1, shown.reduce((a, s) => a + s.elapsed_s + s.remaining_s, 0));
  const act = segs.find((s) => s.state === "active");
  const late = eta?.late?.length ? eta.late : [];
  const stepText = act?.id === "prepare" && step
    ? ` · step ${step.index}/${nSteps ?? "?"}: ${step.label.toLowerCase()}${step.progress?.total
      ? ` ${Math.min(100, Math.round((100 * (step.progress.done ?? 0)) / step.progress.total))}%` : ""}`
    : "";
  return (
    <div className={cn("-mt-3 mb-5", className)} data-testid="run-lifecycle">
      <div className="mb-1.5 flex flex-wrap justify-between gap-x-4 gap-y-1 text-[11.5px] text-subtle num">
        <span className="text-muted">
          {tl.live && act ? <><span className="text-fg">{STAGE_LABEL[act.id]}</span> for {fmtDuration(act.elapsed_s)}{stepText}</>
            : <>{tl.aborted ? "Aborted" : "Finished"} {fmtTime(tl.finished_at)}</>}
        </span>
        <span title={eta ? `estimate: ${eta.basis}` : undefined}>
          {fmtDuration(elapsed_s)} since launch
          {tl.live && remaining_s > 0.5 && <> · about <span className="text-fg">{fmtDuration(remaining_s)}</span> left</>}
          {tl.live && late.length > 0 && <span className="text-warn"> · {late.join(", ")} longer than usual</span>}
        </span>
      </div>
      <div className="flex h-1.5 gap-[2px]" role="img"
        aria-label={shown.map((s) => `${SHORT[s.id]} ${fmtDuration(s.elapsed_s + s.remaining_s)}`).join(", ")}>
        {shown.map((s) => {
          const whole = s.elapsed_s + s.remaining_s;
          const filled = s.state === "done" ? 1 : whole > 0 ? s.elapsed_s / whole : 0;
          return (
            <div key={s.id} className="relative h-full overflow-hidden rounded-full bg-surface-3"
              style={{ flex: `${Math.max(whole, span * 0.015)} 1 0%`, minWidth: 6 }}
              title={`${STAGE_LABEL[s.id]}: ${s.state === "done" ? fmtDuration(s.elapsed_s)
                : s.state === "active" ? `${fmtDuration(s.elapsed_s)} so far, ~${fmtDuration(s.remaining_s)} to go`
                : `~${fmtDuration(s.remaining_s)} expected`}`}>
              <div className={cn("absolute inset-y-0 left-0 transition-[width] duration-1000", FILL[s.id], s.state === "pending" && "opacity-0")}
                style={{ width: `${Math.min(100, filled * 100)}%` }} />
            </div>
          );
        })}
      </div>
      <div className="mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-[10.5px] text-subtle num">
        {shown.map((s) => (
          <span key={s.id} className={cn(s.state === "active" && "text-fg")}>
            <span className={cn("mr-1 inline-block size-1.5 rounded-full align-middle", FILL[s.id])} />
            {SHORT[s.id]} {s.state === "done" ? fmtDuration(s.elapsed_s)
              : s.state === "active" ? `${fmtDuration(s.elapsed_s)}${s.remaining_s > 0.5 ? ` / ~${fmtDuration(s.elapsed_s + s.remaining_s)}` : ""}`
              : `~${fmtDuration(s.remaining_s)}`}
          </span>
        ))}
      </div>
    </div>
  );
}
