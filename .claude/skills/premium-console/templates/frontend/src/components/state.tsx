/**
 * Explicit component states (server/state.py) with one colour each and a legend. A component that is
 * starting or warming is never shown as "bad": state overrides latency colouring.
 */
import { AlertTriangle, CheckCircle2, CircleDashed, Flame, Loader2, XCircle } from "lucide-react";
import { cn } from "@/lib/cn";
import type { ComponentState, ComponentStateName } from "@/lib/types";
import { LegendDots } from "./common";
import { Badge } from "./ui/badge";

export const STATE_META: Record<ComponentStateName, { label: string; tone: "fail" | "info" | "accent" | "pass" | "warn"; dot: string; icon: typeof CheckCircle2; legend: string }> = {
  down: { label: "down", tone: "fail", dot: "bg-fail", icon: XCircle, legend: "down / unreachable" },
  starting: { label: "starting", tone: "info", dot: "bg-info", icon: CircleDashed, legend: "starting" },
  warming: { label: "warming", tone: "accent", dot: "bg-accent", icon: Flame, legend: "warming (busy with a job step)" },
  degraded: { label: "degraded", tone: "warn", dot: "bg-warn", icon: AlertTriangle, legend: "degraded (slow or impaired)" },
  ready: { label: "ready", tone: "pass", dot: "bg-pass", icon: CheckCircle2, legend: "ready" },
};

export function StateBadge({ state }: { state: ComponentStateName }) {
  const m = STATE_META[state] ?? STATE_META.ready;
  const Icon = state === "starting" ? Loader2 : m.icon;
  return <Badge tone={m.tone}><Icon className={cn(state === "starting" && "animate-spin")} />{m.label}</Badge>;
}

export function StateLegend({ className }: { className?: string }) {
  return <LegendDots className={className} items={(Object.keys(STATE_META) as ComponentStateName[]).map((k) => ({ label: STATE_META[k].legend, className: STATE_META[k].dot }))} />;
}

/** Grid of component tiles: name, state, the reason in words, and progress while warming. */
export function ComponentTiles({ states, names }: { states: Record<string, ComponentState> | null | undefined; names?: Record<string, string> }) {
  const entries = Object.entries(states ?? {});
  if (!entries.length) return <div className="py-6 text-center text-[13px] text-subtle">No component states reported yet.</div>;
  return (
    <ul className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
      {entries.map(([id, s]) => {
        const m = STATE_META[s.state] ?? STATE_META.ready;
        return (
          <li key={id} className={cn("rounded-xl border bg-surface-2/40 p-3.5",
            s.state === "down" ? "border-fail/40" : s.state === "degraded" ? "border-warn/40" : s.state === "warming" ? "border-accent/40" : "border-border")}>
            <div className="flex items-center gap-2">
              <span className={cn("size-2 shrink-0 rounded-full", m.dot, s.state !== "ready" && "animate-pulse")} />
              <span className="truncate text-[13px] font-medium text-fg">{names?.[id] ?? id}</span>
              <span className="ml-auto"><StateBadge state={s.state} /></span>
            </div>
            <div className="mt-1.5 min-h-[18px] text-[12px] leading-snug text-muted">{s.reason || (s.state === "ready" ? "healthy" : "")}</div>
            {s.progress !== null && s.progress !== undefined && (
              <div className="mt-2 h-1 overflow-hidden rounded-full bg-surface-3">
                <div className={cn("h-full rounded-full", m.dot)} style={{ width: `${Math.min(1, s.progress) * 100}%` }} />
              </div>
            )}
          </li>
        );
      })}
    </ul>
  );
}
