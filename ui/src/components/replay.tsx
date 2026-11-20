import { useQuery } from "@tanstack/react-query";
import { FastForward, Pause, Play, Rewind } from "lucide-react";
import * as React from "react";
import { Button } from "@/components/ui/button";
import { Segmented } from "@/components/ui/controls";
import { request } from "@/lib/api";
import { cn } from "@/lib/cn";
import { fmtDuration } from "@/lib/format";
import { buildSeries, LiveStore, type LivePoint, type Telemetry, useLiveVersion } from "@/lib/live";
import type { CurrentRun, StartupSnapshot } from "@/lib/types";

/** A past (or running) run's recorded telemetry in its own replay store. */
export function useReplay(runId: string | null, initialOffsetS?: number | null) {
  const store = React.useMemo(() => (runId ? new LiveStore({ replay: true }) : null), [runId]);
  const q = useQuery({
    queryKey: ["telemetry", runId],
    queryFn: () => request<Telemetry>(`/api/runs/${encodeURIComponent(runId!)}/telemetry`),
    enabled: Boolean(runId), staleTime: 30_000, retry: 0,
  });
  React.useEffect(() => {
    if (store && q.data) {
      store.load(q.data);
      if (initialOffsetS !== undefined && initialOffsetS !== null && q.data.run.started_at) store.setClock(q.data.run.started_at + initialOffsetS);
    }
  }, [store, q.data]); // eslint-disable-line react-hooks/exhaustive-deps
  useLiveVersion(store ?? undefined);
  return { store, data: q.data ?? null, loading: q.isLoading, error: q.error };
}

/** The run as Live describes the current one, at the replay clock (phase from the recorded markers). */
export function replayRun(t: Telemetry, clock: number | null): CurrentRun {
  const phases = t.markers.filter((m) => m.kind === "phase" && (clock === null || m.t <= clock));
  const phase = phases.length ? phases[phases.length - 1].label.toUpperCase() : "PREPARE";
  return {
    job_id: null, run_id: t.run.run_id, scenario: t.run.scenario, phase, abort_reason: null, started_at: t.run.started_at,
    rate_multiplier: 1, base_tps: t.run.base_tps, topology: (t.run.topology ?? null) as CurrentRun["topology"],
    budget: (t.run.budget ?? null) as CurrentRun["budget"], duration_s: t.run.duration_s, warmup_s: t.run.warmup_s,
    local: true, owner: null, measure_started_at: t.run.measure_started_at,
  };
}

/** Each startup step's state at `clock`, from its recorded start and end. */
export function startupAt(s: Telemetry["startup"], clock: number | null): StartupSnapshot | null {
  if (!s) return null;
  const now = clock ?? Infinity;
  const steps = s.steps.map((st) => {
    const terminal = ["done", "skipped", "failed", "warn"].includes(st.status);
    let status = st.status;
    if (st.started_at === null || st.started_at > now) status = st.status === "skipped" && st.ended_at !== null && st.ended_at <= now ? "skipped" : "pending";
    else if (st.ended_at !== null && st.ended_at <= now) status = terminal ? st.status : "done";
    else status = "running";
    const dur = st.started_at !== null && st.started_at <= now ? Math.min(now, st.ended_at ?? now) - st.started_at : undefined;
    return { ...st, status, progress: null, duration_s: dur };
  });
  const active = steps.find((x) => x.status === "running");
  return { started_at: s.started_at, updated_at: Math.min(now, Date.now() / 1000), active: active?.id ?? null, steps } as unknown as StartupSnapshot;
}

/** Another run's series aligned on the start of traffic: `t` moved onto the current run's timeline. */
export function ghostSeries(ghost: LiveStore | null, ghostT0: number | null, curT0: number | null): Map<number, LivePoint> {
  const out = new Map<number, LivePoint>();
  if (!ghost || !ghostT0 || !curT0) return out;
  const saved = ghost.clock;
  ghost.clock = null;                 // the whole recorded run, not the part before its own cursor
  try {
    for (const p of buildSeries(1e9, ghost.lastRunId, ghost)) out.set(Math.round(p.t - ghostT0 + curT0), p);
  } finally {
    ghost.clock = saved;
  }
  return out;
}

const SPEEDS = [{ value: "1", label: "1×" }, { value: "10", label: "10×" }, { value: "60", label: "60×" }];

/** Play / pause / scrub a replay; jump to its phases and events. */
export function ReplayBar({ store, data }: { store: LiveStore; data: Telemetry }) {
  useLiveVersion(store);
  const [playing, setPlaying] = React.useState(false);
  const [speed, setSpeed] = React.useState("10");
  const start = data.startup?.started_at ?? data.run.started_at ?? (data.run.first_us ?? 0) / 1e6;
  const end = Math.max((data.run.last_us ?? 0) / 1e6, data.run.ended_at ?? 0, start + 1);
  const clock = store.clock ?? start;
  React.useEffect(() => {
    if (!playing) return;
    const id = window.setInterval(() => {
      const next = (store.clock ?? start) + 0.25 * Number(speed);
      if (next >= end) {
        store.setClock(end);
        setPlaying(false);
      } else store.setClock(next);
    }, 250);
    return () => window.clearInterval(id);
  }, [playing, speed, store, start, end]);
  const marks = data.markers.filter((m) => m.t >= start - 1 && m.t <= end + 1 && (m.kind !== "phase" || !["prepare", "complete"].includes(m.label)));
  const pct = (t: number) => `${(100 * (t - start)) / Math.max(1, end - start)}%`;
  return (
    <div className="mb-5 rounded-2xl border border-border bg-surface p-3 shadow-card" data-testid="replay-bar">
      <div className="flex flex-wrap items-center gap-2">
        <Button size="icon-sm" variant="secondary" aria-label="Back 30 s" onClick={() => store.setClock(Math.max(start, clock - 30))}><Rewind /></Button>
        <Button size="icon-sm" variant="gradient" aria-label={playing ? "Pause" : "Play"} onClick={() => setPlaying((p) => !p)} data-testid="replay-play">{playing ? <Pause /> : <Play />}</Button>
        <Button size="icon-sm" variant="secondary" aria-label="Forward 30 s" onClick={() => store.setClock(Math.min(end, clock + 30))}><FastForward /></Button>
        <Segmented size="sm" value={speed} onChange={setSpeed} options={SPEEDS} />
        <span className="font-mono text-[12px] text-muted num">T+{fmtDuration(Math.max(0, clock - start))} <span className="text-subtle">/ {fmtDuration(end - start)}</span></span>
        <div className="ml-auto flex max-w-full flex-wrap gap-1">
          {marks.map((m) => (
            <button key={`${m.kind}-${m.t}-${m.label}`} type="button" onClick={() => store.setClock(m.t)} title={m.label}
              className={cn("rounded-full border px-2 py-0.5 text-[11px] transition-colors cursor-pointer",
                m.kind === "abort" || m.kind === "health" ? "border-fail/40 text-fail" : "border-border text-muted hover:border-accent/50 hover:text-fg")}>
              {m.label.length > 28 ? `${m.label.slice(0, 27)}…` : m.label}
            </button>
          ))}
        </div>
      </div>
      <div className="relative mt-3">
        <input type="range" min={start} max={end} step={1} value={clock} aria-label="Replay position"
          onChange={(e) => { setPlaying(false); store.setClock(Number(e.target.value)); }}
          className="w-full cursor-pointer accent-[var(--accent)]" data-testid="replay-scrubber" />
        <div className="pointer-events-none relative h-2">
          {marks.map((m) => <span key={`t-${m.kind}-${m.t}`} className={cn("absolute top-0 h-2 w-px", m.kind === "abort" ? "bg-fail" : "bg-border-strong")} style={{ left: pct(m.t) }} />)}
        </div>
      </div>
    </div>
  );
}
